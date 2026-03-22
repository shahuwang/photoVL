package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger 全局日志实例
var Logger *zap.SugaredLogger

// initLogger 初始化日志配置
func initLogger() *zap.SugaredLogger {
	// 自定义日志配置
	config := zap.Config{
		Level:       zap.NewAtomicLevelAt(zap.DebugLevel),
		Development: false,
		Encoding:    "console",
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "time",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			FunctionKey:    zapcore.OmitKey,
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.CapitalColorLevelEncoder, // 彩色日志级别
			EncodeTime:     zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05"),
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder, // 短路径格式: file:line
		},
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	logger, err := config.Build(zap.AddCaller(), zap.AddStacktrace(zap.FatalLevel))
	if err != nil {
		panic(fmt.Sprintf("初始化日志失败: %v", err))
	}

	return logger.Sugar()
}

// OllamaClient 封装 Ollama 客户端配置
type OllamaClient struct {
	BaseURL    string
	Model      string
	HTTPClient *http.Client
	logger     *zap.SugaredLogger
}

// GenerateRequest Ollama API 请求结构
type GenerateRequest struct {
	Model   string   `json:"model"`
	Prompt  string   `json:"prompt"`
	Images  []string `json:"images,omitempty"`
	Stream  bool     `json:"stream"`
	Options Options  `json:"options,omitempty"`
}

// Options 生成选项
type Options struct {
	Temperature float64 `json:"temperature,omitempty"`
	TopP        float64 `json:"top_p,omitempty"`
	MaxTokens   int     `json:"num_predict,omitempty"`
}

// GenerateResponse Ollama API 响应结构
type GenerateResponse struct {
	Model     string `json:"model"`
	CreatedAt string `json:"created_at"`
	Response  string `json:"response"`
	Done      bool   `json:"done"`
	Message   struct {
		Role    string   `json:"role"`
		Content string   `json:"content"`
		Images  []string `json:"images,omitempty"`
	} `json:"message,omitempty"`
}

// ImageAnalysisResult 图片分析结果
type ImageAnalysisResult struct {
	Content   string
	ParsedData *ImageAnalysisData
	Model     string
	CreatedAt time.Time
}

// NewOllamaClient 创建新的 Ollama 客户端
func NewOllamaClient(baseURL, model string) *OllamaClient {
	if baseURL == "" {
		baseURL = "http://localhost:11434"
	}

	return &OllamaClient{
		BaseURL: baseURL,
		Model:   model,
		HTTPClient: &http.Client{
			Timeout: 120 * time.Second,
		},
		logger: Logger,
	}
}

// SetTimeout 设置超时时间
func (c *OllamaClient) SetTimeout(timeout time.Duration) {
	c.logger.Debugw("设置超时时间", "timeout", timeout)
	c.HTTPClient.Timeout = timeout
}

// SetLogger 设置自定义日志器
func (c *OllamaClient) SetLogger(logger *zap.SugaredLogger) {
	c.logger = logger
}

// 全局图片处理器
var imageProcessor *ImageProcessor

// encodeImageToBase64 将图片文件编码为 base64（自动处理缩放）
func encodeImageToBase64(imagePath string) (string, error) {
	if imageProcessor == nil {
		imageProcessor = NewImageProcessor(1024, 85)
	}

	// 使用图片处理器：解码 -> 缩放 -> 编码
	base64Str, err := imageProcessor.ProcessImage(imagePath)
	if err != nil {
		Logger.Errorw("处理图片失败",
			"path", imagePath,
			"error", err,
		)
		return "", err
	}

	return base64Str, nil
}

// AnalyzeImage 分析单张图片
func (c *OllamaClient) AnalyzeImage(ctx context.Context, imagePath, prompt string) (*ImageAnalysisResult, error) {
	base64Image, err := encodeImageToBase64(imagePath)
	if err != nil {
		return nil, err
	}

	req := GenerateRequest{
		Model:  c.Model,
		Prompt: prompt,
		Images: []string{base64Image},
		Stream: false,
		Options: Options{
			Temperature: 0.7,
			TopP:        0.9,
			MaxTokens:   4096,
		},
	}

	result, err := c.sendRequest(ctx, req)
	if err != nil {
		c.logger.Errorw("分析图片失败",
			"imagePath", imagePath,
			"error", err,
		)
		return nil, err
	}

	c.logger.Infow("图片分析完成",
		"imagePath", imagePath,
		"contentLength", len(result.Content),
	)

	return result, nil
}

// AnalyzeImageWithPrompt 使用自定义提示词分析图片
func (c *OllamaClient) AnalyzeImageWithPrompt(ctx context.Context, imagePath, prompt string, opts Options) (*ImageAnalysisResult, error) {
	base64Image, err := encodeImageToBase64(imagePath)
	if err != nil {
		return nil, err
	}

	req := GenerateRequest{
		Model:   c.Model,
		Prompt:  prompt,
		Images:  []string{base64Image},
		Stream:  false,
		Options: opts,
	}

	return c.sendRequest(ctx, req)
}

// AnalyzeImages 分析多张图片
func (c *OllamaClient) AnalyzeImages(ctx context.Context, imagePaths []string, prompt string) (*ImageAnalysisResult, error) {
	if len(imagePaths) == 0 {
		c.logger.Errorw("未提供图片路径")
		return nil, fmt.Errorf("至少需要提供一张图片")
	}

	var base64Images []string
	for _, path := range imagePaths {
		base64Image, err := encodeImageToBase64(path)
		if err != nil {
			return nil, err
		}
		base64Images = append(base64Images, base64Image)
	}

	req := GenerateRequest{
		Model:  c.Model,
		Prompt: prompt,
		Images: base64Images,
		Stream: false,
		Options: Options{
			Temperature: 0.7,
			TopP:        0.9,
			MaxTokens:   4096,
		},
	}

	return c.sendRequest(ctx, req)
}

// StreamAnalyzeImage 流式分析图片
func (c *OllamaClient) StreamAnalyzeImage(ctx context.Context, imagePath, prompt string, callback func(chunk string)) error {
	c.logger.Infow("开始流式分析图片", "imagePath", imagePath)

	base64Image, err := encodeImageToBase64(imagePath)
	if err != nil {
		return err
	}

	req := GenerateRequest{
		Model:  c.Model,
		Prompt: prompt,
		Images: []string{base64Image},
		Stream: true,
		Options: Options{
			Temperature: 0.7,
			TopP:        0.9,
			MaxTokens:   4096,
		},
	}

	return c.streamRequest(ctx, req, callback)
}

// doRequest 实际发送请求并解析响应
func (c *OllamaClient) doRequest(ctx context.Context, req GenerateRequest) (*GenerateResponse, error) {
	jsonData, err := json.Marshal(req)
	if err != nil {
		c.logger.Errorw("序列化请求失败", "error", err)
		return nil, fmt.Errorf("序列化请求失败: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.BaseURL+"/api/generate", bytes.NewBuffer(jsonData))
	if err != nil {
		c.logger.Errorw("创建请求失败", "error", err)
		return nil, fmt.Errorf("创建请求失败: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		c.logger.Errorw("发送请求失败", "error", err)
		return nil, fmt.Errorf("发送请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		c.logger.Errorw("API 返回错误",
			"statusCode", resp.StatusCode,
			"body", string(body),
		)
		return nil, fmt.Errorf("API 返回错误状态码 %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		c.logger.Errorw("读取响应失败", "error", err)
		return nil, fmt.Errorf("读取响应失败: %w", err)
	}

	var genResp GenerateResponse
	if err := json.Unmarshal(body, &genResp); err != nil {
		c.logger.Errorw("解析响应失败", "error", err, "body", string(body))
		return nil, fmt.Errorf("解析响应失败: %w", err)
	}

	return &genResp, nil
}

// validateAndParseJSON 验证并解析 JSON 内容，返回解析后的数据
func validateAndParseJSON(content string) (*ImageAnalysisData, error) {
	data, err := ParseAnalysisResult(content)
	if err != nil {
		return nil, fmt.Errorf("JSON 解析失败: %w", err)
	}

	// 检查必要字段是否存在（description 是核心字段）
	if data.Description == "" {
		return nil, fmt.Errorf("JSON 缺少必要字段: description 为空")
	}

	return data, nil
}

// sendRequest 发送非流式请求，包含重试逻辑
func (c *OllamaClient) sendRequest(ctx context.Context, req GenerateRequest) (*ImageAnalysisResult, error) {
	// 第一次请求
	genResp, err := c.doRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	// 兼容两种响应格式：优先使用 Response，其次使用 Message.Content
	content := genResp.Response
	if content == "" {
		content = genResp.Message.Content
	}

	// 如果响应为空或 JSON 解析失败，等待300ms后重试一次
	var parsedData *ImageAnalysisData
	needRetry := false
	if content == "" {
		c.logger.Warnw("模型返回空响应")
		needRetry = true
	} else {
		// 验证并解析 JSON
		var err error
		parsedData, err = validateAndParseJSON(content)
		if err != nil {
			c.logger.Warnw("模型返回的 JSON 不符合 Schema", "error", err, "content", content)
			needRetry = true
		}
	}

	if needRetry {
		c.logger.Warnw("等待300ms后重试")
		time.Sleep(300 * time.Millisecond)

		// 重试请求
		genResp, err = c.doRequest(ctx, req)
		if err != nil {
			return nil, err
		}

		// 再次提取响应内容
		content = genResp.Response
		if content == "" {
			content = genResp.Message.Content
		}

		// 检查重试后的响应
		if content == "" {
			c.logger.Errorw("重试后模型仍然返回空响应")
			return nil, fmt.Errorf("图片处理失败")
		}

		// 再次验证并解析 JSON
		var err error
		parsedData, err = validateAndParseJSON(content)
		if err != nil {
			c.logger.Errorw("重试后模型返回的 JSON 仍然不符合 Schema", "error", err, "content", content)
			return nil, fmt.Errorf("图片处理失败")
		}
	}

	createdAt, _ := time.Parse(time.RFC3339, genResp.CreatedAt)

	return &ImageAnalysisResult{
		Content:    content,
		ParsedData: parsedData,
		Model:      genResp.Model,
		CreatedAt:  createdAt,
	}, nil
}

// doStreamRequest 实际发送流式请求并处理响应
func (c *OllamaClient) doStreamRequest(ctx context.Context, req GenerateRequest, callback func(chunk string)) (int, error) {
	jsonData, err := json.Marshal(req)
	if err != nil {
		c.logger.Errorw("序列化请求失败", "error", err)
		return 0, fmt.Errorf("序列化请求失败: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.BaseURL+"/api/generate", bytes.NewBuffer(jsonData))
	if err != nil {
		c.logger.Errorw("创建请求失败", "error", err)
		return 0, fmt.Errorf("创建请求失败: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		c.logger.Errorw("发送请求失败", "error", err)
		return 0, fmt.Errorf("发送请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		c.logger.Errorw("API 返回错误", "statusCode", resp.StatusCode, "body", string(body))
		return 0, fmt.Errorf("API 返回错误状态码 %d: %s", resp.StatusCode, string(body))
	}

	scanner := bufio.NewScanner(resp.Body)
	chunkCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		var chunk GenerateResponse
		if err := json.Unmarshal([]byte(line), &chunk); err != nil {
			c.logger.Warnw("解析流式数据失败", "line", line, "error", err)
			continue
		}

		// 兼容两种响应格式：优先使用 Response，其次使用 Message.Content
		content := chunk.Response
		if content == "" {
			content = chunk.Message.Content
		}

		if content != "" {
			callback(content)
			chunkCount++
		}

		if chunk.Done {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		c.logger.Errorw("读取流式响应失败", "error", err)
		return chunkCount, err
	}

	return chunkCount, nil
}

// streamRequest 发送流式请求，包含重试逻辑
func (c *OllamaClient) streamRequest(ctx context.Context, req GenerateRequest, callback func(chunk string)) error {
	c.logger.Debugw("准备发送流式请求")

	// 第一次请求
	chunkCount, err := c.doStreamRequest(ctx, req, callback)
	if err != nil {
		return err
	}

	// 如果没有收到任何内容，等待300ms后重试一次
	if chunkCount == 0 {
		c.logger.Warnw("流式响应为空，等待300ms后重试")
		time.Sleep(300 * time.Millisecond)

		// 重试请求
		chunkCount, err = c.doStreamRequest(ctx, req, callback)
		if err != nil {
			return err
		}

		// 如果重试后仍然没有内容，返回错误
		if chunkCount == 0 {
			c.logger.Errorw("重试后流式响应仍然为空")
			return fmt.Errorf("图片处理失败")
		}
	}

	c.logger.Debugw("流式响应完成", "chunks", chunkCount)

	return nil
}

// CheckHealth 检查 Ollama 服务是否健康
func (c *OllamaClient) CheckHealth() error {
	resp, err := c.HTTPClient.Get(c.BaseURL + "/api/tags")
	if err != nil {
		c.logger.Errorw("无法连接到 Ollama 服务", "error", err)
		return fmt.Errorf("无法连接到 Ollama 服务: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		c.logger.Errorw("Ollama 服务返回异常状态码", "statusCode", resp.StatusCode)
		return fmt.Errorf("Ollama 服务返回异常状态码: %d", resp.StatusCode)
	}

	c.logger.Debugw("Ollama 服务健康检查通过")
	return nil
}

// String 格式化输出结果
func (r *ImageAnalysisResult) String() string {
	return fmt.Sprintf("Model: %s\nCreated: %s\nContent: %s", r.Model, r.CreatedAt.Format("2006-01-02 15:04:05"), r.Content)
}

// readPromptFromFile 从文件读取提示词
func readPromptFromFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		Logger.Errorw("读取提示词文件失败", "path", path, "error", err)
		return "", fmt.Errorf("读取提示词文件失败: %w", err)
	}

	prompt := strings.TrimSpace(string(data))
	if prompt == "" {
		Logger.Warnw("提示词文件为空", "path", path)
		return "", fmt.Errorf("提示词文件为空")
	}

	return prompt, nil
}

func showImageInfo(path string) {
	processor := NewImageProcessor(1024, 85)

	format, width, height, err := processor.GetImageInfo(path)
	if err != nil {
		Logger.Errorw("获取图片信息失败", "path", path, "error", err)
		fmt.Printf("获取图片信息失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("图片信息:\n")
	fmt.Printf("  路径: %s\n", path)
	fmt.Printf("  格式: %s\n", format)
	fmt.Printf("  尺寸: %d x %d\n", width, height)
	fmt.Printf("  最大边: %d\n", max(width, height))

	if max(width, height) > 1024 {
		fmt.Printf("  处理: 将被等比缩放至最大边 1024\n")
	} else {
		fmt.Printf("  处理: 无需缩放\n")
	}
}

// Config 命令行配置
type Config struct {
	ImagePath   string
	DirPath     string // 扫描文件夹路径
	DBPath      string
	Prompt      string
	PromptFile  string
	ShowInfo    bool
	OllamaURL   string
	OllamaModel string
	Opt         string // 操作模式: vl(视觉分析) 或 eb(向量嵌入)
}

// parseFlags 解析命令行参数
func parseFlags() *Config {
	cfg := &Config{}

	flag.StringVar(&cfg.ImagePath, "fpath", "", "要处理的图片路径")
	flag.StringVar(&cfg.DirPath, "dir", "", "要扫描的文件夹路径")
	flag.StringVar(&cfg.DBPath, "db", "photoVL_lancedb", "LanceDB 数据库路径")
	flag.StringVar(&cfg.Prompt, "prompt", "", "分析提示词（直接提供）")
	flag.StringVar(&cfg.PromptFile, "p", "", "提示词文件路径")
	flag.BoolVar(&cfg.ShowInfo, "info", false, "仅显示图片信息，不进行分析")
	flag.StringVar(&cfg.OllamaURL, "ollama-url", "http://localhost:11434", "Ollama 服务地址")
	flag.StringVar(&cfg.OllamaModel, "model", "qwen3-vl:4b", "Ollama 模型名称")
	flag.StringVar(&cfg.Opt, "opt", "vl", "操作模式: vl(视觉分析) 或 eb(向量嵌入)")

	flag.Parse()
	return cfg
}

// AppContext 应用上下文，封装 main 函数中需要共享的数据
type AppContext struct {
	Config    *Config
	DB        *LanceDBManager
	Client    *OllamaClient
	Extractor *MetadataExtractor
}

// setupAndValidate 初始化并验证环境
func setupAndValidate() *AppContext {
	// 初始化日志
	Logger = initLogger()

	// 解析命令行参数
	cfg := parseFlags()

	// 检查必需参数：-fpath 或 -dir 必须提供一个
	if cfg.ImagePath == "" && cfg.DirPath == "" {
		fmt.Fprintf(os.Stderr, "错误: 缺少必需参数，请提供 -fpath（图片路径）或 -dir（文件夹路径）\n\n")
		flag.Usage()
		os.Exit(1)
	}

	// 如果同时提供了 -fpath 和 -dir，优先使用 -dir
	if cfg.ImagePath != "" && cfg.DirPath != "" {
		Logger.Warnw("同时提供了 -fpath 和 -dir，优先使用 -dir 进行文件夹扫描")
		cfg.ImagePath = ""
	}

	// 验证单文件模式
	if cfg.ImagePath != "" {
		// 验证图片文件是否存在
		if _, err := os.Stat(cfg.ImagePath); os.IsNotExist(err) {
			Logger.Errorw("图片文件不存在", "path", cfg.ImagePath)
			fmt.Fprintf(os.Stderr, "错误: 图片文件不存在: %s\n", cfg.ImagePath)
			os.Exit(1)
		}

		// 处理 --info 参数（仅显示图片信息）
		if cfg.ShowInfo {
			showImageInfo(cfg.ImagePath)
			os.Exit(0)
		}
	}

	// 验证文件夹模式
	if cfg.DirPath != "" {
		info, err := os.Stat(cfg.DirPath)
		if err != nil {
			Logger.Errorw("文件夹不存在或无法访问", "path", cfg.DirPath, "error", err)
			fmt.Fprintf(os.Stderr, "错误: 文件夹不存在或无法访问: %s\n", cfg.DirPath)
			os.Exit(1)
		}
		if !info.IsDir() {
			Logger.Errorw("指定的路径不是文件夹", "path", cfg.DirPath)
			fmt.Fprintf(os.Stderr, "错误: 指定的路径不是文件夹: %s\n", cfg.DirPath)
			os.Exit(1)
		}
	}

	// 创建 Ollama 客户端
	client := NewOllamaClient(cfg.OllamaURL, cfg.OllamaModel)

	// 仅在视觉分析模式下检查 Ollama 服务健康状态
	if cfg.Opt != "eb" && cfg.ImagePath != "" {
		if err := client.CheckHealth(); err != nil {
			Logger.Errorw("Ollama 服务连接失败", "error", err)
			fmt.Fprintf(os.Stderr, "错误: 无法连接到 Ollama 服务 (%s)\n", cfg.OllamaURL)
			fmt.Fprintf(os.Stderr, "请确保 Ollama 服务已启动: ollama run %s\n", cfg.OllamaModel)
			os.Exit(1)
		}
	}

	Logger.Infow("解析命令行参数",
		"imagePath", cfg.ImagePath,
		"dirPath", cfg.DirPath,
		"dbPath", cfg.DBPath,
		"promptFile", cfg.PromptFile,
		"opt", cfg.Opt,
	)

	return &AppContext{
		Config: cfg,
		Client: client,
	}
}

// getPrompt 获取分析提示词
func getPrompt(cfg *Config) string {
	if cfg.PromptFile != "" {
		prompt, err := readPromptFromFile(cfg.PromptFile)
		if err != nil {
			Logger.Errorw("无法读取提示词文件", "error", err)
			fmt.Fprintf(os.Stderr, "错误: 无法读取提示词文件: %v\n", err)
			os.Exit(1)
		}
		return prompt
	}
	if cfg.Prompt != "" {
		return cfg.Prompt
	}
	// 使用默认提示词
	return "请详细描述这张图片中的内容，包括主要物体、场景、颜色、文字等细节。"
}

// analyzeImage 分析图片并返回结果
func analyzeImage(ctx context.Context, appCtx *AppContext) *ImageAnalysisResult {
	prompt := getPrompt(appCtx.Config)
	Logger.Infow("图片文件存在", "path", appCtx.Config.ImagePath)
	Logger.Infow("开始非流式图片分析")

	result, err := appCtx.Client.AnalyzeImage(ctx, appCtx.Config.ImagePath, prompt)
	if err != nil {
		Logger.Errorw("分析失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 分析失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n=== 分析结果 ===")
	fmt.Printf("模型: %s\n", result.Model)
	fmt.Printf("分析内容:\n%s\n", result.Content)

	return result
}

// initDatabase 初始化数据库连接
func initDatabase(cfg *Config) *LanceDBManager {
	dbPath := cfg.DBPath
	if dbPath == "" {
		dbPath = GetDefaultDBPath()
	}

	Logger.Infow("初始化数据库", "path", dbPath)
	db, err := NewLanceDBManager(dbPath)
	if err != nil {
		Logger.Errorw("初始化数据库失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 初始化数据库失败: %v\n", err)
		os.Exit(1)
	}
	return db
}

// extractMetadata 提取图片基础元数据
func extractMetadata(appCtx *AppContext) *ImageBasicInfo {
	Logger.Infow("提取图片基础元数据")
	appCtx.Extractor = NewMetadataExtractor()
	basicInfo, err := appCtx.Extractor.ExtractAllMetadata(appCtx.Config.ImagePath)
	if err != nil {
		Logger.Errorw("提取元数据失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 提取元数据失败: %v\n", err)
		os.Exit(1)
	}
	return basicInfo
}

// checkDuplicateFile 检查并处理重复文件
// 返回值: true - 已处理（需要更新元数据），false - 新文件
// 注意：即使 MD5 已存在，也需要用新的分析结果更新元数据
func checkDuplicateFile(appCtx *AppContext, basicInfo *ImageBasicInfo) bool {
	exists, err := appCtx.DB.CheckMD5Exists(basicInfo.MD5)
	if err != nil {
		Logger.Errorw("检查 MD5 存在性失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 检查 MD5 存在性失败: %v\n", err)
		os.Exit(1)
	}

	if !exists {
		return false // 不是重复文件，继续处理新文件
	}

	// MD5 已存在，查询已有的文件路径
	existingPaths, err := appCtx.DB.GetFilePathsByMD5(basicInfo.MD5)
	if err != nil {
		Logger.Errorw("查询文件路径失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 查询文件路径失败: %v\n", err)
		os.Exit(1)
	}

	// 检查当前路径是否已存在
	pathExists := false
	for _, path := range existingPaths {
		if path == appCtx.Config.ImagePath {
			pathExists = true
			break
		}
	}

	if !pathExists {
		// 路径不存在，添加新的文件索引
		Logger.Infow("检测到重复文件（不同路径），添加文件索引",
			"md5", basicInfo.MD5,
			"newPath", appCtx.Config.ImagePath,
			"existingPaths", existingPaths)

		fileIndex := &FileIndex{
			MD5:      basicInfo.MD5,
			FilePath: appCtx.Config.ImagePath,
		}
		if err := appCtx.DB.InsertFileIndex(fileIndex); err != nil {
			Logger.Errorw("插入文件索引失败", "error", err)
			fmt.Fprintf(os.Stderr, "错误: 插入文件索引失败: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("\n检测到不同路径的重复文件，已添加文件索引")
	}

	// 返回 true 表示需要继续执行元数据更新
	return true
}

// saveNewImage 保存新图片的元数据到数据库
func saveNewImage(appCtx *AppContext, basicInfo *ImageBasicInfo, result *ImageAnalysisResult) {
	// 使用已解析的视觉分析结果
	Logger.Infow("使用已解析的视觉分析结果")
	if result.ParsedData == nil {
		Logger.Errorw("解析结果为空")
		fmt.Fprintf(os.Stderr, "错误: 解析结果为空\n")
		os.Exit(1)
	}

	// 合并元数据
	completeMetadata := appCtx.Extractor.MergeMetadata(basicInfo, result.ParsedData)

	// 图片向量保持为空，等待后续向量生成服务集成
	// 空向量表示尚未生成向量，比全零向量更易区分

	// 保存到数据库
	Logger.Infow("保存到数据库")
	imageMeta := completeMetadata.ToImageMetadata()
	if err := appCtx.DB.InsertImageMetadata(imageMeta); err != nil {
		Logger.Errorw("插入图片元数据失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 插入图片元数据失败: %v\n", err)
		os.Exit(1)
	}

	// 保存文件索引
	fileIndex := completeMetadata.ToFileIndex(appCtx.Config.ImagePath)
	if err := appCtx.DB.InsertFileIndex(fileIndex); err != nil {
		Logger.Errorw("插入文件索引失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 插入文件索引失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n图片分析完成并已存入数据库")
	fmt.Printf("MD5: %s\n", basicInfo.MD5)
	fmt.Printf("数据库路径: %s\n", appCtx.Config.DBPath)
	Logger.Infow("程序结束", "md5", basicInfo.MD5)
}

// updateExistingImage 更新已有图片的元数据
func updateExistingImage(appCtx *AppContext, basicInfo *ImageBasicInfo, result *ImageAnalysisResult) {
	// 使用已解析的视觉分析结果
	Logger.Infow("使用已解析的视觉分析结果更新现有数据")
	if result.ParsedData == nil {
		Logger.Errorw("解析结果为空")
		fmt.Fprintf(os.Stderr, "错误: 解析结果为空\n")
		os.Exit(1)
	}

	// 合并元数据
	completeMetadata := appCtx.Extractor.MergeMetadata(basicInfo, result.ParsedData)

	// 图片向量保持为空，等待后续向量生成服务集成

	// 更新数据库（InsertImageMetadata 会自动处理合并逻辑）
	Logger.Infow("更新数据库")
	imageMeta := completeMetadata.ToImageMetadata()
	if err := appCtx.DB.InsertImageMetadata(imageMeta); err != nil {
		Logger.Errorw("更新图片元数据失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 更新图片元数据失败: %v\n", err)
		os.Exit(1)
	}

	// 确保文件索引存在（如果不存在则添加）
	fileIndex := completeMetadata.ToFileIndex(appCtx.Config.ImagePath)
	if err := appCtx.DB.InsertFileIndex(fileIndex); err != nil {
		Logger.Errorw("插入文件索引失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 插入文件索引失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n图片分析完成并已更新数据库")
	fmt.Printf("MD5: %s\n", basicInfo.MD5)
	fmt.Printf("数据库路径: %s\n", appCtx.Config.DBPath)
	Logger.Infow("程序结束", "md5", basicInfo.MD5)
}

func main() {
	// 初始化并验证环境
	appCtx := setupAndValidate()
	defer Logger.Sync()

	// 判断是单文件模式还是文件夹模式
	if appCtx.Config.DirPath != "" {
		// 文件夹扫描模式
		processDirectoryMode(appCtx)
	} else {
		// 单文件模式
		processSingleFileMode(appCtx)
	}
}

// processSingleFileMode 单文件处理模式
func processSingleFileMode(appCtx *AppContext) {
	// 根据操作模式执行不同的处理逻辑
	switch appCtx.Config.Opt {
	case "eb":
		// 向量嵌入模式：获取图片整体向量和人脸向量
		processEmbeddingMode(appCtx)
	case "vl", "":
		// 视觉分析模式（默认）：使用 Ollama 分析图片
		processVisionMode(appCtx)
	default:
		fmt.Fprintf(os.Stderr, "错误: 不支持的操作模式: %s\n", appCtx.Config.Opt)
		fmt.Fprintf(os.Stderr, "支持的模式: vl(视觉分析), eb(向量嵌入)\n")
		os.Exit(1)
	}
}

// processDirectoryMode 文件夹扫描处理模式
func processDirectoryMode(appCtx *AppContext) {
	Logger.Infow("进入文件夹扫描模式", "dir", appCtx.Config.DirPath, "opt", appCtx.Config.Opt)

	// 扫描文件夹获取图片列表
	imageFiles, err := scanDirectoryForImages(appCtx.Config.DirPath)
	if err != nil {
		Logger.Errorw("扫描文件夹失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 扫描文件夹失败: %v\n", err)
		os.Exit(1)
	}

	if len(imageFiles) == 0 {
		fmt.Printf("文件夹中没有找到图片文件: %s\n", appCtx.Config.DirPath)
		return
	}

	fmt.Printf("在文件夹中找到 %d 个图片文件\n", len(imageFiles))
	Logger.Infow("开始处理文件夹中的图片", "count", len(imageFiles))

	// 初始化数据库
	appCtx.DB = initDatabase(appCtx.Config)
	defer appCtx.DB.Close()

	// 创建元数据提取器
	appCtx.Extractor = NewMetadataExtractor()

	// 根据操作模式处理
	switch appCtx.Config.Opt {
	case "eb":
		processDirectoryEmbeddingMode(appCtx, imageFiles)
	case "vl", "":
		processDirectoryVisionMode(appCtx, imageFiles)
	default:
		fmt.Fprintf(os.Stderr, "错误: 不支持的操作模式: %s\n", appCtx.Config.Opt)
		fmt.Fprintf(os.Stderr, "支持的模式: vl(视觉分析), eb(向量嵌入)\n")
		os.Exit(1)
	}

	fmt.Printf("\n文件夹处理完成，共处理 %d 个文件\n", len(imageFiles))
}

// scanDirectoryForImages 扫描文件夹中的图片文件
func scanDirectoryForImages(dirPath string) ([]string, error) {
	var imageFiles []string

	// 支持的图片扩展名
	supportedExts := map[string]bool{
		".jpg": true, ".jpeg": true, ".png": true,
		".gif": true, ".webp": true, ".bmp": true,
		".heic": true, ".heif": true,
	}

	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			Logger.Warnw("访问文件失败", "path", path, "error", err)
			return nil // 继续遍历
		}

		// 跳过目录
		if info.IsDir() {
			return nil
		}

		// 检查扩展名
		ext := strings.ToLower(filepath.Ext(path))
		if supportedExts[ext] {
			imageFiles = append(imageFiles, path)
		}

		return nil
	})

	return imageFiles, err
}

// processDirectoryVisionMode 文件夹视觉分析模式
func processDirectoryVisionMode(appCtx *AppContext, imageFiles []string) {
	// 检查 Ollama 服务健康状态
	if err := appCtx.Client.CheckHealth(); err != nil {
		Logger.Errorw("Ollama 服务连接失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 无法连接到 Ollama 服务 (%s)\n", appCtx.Config.OllamaURL)
		fmt.Fprintf(os.Stderr, "请确保 Ollama 服务已启动: ollama run %s\n", appCtx.Config.OllamaModel)
		os.Exit(1)
	}

	prompt := getPrompt(appCtx.Config)
	ctx := context.Background()

	// 处理每个图片
	for i, imagePath := range imageFiles {
		fmt.Printf("\n[%d/%d] 处理: %s\n", i+1, len(imageFiles), imagePath)
		Logger.Infow("处理图片", "index", i+1, "total", len(imageFiles), "path", imagePath)

		// 提取基础元数据（获取 MD5）
		basicInfo, err := appCtx.Extractor.ExtractAllMetadata(imagePath)
		if err != nil {
			Logger.Errorw("提取元数据失败", "path", imagePath, "error", err)
			fmt.Printf("  提取元数据失败，跳过: %v\n", err)
			continue
		}

		// 检查 MD5 是否已存在
		exists, err := appCtx.DB.CheckMD5Exists(basicInfo.MD5)
		if err != nil {
			Logger.Errorw("检查 MD5 存在性失败", "path", imagePath, "error", err)
			fmt.Printf("  检查 MD5 失败，跳过: %v\n", err)
			continue
		}

		if exists {
			// MD5 已存在，查询已有的文件路径
			existingPaths, err := appCtx.DB.GetFilePathsByMD5(basicInfo.MD5)
			if err != nil {
				Logger.Errorw("查询文件路径失败", "path", imagePath, "error", err)
				fmt.Printf("  查询文件路径失败，跳过: %v\n", err)
				continue
			}

			// 检查当前路径是否已存在
			pathExists := false
			for _, path := range existingPaths {
				if path == imagePath {
					pathExists = true
					break
				}
			}

			if pathExists {
				// 路径已存在，跳过处理
				fmt.Printf("  图片已处理过（相同路径），跳过\n")
				Logger.Infow("图片已处理过，跳过", "path", imagePath, "md5", basicInfo.MD5)
				continue
			}

			// 路径不存在，添加新的文件索引
			fmt.Printf("  检测到重复文件（不同路径），添加文件索引\n")
			Logger.Infow("检测到重复文件（不同路径），添加文件索引",
				"md5", basicInfo.MD5,
				"newPath", imagePath,
				"existingPaths", existingPaths)

			fileIndex := &FileIndex{
				MD5:      basicInfo.MD5,
				FilePath: imagePath,
			}
			if err := appCtx.DB.InsertFileIndex(fileIndex); err != nil {
				Logger.Errorw("插入文件索引失败", "path", imagePath, "error", err)
				fmt.Printf("  插入文件索引失败: %v\n", err)
			}
			continue
		}

		// MD5 不存在，执行视觉分析
		fmt.Printf("  执行视觉分析...\n")
		result, err := appCtx.Client.AnalyzeImage(ctx, imagePath, prompt)
		if err != nil {
			Logger.Errorw("分析图片失败", "path", imagePath, "error", err)
			fmt.Printf("  分析失败，跳过: %v\n", err)
			continue
		}

		// 保存结果到数据库
		if result.ParsedData == nil {
			Logger.Errorw("解析结果为空", "path", imagePath)
			fmt.Printf("  解析结果为空，跳过\n")
			continue
		}

		// 合并元数据
		completeMetadata := appCtx.Extractor.MergeMetadata(basicInfo, result.ParsedData)

		// 保存图片元数据
		imageMeta := completeMetadata.ToImageMetadata()
		if err := appCtx.DB.InsertImageMetadata(imageMeta); err != nil {
			Logger.Errorw("插入图片元数据失败", "path", imagePath, "error", err)
			fmt.Printf("  保存元数据失败: %v\n", err)
			continue
		}

		// 保存文件索引
		fileIndex := completeMetadata.ToFileIndex(imagePath)
		if err := appCtx.DB.InsertFileIndex(fileIndex); err != nil {
			Logger.Errorw("插入文件索引失败", "path", imagePath, "error", err)
			fmt.Printf("  保存文件索引失败: %v\n", err)
			continue
		}

		fmt.Printf("  处理完成，MD5: %s\n", basicInfo.MD5)
		Logger.Infow("图片处理完成", "path", imagePath, "md5", basicInfo.MD5)
	}
}

// processDirectoryEmbeddingMode 文件夹向量嵌入模式
func processDirectoryEmbeddingMode(appCtx *AppContext, imageFiles []string) {
	// 处理每个图片
	for i, imagePath := range imageFiles {
		fmt.Printf("\n[%d/%d] 处理: %s\n", i+1, len(imageFiles), imagePath)
		Logger.Infow("处理图片", "index", i+1, "total", len(imageFiles), "path", imagePath)

		// 提取基础元数据（获取 MD5）
		basicInfo, err := appCtx.Extractor.ExtractAllMetadata(imagePath)
		if err != nil {
			Logger.Errorw("提取元数据失败", "path", imagePath, "error", err)
			fmt.Printf("  提取元数据失败，跳过: %v\n", err)
			continue
		}

		// 检查 MD5 是否已存在
		exists, err := appCtx.DB.CheckMD5Exists(basicInfo.MD5)
		if err != nil {
			Logger.Errorw("检查 MD5 存在性失败", "path", imagePath, "error", err)
			fmt.Printf("  检查 MD5 失败，跳过: %v\n", err)
			continue
		}

		if exists {
			// MD5 已存在，查询已有的文件路径
			existingPaths, err := appCtx.DB.GetFilePathsByMD5(basicInfo.MD5)
			if err != nil {
				Logger.Errorw("查询文件路径失败", "path", imagePath, "error", err)
				fmt.Printf("  查询文件路径失败，跳过: %v\n", err)
				continue
			}

			// 检查当前路径是否已存在
			pathExists := false
			for _, path := range existingPaths {
				if path == imagePath {
					pathExists = true
					break
				}
			}

			if pathExists {
				// 路径已存在，跳过处理
				fmt.Printf("  图片已处理过（相同路径），跳过\n")
				Logger.Infow("图片已处理过，跳过", "path", imagePath, "md5", basicInfo.MD5)
				continue
			}

			// 路径不存在，添加新的文件索引
			fmt.Printf("  检测到重复文件（不同路径），添加文件索引\n")
			Logger.Infow("检测到重复文件（不同路径），添加文件索引",
				"md5", basicInfo.MD5,
				"newPath", imagePath,
				"existingPaths", existingPaths)

			fileIndex := &FileIndex{
				MD5:      basicInfo.MD5,
				FilePath: imagePath,
			}
			if err := appCtx.DB.InsertFileIndex(fileIndex); err != nil {
				Logger.Errorw("插入文件索引失败", "path", imagePath, "error", err)
				fmt.Printf("  插入文件索引失败: %v\n", err)
			}
			continue
		}

		// MD5 不存在，执行向量嵌入处理
		fmt.Printf("  执行向量嵌入处理...\n")
		if err := processSingleImageEmbedding(appCtx, imagePath, basicInfo); err != nil {
			Logger.Errorw("向量嵌入处理失败", "path", imagePath, "error", err)
			fmt.Printf("  向量嵌入处理失败: %v\n", err)
			continue
		}

		fmt.Printf("  处理完成，MD5: %s\n", basicInfo.MD5)
		Logger.Infow("图片处理完成", "path", imagePath, "md5", basicInfo.MD5)
	}
}

// processSingleImageEmbedding 处理单个图片的向量嵌入
func processSingleImageEmbedding(appCtx *AppContext, imagePath string, basicInfo *ImageBasicInfo) error {
	// 创建 ImageEmbedding 实例
	embedding, err := NewImageEmbedding(
		imagePath,
		DefaultModelURL,
		DefaultModelName,
		DefaultModelVersion,
	)
	if err != nil {
		return fmt.Errorf("创建 ImageEmbedding 失败: %w", err)
	}
	defer embedding.Close()
	embedding.SetLogger(Logger)

	// 1. 生成图片整体向量
	fmt.Printf("  生成图片整体向量...\n")
	imageVector, err := embedding.GenerateImageVector(DefaultVectorDimension)
	if err != nil {
		return fmt.Errorf("生成图片整体向量失败: %w", err)
	}
	fmt.Printf("  图片整体向量生成完成，维度: %d\n", len(imageVector))

	// 2. 保存图片整体向量到数据库
	err = SaveImageVectorToMetadata(basicInfo.MD5, imageVector, appCtx.DB)
	if err != nil {
		return fmt.Errorf("保存图片整体向量失败: %w", err)
	}

	// 3. 检测人脸并生成向量
	fmt.Printf("  检测人脸...\n")
	faces, err := embedding.DetectFaces()
	if err != nil {
		return fmt.Errorf("检测人脸失败: %w", err)
	}
	fmt.Printf("  检测到 %d 个人脸\n", len(faces))

	// 4. 保存人脸向量到数据库
	if len(faces) > 0 {
		count, err := SaveFaceVectorsToDB(basicInfo.MD5, faces, appCtx.DB)
		if err != nil {
			return fmt.Errorf("保存人脸向量失败: %w", err)
		}
		fmt.Printf("  成功保存 %d 个人脸向量\n", count)
	}

	// 5. 保存文件索引
	fileIndex := &FileIndex{
		MD5:      basicInfo.MD5,
		FilePath: imagePath,
	}
	if err := appCtx.DB.InsertFileIndex(fileIndex); err != nil {
		return fmt.Errorf("插入文件索引失败: %w", err)
	}

	return nil
}

// processVisionMode 视觉分析模式：使用 Ollama 分析图片
func processVisionMode(appCtx *AppContext) {
	// 分析图片
	ctx := context.Background()
	result := analyzeImage(ctx, appCtx)

	// 初始化数据库
	appCtx.DB = initDatabase(appCtx.Config)
	defer appCtx.DB.Close()

	// 提取元数据
	basicInfo := extractMetadata(appCtx)

	// 检查并处理重复文件
	isExistingFile := checkDuplicateFile(appCtx, basicInfo)

	if isExistingFile {
		// 更新已有图片的元数据
		updateExistingImage(appCtx, basicInfo, result)
	} else {
		// 保存新图片元数据
		saveNewImage(appCtx, basicInfo, result)
	}
}

// processEmbeddingMode 向量嵌入模式：获取图片整体向量和人脸向量
func processEmbeddingMode(appCtx *AppContext) {
	Logger.Infow("进入向量嵌入模式")

	// 提取基础元数据（获取MD5）
	appCtx.Extractor = NewMetadataExtractor()
	basicInfo, err := appCtx.Extractor.ExtractAllMetadata(appCtx.Config.ImagePath)
	if err != nil {
		Logger.Errorw("提取基础元数据失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 提取基础元数据失败: %v\n", err)
		os.Exit(1)
	}

	// 初始化数据库
	appCtx.DB = initDatabase(appCtx.Config)
	defer appCtx.DB.Close()

	// 创建 ImageEmbedding 实例
	// 使用默认的向量服务配置
	embedding, err := NewImageEmbedding(
		appCtx.Config.ImagePath,
		DefaultModelURL,
		DefaultModelName,
		DefaultModelVersion,
	)
	if err != nil {
		Logger.Errorw("创建 ImageEmbedding 失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 创建 ImageEmbedding 失败: %v\n", err)
		os.Exit(1)
	}
	defer embedding.Close()
	embedding.SetLogger(Logger)

	fmt.Println("开始生成图片整体向量...")

	// 1. 生成图片整体向量
	imageVector, err := embedding.GenerateImageVector(DefaultVectorDimension)
	if err != nil {
		Logger.Errorw("生成图片整体向量失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 生成图片整体向量失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("图片整体向量生成完成，维度: %d\n", len(imageVector))

	// 2. 保存图片整体向量到数据库
	err = SaveImageVectorToMetadata(basicInfo.MD5, imageVector, appCtx.DB)
	if err != nil {
		Logger.Errorw("保存图片整体向量失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 保存图片整体向量失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("图片整体向量已保存到数据库")

	// 3. 检测人脸并生成向量
	fmt.Println("开始检测人脸...")
	faces, err := embedding.DetectFaces()
	if err != nil {
		Logger.Errorw("检测人脸失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 检测人脸失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("检测到 %d 个人脸\n", len(faces))

	// 4. 保存人脸向量到数据库
	if len(faces) > 0 {
		count, err := SaveFaceVectorsToDB(basicInfo.MD5, faces, appCtx.DB)
		if err != nil {
			Logger.Errorw("保存人脸向量失败", "error", err)
			fmt.Fprintf(os.Stderr, "错误: 保存人脸向量失败: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("成功保存 %d 个人脸向量到数据库\n", count)
	}

	fmt.Println("\n向量嵌入处理完成")
	fmt.Printf("MD5: %s\n", basicInfo.MD5)
	fmt.Printf("数据库路径: %s\n", appCtx.Config.DBPath)
	Logger.Infow("向量嵌入处理完成", "md5", basicInfo.MD5, "faces", len(faces))
}
