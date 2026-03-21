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
	DBPath      string
	Prompt      string
	PromptFile  string
	ShowInfo    bool
	OllamaURL   string
	OllamaModel string
}

// parseFlags 解析命令行参数
func parseFlags() *Config {
	cfg := &Config{}

	flag.StringVar(&cfg.ImagePath, "fpath", "", "要处理的图片路径")
	flag.StringVar(&cfg.DBPath, "db", "photoVL_lancedb", "LanceDB 数据库路径")
	flag.StringVar(&cfg.Prompt, "prompt", "", "分析提示词（直接提供）")
	flag.StringVar(&cfg.PromptFile, "p", "", "提示词文件路径")
	flag.BoolVar(&cfg.ShowInfo, "info", false, "仅显示图片信息，不进行分析")
	flag.StringVar(&cfg.OllamaURL, "ollama-url", "http://localhost:11434", "Ollama 服务地址")
	flag.StringVar(&cfg.OllamaModel, "model", "qwen3-vl:4b", "Ollama 模型名称")

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

	// 检查必需参数
	if cfg.ImagePath == "" {
		fmt.Fprintf(os.Stderr, "错误: 缺少必需参数 -fpath\n\n")
		flag.Usage()
		os.Exit(1)
	}

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

	// 创建 Ollama 客户端
	client := NewOllamaClient(cfg.OllamaURL, cfg.OllamaModel)

	// 检查服务健康状态
	if err := client.CheckHealth(); err != nil {
		Logger.Errorw("Ollama 服务连接失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 无法连接到 Ollama 服务 (%s)\n", cfg.OllamaURL)
		fmt.Fprintf(os.Stderr, "请确保 Ollama 服务已启动: ollama run %s\n", cfg.OllamaModel)
		os.Exit(1)
	}

	Logger.Infow("解析命令行参数",
		"imagePath", cfg.ImagePath,
		"dbPath", cfg.DBPath,
		"promptFile", cfg.PromptFile,
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
// 返回值: true - 已处理（重复文件），false - 需要继续处理新文件
func checkDuplicateFile(appCtx *AppContext, basicInfo *ImageBasicInfo) bool {
	exists, err := appCtx.DB.CheckMD5Exists(basicInfo.MD5)
	if err != nil {
		Logger.Errorw("检查 MD5 存在性失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 检查 MD5 存在性失败: %v\n", err)
		os.Exit(1)
	}

	if !exists {
		return false // 不是重复文件，继续处理
	}

	// 查询该 MD5 已有的文件路径
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

	if pathExists {
		// 路径已存在，说明是完全相同的重复处理，静默退出
		fmt.Println("\n图片已存在（相同路径），跳过处理")
		Logger.Infow("程序结束")
		return true
	}

	// 路径不存在，说明是不同路径的重复文件
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
	fmt.Println("\n图片已存在（不同路径的重复文件），已添加文件索引")
	Logger.Infow("程序结束")
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

func main() {
	// 初始化并验证环境
	appCtx := setupAndValidate()
	defer Logger.Sync()

	// 分析图片
	ctx := context.Background()
	result := analyzeImage(ctx, appCtx)

	// 初始化数据库
	appCtx.DB = initDatabase(appCtx.Config)
	defer appCtx.DB.Close()

	// 提取元数据
	basicInfo := extractMetadata(appCtx)

	// 检查并处理重复文件
	if checkDuplicateFile(appCtx, basicInfo) {
		return // 重复文件已处理，直接返回
	}

	// 保存新图片元数据
	saveNewImage(appCtx, basicInfo, result)
}
