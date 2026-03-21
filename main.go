package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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
	Model     string
	CreatedAt time.Time
}

// ImageAnalysisSchema 图片分析的 JSON Schema 结构
type ImageAnalysisSchema struct {
	Description string   `json:"description"`
	Theme       []string `json:"theme"`
	Objects     []string `json:"objects"`
	Action      []string `json:"action"`
	Mood        []string `json:"mood"`
	Colors      string   `json:"colors"`
	Address     []string `json:"address"`
}

// NewOllamaClient 创建新的 Ollama 客户端
func NewOllamaClient(baseURL, model string) *OllamaClient {
	if baseURL == "" {
		baseURL = "http://localhost:11434"
	}

	// Logger.Debugw("创建 Ollama 客户端",
	// 	"baseURL", baseURL,
	// 	"model", model,
	// )

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
	// Logger.Debugw("开始处理图片", "path", imagePath)

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
	// c.logger.Infow("开始分析图片",
	// 	"imagePath", imagePath,
	// 	"model", c.Model,
	// )

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
	// c.logger.Infow("使用自定义选项分析图片",
	// 	"imagePath", imagePath,
	// 	"temperature", opts.Temperature,
	// 	"topP", opts.TopP,
	// 	"maxTokens", opts.MaxTokens,
	// )

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
	// c.logger.Infow("开始分析多张图片",
	// 	"count", len(imagePaths),
	// )

	if len(imagePaths) == 0 {
		c.logger.Errorw("未提供图片路径")
		return nil, fmt.Errorf("至少需要提供一张图片")
	}

	var base64Images []string
	for _, path := range imagePaths {
		// c.logger.Debugw("编码第 N 张图片", "index", i+1, "path", path)
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

// validateJSONSchema 验证响应内容是否符合 JSON Schema 定义
func validateJSONSchema(content string) error {
	var schema ImageAnalysisSchema
	if err := json.Unmarshal([]byte(content), &schema); err != nil {
		return fmt.Errorf("JSON 解析失败: %w", err)
	}
	
	// 检查必要字段是否存在（description 是核心字段）
	if schema.Description == "" {
		return fmt.Errorf("JSON 缺少必要字段: description 为空")
	}
	
	return nil
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
	needRetry := false
	if content == "" {
		c.logger.Warnw("模型返回空响应")
		needRetry = true
	} else {
		// 验证 JSON Schema
		if err := validateJSONSchema(content); err != nil {
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

		// 再次验证 JSON Schema
		if err := validateJSONSchema(content); err != nil {
			c.logger.Errorw("重试后模型返回的 JSON 仍然不符合 Schema", "error", err, "content", content)
			return nil, fmt.Errorf("图片处理失败")
		}
	}

	createdAt, _ := time.Parse(time.RFC3339, genResp.CreatedAt)

	return &ImageAnalysisResult{
		Content:   content,
		Model:     genResp.Model,
		CreatedAt: createdAt,
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
	// c.logger.Debugw("检查 Ollama 服务健康状态", "url", c.BaseURL)

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

func printUsage() {
	fmt.Println("用法:")
	fmt.Println("  go run main.go <图片路径> [提示词]              分析图片（直接提供提示词）")
	fmt.Println("  go run main.go --prompt-file <文件> <图片路径>  从文件读取提示词分析图片")
	fmt.Println("  go run main.go --info <图片路径>                查看图片信息")
	fmt.Println("")
	fmt.Println("示例:")
	fmt.Println("  go run main.go ./test.jpg '请描述这张图片'")
	fmt.Println("  go run main.go --prompt-file prompt.txt ./test.jpg")
	fmt.Println("  go run main.go -p prompt.txt ./test.png")
	fmt.Println("  go run main.go --info ./test.heic")
	fmt.Println("")
	fmt.Println("支持的格式: jpg, jpeg, png, webp, heic, heif")
}

// readPromptFromFile 从文件读取提示词
func readPromptFromFile(path string) (string, error) {
	// Logger.Debugw("读取提示词文件", "path", path)

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

	// Logger.Infow("提示词文件读取成功",
	// 	"path", path,
	// 	"length", len(prompt),
	// )

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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func main() {
	// 初始化日志
	Logger = initLogger()
	defer Logger.Sync()

	// Logger.Infow("程序启动", "version", "1.0.0")

	// 检查命令行参数
	if len(os.Args) < 2 {
		Logger.Errorw("缺少必要的命令行参数")
		printUsage()
		os.Exit(1)
	}

	// 处理 --info 参数
	if os.Args[1] == "--info" || os.Args[1] == "-i" {
		if len(os.Args) < 3 {
			Logger.Errorw("缺少图片路径参数")
			printUsage()
			os.Exit(1)
		}
		imagePath := os.Args[2]

		// 验证图片文件是否存在
		if _, err := os.Stat(imagePath); os.IsNotExist(err) {
			Logger.Errorw("图片文件不存在", "path", imagePath)
			os.Exit(1)
		}

		showImageInfo(imagePath)
		return
	}

	// 创建 Ollama 客户端
	client := NewOllamaClient("http://localhost:11434", "qwen3-vl:4b")

	// 检查服务健康状态
	// Logger.Infow("正在检查 Ollama 服务...")
	if err := client.CheckHealth(); err != nil {
		Logger.Errorw("Ollama 服务连接失败", "error", err)
		Logger.Infow("请确保 Ollama 服务已启动", "command", "ollama run qwen3-vl:4b")
		os.Exit(1)
	}
	// Logger.Infow("Ollama 服务连接成功")

	// 解析命令行参数
	var imagePath string
	var prompt string
	var promptFile string

	// 检查是否有 --prompt-file 或 -p 参数
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "--prompt-file" || os.Args[i] == "-p" {
			if i+1 >= len(os.Args) {
				Logger.Errorw("缺少提示词文件路径")
				printUsage()
				os.Exit(1)
			}
			promptFile = os.Args[i+1]
			// 移除这两个参数，重新整理参数列表
			os.Args = append(os.Args[:i], os.Args[i+2:]...)
			break
		}
	}

	// 重新检查参数数量
	if len(os.Args) < 2 {
		Logger.Errorw("缺少必要的命令行参数")
		printUsage()
		os.Exit(1)
	}

	imagePath = os.Args[1]

	// 如果指定了提示词文件，从文件读取
	if promptFile != "" {
		var err error
		prompt, err = readPromptFromFile(promptFile)
		if err != nil {
			Logger.Errorw("无法读取提示词文件", "error", err)
			os.Exit(1)
		}
	} else if len(os.Args) >= 3 {
		// 从命令行参数获取提示词
		prompt = os.Args[2]
	} else {
		// 使用默认提示词
		prompt = "请详细描述这张图片中的内容，包括主要物体、场景、颜色、文字等细节。"
	}

	Logger.Infow("解析命令行参数",
		"imagePath", imagePath,
		"promptFile", promptFile,
		"promptLength", len(prompt),
	)

	// 验证图片文件是否存在
	if _, err := os.Stat(imagePath); os.IsNotExist(err) {
		Logger.Errorw("图片文件不存在", "path", imagePath)
		os.Exit(1)
	}

	Logger.Infow("图片文件存在", "path", imagePath)

	ctx := context.Background()

	// 方式1: 非流式调用
	Logger.Infow("开始非流式图片分析")
	result, err := client.AnalyzeImage(ctx, imagePath, prompt)
	if err != nil {
		Logger.Errorw("分析失败", "error", err)
		os.Exit(1)
	}

	fmt.Println("\n=== 分析结果 ===")
	fmt.Printf("模型: %s\n", result.Model)
	fmt.Printf("分析内容:\n%s\n", result.Content)

	// // 方式2: 流式调用示例（可选）
	fmt.Println("\n=== 流式输出示例 ===")
	Logger.Infow("开始流式图片分析")

	err = client.StreamAnalyzeImage(ctx, imagePath, prompt, func(chunk string) {
		fmt.Print(chunk)
	})
	if err != nil {
		Logger.Errorw("流式分析失败", "error", err)
	} else {
		Logger.Infow("流式分析完成")
	}
	fmt.Println()

	Logger.Infow("程序结束")
}
