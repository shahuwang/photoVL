package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"
)

const (
	// DefaultTextModelURL 默认文本向量服务地址
	DefaultTextModelURL = "http://localhost:5000"
	// DefaultTextModelName 默认模型名称
	DefaultTextModelName = "qwen3-vl-embedding-8b"
	// DefaultTopN 默认返回的相似图片数量
	DefaultTopN = 10
)

// TextQueryClient 文本查询客户端
type TextQueryClient struct {
	ModelURL   string
	ModelName  string
	HTTPClient *http.Client
	logger     *zap.SugaredLogger
}

// TextEmbeddingResponse 文本向量服务响应结构
type TextEmbeddingResponse struct {
	Model         string      `json:"model"`
	Embeddings    [][]float64 `json:"embeddings"` // 注意：服务返回的是float64
	TotalDuration int64       `json:"total_duration"`
}

// SimilarImage 相似图片结果
type SimilarImage struct {
	MD5        string
	FilePaths  []string
	Similarity float64
}

// NewTextQueryClient 创建新的文本查询客户端
func NewTextQueryClient(modelURL, modelName string) *TextQueryClient {
	if modelURL == "" {
		modelURL = DefaultTextModelURL
	}
	if modelName == "" {
		modelName = DefaultTextModelName
	}

	return &TextQueryClient{
		ModelURL:  modelURL,
		ModelName: modelName,
		HTTPClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		logger: Logger,
	}
}

// SetLogger 设置日志器
func (c *TextQueryClient) SetLogger(logger *zap.SugaredLogger) {
	c.logger = logger
}

// GenerateTextVector 生成文本向量
// text: 输入文本
// 返回4096维float32向量
func (c *TextQueryClient) GenerateTextVector(text string) ([]float32, error) {
	if text == "" {
		return nil, fmt.Errorf("文本不能为空")
	}

	// 构建请求
	requestBody := map[string]interface{}{
		"model": c.ModelName,
		"input": text,
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("序列化请求失败: %w", err)
	}

	// 发送请求
	url := c.ModelURL + "/api/embed_text"
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	if c.logger != nil {
		c.logger.Debugw("发送文本向量化请求", "url", url, "model", c.ModelName, "text", text)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求向量服务失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&errResp); err == nil {
			if errMsg, ok := errResp["error"]; ok {
				return nil, fmt.Errorf("向量服务返回错误: %s", errMsg)
			}
		}
		return nil, fmt.Errorf("向量服务返回错误状态码: %d", resp.StatusCode)
	}

	// 解析响应
	var embedResp TextEmbeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&embedResp); err != nil {
		return nil, fmt.Errorf("解析响应失败: %w", err)
	}

	if len(embedResp.Embeddings) == 0 {
		return nil, fmt.Errorf("向量服务返回空向量")
	}

	// 转换为float32
	vector64 := embedResp.Embeddings[0]
	vector := make([]float32, len(vector64))
	for i, v := range vector64 {
		vector[i] = float32(v)
	}

	if c.logger != nil {
		c.logger.Debugw("文本向量化完成", "dimension", len(vector))
	}

	return vector, nil
}

// CheckModelHealth 检查向量服务健康状态
func (c *TextQueryClient) CheckModelHealth() error {
	url := c.ModelURL + "/api/health"
	resp, err := c.HTTPClient.Get(url)
	if err != nil {
		return fmt.Errorf("无法连接到向量服务: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("向量服务返回异常状态码: %d", resp.StatusCode)
	}

	return nil
}

// cosineSimilarity 计算两个向量的余弦相似度
// 返回值范围：[-1, 1]，越接近1表示越相似
func cosineSimilarity(a, b []float32) float64 {
	if len(a) != len(b) {
		return 0
	}

	var dotProduct float64
	var normA float64
	var normB float64

	for i := 0; i < len(a); i++ {
		dotProduct += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}

	if normA == 0 || normB == 0 {
		return 0
	}

	return dotProduct / (sqrt(normA) * sqrt(normB))
}

// sqrt 计算平方根
func sqrt(x float64) float64 {
	if x == 0 {
		return 0
	}
	// 使用牛顿迭代法计算平方根
	z := x
	for i := 0; i < 10; i++ {
		z = (z + x/z) / 2
	}
	return z
}

// SearchSimilarImages 搜索相似图片
// queryVector: 查询向量
// queryText: 查询文本（用于文本匹配）
// topN: 返回结果数量
// db: 数据库管理器
// 返回按相似度排序的SimilarImage列表
func SearchSimilarImages(queryVector []float32, queryText string, topN int, db *LanceDBManager) ([]SimilarImage, error) {
	if db == nil {
		return nil, fmt.Errorf("数据库管理器不能为空")
	}

	if len(queryVector) == 0 {
		return nil, fmt.Errorf("查询向量不能为空")
	}

	if topN <= 0 {
		topN = DefaultTopN
	}

	// 获取所有有向量的图片元数据
	allMetadata, err := db.GetAllImageMetadataWithVector()
	if err != nil {
		return nil, fmt.Errorf("获取图片元数据失败: %w", err)
	}

	if len(allMetadata) == 0 {
		return nil, fmt.Errorf("数据库中没有图片向量数据，请先使用 eb 模式处理图片")
	}

	// 解析查询关键词
	queryKeywords := extractKeywords(queryText)

	// 计算相似度（向量相似度 + 文本匹配加分）
	var similarities []SimilarImage
	for _, meta := range allMetadata {
		if len(meta.ImageVector) == 0 {
			continue
		}

		// 基础向量相似度
		vectorSim := cosineSimilarity(queryVector, meta.ImageVector)
		
		// 文本匹配加分
		textBonus := calculateTextBonus(queryKeywords, meta)
		
		// 综合得分：向量相似度占主要权重，文本匹配作为辅助
		finalScore := vectorSim*0.7 + textBonus*0.3
		
		similarities = append(similarities, SimilarImage{
			MD5:        meta.MD5,
			Similarity: finalScore,
		})
	}

	if len(similarities) == 0 {
		return nil, fmt.Errorf("没有可比较的图片向量")
	}

	// 按相似度降序排序
	sort.Slice(similarities, func(i, j int) bool {
		return similarities[i].Similarity > similarities[j].Similarity
	})

	// 取前topN个
	if len(similarities) > topN {
		similarities = similarities[:topN]
	}

	// 查询文件路径
	for i := range similarities {
		paths, err := db.GetFilePathsByMD5(similarities[i].MD5)
		if err != nil {
			if Logger != nil {
				Logger.Warnw("查询文件路径失败", "md5", similarities[i].MD5, "error", err)
			}
			continue
		}
		similarities[i].FilePaths = paths
		if len(paths) == 0 && Logger != nil {
			Logger.Warnw("未找到文件路径", "md5", similarities[i].MD5)
		}
	}

	return similarities, nil
}

// extractKeywords 从查询文本中提取关键词
func extractKeywords(queryText string) []string {
	// 简单的关键词提取：按空格和标点分割
	queryText = strings.ToLower(queryText)
	// 替换标点符号为空格
	punctuations := "，。！？、；：\"'（）【】"
	for _, r := range punctuations {
		queryText = strings.ReplaceAll(queryText, string(r), " ")
	}
	// 分割并过滤空字符串
	words := strings.Fields(queryText)
	var keywords []string
	for _, w := range words {
		if w != "" {
			keywords = append(keywords, w)
		}
	}
	return keywords
}

// calculateTextBonus 计算文本匹配加分（0-1之间）
func calculateTextBonus(keywords []string, meta *ImageMetadata) float64 {
	if len(keywords) == 0 {
		return 0
	}

	matchCount := 0
	// 检查各个文本字段
	textFields := []string{
		strings.ToLower(meta.Description),
		strings.ToLower(meta.Place),
		strings.ToLower(meta.Colors),
	}
	for _, theme := range meta.Theme {
		textFields = append(textFields, strings.ToLower(theme))
	}
	for _, obj := range meta.Objects {
		textFields = append(textFields, strings.ToLower(obj))
	}
	for _, mood := range meta.Mood {
		textFields = append(textFields, strings.ToLower(mood))
	}
	for _, action := range meta.Action {
		textFields = append(textFields, strings.ToLower(action))
	}
	for _, addr := range meta.Address {
		textFields = append(textFields, strings.ToLower(addr))
	}

	for _, keyword := range keywords {
		for _, field := range textFields {
			if strings.Contains(field, keyword) {
				matchCount++
				break
			}
		}
	}

	// 返回匹配比例（0-1）
	return float64(matchCount) / float64(len(keywords))
}

// readQueryText 从标准输入读取查询文字
func readQueryText() (string, error) {
	fmt.Print("请输入查询文字: ")
	reader := bufio.NewReader(os.Stdin)
	text, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	// 去掉末尾的换行符
	if len(text) > 0 && text[len(text)-1] == '\n' {
		text = text[:len(text)-1]
	}
	return text, nil
}

// processTextQueryMode 处理 textQuery 模式
func processTextQueryMode(appCtx *AppContext) {
	Logger.Infow("进入文本查询模式")

	// 检查向量服务健康状态
	client := NewTextQueryClient(DefaultTextModelURL, DefaultTextModelName)
	client.SetLogger(Logger)

	if err := client.CheckModelHealth(); err != nil {
		Logger.Errorw("向量服务连接失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 无法连接到向量服务 (%s)\n", DefaultTextModelURL)
		fmt.Fprintf(os.Stderr, "请确保向量服务已启动: python qwen3_vl_embedding_server.py\n")
		os.Exit(1)
	}

	// 初始化数据库
	appCtx.DB = initDatabase(appCtx.Config)
	defer appCtx.DB.Close()

	// 读取用户输入
	queryText, err := readQueryText()
	if err != nil {
		Logger.Errorw("读取输入失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 读取输入失败: %v\n", err)
		os.Exit(1)
	}

	if queryText == "" {
		fmt.Println("查询文字不能为空")
		os.Exit(1)
	}

	Logger.Infow("用户输入", "text", queryText)
	fmt.Printf("\n正在生成文本向量...\n")

	// 生成文本向量
	queryVector, err := client.GenerateTextVector(queryText)
	if err != nil {
		Logger.Errorw("生成文本向量失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 生成文本向量失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("文本向量生成完成，维度: %d\n", len(queryVector))
	fmt.Printf("正在搜索相似图片...\n")

	// 搜索相似图片
	topN := appCtx.Config.TopN
	if topN <= 0 {
		topN = DefaultTopN
	}

	similarImages, err := SearchSimilarImages(queryVector, queryText, topN, appCtx.DB)
	if err != nil {
		Logger.Errorw("搜索相似图片失败", "error", err)
		fmt.Fprintf(os.Stderr, "错误: 搜索相似图片失败: %v\n", err)
		os.Exit(1)
	}

	// 输出结果
	fmt.Printf("\n=== 搜索结果 ===\n")
	fmt.Printf("查询: %s\n", queryText)
	fmt.Printf("找到 %d 个相似图片:\n\n", len(similarImages))

	for i, img := range similarImages {
		fmt.Printf("%d. MD5: %s (相似度: %.4f)\n", i+1, img.MD5, img.Similarity)
		if len(img.FilePaths) == 0 {
			fmt.Printf("   路径: (未记录)\n")
		} else {
			for _, path := range img.FilePaths {
				fmt.Printf("   路径: %s\n", path)
			}
		}
		fmt.Println()
	}

	Logger.Infow("文本查询完成", "query", queryText, "results", len(similarImages))
}
