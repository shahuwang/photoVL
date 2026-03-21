package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
)

// ImagePipeline 图片处理流程
type ImagePipeline struct {
	db         *LanceDBManager
	extractor  *MetadataExtractor
	ollama     *OllamaClient
	promptFile string
	logger     *zap.SugaredLogger
}

// ImageProcessResult 图片处理结果
type ImageProcessResult struct {
	Success     bool
	MD5         string
	IsDuplicate bool
	Error       error
}

// NewImagePipeline 创建新的图片处理流程
func NewImagePipeline(dbPath string, ollamaURL string, ollamaModel string, promptFile string) (*ImagePipeline, error) {
	// 初始化数据库
	db, err := NewLanceDBManager(dbPath)
	if err != nil {
		return nil, fmt.Errorf("初始化数据库失败: %w", err)
	}

	// 初始化元数据提取器
	extractor := NewMetadataExtractor()

	// 初始化 Ollama 客户端
	ollama := NewOllamaClient(ollamaURL, ollamaModel)

	pipeline := &ImagePipeline{
		db:         db,
		extractor:  extractor,
		ollama:     ollama,
		promptFile: promptFile,
		logger:     Logger,
	}

	return pipeline, nil
}

// SetLogger 设置日志器
func (p *ImagePipeline) SetLogger(logger *zap.SugaredLogger) {
	p.logger = logger
	p.extractor.SetLogger(logger)
	p.ollama.SetLogger(logger)
}

// Close 关闭资源
func (p *ImagePipeline) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

// ProcessImage 处理单张图片（完整流程）
func (p *ImagePipeline) ProcessImage(ctx context.Context, imagePath string) (*ImageProcessResult, error) {
	p.logger.Infow("开始处理图片", "path", imagePath)

	// 1. 提取基础元数据
	basicInfo, err := p.extractor.ExtractAllMetadata(imagePath)
	if err != nil {
		return &ImageProcessResult{Success: false, Error: err}, fmt.Errorf("提取元数据失败: %w", err)
	}

	p.logger.Infow("基础元数据提取完成",
		"md5", basicInfo.MD5,
		"ext", basicInfo.Ext,
		"size", basicInfo.Size,
		"width", basicInfo.Width,
		"height", basicInfo.Height,
	)

	// 2. 检查 MD5 是否已存在（重复文件检测）
	exists, err := p.db.CheckMD5Exists(basicInfo.MD5)
	if err != nil {
		return &ImageProcessResult{Success: false, Error: err}, fmt.Errorf("检查 MD5 存在性失败: %w", err)
	}

	if exists {
		p.logger.Infow("检测到重复文件，仅更新文件索引", "md5", basicInfo.MD5, "path", imagePath)
		
		// 只添加文件索引
		fileIndex := &FileIndex{
			MD5:      basicInfo.MD5,
			FilePath: imagePath,
		}
		if err := p.db.InsertFileIndex(fileIndex); err != nil {
			return &ImageProcessResult{Success: false, MD5: basicInfo.MD5, IsDuplicate: true, Error: err}, 
				fmt.Errorf("插入文件索引失败: %w", err)
		}
		
		return &ImageProcessResult{
			Success:     true,
			MD5:         basicInfo.MD5,
			IsDuplicate: true,
		}, nil
	}

	// 3. 读取提示词
	prompt, err := p.readPrompt()
	if err != nil {
		return &ImageProcessResult{Success: false, MD5: basicInfo.MD5, Error: err}, 
			fmt.Errorf("读取提示词失败: %w", err)
	}

	// 4. 调用视觉模型进行分析
	p.logger.Infow("开始视觉分析", "path", imagePath, "model", p.ollama.Model)
	analysisResult, err := p.ollama.AnalyzeImage(ctx, imagePath, prompt)
	if err != nil {
		return &ImageProcessResult{Success: false, MD5: basicInfo.MD5, Error: err}, 
			fmt.Errorf("视觉分析失败: %w", err)
	}

	p.logger.Infow("视觉分析完成", "path", imagePath, "contentLength", len(analysisResult.Content))

	// 5. 解析视觉分析结果
	analysisData, err := p.parseAnalysisContent(analysisResult.Content)
	if err != nil {
		p.logger.Warnw("解析视觉分析结果失败，使用原始内容", "error", err)
		analysisData = &ImageAnalysisData{
			Description: analysisResult.Content,
			Theme:       []string{},
			Objects:     []string{},
			Action:      []string{},
			Mood:        []string{},
			Colors:      "",
		}
	}

	// 6. 合并元数据
	completeMetadata := p.extractor.MergeMetadata(basicInfo, analysisData)

	// 7. 图片向量保持为空，等待后续向量生成服务集成

	// 8. 存入数据库
	if err := p.saveToDatabase(completeMetadata, imagePath); err != nil {
		return &ImageProcessResult{Success: false, MD5: basicInfo.MD5, Error: err}, 
			fmt.Errorf("保存到数据库失败: %w", err)
	}

	p.logger.Infow("图片处理完成", "path", imagePath, "md5", basicInfo.MD5)

	return &ImageProcessResult{
		Success:     true,
		MD5:         basicInfo.MD5,
		IsDuplicate: false,
	}, nil
}

// ProcessImagesBatch 批量处理图片
func (p *ImagePipeline) ProcessImagesBatch(ctx context.Context, imagePaths []string) ([]*ImageProcessResult, error) {
	p.logger.Infow("开始批量处理图片", "count", len(imagePaths))

	results := make([]*ImageProcessResult, 0, len(imagePaths))
	
	for i, path := range imagePaths {
		p.logger.Infow("处理第 N 张图片", "index", i+1, "total", len(imagePaths), "path", path)
		
		result, err := p.ProcessImage(ctx, path)
		if err != nil {
			p.logger.Errorw("处理图片失败", "path", path, "error", err)
			results = append(results, &ImageProcessResult{
				Success: false,
				Error:   err,
			})
			continue
		}
		
		results = append(results, result)
		
		// 添加短暂延迟，避免请求过于频繁
		if i < len(imagePaths)-1 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	p.logger.Infow("批量处理完成", "total", len(imagePaths), "success", p.countSuccess(results))

	return results, nil
}

// readPrompt 读取提示词
func (p *ImagePipeline) readPrompt() (string, error) {
	if p.promptFile == "" {
		// 使用默认提示词
		return p.getDefaultPrompt(), nil
	}

	data, err := readPromptFromFile(p.promptFile)
	if err != nil {
		return "", err
	}

	return data, nil
}

// getDefaultPrompt 获取默认提示词
func (p *ImagePipeline) getDefaultPrompt() string {
	return `请详细分析这张图片，并以 JSON 格式返回以下信息：
{
  "description": "详细的中文图片描述",
  "theme": ["主题标签1", "主题标签2"],
  "objects": ["物体1", "物体2", "物体3"],
  "action": ["动作1", "动作2"],
  "mood": ["氛围1", "氛围2"],
  "colors": ["颜色1", "颜色2"],
  "address": "拍摄地址（如果可从图片中识别）",
  "place": "场所类型（如：室内、室外、公园、海边等）"
}`
}

// parseAnalysisContent 解析视觉分析内容
func (p *ImagePipeline) parseAnalysisContent(content string) (*ImageAnalysisData, error) {
	data := &ImageAnalysisData{
		Theme:   []string{},
		Objects: []string{},
		Action:  []string{},
		Mood:    []string{},
		Colors:  "",
	}

	// 尝试从内容中提取 JSON
	jsonStart := strings.Index(content, "{")
	jsonEnd := strings.LastIndex(content, "}")
	
	if jsonStart >= 0 && jsonEnd > jsonStart {
		jsonStr := content[jsonStart : jsonEnd+1]
		
		var parsed struct {
			Description string   `json:"description"`
			Theme       []string `json:"theme"`
			Objects     []string `json:"objects"`
			Action      []string `json:"action"`
			Mood        []string `json:"mood"`
			Colors      string   `json:"colors"`
			Address     []string `json:"address"`
			Place       string   `json:"place"`
		}
		
		if err := json.Unmarshal([]byte(jsonStr), &parsed); err == nil {
			data.Description = parsed.Description
			data.Theme = parsed.Theme
			data.Objects = parsed.Objects
			data.Action = parsed.Action
			data.Mood = parsed.Mood
			data.Colors = parsed.Colors
			data.Address = parsed.Address
			data.Place = parsed.Place
			return data, nil
		}
	}

	// 如果 JSON 解析失败，使用原始内容作为描述
	data.Description = strings.TrimSpace(content)
	
	// 尝试从文本中提取一些关键词
	data.Theme = p.extractKeywords(content, []string{"人物", "风景", "建筑", "动物", "植物", "食物", "活动"})
	data.Objects = p.extractKeywords(content, []string{"人", "树", "花", "水", "山", "建筑", "车", "动物"})
	
	return data, nil
}

// extractKeywords 从文本中提取关键词（简单实现）
func (p *ImagePipeline) extractKeywords(text string, keywords []string) []string {
	found := []string{}
	for _, kw := range keywords {
		if strings.Contains(text, kw) {
			found = append(found, kw)
		}
	}
	return found
}

// saveToDatabase 保存到数据库
func (p *ImagePipeline) saveToDatabase(metadata *CompleteImageMetadata, filePath string) error {
	// 1. 保存图片元数据
	imageMeta := metadata.ToImageMetadata()
	if err := p.db.InsertImageMetadata(imageMeta); err != nil {
		return fmt.Errorf("插入图片元数据失败: %w", err)
	}

	// 2. 保存文件索引
	fileIndex := metadata.ToFileIndex(filePath)
	if err := p.db.InsertFileIndex(fileIndex); err != nil {
		return fmt.Errorf("插入文件索引失败: %w", err)
	}

	return nil
}

// countSuccess 统计成功数量
func (p *ImagePipeline) countSuccess(results []*ImageProcessResult) int {
	count := 0
	for _, r := range results {
		if r.Success {
			count++
		}
	}
	return count
}

// GetImageMetadata 根据 MD5 获取图片元数据
func (p *ImagePipeline) GetImageMetadata(md5 string) (*ImageMetadata, error) {
	return p.db.GetImageMetadataByMD5(md5)
}

// GetFilePaths 根据 MD5 获取文件路径列表
func (p *ImagePipeline) GetFilePaths(md5 string) ([]string, error) {
	return p.db.GetFilePathsByMD5(md5)
}

// CheckHealth 检查服务健康状态
func (p *ImagePipeline) CheckHealth() error {
	return p.ollama.CheckHealth()
}
