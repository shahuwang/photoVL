package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jdeng/goheif"
	"github.com/rwcarlsen/goexif/exif"
	"go.uber.org/zap"
	"golang.org/x/image/bmp"
	"golang.org/x/image/webp"
)

// ImageBasicInfo 图片基础信息（从文件和EXIF提取）
type ImageBasicInfo struct {
	MD5         string
	Ext         string
	Size        int64
	Width       int
	Height      int
	Datetime    time.Time
	Coordinates []float32 // [lng, lat]
	HasEXIF     bool
}

// ImageAnalysisData 视觉分析结果数据
type ImageAnalysisData struct {
	Description string
	Theme       []string
	Objects     []string
	Action      []string
	Mood        []string
	Colors      []string
	Address     string
	Place       string
	ImageVector []float32
}

// CompleteImageMetadata 完整的图片元数据（用于存入数据库）
type CompleteImageMetadata struct {
	ImageBasicInfo
	ImageAnalysisData
}

// MetadataExtractor 元数据提取器
type MetadataExtractor struct {
	logger *zap.SugaredLogger
}

// NewMetadataExtractor 创建新的元数据提取器
func NewMetadataExtractor() *MetadataExtractor {
	return &MetadataExtractor{
		logger: Logger,
	}
}

// SetLogger 设置日志器
func (e *MetadataExtractor) SetLogger(logger *zap.SugaredLogger) {
	e.logger = logger
}

// CalculateMD5 计算文件的 MD5 值
func (e *MetadataExtractor) CalculateMD5(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("打开文件失败: %w", err)
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("计算 MD5 失败: %w", err)
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// GetFileSize 获取文件大小
func (e *MetadataExtractor) GetFileSize(filePath string) (int64, error) {
	info, err := os.Stat(filePath)
	if err != nil {
		return 0, fmt.Errorf("获取文件信息失败: %w", err)
	}
	return info.Size(), nil
}

// GetFileExtension 获取文件扩展名（标准化为 jpg, png, webp, heic）
func (e *MetadataExtractor) GetFileExtension(filePath string) string {
	ext := strings.ToLower(filepath.Ext(filePath))
	ext = strings.TrimPrefix(ext, ".")

	switch ext {
	case "jpeg":
		return "jpg"
	case "heif":
		return "heic"
	default:
		return ext
	}
}

// ExtractEXIFData 从图片中提取 EXIF 数据
func (e *MetadataExtractor) ExtractEXIFData(filePath string) (*ImageBasicInfo, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("打开文件失败: %w", err)
	}
	defer file.Close()

	info := &ImageBasicInfo{
		Coordinates: []float32{},
	}

	// 尝试解析 EXIF
	x, err := exif.Decode(file)
	if err != nil {
		// 很多图片可能没有 EXIF 信息，这不是错误
		e.logger.Debugw("图片没有 EXIF 信息或解析失败", "path", filePath, "error", err)
		info.HasEXIF = false
		return info, nil
	}

	info.HasEXIF = true

	// 提取拍摄时间
	if tm, err := x.DateTime(); err == nil {
		info.Datetime = tm
		e.logger.Debugw("提取到拍摄时间", "path", filePath, "datetime", tm)
	}

	// 提取 GPS 坐标
	lat, long, err := x.LatLong()
	if err == nil {
		info.Coordinates = []float32{float32(long), float32(lat)} // [lng, lat]
		e.logger.Debugw("提取到 GPS 坐标", "path", filePath, "lat", lat, "lng", long)
	}

	return info, nil
}

// GetImageDimensions 获取图片像素尺寸
func (e *MetadataExtractor) GetImageDimensions(filePath string) (width, height int, err error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, 0, fmt.Errorf("打开文件失败: %w", err)
	}
	defer file.Close()

	// 根据文件扩展名选择解码器
	ext := strings.ToLower(filepath.Ext(filePath))

	switch ext {
	case ".webp":
		config, err := webp.DecodeConfig(file)
		if err != nil {
			return 0, 0, fmt.Errorf("解码 WebP 失败: %w", err)
		}
		return config.Width, config.Height, nil

	case ".bmp":
		config, err := bmp.DecodeConfig(file)
		if err != nil {
			return 0, 0, fmt.Errorf("解码 BMP 失败: %w", err)
		}
		return config.Width, config.Height, nil

	case ".heic", ".heif":
		// HEIC/HEIF 格式使用 goheif 解码
		img, err := goheif.Decode(file)
		if err != nil {
			// 尝试从 EXIF 获取尺寸
			file.Seek(0, 0)
			if x, err := exif.Decode(file); err == nil {
				if w, err := x.Get(exif.ImageWidth); err == nil {
					if h, err := x.Get(exif.ImageLength); err == nil {
						// 从 EXIF 标签解析尺寸
						widthStr := w.String()
						heightStr := h.String()
						// 尝试解析整数值
						fmt.Sscanf(widthStr, "%d", &width)
						fmt.Sscanf(heightStr, "%d", &height)
						if width > 0 && height > 0 {
							return width, height, nil
						}
					}
				}
			}
			return 0, 0, fmt.Errorf("解码 HEIC/HEIF 失败: %w", err)
		}
		bounds := img.Bounds()
		return bounds.Dx(), bounds.Dy(), nil

	default:
		// 使用标准库解码 (JPEG, PNG, GIF)
		config, _, err := image.DecodeConfig(file)
		if err != nil {
			return 0, 0, fmt.Errorf("解码图片失败: %w", err)
		}
		return config.Width, config.Height, nil
	}
}

// ExtractAllMetadata 提取图片的所有元数据
func (e *MetadataExtractor) ExtractAllMetadata(filePath string) (*ImageBasicInfo, error) {
	e.logger.Infow("开始提取图片元数据", "path", filePath)

	// 1. 计算 MD5
	md5Hash, err := e.CalculateMD5(filePath)
	if err != nil {
		return nil, fmt.Errorf("计算 MD5 失败: %w", err)
	}
	e.logger.Debugw("计算 MD5 完成", "md5", md5Hash)

	// 2. 获取文件大小
	fileSize, err := e.GetFileSize(filePath)
	if err != nil {
		return nil, fmt.Errorf("获取文件大小失败: %w", err)
	}

	// 3. 获取文件扩展名
	ext := e.GetFileExtension(filePath)

	// 4. 提取 EXIF 数据（时间、坐标等）
	exifInfo, err := e.ExtractEXIFData(filePath)
	if err != nil {
		e.logger.Warnw("提取 EXIF 数据失败", "path", filePath, "error", err)
		exifInfo = &ImageBasicInfo{}
	}

	// 5. 获取图片尺寸
	width, height, err := e.GetImageDimensions(filePath)
	if err != nil {
		e.logger.Warnw("获取图片尺寸失败", "path", filePath, "error", err)
		// 不返回错误，继续处理
	}

	// 合并所有信息
	metadata := &ImageBasicInfo{
		MD5:         md5Hash,
		Ext:         ext,
		Size:        fileSize,
		Width:       width,
		Height:      height,
		Datetime:    exifInfo.Datetime,
		Coordinates: exifInfo.Coordinates,
		HasEXIF:     exifInfo.HasEXIF,
	}

	e.logger.Infow("元数据提取完成",
		"path", filePath,
		"md5", md5Hash,
		"ext", ext,
		"size", fileSize,
		"dimensions", fmt.Sprintf("%dx%d", width, height),
		"has_datetime", !metadata.Datetime.IsZero(),
		"has_coordinates", len(metadata.Coordinates) > 0,
	)

	return metadata, nil
}

// ParseAnalysisResult 解析视觉分析结果（从模型返回的 JSON）
func (e *MetadataExtractor) ParseAnalysisResult(content string) (*ImageAnalysisData, error) {
	// 这里假设模型返回的是 JSON 格式
	// 根据 prompt 的设计，可能需要调整解析逻辑

	data := &ImageAnalysisData{
		Theme:   []string{},
		Objects: []string{},
		Action:  []string{},
		Mood:    []string{},
		Colors:  []string{},
	}

	// 尝试解析 JSON
	// 注意：这里需要根据实际的模型输出格式进行调整
	// 如果模型返回的是结构化 JSON，可以直接解析
	// 如果返回的是文本，需要提取关键信息

	// 简单处理：将内容作为描述
	data.Description = strings.TrimSpace(content)

	return data, nil
}

// MergeMetadata 合并基础元数据和视觉分析结果
func (e *MetadataExtractor) MergeMetadata(basic *ImageBasicInfo, analysis *ImageAnalysisData) *CompleteImageMetadata {
	return &CompleteImageMetadata{
		ImageBasicInfo:    *basic,
		ImageAnalysisData: *analysis,
	}
}

// ToImageMetadata 转换为数据库存储格式
func (c *CompleteImageMetadata) ToImageMetadata() *ImageMetadata {
	// 转换颜色为字符串列表
	colors := c.Colors
	if colors == nil {
		colors = []string{}
	}

	// 转换坐标为 float32 切片
	var coordinates []float32
	if len(c.Coordinates) == 2 {
		coordinates = c.Coordinates
	}

	// 转换尺寸为 float32 切片 [xsize, ysize]
	dimensions := []float32{float32(c.Width), float32(c.Height)}

	// 如果没有拍摄时间，使用零值时间
	datetime := c.Datetime
	if datetime.IsZero() {
		datetime = time.Time{}
	}

	return &ImageMetadata{
		MD5:         c.MD5,
		Theme:       c.Theme,
		Description: c.Description,
		Objects:     c.Objects,
		Coordinates: coordinates,
		Datetime:    datetime,
		Address:     c.Address,
		Dimensions:  dimensions,
		Ext:         c.Ext,
		Size:        int32(c.Size),
		Place:       c.Place,
		Colors:      colors,
		Mood:        c.Mood,
		Action:      c.Action,
		ImageVector: c.ImageVector,
	}
}

// ToFileIndex 转换为文件索引格式
func (c *CompleteImageMetadata) ToFileIndex(filePath string) *FileIndex {
	return &FileIndex{
		MD5:      c.MD5,
		FilePath: filePath,
	}
}
