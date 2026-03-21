package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/jdeng/goheif"
	"go.uber.org/zap"
	"golang.org/x/image/webp"
)

// ImageProcessor 图片处理器
type ImageProcessor struct {
	MaxDimension int
	Quality      int
	logger       *zap.SugaredLogger
}

// NewImageProcessor 创建新的图片处理器
func NewImageProcessor(maxDimension, quality int) *ImageProcessor {
	if maxDimension <= 0 {
		maxDimension = 1024
	}
	if quality <= 0 || quality > 100 {
		quality = 85
	}
	
	Logger.Debugw("创建图片处理器", 
		"maxDimension", maxDimension, 
		"quality", quality,
	)
	
	return &ImageProcessor{
		MaxDimension: maxDimension,
		Quality:      quality,
		logger:       Logger,
	}
}

// SetLogger 设置日志器
func (p *ImageProcessor) SetLogger(logger *zap.SugaredLogger) {
	p.logger = logger
}

// GetImageFormat 获取图片格式
func (p *ImageProcessor) GetImageFormat(path string) (string, error) {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".jpg", ".jpeg":
		return "jpeg", nil
	case ".png":
		return "png", nil
	case ".webp":
		return "webp", nil
	case ".heic", ".heif":
		return "heic", nil
	default:
		// 尝试通过文件头检测
		file, err := os.Open(path)
		if err != nil {
			return "", err
		}
		defer file.Close()
		
		buffer := make([]byte, 512)
		n, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			return "", err
		}
		buffer = buffer[:n]
		
		return detectFormatByHeader(buffer), nil
	}
}

// detectFormatByHeader 通过文件头检测图片格式
func detectFormatByHeader(header []byte) string {
	if len(header) < 8 {
		return "unknown"
	}
	
	// JPEG: FF D8 FF
	if header[0] == 0xFF && header[1] == 0xD8 && header[2] == 0xFF {
		return "jpeg"
	}
	
	// PNG: 89 50 4E 47 0D 0A 1A 0A
	if header[0] == 0x89 && header[1] == 0x50 && header[2] == 0x4E && header[3] == 0x47 {
		return "png"
	}
	
	// WebP: RIFF....WEBP
	if header[0] == 0x52 && header[1] == 0x49 && header[2] == 0x46 && header[3] == 0x46 &&
		len(header) >= 12 && header[8] == 0x57 && header[9] == 0x45 && header[10] == 0x42 && header[11] == 0x50 {
		return "webp"
	}
	
	// HEIC/HEIF: ftypheic, ftypheix, ftyphevc, ftypheim
	if len(header) >= 12 {
		ftyp := string(header[4:8])
		if ftyp == "ftyp" {
			brand := string(header[8:12])
			if brand == "heic" || brand == "heix" || brand == "hevc" || brand == "heim" ||
			   brand == "mif1" || brand == "msf1" {
				return "heic"
			}
		}
	}
	
	return "unknown"
}

// DecodeImage 解码图片（支持多种格式）
func (p *ImageProcessor) DecodeImage(path string) (image.Image, string, error) {
	format, err := p.GetImageFormat(path)
	if err != nil {
		return nil, "", fmt.Errorf("检测图片格式失败: %w", err)
	}
	
	p.logger.Debugw("检测到图片格式", "path", path, "format", format)
	
	file, err := os.Open(path)
	if err != nil {
		return nil, "", fmt.Errorf("打开图片文件失败: %w", err)
	}
	defer file.Close()
	
	var img image.Image
	
	switch format {
	case "jpeg":
		img, err = jpeg.Decode(file)
		if err != nil {
			return nil, "", fmt.Errorf("解码 JPEG 失败: %w", err)
		}
	case "png":
		img, err = png.Decode(file)
		if err != nil {
			return nil, "", fmt.Errorf("解码 PNG 失败: %w", err)
		}
	case "webp":
		img, err = webp.Decode(file)
		if err != nil {
			return nil, "", fmt.Errorf("解码 WebP 失败: %w", err)
		}
	case "heic":
		img, err = goheif.Decode(file)
		if err != nil {
			return nil, "", fmt.Errorf("解码 HEIC 失败: %w", err)
		}
	default:
		// 尝试使用标准库自动检测
		img, format, err = image.Decode(file)
		if err != nil {
			return nil, "", fmt.Errorf("解码图片失败，不支持的格式: %w", err)
		}
	}
	
	p.logger.Debugw("图片解码成功", 
		"path", path, 
		"format", format,
		"width", img.Bounds().Dx(),
		"height", img.Bounds().Dy(),
	)
	
	return img, format, nil
}

// NeedResize 判断是否需要缩放
func (p *ImageProcessor) NeedResize(img image.Image) bool {
	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()
	
	maxSide := width
	if height > maxSide {
		maxSide = height
	}
	
	return maxSide > p.MaxDimension
}

// ResizeImage 等比缩放图片，确保最大边不超过 MaxDimension
func (p *ImageProcessor) ResizeImage(img image.Image) image.Image {
	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()
	
	maxSide := width
	if height > maxSide {
		maxSide = height
	}
	
	// 如果不需要缩放，直接返回原图
	if maxSide <= p.MaxDimension {
		p.logger.Debugw("图片不需要缩放", 
			"width", width, 
			"height", height,
			"maxSide", maxSide,
		)
		return img
	}
	
	// 计算缩放比例
	scale := float64(p.MaxDimension) / float64(maxSide)
	newWidth := int(float64(width) * scale)
	newHeight := int(float64(height) * scale)
	
	p.logger.Infow("缩放图片", 
		"originalWidth", width, 
		"originalHeight", height,
		"newWidth", newWidth,
		"newHeight", newHeight,
		"scale", scale,
	)
	
	// 使用高质量缩放算法
	resized := resizeImage(img, newWidth, newHeight)
	
	return resized
}

// resizeImage 使用双线性插值进行图片缩放
func resizeImage(src image.Image, newWidth, newHeight int) image.Image {
	bounds := src.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()
	
	// 创建目标图片
	dst := image.NewRGBA(image.Rect(0, 0, newWidth, newHeight))
	
	// 双线性插值缩放
	xRatio := float64(width) / float64(newWidth)
	yRatio := float64(height) / float64(newHeight)
	
	for y := 0; y < newHeight; y++ {
		for x := 0; x < newWidth; x++ {
			srcX := float64(x) * xRatio
			srcY := float64(y) * yRatio
			
			// 获取插值后的颜色
			c := bilinearInterpolate(src, srcX, srcY)
			dst.Set(x, y, c)
		}
	}
	
	return dst
}

// bilinearInterpolate 双线性插值
func bilinearInterpolate(img image.Image, x, y float64) color.Color {
	bounds := img.Bounds()
	width := float64(bounds.Dx())
	height := float64(bounds.Dy())
	
	// 边界检查
	if x < 0 {
		x = 0
	}
	if x >= width-1 {
		x = width - 1.001
	}
	if y < 0 {
		y = 0
	}
	if y >= height-1 {
		y = height - 1.001
	}
	
	x0 := int(x)
	y0 := int(y)
	x1 := x0 + 1
	y1 := y0 + 1
	
	dx := x - float64(x0)
	dy := y - float64(y0)
	
	// 获取四个点的颜色
	c00 := img.At(bounds.Min.X+x0, bounds.Min.Y+y0)
	c10 := img.At(bounds.Min.X+x1, bounds.Min.Y+y0)
	c01 := img.At(bounds.Min.X+x0, bounds.Min.Y+y1)
	c11 := img.At(bounds.Min.X+x1, bounds.Min.Y+y1)
	
	// 插值计算
	r00, g00, b00, a00 := c00.RGBA()
	r10, g10, b10, a10 := c10.RGBA()
	r01, g01, b01, a01 := c01.RGBA()
	r11, g11, b11, a11 := c11.RGBA()
	
	r := uint8((float64(r00>>8)*(1-dx)*(1-dy) + float64(r10>>8)*dx*(1-dy) +
		float64(r01>>8)*(1-dx)*dy + float64(r11>>8)*dx*dy))
	g := uint8((float64(g00>>8)*(1-dx)*(1-dy) + float64(g10>>8)*dx*(1-dy) +
		float64(g01>>8)*(1-dx)*dy + float64(g11>>8)*dx*dy))
	b := uint8((float64(b00>>8)*(1-dx)*(1-dy) + float64(b10>>8)*dx*(1-dy) +
		float64(b01>>8)*(1-dx)*dy + float64(b11>>8)*dx*dy))
	a := uint8((float64(a00>>8)*(1-dx)*(1-dy) + float64(a10>>8)*dx*(1-dy) +
		float64(a01>>8)*(1-dx)*dy + float64(a11>>8)*dx*dy))
	
	return color.RGBA{R: r, G: g, B: b, A: a}
}

// EncodeToBase64 将图片编码为 base64 JPEG 格式
func (p *ImageProcessor) EncodeToBase64(img image.Image) (string, error) {
	var buf bytes.Buffer
	
	// 使用 JPEG 编码，设置质量
	options := &jpeg.Options{Quality: p.Quality}
	
	if err := jpeg.Encode(&buf, img, options); err != nil {
		return "", fmt.Errorf("编码 JPEG 失败: %w", err)
	}
	
	p.logger.Debugw("图片编码完成", 
		"size", buf.Len(), 
		"quality", p.Quality,
	)
	
	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

// ProcessImage 处理图片：解码 -> 缩放 -> 编码为 base64
func (p *ImageProcessor) ProcessImage(path string) (string, error) {
	p.logger.Infow("开始处理图片", "path", path)
	
	// 1. 解码图片
	img, format, err := p.DecodeImage(path)
	if err != nil {
		return "", err
	}
	
	p.logger.Infow("图片解码成功", 
		"path", path, 
		"format", format,
		"width", img.Bounds().Dx(),
		"height", img.Bounds().Dy(),
	)
	
	// 2. 检查并缩放图片
	var processedImg image.Image
	if p.NeedResize(img) {
		processedImg = p.ResizeImage(img)
	} else {
		processedImg = img
	}
	
	// 3. 编码为 base64
	base64Str, err := p.EncodeToBase64(processedImg)
	if err != nil {
		return "", err
	}
	
	p.logger.Infow("图片处理完成", 
		"path", path, 
		"base64Length", len(base64Str),
	)
	
	return base64Str, nil
}

// GetImageInfo 获取图片信息（不处理）
func (p *ImageProcessor) GetImageInfo(path string) (format string, width, height int, err error) {
	img, format, err := p.DecodeImage(path)
	if err != nil {
		return "", 0, 0, err
	}
	
	bounds := img.Bounds()
	return format, bounds.Dx(), bounds.Dy(), nil
}
