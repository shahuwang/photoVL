package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/Kagami/go-face"
	"go.uber.org/zap"
)

/**
封装一个ImageEmbeddding的结构体，结构体初始化传入图片数据，模型调用接口url，模型名称及版本，目前本地使用的是qwen-vl-embedding-8B的模型
1、提供一个生成图片向量的方法，可设置向量维度，默认4096维
2、提供一个方法，返回照片中人脸的坐标和特征向量，使用Go-face库进行人脸检测和特征提取
3、提供一个方法，传入人脸的坐标和特征向量，裁剪出人脸来，返回这部分人脸图片数据
4、需要支持jpg、png、bmp、webp、heic、heic、gif等常见图片格式
**/

const (
	// DefaultVectorDimension 默认向量维度
	DefaultVectorDimension = 4096
	// DefaultModelURL 默认模型服务地址
	DefaultModelURL = "http://localhost:5000"
	// DefaultModelName 默认模型名称
	DefaultModelName = "qwen3-vl-embedding-8b"
	// DefaultModelVersion 默认模型版本
	DefaultModelVersion = "1.0"
	// DefaultHTTPTimeout HTTP请求超时时间
	DefaultHTTPTimeout = 60 * time.Second
	// DefaultFaceModelDir 默认人脸模型目录
	DefaultFaceModelDir = "./models/face"
)

// FaceInfo 人脸信息
type FaceInfo struct {
	Box        [4]float32 // [x1, y1, x2, y2] 坐标（像素坐标）
	Descriptor []float32  // 人脸特征向量（通过向量服务生成）
	Confidence float32    // 检测置信度（go-face不提供，固定为1.0）
}

// ImageEmbedding 图片嵌入结构体
type ImageEmbedding struct {
	// 图片数据
	imageData image.Image
	imagePath string

	// 模型配置
	modelURL     string // 向量服务地址
	modelName    string // 模型名称
	modelVersion string // 模型版本

	// 人脸检测配置
	faceModelDir   string
	faceRecognizer *face.Recognizer

	// HTTP客户端
	httpClient *http.Client

	logger *zap.SugaredLogger
}

// EmbeddingResponse 向量服务响应结构
type EmbeddingResponse struct {
	Model         string      `json:"model"`
	Embeddings    [][]float64 `json:"embeddings"` // 注意：服务返回的是float64
	TotalDuration int64       `json:"total_duration"`
}

// NewImageEmbedding 从文件路径创建ImageEmbedding
// imagePath: 图片文件路径
// modelURL: 向量服务地址，如 http://localhost:5000，为空则使用默认值
// modelName: 模型名称，为空则使用默认值
// modelVersion: 模型版本，为空则使用默认值
func NewImageEmbedding(imagePath, modelURL, modelName, modelVersion string) (*ImageEmbedding, error) {
	// 验证文件存在
	if _, err := os.Stat(imagePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("图片文件不存在: %s", imagePath)
	}

	// 使用默认值
	if modelURL == "" {
		modelURL = DefaultModelURL
	}
	if modelName == "" {
		modelName = DefaultModelName
	}
	if modelVersion == "" {
		modelVersion = DefaultModelVersion
	}

	return &ImageEmbedding{
		imagePath:    imagePath,
		modelURL:     modelURL,
		modelName:    modelName,
		modelVersion: modelVersion,
		faceModelDir: DefaultFaceModelDir,
		httpClient: &http.Client{
			Timeout: DefaultHTTPTimeout,
		},
		logger: Logger,
	}, nil
}

// NewImageEmbeddingFromImage 从image.Image创建ImageEmbedding
// img: 图片对象
// modelURL: 向量服务地址，如 http://localhost:5000，为空则使用默认值
// modelName: 模型名称，为空则使用默认值
// modelVersion: 模型版本，为空则使用默认值
func NewImageEmbeddingFromImage(img image.Image, modelURL, modelName, modelVersion string) *ImageEmbedding {
	// 使用默认值
	if modelURL == "" {
		modelURL = DefaultModelURL
	}
	if modelName == "" {
		modelName = DefaultModelName
	}
	if modelVersion == "" {
		modelVersion = DefaultModelVersion
	}

	return &ImageEmbedding{
		imageData:    img,
		modelURL:     modelURL,
		modelName:    modelName,
		modelVersion: modelVersion,
		faceModelDir: DefaultFaceModelDir,
		httpClient: &http.Client{
			Timeout: DefaultHTTPTimeout,
		},
		logger: Logger,
	}
}

// SetLogger 设置日志器
func (ie *ImageEmbedding) SetLogger(logger *zap.SugaredLogger) {
	ie.logger = logger
}

// SetFaceModelDir 设置人脸模型目录
func (ie *ImageEmbedding) SetFaceModelDir(dir string) {
	ie.faceModelDir = dir
}

// SetHTTPTimeout 设置HTTP超时时间
func (ie *ImageEmbedding) SetHTTPTimeout(timeout time.Duration) {
	ie.httpClient.Timeout = timeout
}

// getImage 获取图片对象（懒加载解码）
func (ie *ImageEmbedding) getImage() (image.Image, error) {
	if ie.imageData != nil {
		return ie.imageData, nil
	}

	if ie.imagePath == "" {
		return nil, fmt.Errorf("没有可用的图片数据")
	}

	// 使用ImageProcessor解码图片
	processor := NewImageProcessor(2048, 85) // 人脸检测需要较高分辨率
	processor.SetLogger(ie.logger)

	img, _, err := processor.DecodeImage(ie.imagePath)
	if err != nil {
		return nil, fmt.Errorf("解码图片失败: %w", err)
	}

	ie.imageData = img
	return img, nil
}

// encodeImageToBase64 将图片编码为base64 JPEG
func (ie *ImageEmbedding) encodeImageToBase64(img image.Image) (string, error) {
	var buf bytes.Buffer

	// 使用JPEG编码，质量85
	options := &jpeg.Options{Quality: 85}
	if err := jpeg.Encode(&buf, img, options); err != nil {
		return "", fmt.Errorf("编码JPEG失败: %w", err)
	}

	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

// GenerateImageVector 生成图片向量
// dimension: 向量维度，默认4096（当前模型固定4096维，此参数用于未来扩展）
// 返回4096维float32向量
func (ie *ImageEmbedding) GenerateImageVector(dimension int) ([]float32, error) {
	if dimension <= 0 {
		dimension = DefaultVectorDimension
	}

	// 获取图片
	img, err := ie.getImage()
	if err != nil {
		return nil, err
	}

	// 编码为base64
	base64Image, err := ie.encodeImageToBase64(img)
	if err != nil {
		return nil, err
	}

	// 构建请求
	requestBody := map[string]interface{}{
		"model": ie.modelName,
		"input": base64Image,
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("序列化请求失败: %w", err)
	}

	// 发送请求
	url := ie.modelURL + "/api/embed"
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	if ie.logger != nil {
		ie.logger.Debugw("发送图片向量化请求", "url", url, "model", ie.modelName)
	}

	resp, err := ie.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求向量服务失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("向量服务返回错误状态码 %d: %s", resp.StatusCode, string(body))
	}

	// 解析响应
	var embedResp EmbeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&embedResp); err != nil {
		return nil, fmt.Errorf("解析响应失败: %w", err)
	}

	if len(embedResp.Embeddings) == 0 {
		return nil, fmt.Errorf("向量服务返回空向量")
	}

	// 转换为float32
	vector64 := embedResp.Embeddings[0]
	if len(vector64) != dimension {
		if ie.logger != nil {
			ie.logger.Warnw("向量维度不匹配", "expected", dimension, "actual", len(vector64))
		}
	}

	vector := make([]float32, len(vector64))
	for i, v := range vector64 {
		vector[i] = float32(v)
	}

	if ie.logger != nil {
		ie.logger.Debugw("图片向量化完成", "dimension", len(vector))
	}

	return vector, nil
}

// initFaceRecognizer 初始化人脸识别器（懒加载）
func (ie *ImageEmbedding) initFaceRecognizer() error {
	if ie.faceRecognizer != nil {
		return nil
	}

	// 检查模型目录是否存在
	if _, err := os.Stat(ie.faceModelDir); os.IsNotExist(err) {
		return fmt.Errorf("人脸模型目录不存在: %s，请运行 install_go_face.sh 安装模型", ie.faceModelDir)
	}

	// 初始化识别器
	rec, err := face.NewRecognizer(ie.faceModelDir)
	if err != nil {
		return fmt.Errorf("初始化人脸识别器失败: %w", err)
	}

	ie.faceRecognizer = rec
	return nil
}

// DetectFaces 检测人脸，返回人脸坐标和特征向量
// 使用go-face库进行人脸检测和特征提取
// 返回的人脸坐标为像素坐标 [x1, y1, x2, y2]
// 特征向量通过向量服务对裁剪后的人脸生成
func (ie *ImageEmbedding) DetectFaces() ([]FaceInfo, error) {
	// 初始化识别器
	if err := ie.initFaceRecognizer(); err != nil {
		return nil, err
	}

	// 读取图片文件为JPEG字节
	imgData, err := os.ReadFile(ie.imagePath)
	if err != nil {
		return nil, fmt.Errorf("读取图片文件失败: %w", err)
	}

	// 识别人脸（go-face只支持JPEG格式）
	faces, err := ie.faceRecognizer.Recognize(imgData)
	if err != nil {
		return nil, fmt.Errorf("人脸检测失败: %w", err)
	}

	if ie.logger != nil {
		ie.logger.Debugw("人脸检测完成", "count", len(faces))
	}

	// 转换为FaceInfo
	result := make([]FaceInfo, 0, len(faces))
	for _, f := range faces {
		faceInfo := FaceInfo{
			Box: [4]float32{
				float32(f.Rectangle.Min.X),
				float32(f.Rectangle.Min.Y),
				float32(f.Rectangle.Max.X),
				float32(f.Rectangle.Max.Y),
			},
			Confidence: 1.0, // go-face不提供置信度，固定为1.0
		}

		// 裁剪人脸并生成向量
		faceImg, err := ie.CropFace(faceInfo.Box)
		if err != nil {
			if ie.logger != nil {
				ie.logger.Warnw("裁剪人脸失败", "error", err)
			}
			continue
		}

		// 为裁剪后的人脸生成向量
		tempIE := NewImageEmbeddingFromImage(faceImg, ie.modelURL, ie.modelName, ie.modelVersion)
		tempIE.SetLogger(ie.logger)

		vector, err := tempIE.GenerateImageVector(DefaultVectorDimension)
		if err != nil {
			if ie.logger != nil {
				ie.logger.Warnw("生成人脸向量失败", "error", err)
			}
			// 即使向量生成失败，也保留人脸坐标信息
		} else {
			faceInfo.Descriptor = vector
		}

		result = append(result, faceInfo)
	}

	return result, nil
}

// DetectFacesOnly 仅检测人脸坐标（不生成向量）
// 如果需要批量处理，可以先调用此方法，然后使用CropFaces裁剪人脸
func (ie *ImageEmbedding) DetectFacesOnly() ([]FaceInfo, error) {
	// 初始化识别器
	if err := ie.initFaceRecognizer(); err != nil {
		return nil, err
	}

	// 读取图片文件为JPEG字节
	imgData, err := os.ReadFile(ie.imagePath)
	if err != nil {
		return nil, fmt.Errorf("读取图片文件失败: %w", err)
	}

	// 识别人脸（go-face只支持JPEG格式）
	faces, err := ie.faceRecognizer.Recognize(imgData)
	if err != nil {
		return nil, fmt.Errorf("人脸检测失败: %w", err)
	}

	if ie.logger != nil {
		ie.logger.Debugw("人脸检测完成", "count", len(faces))
	}

	// 转换为FaceInfo（不包含向量）
	result := make([]FaceInfo, 0, len(faces))
	for _, f := range faces {
		faceInfo := FaceInfo{
			Box: [4]float32{
				float32(f.Rectangle.Min.X),
				float32(f.Rectangle.Min.Y),
				float32(f.Rectangle.Max.X),
				float32(f.Rectangle.Max.Y),
			},
			Confidence: 1.0,
		}
		result = append(result, faceInfo)
	}

	return result, nil
}

// CropFace 裁剪人脸区域
// box: [x1, y1, x2, y2] 像素坐标
// 返回裁剪后的人脸图片
func (ie *ImageEmbedding) CropFace(box [4]float32) (image.Image, error) {
	// 获取原图
	img, err := ie.getImage()
	if err != nil {
		return nil, err
	}

	bounds := img.Bounds()

	// 转换为整数坐标，并确保在图片范围内
	x1 := int(box[0])
	y1 := int(box[1])
	x2 := int(box[2])
	y2 := int(box[3])

	// 边界检查
	if x1 < bounds.Min.X {
		x1 = bounds.Min.X
	}
	if y1 < bounds.Min.Y {
		y1 = bounds.Min.Y
	}
	if x2 > bounds.Max.X {
		x2 = bounds.Max.X
	}
	if y2 > bounds.Max.Y {
		y2 = bounds.Max.Y
	}

	if x1 >= x2 || y1 >= y2 {
		return nil, fmt.Errorf("无效的裁剪区域: x1=%d, y1=%d, x2=%d, y2=%d", x1, y1, x2, y2)
	}

	// 创建裁剪后的图片
	width := x2 - x1
	height := y2 - y1
	cropped := image.NewRGBA(image.Rect(0, 0, width, height))

	// 复制像素
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			srcX := x1 + x
			srcY := y1 + y
			cropped.Set(x, y, img.At(srcX, srcY))
		}
	}

	if ie.logger != nil {
		ie.logger.Debugw("人脸裁剪完成", "x1", x1, "y1", y1, "x2", x2, "y2", y2, "width", width, "height", height)
	}

	return cropped, nil
}

// CropFaces 裁剪所有检测到的人脸
// 返回所有人脸图片和对应的FaceInfo（不包含向量）
func (ie *ImageEmbedding) CropFaces() ([]image.Image, []FaceInfo, error) {
	faces, err := ie.DetectFacesOnly()
	if err != nil {
		return nil, nil, err
	}

	images := make([]image.Image, 0, len(faces))
	for _, face := range faces {
		img, err := ie.CropFace(face.Box)
		if err != nil {
			if ie.logger != nil {
				ie.logger.Warnw("裁剪人脸失败", "error", err)
			}
			continue
		}
		images = append(images, img)
	}

	return images, faces, nil
}

// GenerateFaceVectors 生成所有人脸的向量
// 先检测人脸，然后裁剪，最后对每个裁剪后的人脸生成向量
// 返回人脸向量列表和对应的人脸信息
func (ie *ImageEmbedding) GenerateFaceVectors() ([][]float32, []FaceInfo, error) {
	// 检测人脸并生成向量
	faces, err := ie.DetectFaces()
	if err != nil {
		return nil, nil, err
	}

	if len(faces) == 0 {
		return [][]float32{}, []FaceInfo{}, nil
	}

	// 提取向量
	vectors := make([][]float32, 0, len(faces))
	for _, face := range faces {
		if face.Descriptor != nil {
			vectors = append(vectors, face.Descriptor)
		}
	}

	return vectors, faces, nil
}

// Close 释放资源
// 关闭人脸识别器
func (ie *ImageEmbedding) Close() error {
	if ie.faceRecognizer != nil {
		ie.faceRecognizer.Close()
		ie.faceRecognizer = nil
	}
	return nil
}

// CheckModelHealth 检查向量服务健康状态
func (ie *ImageEmbedding) CheckModelHealth() error {
	url := ie.modelURL + "/api/health"
	resp, err := ie.httpClient.Get(url)
	if err != nil {
		return fmt.Errorf("无法连接到向量服务: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("向量服务返回异常状态码: %d", resp.StatusCode)
	}

	return nil
}

// GetModelInfo 获取模型信息
func (ie *ImageEmbedding) GetModelInfo() map[string]string {
	return map[string]string{
		"url":     ie.modelURL,
		"name":    ie.modelName,
		"version": ie.modelVersion,
	}
}

// GetImageInfo 获取图片信息
func (ie *ImageEmbedding) GetImageInfo() (width, height int, format string, err error) {
	img, err := ie.getImage()
	if err != nil {
		return 0, 0, "", err
	}

	bounds := img.Bounds()
	return bounds.Dx(), bounds.Dy(), "unknown", nil
}

// SaveImageVectorToMetadata 保存图片整体向量到 image_metadata 表
// md5: 图片的MD5值
// vector: 图片整体向量
// db: LanceDBManager 实例
// 如果该MD5已存在，则更新其向量字段；如果不存在，则创建新记录
func SaveImageVectorToMetadata(md5 string, vector []float32, db *LanceDBManager) error {
	if db == nil {
		return fmt.Errorf("数据库管理器不能为空")
	}

	if md5 == "" {
		return fmt.Errorf("MD5不能为空")
	}

	if len(vector) == 0 {
		return fmt.Errorf("向量不能为空")
	}

	// 检查是否已存在该MD5的记录
	existing, err := db.GetImageMetadataByMD5(md5)
	if err != nil {
		return fmt.Errorf("查询现有记录失败: %w", err)
	}

	if existing != nil {
		// 已存在，更新向量字段
		existing.ImageVector = vector
		existing.UpdateTime = time.Now()
		return db.InsertImageMetadata(existing)
	}

	// 不存在，创建新记录（只填充必要字段）
	newMeta := &ImageMetadata{
		MD5:         md5,
		ImageVector: vector,
		CreateTime:  time.Now(),
		UpdateTime:  time.Now(),
	}

	return db.InsertImageMetadata(newMeta)
}

// SaveFaceVectorsToDB 保存图片的人脸坐标及人脸向量到 face_vectors 表
// md5: 整体图片的MD5值
// faces: 人脸信息列表（包含坐标和向量）
// db: LanceDBManager 实例
// 返回保存的人脸数量
func SaveFaceVectorsToDB(md5 string, faces []FaceInfo, db *LanceDBManager) (int, error) {
	if db == nil {
		return 0, fmt.Errorf("数据库管理器不能为空")
	}

	if md5 == "" {
		return 0, fmt.Errorf("MD5不能为空")
	}

	if len(faces) == 0 {
		return 0, nil
	}

	// 转换为 FaceVector 结构
	faceVectors := make([]FaceVector, 0, len(faces))
	for _, face := range faces {
		// 跳过没有向量的记录
		if len(face.Descriptor) == 0 {
			continue
		}

		fv := FaceVector{
			MD5:        md5,
			FaceVector: face.Descriptor,
			Box:        face.Box[:],
		}
		faceVectors = append(faceVectors, fv)
	}

	if len(faceVectors) == 0 {
		return 0, nil
	}

	// 批量插入
	if err := db.InsertFaceVectors(faceVectors); err != nil {
		return 0, fmt.Errorf("插入人脸向量失败: %w", err)
	}

	return len(faceVectors), nil
}
