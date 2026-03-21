package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/google/uuid"
	"github.com/lancedb/lancedb-go/pkg/contracts"
	lancedb "github.com/lancedb/lancedb-go/pkg/lancedb"
)

const (
	TableImageMetadata = "image_metadata"
	TableFaceVectors   = "face_vectors"
	TableFileIndex     = "file_index"
	VectorDimension    = 1024
)

// LanceDBManager LanceDB 管理器
type LanceDBManager struct {
	db     contracts.IConnection
	ctx    context.Context
	dbPath string
}

// ImageMetadata 图片元数据结构
type ImageMetadata struct {
	MD5         string
	Theme       []string
	Description string
	Objects     []string
	Coordinates []float32 // [lng, lat]
	Datetime    time.Time
	Address     []string
	Dimensions  []float32 // [xsize, ysize]
	Ext         string
	Size        int32
	Place       string
	Colors      string
	Mood        []string
	Action      []string
	ImageVector []float32 // 1024-dim vector
}

// FaceVector 人脸向量结构
type FaceVector struct {
	FaceID     string
	MD5        string
	FaceVector []float32 // 1024-dim vector
	Box        []float32 // [x1, y1, x2, y2]
}

// FileIndex 文件索引结构
type FileIndex struct {
	MD5      string
	FilePath string
}

// NewLanceDBManager 创建 LanceDB 管理器
func NewLanceDBManager(dbPath string) (*LanceDBManager, error) {
	ctx := context.Background()

	// 确保数据库目录存在
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create db directory: %w", err)
	}

	// 连接 LanceDB
	db, err := lancedb.Connect(ctx, dbPath, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to lancedb: %w", err)
	}

	manager := &LanceDBManager{
		db:     db,
		ctx:    ctx,
		dbPath: dbPath,
	}

	// 初始化表
	if err := manager.initTables(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to init tables: %w", err)
	}

	return manager, nil
}

// Close 关闭数据库连接
func (m *LanceDBManager) Close() error {
	return m.db.Close()
}

// initTables 初始化所有表
func (m *LanceDBManager) initTables() error {
	// 获取已存在的表
	tables, err := m.db.TableNames(m.ctx)
	if err != nil {
		return fmt.Errorf("failed to get table names: %w", err)
	}

	tableSet := make(map[string]bool)
	for _, t := range tables {
		tableSet[t] = true
	}

	// 创建 image_metadata 表
	if !tableSet[TableImageMetadata] {
		if err := m.createImageMetadataTable(); err != nil {
			return fmt.Errorf("failed to create image_metadata table: %w", err)
		}
	}

	// 创建 face_vectors 表
	if !tableSet[TableFaceVectors] {
		if err := m.createFaceVectorsTable(); err != nil {
			return fmt.Errorf("failed to create face_vectors table: %w", err)
		}
	}

	// 创建 file_index 表
	if !tableSet[TableFileIndex] {
		if err := m.createFileIndexTable(); err != nil {
			return fmt.Errorf("failed to create file_index table: %w", err)
		}
	}

	return nil
}

// createImageMetadataTable 创建图片元数据表
func (m *LanceDBManager) createImageMetadataTable() error {
	// 使用 SchemaBuilder 创建 schema
	schema, err := lancedb.NewSchemaBuilder().
		AddStringField("md5", false).
		AddStringField("theme", true).
		AddStringField("description", true).
		AddStringField("objects", true).
		AddStringField("coordinates", true).
		AddTimestampField("datetime", arrow.Microsecond, true).
		AddStringField("address", true).
		AddStringField("dimensions", true).
		AddStringField("ext", true).
		AddInt32Field("size", true).
		AddStringField("place", true).
		AddStringField("colors", true).
		AddStringField("mood", true).
		AddStringField("action", true).
		AddVectorField("image_vector", VectorDimension, contracts.VectorDataTypeFloat32, true).
		Build()
	if err != nil {
		return fmt.Errorf("failed to build schema: %w", err)
	}

	_, err = m.db.CreateTable(m.ctx, TableImageMetadata, schema)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

// createFaceVectorsTable 创建人脸向量表
func (m *LanceDBManager) createFaceVectorsTable() error {
	schema, err := lancedb.NewSchemaBuilder().
		AddStringField("face_id", false).
		AddStringField("md5", false).
		AddVectorField("face_vector", VectorDimension, contracts.VectorDataTypeFloat32, true).
		AddStringField("box", true).
		Build()
	if err != nil {
		return fmt.Errorf("failed to build schema: %w", err)
	}

	_, err = m.db.CreateTable(m.ctx, TableFaceVectors, schema)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

// createFileIndexTable 创建文件索引表
func (m *LanceDBManager) createFileIndexTable() error {
	schema, err := lancedb.NewSchemaBuilder().
		AddStringField("md5", false).
		AddStringField("file_path", false).
		Build()
	if err != nil {
		return fmt.Errorf("failed to build schema: %w", err)
	}

	_, err = m.db.CreateTable(m.ctx, TableFileIndex, schema)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

// InsertImageMetadata 插入图片元数据
func (m *LanceDBManager) InsertImageMetadata(data *ImageMetadata) error {
	tbl, err := m.db.OpenTable(m.ctx, TableImageMetadata)
	if err != nil {
		return fmt.Errorf("failed to open table %s: %w", TableImageMetadata, err)
	}
	defer tbl.Close()

	// 验证向量维度
	if len(data.ImageVector) != VectorDimension {
		return fmt.Errorf("image_vector dimension must be %d, got %d", VectorDimension, len(data.ImageVector))
	}

	// 创建 Arrow Record
	record, err := m.buildImageMetadataRecord([]*ImageMetadata{data})
	if err != nil {
		return fmt.Errorf("failed to build record: %w", err)
	}
	defer record.Release()

	if err := tbl.Add(m.ctx, record, nil); err != nil {
		return fmt.Errorf("failed to insert image metadata: %w", err)
	}

	return nil
}

// InsertFaceVectors 插入人脸向量（支持批量插入）
func (m *LanceDBManager) InsertFaceVectors(data []FaceVector) error {
	if len(data) == 0 {
		return nil
	}

	tbl, err := m.db.OpenTable(m.ctx, TableFaceVectors)
	if err != nil {
		return fmt.Errorf("failed to open table %s: %w", TableFaceVectors, err)
	}
	defer tbl.Close()

	// 验证所有人脸向量维度并生成 UUID
	for i := range data {
		if len(data[i].FaceVector) != VectorDimension {
			return fmt.Errorf("face_vector at index %d dimension must be %d, got %d", i, VectorDimension, len(data[i].FaceVector))
		}
		if data[i].FaceID == "" {
			data[i].FaceID = uuid.New().String()
		}
	}

	// 创建 Arrow Record
	record, err := m.buildFaceVectorRecord(data)
	if err != nil {
		return fmt.Errorf("failed to build record: %w", err)
	}
	defer record.Release()

	if err := tbl.Add(m.ctx, record, nil); err != nil {
		return fmt.Errorf("failed to insert face vectors: %w", err)
	}

	return nil
}

// InsertFileIndex 插入文件索引
func (m *LanceDBManager) InsertFileIndex(data *FileIndex) error {
	return m.InsertFileIndexBatch([]FileIndex{*data})
}

// InsertFileIndexBatch 批量插入文件索引
func (m *LanceDBManager) InsertFileIndexBatch(data []FileIndex) error {
	if len(data) == 0 {
		return nil
	}

	tbl, err := m.db.OpenTable(m.ctx, TableFileIndex)
	if err != nil {
		return fmt.Errorf("failed to open table %s: %w", TableFileIndex, err)
	}
	defer tbl.Close()

	// 创建 Arrow Record
	record, err := m.buildFileIndexRecord(data)
	if err != nil {
		return fmt.Errorf("failed to build record: %w", err)
	}
	defer record.Release()

	if err := tbl.Add(m.ctx, record, nil); err != nil {
		return fmt.Errorf("failed to insert file index batch: %w", err)
	}

	return nil
}

// buildImageMetadataRecord 构建图片元数据的 Arrow Record
func (m *LanceDBManager) buildImageMetadataRecord(data []*ImageMetadata) (arrow.Record, error) {
	pool := memory.NewGoAllocator()

	// 创建 builders
	md5Builder := array.NewStringBuilder(pool)
	defer md5Builder.Release()

	themeBuilder := array.NewStringBuilder(pool)
	defer themeBuilder.Release()

	descBuilder := array.NewStringBuilder(pool)
	defer descBuilder.Release()

	objectsBuilder := array.NewStringBuilder(pool)
	defer objectsBuilder.Release()

	coordsBuilder := array.NewStringBuilder(pool)
	defer coordsBuilder.Release()

	datetimeBuilder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Microsecond})
	defer datetimeBuilder.Release()

	addressBuilder := array.NewStringBuilder(pool)
	defer addressBuilder.Release()

	dimensionsBuilder := array.NewStringBuilder(pool)
	defer dimensionsBuilder.Release()

	extBuilder := array.NewStringBuilder(pool)
	defer extBuilder.Release()

	sizeBuilder := array.NewInt32Builder(pool)
	defer sizeBuilder.Release()

	placeBuilder := array.NewStringBuilder(pool)
	defer placeBuilder.Release()

	colorsBuilder := array.NewStringBuilder(pool)
	defer colorsBuilder.Release()

	moodBuilder := array.NewStringBuilder(pool)
	defer moodBuilder.Release()

	actionBuilder := array.NewStringBuilder(pool)
	defer actionBuilder.Release()

	// 向量使用 FixedSizeList 存储
	vectorBuilder := array.NewFixedSizeListBuilder(pool, VectorDimension, arrow.PrimitiveTypes.Float32)
	defer vectorBuilder.Release()
	vectorValueBuilder := vectorBuilder.ValueBuilder().(*array.Float32Builder)

	// 填充数据
	for _, item := range data {
		md5Builder.Append(item.MD5)
		themeBuilder.Append(marshalStringSlice(item.Theme))
		descBuilder.Append(item.Description)
		objectsBuilder.Append(marshalStringSlice(item.Objects))
		coordsBuilder.Append(marshalFloat32SliceToJSON(item.Coordinates))
		datetimeBuilder.Append(arrow.Timestamp(item.Datetime.UnixMicro()))
		addressBuilder.Append(marshalStringSlice(item.Address))
		dimensionsBuilder.Append(marshalFloat32SliceToJSON(item.Dimensions))
		extBuilder.Append(item.Ext)
		sizeBuilder.Append(item.Size)
		placeBuilder.Append(item.Place)
		colorsBuilder.Append(item.Colors)
		moodBuilder.Append(marshalStringSlice(item.Mood))
		actionBuilder.Append(marshalStringSlice(item.Action))

		// 添加向量
		vectorBuilder.Append(true)
		for _, v := range item.ImageVector {
			vectorValueBuilder.Append(v)
		}
	}

	// 创建数组
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "md5", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "theme", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "description", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "objects", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "coordinates", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "datetime", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
		{Name: "address", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "dimensions", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "ext", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "size", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "place", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "colors", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "mood", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "action", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "image_vector", Type: arrow.FixedSizeListOf(VectorDimension, arrow.PrimitiveTypes.Float32), Nullable: true},
	}, nil)

	columns := []arrow.Array{
		md5Builder.NewArray(),
		themeBuilder.NewArray(),
		descBuilder.NewArray(),
		objectsBuilder.NewArray(),
		coordsBuilder.NewArray(),
		datetimeBuilder.NewArray(),
		addressBuilder.NewArray(),
		dimensionsBuilder.NewArray(),
		extBuilder.NewArray(),
		sizeBuilder.NewArray(),
		placeBuilder.NewArray(),
		colorsBuilder.NewArray(),
		moodBuilder.NewArray(),
		actionBuilder.NewArray(),
		vectorBuilder.NewArray(),
	}

	return array.NewRecord(schema, columns, int64(len(data))), nil
}

// buildFaceVectorRecord 构建人脸向量的 Arrow Record
func (m *LanceDBManager) buildFaceVectorRecord(data []FaceVector) (arrow.Record, error) {
	pool := memory.NewGoAllocator()

	faceIDBuilder := array.NewStringBuilder(pool)
	defer faceIDBuilder.Release()

	md5Builder := array.NewStringBuilder(pool)
	defer md5Builder.Release()

	// 向量使用 FixedSizeList 存储
	vectorBuilder := array.NewFixedSizeListBuilder(pool, VectorDimension, arrow.PrimitiveTypes.Float32)
	defer vectorBuilder.Release()
	vectorValueBuilder := vectorBuilder.ValueBuilder().(*array.Float32Builder)

	boxBuilder := array.NewStringBuilder(pool)
	defer boxBuilder.Release()

	for _, item := range data {
		faceIDBuilder.Append(item.FaceID)
		md5Builder.Append(item.MD5)

		// 添加向量
		vectorBuilder.Append(true)
		for _, v := range item.FaceVector {
			vectorValueBuilder.Append(v)
		}

		boxBuilder.Append(marshalFloat32SliceToJSON(item.Box))
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "face_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "md5", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "face_vector", Type: arrow.FixedSizeListOf(VectorDimension, arrow.PrimitiveTypes.Float32), Nullable: true},
		{Name: "box", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	columns := []arrow.Array{
		faceIDBuilder.NewArray(),
		md5Builder.NewArray(),
		vectorBuilder.NewArray(),
		boxBuilder.NewArray(),
	}

	return array.NewRecord(schema, columns, int64(len(data))), nil
}

// buildFileIndexRecord 构建文件索引的 Arrow Record
func (m *LanceDBManager) buildFileIndexRecord(data []FileIndex) (arrow.Record, error) {
	pool := memory.NewGoAllocator()

	md5Builder := array.NewStringBuilder(pool)
	defer md5Builder.Release()

	pathBuilder := array.NewStringBuilder(pool)
	defer pathBuilder.Release()

	for _, item := range data {
		md5Builder.Append(item.MD5)
		pathBuilder.Append(item.FilePath)
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "md5", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "file_path", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	columns := []arrow.Array{
		md5Builder.NewArray(),
		pathBuilder.NewArray(),
	}

	return array.NewRecord(schema, columns, int64(len(data))), nil
}

// marshalStringSlice 将字符串切片序列化为 JSON 字符串
func marshalStringSlice(s []string) string {
	if len(s) == 0 {
		return "[]"
	}
	bytes, _ := json.Marshal(s)
	return string(bytes)
}

// unmarshalStringSlice 将 JSON 字符串反序列化为字符串切片
func unmarshalStringSlice(s string) []string {
	if s == "" || s == "[]" {
		return []string{}
	}
	var result []string
	json.Unmarshal([]byte(s), &result)
	return result
}

// marshalFloat32SliceToJSON 将 float32 切片序列化为 JSON 字符串
func marshalFloat32SliceToJSON(f []float32) string {
	if len(f) == 0 {
		return "[]"
	}
	bytes, _ := json.Marshal(f)
	return string(bytes)
}

// marshalFloat32SliceToBytes 将 float32 切片序列化为二进制
func marshalFloat32SliceToBytes(f []float32) []byte {
	// 每个 float32 占 4 字节
	result := make([]byte, len(f)*4)
	for i, v := range f {
		// 简单的小端序编码
		bits := *(*uint32)(unsafe.Pointer(&v))
		result[i*4] = byte(bits)
		result[i*4+1] = byte(bits >> 8)
		result[i*4+2] = byte(bits >> 16)
		result[i*4+3] = byte(bits >> 24)
	}
	return result
}

// GetImageMetadataByMD5 根据 MD5 获取图片元数据
func (m *LanceDBManager) GetImageMetadataByMD5(md5 string) (*ImageMetadata, error) {
	tbl, err := m.db.OpenTable(m.ctx, TableImageMetadata)
	if err != nil {
		return nil, fmt.Errorf("failed to open table %s: %w", TableImageMetadata, err)
	}
	defer tbl.Close()

	results, err := tbl.SelectWithFilter(m.ctx, fmt.Sprintf("md5 = '%s'", md5))
	if err != nil {
		return nil, fmt.Errorf("failed to query image metadata: %w", err)
	}

	if len(results) == 0 {
		return nil, nil
	}

	return m.parseImageMetadata(results[0])
}

// GetFaceVectorsByMD5 根据 MD5 获取人脸向量列表
func (m *LanceDBManager) GetFaceVectorsByMD5(md5 string) ([]FaceVector, error) {
	tbl, err := m.db.OpenTable(m.ctx, TableFaceVectors)
	if err != nil {
		return nil, fmt.Errorf("failed to open table %s: %w", TableFaceVectors, err)
	}
	defer tbl.Close()

	results, err := tbl.SelectWithFilter(m.ctx, fmt.Sprintf("md5 = '%s'", md5))
	if err != nil {
		return nil, fmt.Errorf("failed to query face vectors: %w", err)
	}

	faceVectors := make([]FaceVector, 0, len(results))
	for _, r := range results {
		fv, err := m.parseFaceVector(r)
		if err != nil {
			continue
		}
		faceVectors = append(faceVectors, *fv)
	}

	return faceVectors, nil
}

// GetFilePathsByMD5 根据 MD5 获取文件路径列表
func (m *LanceDBManager) GetFilePathsByMD5(md5 string) ([]string, error) {
	tbl, err := m.db.OpenTable(m.ctx, TableFileIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to open table %s: %w", TableFileIndex, err)
	}
	defer tbl.Close()

	results, err := tbl.SelectWithFilter(m.ctx, fmt.Sprintf("md5 = '%s'", md5))
	if err != nil {
		return nil, fmt.Errorf("failed to query file index: %w", err)
	}

	paths := make([]string, 0, len(results))
	for _, r := range results {
		if path, ok := r["file_path"].(string); ok {
			paths = append(paths, path)
		}
	}

	return paths, nil
}

// CheckMD5Exists 检查 MD5 是否已存在
func (m *LanceDBManager) CheckMD5Exists(md5 string) (bool, error) {
	tbl, err := m.db.OpenTable(m.ctx, TableImageMetadata)
	if err != nil {
		return false, fmt.Errorf("failed to open table %s: %w", TableImageMetadata, err)
	}
	defer tbl.Close()

	results, err := tbl.SelectWithFilter(m.ctx, fmt.Sprintf("md5 = '%s'", md5))
	if err != nil {
		return false, fmt.Errorf("failed to query md5: %w", err)
	}

	return len(results) > 0, nil
}

// DeleteByMD5 根据 MD5 删除所有相关数据
func (m *LanceDBManager) DeleteByMD5(md5 string) error {
	// 删除 image_metadata
	tbl, err := m.db.OpenTable(m.ctx, TableImageMetadata)
	if err != nil {
		return fmt.Errorf("failed to open table %s: %w", TableImageMetadata, err)
	}
	if err := tbl.Delete(m.ctx, fmt.Sprintf("md5 = '%s'", md5)); err != nil {
		tbl.Close()
		return fmt.Errorf("failed to delete from image_metadata: %w", err)
	}
	tbl.Close()

	// 删除 face_vectors
	tbl, err = m.db.OpenTable(m.ctx, TableFaceVectors)
	if err != nil {
		return fmt.Errorf("failed to open table %s: %w", TableFaceVectors, err)
	}
	if err := tbl.Delete(m.ctx, fmt.Sprintf("md5 = '%s'", md5)); err != nil {
		tbl.Close()
		return fmt.Errorf("failed to delete from face_vectors: %w", err)
	}
	tbl.Close()

	// 删除 file_index
	tbl, err = m.db.OpenTable(m.ctx, TableFileIndex)
	if err != nil {
		return fmt.Errorf("failed to open table %s: %w", TableFileIndex, err)
	}
	if err := tbl.Delete(m.ctx, fmt.Sprintf("md5 = '%s'", md5)); err != nil {
		tbl.Close()
		return fmt.Errorf("failed to delete from file_index: %w", err)
	}
	tbl.Close()

	return nil
}

// parseImageMetadata 解析图片元数据
func (m *LanceDBManager) parseImageMetadata(data map[string]interface{}) (*ImageMetadata, error) {
	result := &ImageMetadata{}

	if v, ok := data["md5"].(string); ok {
		result.MD5 = v
	}
	if v, ok := data["description"].(string); ok {
		result.Description = v
	}
	if v, ok := data["address"].(string); ok {
		result.Address = unmarshalStringSlice(v)
	}
	if v, ok := data["ext"].(string); ok {
		result.Ext = v
	}
	if v, ok := data["place"].(string); ok {
		result.Place = v
	}
	if v, ok := data["size"].(int32); ok {
		result.Size = v
	}

	// 解析时间戳
	if v, ok := data["datetime"].(arrow.Timestamp); ok {
		result.Datetime = time.UnixMicro(int64(v))
	}

	return result, nil
}

// parseFaceVector 解析人脸向量
func (m *LanceDBManager) parseFaceVector(data map[string]interface{}) (*FaceVector, error) {
	result := &FaceVector{}

	if v, ok := data["face_id"].(string); ok {
		result.FaceID = v
	}
	if v, ok := data["md5"].(string); ok {
		result.MD5 = v
	}

	return result, nil
}

// GetDefaultDBPath 获取默认数据库路径
func GetDefaultDBPath() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "./photoVL_db"
	}
	return filepath.Join(homeDir, ".photoVL", "lancedb")
}
