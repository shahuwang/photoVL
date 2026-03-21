package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

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
	CreateTime  time.Time
	UpdateTime  time.Time
}

// FaceVector 人脸向量结构
type FaceVector struct {
	FaceID     string
	MD5        string
	FaceVector []float32 // 1024-dim vector
	Box        []float32 // [x1, y1, x2, y2]
	CreateTime time.Time
	UpdateTime time.Time
}

// FileIndex 文件索引结构
type FileIndex struct {
	MD5        string
	FilePath   string
	CreateTime time.Time
	UpdateTime time.Time
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
	// 注意：create_time 和 update_time 使用 Int64 存储 Unix 微秒时间戳
	// 因为 LanceDB Go SDK 不能正确转换 Timestamp 类型
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
		AddInt64Field("create_time", true).
		AddInt64Field("update_time", true).
		Build()
	if err != nil {
		return fmt.Errorf("failed to build schema: %w", err)
	}

	tbl, err := m.db.CreateTable(m.ctx, TableImageMetadata, schema)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	defer tbl.Close()

	// 为 md5 字段创建 BTree 索引
	if err := tbl.CreateIndex(m.ctx, []string{"md5"}, contracts.IndexTypeBTree); err != nil {
		return fmt.Errorf("failed to create index on md5: %w", err)
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
		AddTimestampField("create_time", arrow.Microsecond, true).
		AddTimestampField("update_time", arrow.Microsecond, true).
		Build()
	if err != nil {
		return fmt.Errorf("failed to build schema: %w", err)
	}

	tbl, err := m.db.CreateTable(m.ctx, TableFaceVectors, schema)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	defer tbl.Close()

	// 为 md5 字段创建 BTree 索引
	if err := tbl.CreateIndex(m.ctx, []string{"md5"}, contracts.IndexTypeBTree); err != nil {
		return fmt.Errorf("failed to create index on md5: %w", err)
	}

	return nil
}

// createFileIndexTable 创建文件索引表
func (m *LanceDBManager) createFileIndexTable() error {
	schema, err := lancedb.NewSchemaBuilder().
		AddStringField("md5", false).
		AddStringField("file_path", false).
		AddTimestampField("create_time", arrow.Microsecond, true).
		AddTimestampField("update_time", arrow.Microsecond, true).
		Build()
	if err != nil {
		return fmt.Errorf("failed to build schema: %w", err)
	}

	tbl, err := m.db.CreateTable(m.ctx, TableFileIndex, schema)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	defer tbl.Close()

	// 为 md5 字段创建 BTree 索引
	if err := tbl.CreateIndex(m.ctx, []string{"md5"}, contracts.IndexTypeBTree); err != nil {
		return fmt.Errorf("failed to create index on md5: %w", err)
	}

	return nil
}

// InsertImageMetadata 插入或更新图片元数据
// 如果 MD5 已存在，则合并数据：新数据有值的字段更新，新数据无值的字段保留旧值
// 如果没有字段需要更新，则跳过数据库操作
func (m *LanceDBManager) InsertImageMetadata(data *ImageMetadata) error {
	// 验证向量维度（如果提供了向量）
	if len(data.ImageVector) > 0 && len(data.ImageVector) != VectorDimension {
		return fmt.Errorf("image_vector dimension must be %d, got %d", VectorDimension, len(data.ImageVector))
	}

	// 检查是否已存在
	existing, err := m.GetImageMetadataByMD5(data.MD5)
	if err != nil {
		return fmt.Errorf("failed to check existing metadata: %w", err)
	}

	if existing != nil {
		// 已存在，合并数据
		updated := m.mergeImageMetadata(existing, data)
		if !updated {
			// 没有字段需要更新，跳过
			return nil
		}
		data = existing

		// 删除旧记录，然后插入新记录（LanceDB 的 merge_insert 替代方案）
		if err := m.deleteImageMetadataByMD5(data.MD5); err != nil {
			return fmt.Errorf("failed to delete old metadata: %w", err)
		}
	}

	tbl, err := m.db.OpenTable(m.ctx, TableImageMetadata)
	if err != nil {
		return fmt.Errorf("failed to open table %s: %w", TableImageMetadata, err)
	}
	defer tbl.Close()

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

// mergeImageMetadata 合并图片元数据
// 规则：新数据有值的字段覆盖旧数据，新数据无值的字段保留旧数据
// 返回值：是否有任何字段被更新
func (m *LanceDBManager) mergeImageMetadata(old, new *ImageMetadata) bool {
	updated := false

	// 字符串字段：非空则更新
	if new.Description != "" {
		old.Description = new.Description
		updated = true
	}
	if new.Ext != "" {
		old.Ext = new.Ext
		updated = true
	}
	if new.Place != "" {
		old.Place = new.Place
		updated = true
	}
	if new.Colors != "" {
		old.Colors = new.Colors
		updated = true
	}

	// 切片字段：非空则更新
	if len(new.Theme) > 0 {
		old.Theme = new.Theme
		updated = true
	}
	if len(new.Objects) > 0 {
		old.Objects = new.Objects
		updated = true
	}
	if len(new.Address) > 0 {
		old.Address = new.Address
		updated = true
	}
	if len(new.Mood) > 0 {
		old.Mood = new.Mood
		updated = true
	}
	if len(new.Action) > 0 {
		old.Action = new.Action
		updated = true
	}
	if len(new.Coordinates) > 0 {
		old.Coordinates = new.Coordinates
		updated = true
	}
	if len(new.Dimensions) > 0 {
		old.Dimensions = new.Dimensions
		updated = true
	}

	// 数值字段：非零则更新
	if new.Size != 0 {
		old.Size = new.Size
		updated = true
	}

	// 时间字段：有效时间则更新
	if !new.Datetime.IsZero() {
		old.Datetime = new.Datetime
		updated = true
	}

	// 向量字段：非空则更新（空向量表示尚未生成）
	if len(new.ImageVector) > 0 {
		old.ImageVector = new.ImageVector
		updated = true
	}

	// 只有在有字段被更新时才更新时间
	if updated {
		old.UpdateTime = time.Now()
	}

	return updated
}

// deleteImageMetadataByMD5 根据 MD5 删除图片元数据
func (m *LanceDBManager) deleteImageMetadataByMD5(md5 string) error {
	tbl, err := m.db.OpenTable(m.ctx, TableImageMetadata)
	if err != nil {
		return fmt.Errorf("failed to open table %s: %w", TableImageMetadata, err)
	}
	defer tbl.Close()

	return tbl.Delete(m.ctx, fmt.Sprintf("md5 = '%s'", md5))
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
// 如果 MD5 存在但 file_path 不同，则插入新记录（同一文件多个路径）
// 如果 MD5 和 file_path 都已存在，则跳过
func (m *LanceDBManager) InsertFileIndex(data *FileIndex) error {
	// 检查是否已存在相同的 MD5 + file_path 组合
	exists, err := m.checkFileIndexExists(data.MD5, data.FilePath)
	if err != nil {
		return fmt.Errorf("failed to check file index existence: %w", err)
	}
	if exists {
		// 已存在相同的 MD5 + file_path 组合，跳过插入
		return nil
	}
	return m.InsertFileIndexBatch([]FileIndex{*data})
}

// checkFileIndexExists 检查指定的 MD5 + file_path 组合是否已存在
func (m *LanceDBManager) checkFileIndexExists(md5, filePath string) (bool, error) {
	tbl, err := m.db.OpenTable(m.ctx, TableFileIndex)
	if err != nil {
		return false, fmt.Errorf("failed to open table %s: %w", TableFileIndex, err)
	}
	defer tbl.Close()

	// 使用 SQL 过滤查询
	filter := fmt.Sprintf("md5 = '%s' AND file_path = '%s'", md5, filePath)
	results, err := tbl.SelectWithFilter(m.ctx, filter)
	if err != nil {
		return false, fmt.Errorf("failed to query file index: %w", err)
	}

	return len(results) > 0, nil
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

	// create_time 和 update_time 使用 Int64Builder 存储 Unix 微秒时间戳
	createTimeBuilder := array.NewInt64Builder(pool)
	defer createTimeBuilder.Release()

	updateTimeBuilder := array.NewInt64Builder(pool)
	defer updateTimeBuilder.Release()

	// 填充数据
	now := time.Now()
	for _, item := range data {
		md5Builder.Append(item.MD5)
		themeBuilder.Append(marshalStringSlice(item.Theme))
		descBuilder.Append(item.Description)
		objectsBuilder.Append(marshalStringSlice(item.Objects))
		coordsBuilder.Append(marshalFloat32SliceToJSON(item.Coordinates))
		// 拍摄时间也使用 UTC 存储
		if !item.Datetime.IsZero() {
			datetimeBuilder.Append(arrow.Timestamp(item.Datetime.UTC().UnixMicro()))
		} else {
			datetimeBuilder.AppendNull()
		}
		addressBuilder.Append(marshalStringSlice(item.Address))
		dimensionsBuilder.Append(marshalFloat32SliceToJSON(item.Dimensions))
		extBuilder.Append(item.Ext)
		sizeBuilder.Append(item.Size)
		placeBuilder.Append(item.Place)
		colorsBuilder.Append(item.Colors)
		moodBuilder.Append(marshalStringSlice(item.Mood))
		actionBuilder.Append(marshalStringSlice(item.Action))

		// 添加向量：如果有值则添加，否则标记为 null
		if len(item.ImageVector) > 0 {
			vectorBuilder.Append(true)
			for _, v := range item.ImageVector {
				vectorValueBuilder.Append(v)
			}
		} else {
			vectorBuilder.AppendNull()
		}

		// 设置创建时间和更新时间（使用 UTC 时间存储，避免时区问题）
		if item.CreateTime.IsZero() {
			item.CreateTime = now
		}
		if item.UpdateTime.IsZero() {
			item.UpdateTime = now
		}
		// 存储为 Unix 微秒时间戳（Int64）
		createTimeBuilder.Append(item.CreateTime.UTC().UnixMicro())
		updateTimeBuilder.Append(item.UpdateTime.UTC().UnixMicro())
	}

	// 创建数组
	// 注意：create_time 和 update_time 使用 Int64 存储 Unix 微秒时间戳
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
		{Name: "create_time", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "update_time", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
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
		createTimeBuilder.NewArray(),
		updateTimeBuilder.NewArray(),
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

	createTimeBuilder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Microsecond})
	defer createTimeBuilder.Release()

	updateTimeBuilder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Microsecond})
	defer updateTimeBuilder.Release()

	now := time.Now()
	for _, item := range data {
		faceIDBuilder.Append(item.FaceID)
		md5Builder.Append(item.MD5)

		// 添加向量
		vectorBuilder.Append(true)
		for _, v := range item.FaceVector {
			vectorValueBuilder.Append(v)
		}

		boxBuilder.Append(marshalFloat32SliceToJSON(item.Box))

		// 设置创建时间和更新时间
		if item.CreateTime.IsZero() {
			item.CreateTime = now
		}
		if item.UpdateTime.IsZero() {
			item.UpdateTime = now
		}
		createTimeBuilder.Append(arrow.Timestamp(item.CreateTime.UnixMicro()))
		updateTimeBuilder.Append(arrow.Timestamp(item.UpdateTime.UnixMicro()))
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "face_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "md5", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "face_vector", Type: arrow.FixedSizeListOf(VectorDimension, arrow.PrimitiveTypes.Float32), Nullable: true},
		{Name: "box", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "create_time", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
		{Name: "update_time", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
	}, nil)

	columns := []arrow.Array{
		faceIDBuilder.NewArray(),
		md5Builder.NewArray(),
		vectorBuilder.NewArray(),
		boxBuilder.NewArray(),
		createTimeBuilder.NewArray(),
		updateTimeBuilder.NewArray(),
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

	createTimeBuilder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Microsecond})
	defer createTimeBuilder.Release()

	updateTimeBuilder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Microsecond})
	defer updateTimeBuilder.Release()

	now := time.Now()
	for _, item := range data {
		md5Builder.Append(item.MD5)
		pathBuilder.Append(item.FilePath)

		// 设置创建时间和更新时间
		if item.CreateTime.IsZero() {
			item.CreateTime = now
		}
		if item.UpdateTime.IsZero() {
			item.UpdateTime = now
		}
		createTimeBuilder.Append(arrow.Timestamp(item.CreateTime.UnixMicro()))
		updateTimeBuilder.Append(arrow.Timestamp(item.UpdateTime.UnixMicro()))
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "md5", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "file_path", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "create_time", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
		{Name: "update_time", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
	}, nil)

	columns := []arrow.Array{
		md5Builder.NewArray(),
		pathBuilder.NewArray(),
		createTimeBuilder.NewArray(),
		updateTimeBuilder.NewArray(),
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
	if v, ok := data["theme"].(string); ok {
		result.Theme = unmarshalStringSlice(v)
	}
	if v, ok := data["description"].(string); ok {
		result.Description = v
	}
	if v, ok := data["objects"].(string); ok {
		result.Objects = unmarshalStringSlice(v)
	}
	if v, ok := data["coordinates"].(string); ok {
		result.Coordinates = unmarshalFloat32Slice(v)
	}
	if v, ok := data["address"].(string); ok {
		result.Address = unmarshalStringSlice(v)
	}
	if v, ok := data["dimensions"].(string); ok {
		result.Dimensions = unmarshalFloat32Slice(v)
	}
	if v, ok := data["ext"].(string); ok {
		result.Ext = v
	}
	if v, ok := data["place"].(string); ok {
		result.Place = v
	}
	if v, ok := data["colors"].(string); ok {
		result.Colors = v
	}
	if v, ok := data["mood"].(string); ok {
		result.Mood = unmarshalStringSlice(v)
	}
	if v, ok := data["action"].(string); ok {
		result.Action = unmarshalStringSlice(v)
	}
	if v, ok := data["size"].(int32); ok {
		result.Size = v
	}

	// 解析时间戳（支持 arrow.Timestamp 和 time.Time 两种类型）
	result.Datetime = parseTimestampFromArrow(data["datetime"])
	result.CreateTime = parseTimestampFromArrow(data["create_time"])
	result.UpdateTime = parseTimestampFromArrow(data["update_time"])

	// 解析向量
	if v, ok := data["image_vector"].(arrow.Array); ok {
		// 从 Arrow FixedSizeListArray 解析向量
		if listArr, ok := v.(*array.FixedSizeList); ok {
			if listArr.Len() > 0 && !listArr.IsNull(0) {
				// 获取值数组的起始和结束偏移
				start, end := listArr.ValueOffsets(0)
				values := listArr.ListValues()
				if floatArr, ok := values.(*array.Float32); ok {
					result.ImageVector = make([]float32, end-start)
					for i := int64(0); i < end-start; i++ {
						result.ImageVector[i] = floatArr.Value(int(start + i))
					}
				}
			}
		}
	}

	return result, nil
}

// unmarshalFloat32Slice 将 JSON 字符串反序列化为 float32 切片
func unmarshalFloat32Slice(s string) []float32 {
	if s == "" || s == "[]" {
		return []float32{}
	}
	var result []float32
	json.Unmarshal([]byte(s), &result)
	return result
}

// parseTimestampFromArrow 从 Arrow 数组或值解析时间戳
// 支持 Int64 存储的 Unix 微秒时间戳
func parseTimestampFromArrow(v interface{}) time.Time {
	if v == nil {
		return time.Time{}
	}

	// 尝试直接解析为时间戳值
	switch t := v.(type) {
	case int64:
		return time.UnixMicro(t).Local()
	case float64:
		return time.UnixMicro(int64(t)).Local()
	case arrow.Timestamp:
		return time.UnixMicro(int64(t)).Local()
	case time.Time:
		return t.Local()
	}

	// 尝试解析为 Arrow 数组（LanceDB 返回的是数组）
	if arr, ok := v.(arrow.Array); ok {
		if arr.Len() == 0 || arr.IsNull(0) {
			return time.Time{}
		}

		switch a := arr.(type) {
		case *array.Int64:
			return time.UnixMicro(a.Value(0)).Local()
		case *array.Timestamp:
			return time.UnixMicro(int64(a.Value(0))).Local()
		default:
			Logger.Warnw("parseTimestampFromArrow: unknown array type", "type", fmt.Sprintf("%T", v))
			return time.Time{}
		}
	}

	Logger.Warnw("parseTimestampFromArrow: unknown type", "type", fmt.Sprintf("%T", v), "value", v)
	return time.Time{}
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
	return "./photoVL_db"
}
