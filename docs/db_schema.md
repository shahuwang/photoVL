# LanceDB Table Schemas

## 1. Table: `image_metadata` (主表)
- **描述**: 存储图片唯一元数据及整体向量。
- **主键**: `md5` (String)
- **字段**:
    - `md5`: String (图片唯一标识)
    - `theme`: List<String> (主题关键词)
    - `description`: String (文本描述)
    - `objects`: List<String> (物体列表)
    - `coordinates`: FixedSizeList<Float32>(2) [lng, lat] (拍摄时的坐标)
    - `datetime`: Datetime (照片拍摄时间)
    - `address`: String (照片拍摄详细地址)
    - `Dimensions`: FixedSizeList<Float32>(2) [xsize, ysize] (图片像素)
    - `ext`: String (文件类型，jpg, png等)
    - `size`: Int32 (文件大小)
    - `place`: String (场所)
    - `colors`: List<String> (颜色描述),
    - `mood`: List<String> (图片的氛围描述)
    - `action`: List<String> (图片里人的动作)
    - `image_vector`: Vector(1024) (整体特征向量)

## 2. Table: `face_vectors` (子表)
- **描述**: 1:N 存储图片中的所有人脸向量。
- **关联**: 通过 `md5` 关联主表。
- **字段**: 
    - `face_id` String (一个随机且唯一的 UUID), 
    - `md5` String (图片整体的md5), 
    - `face_vector` Vector(1024) (人脸的向量), 
    - `box` List<Float> （人脸的坐标）

## 3. Table: `file_index` (路径表)
- **描述**: 解决重复文件，记录 MD5 与物理路径的映射。
- **字段**: 
    - `md5` （图片的md5）, 
    - `file_path` （图片的存储路径）