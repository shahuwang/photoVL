# go-face 手动安装指南

由于 go-face 依赖 dlib C++ 库，需要安装系统依赖才能编译。

## 快速安装（Ubuntu/Debian）

```bash
# 1. 安装系统依赖
sudo apt-get update
sudo apt-get install -y libdlib-dev libblas-dev libatlas-base-dev liblapack-dev libjpeg-turbo8-dev

# 2. 下载模型文件
mkdir -p ./models/face
cd ./models/face

# 下载并解压模型文件
wget http://dlib.net/files/shape_predictor_5_face_landmarks.dat.bz2
bunzip2 shape_predictor_5_face_landmarks.dat.bz2

wget http://dlib.net/files/dlib_face_recognition_resnet_model_v1.dat.bz2
bunzip2 dlib_face_recognition_resnet_model_v1.dat.bz2

cd ../..

# 3. 编译项目
go build -o photoVL
```

## 模型文件说明

| 文件 | 大小 | 用途 |
|------|------|------|
| `shape_predictor_5_face_landmarks.dat` | ~6MB | 人脸关键点检测（5点） |
| `dlib_face_recognition_resnet_model_v1.dat` | ~100MB | 人脸识别特征提取 |

## 常见问题

### 1. 编译错误：dlib/graph_utils.h: No such file or directory

**原因**: dlib 开发库未安装

**解决**:
```bash
sudo apt-get install libdlib-dev
```

### 2. 编译错误：undefined reference to `cblas_sgemm`

**原因**: BLAS 库未安装

**解决**:
```bash
sudo apt-get install libblas-dev libatlas-base-dev
```

### 3. macOS 安装

```bash
brew install dlib
```

## 替代方案

如果无法安装 dlib，可以考虑：

1. **使用 Docker**: 在已配置好环境的容器中运行
2. **使用 Python 服务**: 将人脸检测放到 Python 服务中，通过 HTTP API 调用
3. **使用云端 API**: 调用阿里云、腾讯云等的人脸检测服务
