#!/bin/bash

# 安装 go-face 依赖脚本
# go-face 需要 dlib 库和模型文件

set -e

echo "=========================================="
echo "  go-face 依赖安装脚本"
echo "=========================================="
echo ""

# 检测操作系统
OS=""
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    OS="linux"
    if [ -f /etc/debian_version ]; then
        DISTRO="debian"
    elif [ -f /etc/redhat-release ]; then
        DISTRO="redhat"
    else
        DISTRO="unknown"
    fi
elif [[ "$OSTYPE" == "darwin"* ]]; then
    OS="macos"
else
    echo "不支持的操作系统: $OSTYPE"
    exit 1
fi

echo "检测到操作系统: $OS"

# 安装系统依赖
echo ""
echo "[1/4] 安装系统依赖..."

if [ "$OS" == "linux" ]; then
    if [ "$DISTRO" == "debian" ]; then
        # Debian/Ubuntu
        echo "正在安装 Debian/Ubuntu 依赖..."
        sudo apt-get update
        sudo apt-get install -y \
            libdlib-dev \
            libblas-dev \
            libatlas-base-dev \
            liblapack-dev \
            libjpeg-turbo8-dev \
            cmake \
            build-essential
    elif [ "$DISTRO" == "redhat" ]; then
        # CentOS/RHEL/Fedora
        echo "正在安装 RedHat 系依赖..."
        sudo yum install -y \
            dlib-devel \
            blas-devel \
            atlas-devel \
            lapack-devel \
            libjpeg-turbo-devel \
            cmake \
            gcc-c++ \
            make
    else
        echo "未知的 Linux 发行版，请手动安装以下依赖:"
        echo "  - dlib (>= 19.10)"
        echo "  - libblas"
        echo "  - libatlas"
        echo "  - liblapack"
        echo "  - libjpeg"
        echo "  - cmake"
        exit 1
    fi
elif [ "$OS" == "macos" ]; then
    # macOS
    if ! command -v brew &> /dev/null; then
        echo "未检测到 Homebrew，请先安装: https://brew.sh"
        exit 1
    fi
    echo "正在安装 macOS 依赖..."
    brew install dlib
fi

echo "系统依赖安装完成"

# 创建模型目录
echo ""
echo "[2/4] 创建模型目录..."
MODEL_DIR="./models/face"
mkdir -p "$MODEL_DIR"
echo "模型目录: $MODEL_DIR"

# 下载模型文件
echo ""
echo "[3/4] 下载 dlib 模型文件..."
echo "注意: 模型文件较大，下载可能需要几分钟"
echo ""

# 模型文件 URLs
SHAPE_PREDICTOR_URL="http://dlib.net/files/shape_predictor_5_face_landmarks.dat.bz2"
FACE_RECognition_URL="http://dlib.net/files/dlib_face_recognition_resnet_model_v1.dat.bz2"

cd "$MODEL_DIR"

# 下载 shape_predictor_5_face_landmarks.dat.bz2
if [ ! -f "shape_predictor_5_face_landmarks.dat" ]; then
    echo "下载 shape_predictor_5_face_landmarks.dat..."
    if command -v wget &> /dev/null; then
        wget -q --show-progress "$SHAPE_PREDICTOR_URL" -O shape_predictor_5_face_landmarks.dat.bz2
    elif command -v curl &> /dev/null; then
        curl -L --progress-bar "$SHAPE_PREDICTOR_URL" -o shape_predictor_5_face_landmarks.dat.bz2
    else
        echo "错误: 需要 wget 或 curl 来下载模型文件"
        exit 1
    fi
    
    echo "解压 shape_predictor_5_face_landmarks.dat..."
    bunzip2 shape_predictor_5_face_landmarks.dat.bz2
    echo "✓ shape_predictor_5_face_landmarks.dat 准备完成"
else
    echo "✓ shape_predictor_5_face_landmarks.dat 已存在"
fi

echo ""

# 下载 dlib_face_recognition_resnet_model_v1.dat.bz2
if [ ! -f "dlib_face_recognition_resnet_model_v1.dat" ]; then
    echo "下载 dlib_face_recognition_resnet_model_v1.dat..."
    if command -v wget &> /dev/null; then
        wget -q --show-progress "$FACE_RECognition_URL" -O dlib_face_recognition_resnet_model_v1.dat.bz2
    elif command -v curl &> /dev/null; then
        curl -L --progress-bar "$FACE_RECognition_URL" -o dlib_face_recognition_resnet_model_v1.dat.bz2
    fi
    
    echo "解压 dlib_face_recognition_resnet_model_v1.dat..."
    bunzip2 dlib_face_recognition_resnet_model_v1.dat.bz2
    echo "✓ dlib_face_recognition_resnet_model_v1.dat 准备完成"
else
    echo "✓ dlib_face_recognition_resnet_model_v1.dat 已存在"
fi

cd - > /dev/null

# 验证模型文件
echo ""
echo "[4/4] 验证模型文件..."

MISSING=0
for file in "shape_predictor_5_face_landmarks.dat" "dlib_face_recognition_resnet_model_v1.dat"; do
    if [ ! -f "$MODEL_DIR/$file" ]; then
        echo "✗ 缺失: $file"
        MISSING=1
    else
        SIZE=$(du -h "$MODEL_DIR/$file" | cut -f1)
        echo "✓ $file ($SIZE)"
    fi
done

if [ $MISSING -eq 1 ]; then
    echo ""
    echo "错误: 部分模型文件缺失"
    exit 1
fi

echo ""
echo "=========================================="
echo "  go-face 依赖安装完成!"
echo "=========================================="
echo ""
echo "模型文件位置: $MODEL_DIR"
echo ""
echo "现在可以编译项目:"
echo "  go build -o photoVL"
echo ""
echo "如果遇到编译错误，请确保:"
echo "  1. CGO 已启用 (export CGO_ENABLED=1)"
echo "  2. GCC 编译器已安装"
echo "  3. dlib 头文件路径正确"
echo ""
