#!/bin/bash

# 下载 go-face 模型文件脚本（不需要 sudo）

set -e

echo "=========================================="
echo "  go-face 模型文件下载脚本"
echo "=========================================="
echo ""

# 创建模型目录
echo "[1/2] 创建模型目录..."
MODEL_DIR="./models/face"
mkdir -p "$MODEL_DIR"
echo "模型目录: $MODEL_DIR"

# 下载模型文件
echo ""
echo "[2/2] 下载 dlib 模型文件..."
echo "注意: 模型文件较大，下载可能需要几分钟"
echo ""

# 模型文件 URLs
SHAPE_PREDICTOR_URL="http://dlib.net/files/shape_predictor_5_face_landmarks.dat.bz2"
FACE_RECOGNITION_URL="http://dlib.net/files/dlib_face_recognition_resnet_model_v1.dat.bz2"

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
        wget -q --show-progress "$FACE_RECOGNITION_URL" -O dlib_face_recognition_resnet_model_v1.dat.bz2
    elif command -v curl &> /dev/null; then
        curl -L --progress-bar "$FACE_RECOGNITION_URL" -o dlib_face_recognition_resnet_model_v1.dat.bz2
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
echo "验证模型文件..."

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
echo "  go-face 模型文件准备完成!"
echo "=========================================="
echo ""
echo "模型文件位置: $MODEL_DIR"
