#!/bin/bash

# LanceDB 安装脚本
# 自动下载并安装 lancedb-go 所需的依赖库

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB_DIR="${SCRIPT_DIR}/lib/linux_amd64"
INCLUDE_DIR="${SCRIPT_DIR}/include"

echo "=== LanceDB 安装脚本 ==="
echo ""

# 检查操作系统和架构
OS=$(uname -s)
ARCH=$(uname -m)

if [ "$OS" != "Linux" ] || [ "$ARCH" != "x86_64" ]; then
    echo "错误: 当前只支持 Linux x86_64 架构"
    echo "当前系统: $OS $ARCH"
    exit 1
fi

# 检查依赖命令
if ! command -v curl &> /dev/null && ! command -v wget &> /dev/null; then
    echo "错误: 需要 curl 或 wget 来下载文件"
    exit 1
fi

# 创建目录
mkdir -p "$LIB_DIR"
mkdir -p "$INCLUDE_DIR"

# 检查是否已安装
if [ -f "$LIB_DIR/liblancedb_go.so" ] && [ -f "$INCLUDE_DIR/lancedb.h" ]; then
    echo "✓ LanceDB 库文件已存在"
    echo ""
    echo "库文件: $LIB_DIR/liblancedb_go.so"
    echo "头文件: $INCLUDE_DIR/lancedb.h"
    echo ""
    ls -lh "$LIB_DIR/liblancedb_go.so"
    echo ""
    echo "如需重新安装，请先删除现有文件:"
    echo "  rm -rf $LIB_DIR $INCLUDE_DIR"
    echo ""
    exit 0
fi

echo "正在下载 LanceDB Go 库..."
echo ""

# 使用临时目录
TMP_DIR=$(mktemp -d)
trap "rm -rf $TMP_DIR" EXIT

cd "$TMP_DIR"

# 下载最新的 native binaries
DOWNLOAD_URL="https://github.com/lancedb/lancedb-go/releases/latest/download/lancedb-go-native-binaries.tar.gz"

echo "下载地址: $DOWNLOAD_URL"
echo ""

DOWNLOAD_SUCCESS=false

if command -v curl &> /dev/null; then
    echo "使用 curl 下载..."
    if curl -L --progress-bar -o lancedb-go.tar.gz "$DOWNLOAD_URL"; then
        DOWNLOAD_SUCCESS=true
    else
        echo "curl 下载失败，尝试 wget..."
    fi
fi

if [ "$DOWNLOAD_SUCCESS" = false ] && command -v wget &> /dev/null; then
    echo "使用 wget 下载..."
    if wget --progress=bar:force -O lancedb-go.tar.gz "$DOWNLOAD_URL"; then
        DOWNLOAD_SUCCESS=true
    fi
fi

if [ "$DOWNLOAD_SUCCESS" = false ]; then
    echo ""
    echo "错误: 下载失败"
    exit 1
fi

echo ""
echo "下载完成，正在解压..."

# 解压文件
tar -xzf lancedb-go.tar.gz

echo ""
echo "安装文件..."

# 复制库文件 (在 lib/linux_amd64/ 子目录中)
if [ -f "lib/linux_amd64/liblancedb_go.so" ]; then
    cp lib/linux_amd64/liblancedb_go.so "$LIB_DIR/"
    echo "✓ 库文件已复制"
else
    echo "错误: 未找到 liblancedb_go.so"
    echo "解压后的文件列表:"
    find . -name "*.so" -o -name "*.h" 2>/dev/null
    exit 1
fi

# 复制头文件 (在 include/ 子目录中)
if [ -f "include/lancedb.h" ]; then
    cp include/lancedb.h "$INCLUDE_DIR/"
    echo "✓ 头文件已复制"
else
    echo "警告: 未找到 lancedb.h"
fi

# 设置权限
chmod 755 "$LIB_DIR"/*.so

echo ""
echo "=== 安装完成 ==="
echo ""
echo "库文件: $LIB_DIR/liblancedb_go.so"
echo "头文件: $INCLUDE_DIR/lancedb.h"
echo ""
ls -lh "$LIB_DIR/liblancedb_go.so"
echo ""

# 配置运行环境
echo "=== 配置运行环境 ==="
echo ""
echo "请选择运行方式:"
echo ""
echo "【推荐】方法1 - 编译时嵌入 rpath (无需额外配置):"
echo "  go build -o photoVL ."
echo "  ./photoVL"
echo ""
echo "方法2 - 使用环境变量:"
echo "  export LD_LIBRARY_PATH=${LIB_DIR}:\$LD_LIBRARY_PATH"
echo "  ./photoVL"
echo ""
echo "方法3 - 添加到 ~/.bashrc (永久生效):"
echo "  echo 'export LD_LIBRARY_PATH=${LIB_DIR}:\$LD_LIBRARY_PATH' >> ~/.bashrc"
echo "  source ~/.bashrc"
echo ""
echo "方法4 - 安装到系统路径 (需要 sudo):"
echo "  sudo cp ${LIB_DIR}/liblancedb_go.so /usr/local/lib/"
echo "  sudo ldconfig"
echo ""

# 检查 cgo_config.go
if [ -f "${SCRIPT_DIR}/cgo_config.go" ]; then
    if grep -q "rpath" "${SCRIPT_DIR}/cgo_config.go"; then
        echo "✓ cgo_config.go 已配置 rpath，可以直接编译运行"
    else
        echo "! 警告: cgo_config.go 中未找到 rpath 配置"
        echo "  建议修改 cgo_config.go，添加: -Wl,-rpath,\${SRCDIR}/lib/linux_amd64"
    fi
else
    echo "! 警告: 未找到 cgo_config.go"
fi

echo ""
echo "现在可以编译和运行程序:"
echo "  cd ${SCRIPT_DIR}"
echo "  go build -o photoVL ."
echo "  ./photoVL <图片路径>"
