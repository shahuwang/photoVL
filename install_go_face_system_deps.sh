#!/bin/bash

# 安装 go-face 系统依赖脚本（需要 sudo 权限）
# 用于安装 dlib 等系统库

set -e

echo "=========================================="
echo "  go-face 系统依赖安装脚本"
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
echo "[1/2] 安装系统依赖..."

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

echo ""
echo "[2/2] 系统依赖安装完成！"
echo ""
echo "现在可以执行 'make install' 来安装其他依赖"
