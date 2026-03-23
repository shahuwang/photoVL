.PHONY: build clean install install-deps install-system-deps install-lancedb install-models run-vl run-eb run-textquery run-dir-vl run-dir-eb test dev prod help

# 项目配置
PROJECT_NAME := photoVL
BUILD_DIR := .
LIB_DIR := $(shell pwd)/lib/linux_amd64
INCLUDE_DIR := $(shell pwd)/include

# CGO 编译配置
export CGO_CFLAGS := -I$(INCLUDE_DIR)
export CGO_CXXFLAGS := -I$(INCLUDE_DIR) -std=c++1z
export CGO_LDFLAGS := -L$(LIB_DIR)

# 默认目标
all: build

# 安装所有依赖（不需要 sudo）
# 注意：go-face 需要系统依赖，请先执行: make install-system-deps
install: install-deps
	@echo ""
	@echo "=========================================="
	@echo "  所有依赖安装完成！"
	@echo "=========================================="
	@echo ""
	@echo "现在可以执行 'make build' 来编译项目"

# 编译项目
build:
	export CGO_ENABLED=1 && go build -o $(PROJECT_NAME) .

# 清理编译产物
clean:
	rm -f $(PROJECT_NAME)

# 清理所有生成的文件和依赖
distclean: clean
	rm -rf lib include models/face

# 安装 LanceDB 依赖
install-lancedb:
	@echo "安装 LanceDB 依赖..."
	bash install_lancedb.sh

# 仅下载 go-face 模型文件（不需要 sudo）
install-models:
	@echo "下载 go-face 模型文件..."
	bash install_go_face_models.sh

# 安装系统依赖（需要 sudo）
install-system-deps:
	@echo "安装系统依赖（需要 sudo 权限）..."
	bash install_go_face_system_deps.sh

# 安装所有依赖（LanceDB + 模型文件，不需要 sudo）
# 注意：系统依赖需要单独执行: make install-system-deps
install-deps: install-lancedb install-models

# 运行视觉分析模式（单文件）
run-vl:
	./$(PROJECT_NAME) -fpath $(IMG) -opt vl

# 运行向量嵌入模式（单文件）
run-eb:
	./$(PROJECT_NAME) -fpath $(IMG) -opt eb

# 运行文本查询模式
run-textquery:
	./$(PROJECT_NAME) -opt textQuery -topN $(TOPN)

# 运行文件夹视觉分析模式
run-dir-vl:
	./$(PROJECT_NAME) -dir $(DIR) -opt vl

# 运行文件夹向量嵌入模式
run-dir-eb:
	./$(PROJECT_NAME) -dir $(DIR) -opt eb

# 运行文件夹文本查询模式
run-dir-textquery:
	./$(PROJECT_NAME) -dir $(DIR) -opt textQuery -topN $(TOPN)

# 测试编译
test:
	export CGO_ENABLED=1 && go test ./...

# 开发模式编译（带调试信息）
dev:
	export CGO_ENABLED=1 && go build -gcflags="all=-N -l" -o $(PROJECT_NAME) .

# 生产模式编译（优化）
prod:
	export CGO_ENABLED=1 && go build -ldflags="-s -w" -o $(PROJECT_NAME) .

# 显示帮助信息
help:
	@echo "使用方法:"
	@echo ""
	@echo "编译相关:"
	@echo "  make build          - 编译项目（默认）"
	@echo "  make dev            - 开发模式编译（带调试信息）"
	@echo "  make prod           - 生产模式编译（优化）"
	@echo "  make clean          - 清理编译产物"
	@echo "  make distclean      - 清理编译产物和所有依赖"
	@echo ""
	@echo "依赖安装:"
	@echo "  make install-system-deps - 安装系统依赖（需要 sudo，只需执行一次）"
	@echo "  make install        - 安装项目依赖（LanceDB + 模型文件，不需要 sudo）"
	@echo "  make install-deps   - 同上"
	@echo "  make install-lancedb - 仅安装 LanceDB 依赖"
	@echo "  make install-models - 仅下载 go-face 模型文件"
	@echo ""
	@echo "运行模式 - 单文件:"
	@echo "  make run-vl IMG=photo.jpg"
	@echo "                      - 视觉分析模式"
	@echo "  make run-eb IMG=photo.jpg"
	@echo "                      - 向量嵌入模式"
	@echo "  make run-textquery TOPN=10"
	@echo "                      - 文本查询模式（交互式）"
	@echo ""
	@echo "运行模式 - 文件夹:"
	@echo "  make run-dir-vl DIR=/path/to/images"
	@echo "                      - 文件夹视觉分析"
	@echo "  make run-dir-eb DIR=/path/to/images"
	@echo "                      - 文件夹向量嵌入"
	@echo "  make run-dir-textquery DIR=/path/to/images TOPN=10"
	@echo "                      - 文件夹文本查询"
	@echo ""
	@echo "其他:"
	@echo "  make test           - 运行测试"
	@echo "  make help           - 显示此帮助信息"
	@echo ""
	@echo "示例:"
	@echo "  # 编译项目"
	@echo "  make build"
	@echo ""
	@echo "  # 分析单张图片"
	@echo "  make run-vl IMG=./images/photo.jpg"
	@echo ""
	@echo "  # 处理文件夹"
	@echo "  make run-dir-eb DIR=./images/"
	@echo ""
	@echo "  # 文本查询（需要先使用 eb 模式处理图片）"
	@echo "  make run-textquery TOPN=5"

# 旧版注释说明（保留供参考）
# 编译: make build
# 清理: make clean
# 视觉分析: make run-vl IMG=photo.jpg
# 向量嵌入: make run-eb IMG=photo.jpg
# 文本查询: make run-textquery TOPN=10
# 文件夹视觉分析: make run-dir-vl DIR=/path/to/images
# 文件夹向量嵌入: make run-dir-eb DIR=/path/to/images
# 安装依赖: make install-deps
