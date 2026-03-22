.PHONY: build clean install-deps run-vl run-eb test

# 默认目标
build:
	go build -mod=mod -o photoVL .

# 清理编译产物
clean:
	rm -f photoVL

# 安装 go-face 依赖（模型文件和系统库）
install-deps:
	@echo "安装 go-face 依赖..."
	bash install_go_face.sh

# 运行视觉分析模式（默认）
run-vl:
	./photoVL -fpath $(IMG) -opt vl

# 运行向量嵌入模式
run-eb:
	./photoVL -fpath $(IMG) -opt eb

# 测试编译
test:
	go test -mod=mod ./...

# 开发模式编译（带调试信息）
dev:
	go build -mod=mod -gcflags="all=-N -l" -o photoVL .

# 生产模式编译（优化）
prod:
	go build -mod=mod -ldflags="-s -w" -o photoVL .

# 编译
#make build

# 清理
#make clean

# 运行视觉分析
#make run-vl IMG=photo.jpg

# 运行向量嵌入
#make run-eb IMG=photo.jpg

# 安装依赖
#make install-deps