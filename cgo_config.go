package main

//go:generate go build -o photoVL .

/*
#cgo CFLAGS: -I${SRCDIR}/include
#cgo LDFLAGS: -L${SRCDIR}/lib/linux_amd64 -llancedb_go -Wl,-rpath,${SRCDIR}/lib/linux_amd64 -lm -ldl -lpthread
*/
import "C"

// 这个文件仅用于配置 CGO 编译选项
// 实际的 LanceDB 代码在 lanceDB.go 中
//
// 编译方式：
//   1. 直接编译: go build
//   2. 使用 go generate: go generate ./...

