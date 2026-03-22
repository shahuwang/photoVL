#!/usr/bin/env python3
"""
Qwen3-VL-Embedding-8B Flask Server
提供类似 Ollama 的 API 接口，支持图片向量化（4-bit 量化）
"""

from __future__ import annotations

import io
import base64
import time
from typing import Any

import torch
import torch.nn as nn
from PIL import Image
from flask import Flask, request, jsonify
from flask_cors import CORS
from transformers import AutoModel, AutoProcessor, BitsAndBytesConfig, PreTrainedTokenizer

app = Flask(__name__)
CORS(app)

# 模型配置
MODEL_PATH = "./models/qwen3-vl-embedding-8b"
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

# 全局变量存储模型和处理器
model: nn.Module | None = None
processor: Any = None
tokenizer: PreTrainedTokenizer | None = None


def load_model() -> None:
    """加载 Qwen3-VL-Embedding-8B 模型（4-bit 量化）"""
    global model, processor, tokenizer
    
    print(f"正在加载模型: {MODEL_PATH}")
    print(f"使用设备: {DEVICE}")
    
    # 配置 4-bit 量化
    quantization_config = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_compute_dtype=torch.float16,
        bnb_4bit_use_double_quant=True,
        bnb_4bit_quant_type="nf4",
    )
    
    # 加载处理器
    processor = AutoProcessor.from_pretrained(MODEL_PATH, trust_remote_code=True)
    tokenizer = processor.tokenizer  # type: ignore[attr-defined]
    
    # 加载模型（4-bit 量化）
    model = AutoModel.from_pretrained(
        MODEL_PATH,
        trust_remote_code=True,
        quantization_config=quantization_config,
        device_map="auto",
        torch_dtype=torch.float16,
    )
    
    model.eval()  # type: ignore[attr-defined]
    print("模型加载完成！")


def process_image_to_vector(image: Image.Image) -> list[float]:
    """
    处理图片并返回向量
    
    Args:
        image: PIL Image 对象
    
    Returns:
        向量列表
    """
    global model, processor, tokenizer
    
    if model is None or processor is None or tokenizer is None:
        raise RuntimeError("模型未加载")
    
    # 构建对话格式
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "image", "image": image},
            ],
        }
    ]
    
    # 处理输入 - 使用 tokenizer 的 apply_chat_template
    text = tokenizer.apply_chat_template(
        messages, 
        tokenize=False, 
        add_generation_prompt=True
    )
    
    inputs = processor(  # type: ignore[operator]
        text=[text], 
        images=[image], 
        return_tensors="pt", 
        padding=True
    )
    
    # 移动到 GPU
    inputs = {k: v.to(DEVICE) if isinstance(v, torch.Tensor) else v for k, v in inputs.items()}
    
    # 推理
    with torch.no_grad():
        outputs = model(**inputs)  # type: ignore[operator]
        # 获取 [CLS] token 的嵌入作为图片向量
        embeddings = outputs.last_hidden_state[:, 0, :].cpu().numpy()
    
    return embeddings[0].tolist()


@app.route("/api/embed", methods=["POST"])
def embed() -> tuple[Any, int]:
    """
    图片向量化接口
    
    请求格式（类似 Ollama）：
    {
        "model": "qwen3-vl-embedding-8b",
        "input": "base64编码的图片" 或 {"image": "base64编码的图片"}
    }
    
    或者使用文件上传:
    - file: 图片文件
    """
    start_time = time.time()
    
    try:
        # 检查是否有文件上传
        if "file" in request.files:
            file = request.files["file"]
            image = Image.open(file.stream).convert("RGB")
        else:
            # 解析 JSON 请求
            data = request.get_json()
            
            if not data:
                return jsonify({"error": "请求体不能为空"}), 400
            
            # 获取图片数据
            image_input = data.get("input") or data.get("image")
            
            if not image_input:
                return jsonify({"error": "缺少图片数据（input 或 image 字段）"}), 400
            
            # 处理 base64 图片
            if isinstance(image_input, str):
                # 移除可能的 base64 前缀
                if "," in image_input:
                    image_input = image_input.split(",")[1]
                
                image_bytes = base64.b64decode(image_input)
                image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
            elif isinstance(image_input, dict) and "image" in image_input:
                image_b64 = image_input["image"]
                if "," in image_b64:
                    image_b64 = image_b64.split(",")[1]
                image_bytes = base64.b64decode(image_b64)
                image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
            else:
                return jsonify({"error": "不支持的图片格式"}), 400
        
        # 处理图片获取向量
        embedding = process_image_to_vector(image)
        
        total_duration = int((time.time() - start_time) * 1e9)  # 转换为纳秒
        
        # 返回结果（类似 Ollama 格式）
        return jsonify({
            "model": "qwen3-vl-embedding-8b",
            "embeddings": [embedding],
            "total_duration": total_duration,
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/embeddings", methods=["POST"])
def embeddings() -> tuple[Any, int]:
    """
    OpenAI 兼容的 embeddings 接口
    """
    return embed()


@app.route("/api/tags", methods=["GET"])
def tags() -> Any:
    """
    获取可用模型列表（Ollama 兼容）
    """
    return jsonify({
        "models": [
            {
                "name": "qwen3-vl-embedding-8b",
                "model": "qwen3-vl-embedding-8b",
                "modified_at": "2024-01-01T00:00:00Z",
                "size": 0,
                "digest": "",
                "details": {
                    "format": "gguf",
                    "family": "qwen",
                    "families": ["qwen"],
                    "parameter_size": "8B",
                    "quantization_level": "Q4_0"
                }
            }
        ]
    })


@app.route("/api/health", methods=["GET"])
def health() -> Any:
    """健康检查接口"""
    return jsonify({
        "status": "ok",
        "model": "qwen3-vl-embedding-8b",
        "device": DEVICE,
        "quantization": "4-bit"
    })


@app.route("/", methods=["GET"])
def index() -> Any:
    """首页"""
    return jsonify({
        "name": "Qwen3-VL-Embedding-8B Server",
        "version": "1.0.0",
        "endpoints": {
            "POST /api/embed": "图片向量化（支持 base64 或文件上传）",
            "POST /api/embeddings": "OpenAI 兼容的 embeddings 接口",
            "GET /api/tags": "获取可用模型列表",
            "GET /api/health": "健康检查"
        }
    })


if __name__ == "__main__":
    # 加载模型
    load_model()
    
    # 启动服务
    print("启动 Flask 服务，监听端口 5000...")
    app.run(host="0.0.0.0", port=5000, threaded=True)
