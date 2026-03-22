pip install torch --index-url https://download.pytorch.org/whl/cu121
pip install torchvision --index-url https://download.pytorch.org/whl/cu121
pip install modelscope
pip install transformers
pip install accelerate
pip install huggingface_hub
pip install bitsandbytes
pip install fastapi
pip install uvicorn
pip install pillow
pip install numpy
pip install flask
pip install flask-cors

modelscope download --model Qwen/Qwen3-VL-Embedding-8B --local_dir ./models/qwen3-vl-embedding-8b
