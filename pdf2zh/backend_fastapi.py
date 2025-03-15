from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pdf2zh.doclayout import ModelInstance
import json
import io
import tqdm
import asyncio
from pdf2zh import translate_stream
import uuid
import logging
import requests  # 用于处理 URL 下载

# 配置日志
logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

# 初始化 FastAPI 应用
app = FastAPI()

# # 允许 CORS 访问（如果前端和后端端口不同）
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],  
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# 存储任务状态的简单方式
tasks = {}

# 下载 URL 对应的文件并返回文件对象
def download_pdf(url: str) -> bytes:
    response = requests.get(url)
    if response.status_code != 200:
        raise HTTPException(status_code=400, detail="Failed to download PDF")
    return response.content

# async def translate_task(stream: bytes, args: dict):
#     async def progress_bar(t: tqdm.tqdm):
#         tasks[args.get("task_id")]["state"] = "IN_PROGRESS"
#         tasks[args.get("task_id")]["progress"] = {
#             "n": t.n,
#             "total": t.total
#         }
#         logging.info(f"Translating {t.n} / {t.total} pages")

#     # **确保 translate_stream 是异步的，并 await 它**
#     doc_mono, doc_dual =  translate_stream(
#         stream,
#         # callback=progress_bar,
#         model=ModelInstance.value,
#         **args,
#     )

#     tasks[args.get("task_id")]["state"] = "SUCCESS"
#     tasks[args.get("task_id")]["result"] = (doc_mono, doc_dual)

# 异步任务：使用 run_in_executor 将同步的 translate_stream 放入后台线程池中执行
async def translate_task(stream: bytes, args: dict):
    loop = asyncio.get_running_loop()
    
    # 定义同步的 progress_bar 回调函数
    def progress_bar(t: tqdm.tqdm):
        task_id = args.get("task_id")
        tasks[task_id]["state"] = "IN_PROGRESS"
        tasks[task_id]["progress"] = {"n": t.n, "total": t.total}
        logger.info(f"Translating {t.n} / {t.total} pages")
    
    # 显式传递必要的参数给 translate_stream
    doc_mono, doc_dual = await loop.run_in_executor(
        None,
        lambda: translate_stream(
            stream,
            None,                     # pages: 默认传 None
            args["lang_in"],
            args["lang_out"],
            args["service"],
            args["thread"],
            "",                       # vfont 默认空字符串
            "",                       # vchar 默认空字符串
            progress_bar,             # 回调
            None,                     # cancellation_event
            ModelInstance.value       # model
        )
    )
    
    task_id = args.get("task_id")
    tasks[task_id]["state"] = "SUCCESS"
    tasks[task_id]["result"] = (doc_mono, doc_dual)

@app.post("/v1/translate")
async def create_translate_tasks(file: UploadFile = File(...), url: str = Form(...), data: str = Form(...)):
    try:
        # 获取文件内容
        file_content = None
        if url:
            file_content = download_pdf(url)  # 下载 URL 对应的文件
        elif file:
            file_content = await file.read()  # 获取上传的文件内容

        if not file_content:
            raise HTTPException(status_code=400, detail="Either file or URL must be provided.")

        # 解析请求的额外数据
        args = json.loads(data)  # 解析 JSON 字符串
        logging.info(f"Parsed arguments: {args}")
        
        # 生成唯一任务ID
        task_id = str(uuid.uuid4())  # 使用 UUID 生成唯一的 ID
        tasks[task_id] = {
            "state": "PENDING", 
            "progress": {
                "n": 0,  # 初始页数为 0
                "total": 0  # 总页数还未确定
            }, 
            "result": None}
        args["task_id"] = task_id
        

        # 启动异步翻译任务
        asyncio.create_task(translate_task(file_content, args))
        return {"id": task_id}
        

    except Exception as e:
        logging.error(f"Error: {e}")
        raise HTTPException(status_code=400, detail=f"Error: {e}")

@app.get("/v1/translate/{id}")
async def get_translate_task(id: str):
    """
    获取翻译任务的状态
    """
    task = tasks.get(id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"state": task["state"], "info": task["progress"]}

@app.get("/v1/translate/{id}/{format}")
async def get_translate_result(id: str, format: str):
    """
    获取翻译结果
    """
    task = tasks.get(id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    if task["state"] != "SUCCESS":
        raise HTTPException(status_code=400, detail="Task is not finished yet")

    doc_mono, doc_dual = task["result"]
    to_send = doc_mono if format == "mono" else doc_dual
    return StreamingResponse(io.BytesIO(to_send), media_type="application/pdf")
