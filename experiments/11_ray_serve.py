import ray
from ray import serve
import requests
import time
import os

def main():
    # 1. 启动 Ray 和 Serve
    # Serve 会在后台启动一个 HTTP 代理（默认端口 8000）
    ray.init(ignore_reinit_error=True)
    serve.start(detached=False) # detached=False 意味着脚本结束服务就关闭

    print("=" * 60)
    print("实验 8：Ray Serve —— 模型服务化")
    print("=" * 60)

    # ==========================================
    # 定义一个服务 (Deployment)
    # ==========================================
    # @serve.deployment 是核心装饰器
    # num_replicas=2：重点！我们要启动 2 个副本，实现负载均衡
    @serve.deployment(num_replicas=2)
    class TranslatorModel:
        def __init__(self):
            import os
            self.pid = os.getpid()
            print(f"🔥 模型加载完成 (进程 ID: {self.pid})")

        # 这里的 __call__ 方法处理 HTTP 请求
        async def __call__(self, http_request):
            # 解析 HTTP 请求
            data = await http_request.json()
            text = data.get("text", "")
            
            # 模拟耗时的模型推理 (0.5秒)
            time.sleep(0.5)
            
            # 返回结果
            result = f"Translated '{text}' to French (by PID {self.pid})"
            return {"result": result, "backend_pid": self.pid}

    # ==========================================
    # 部署服务
    # ==========================================
    print("\n>>> 正在部署服务 (启动 2 个副本)...")
    # bind() 把类打包，serve.run() 把它跑起来
    translator_app = TranslatorModel.bind()
    serve.run(translator_app, name="translator")
    
    print("✅ 服务已启动！监听端口: 8000")

    # ==========================================
    # 模拟客户端请求 (Client)
    # ==========================================
    print("\n>>> 模拟发送 5 个 HTTP 请求...")
    
    # 我们连续发 5 个请求，看看是谁处理的
    for i in range(5):
        try:
            resp = requests.post(
                "http://127.0.0.1:8000/", 
                json={"text": f"Hello {i}"}
            )
            data = resp.json()
            print(f"请求 {i} -> 响应: {data['result']}")
        except Exception as e:
            print(f"请求失败: {e}")

    print("\n=== 观察点 ===")
    print("1. 你应该看到两个不同的 PID 交替处理请求（负载均衡）。")
    print("2. 我们完全没写 Socket 或 Server 代码，Ray Serve 全包了。")

    # 清理
    serve.shutdown()
    ray.shutdown()

if __name__ == "__main__":
    main()