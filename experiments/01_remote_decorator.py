"""
实验 1：理解 ray.remote() 装饰器

目标：
- 理解如何将普通函数转换为分布式任务
- 理解 ObjectRef 的概念
- 观察并行执行的效果

观察点：
- 普通函数串行执行，总耗时约 5 秒
- Ray 远程函数并行执行，总耗时约 1 秒
- remote() 返回 ObjectRef（Future），不是实际结果

思考题：
1. 为什么 Ray 能并行执行？
2. ObjectRef 是什么？为什么需要它？
"""

import ray
import time

def main():
    # 初始化 Ray（单机模式）
    ray.init(ignore_reinit_error=True)
    
    print("=" * 60)
    print("实验 1：理解 ray.remote() 装饰器")
    print("=" * 60)
    
    # 普通函数
    def normal_function(x):
        """普通函数，串行执行"""
        time.sleep(1)  # 模拟耗时操作
        return x * 2
    
    # Ray 远程函数
    @ray.remote
    def remote_function(x):
        """Ray 远程函数，可以并行执行"""
        time.sleep(1)  # 模拟耗时操作
        return x * 2
    
    # ========================================
    # 实验对比
    # ========================================
    print("\n=== 普通函数（串行）===")
    start = time.time()
    results = [normal_function(i) for i in range(5)]
    elapsed = time.time() - start
    print(f"耗时: {elapsed:.2f}秒")
    print(f"结果: {results}")
    
    print("\n=== Ray 远程函数（并行）===")
    start = time.time()
    # 异步调用，返回 ObjectRef
    futures = [remote_function.remote(i) for i in range(5)]
    print(f"Futures 类型: {type(futures[0])}")
    print(f"Futures: {futures}")
    
    # 等待所有结果
    results = ray.get(futures)
    elapsed = time.time() - start
    print(f"耗时: {elapsed:.2f}秒")
    print(f"结果: {results}")
    
    print("\n=== 观察点 ===")
    print("1. 普通函数串行执行，总耗时约 5 秒")
    print("2. Ray 远程函数并行执行，总耗时约 1 秒")
    print("3. remote() 返回 ObjectRef（Future），不是实际结果")
    
    print("\n=== 思考题 ===")
    print("1. 为什么 Ray 能并行执行？")
    print("2. ObjectRef 是什么？为什么需要它？")
    
    ray.shutdown()

if __name__ == "__main__":
    main()

