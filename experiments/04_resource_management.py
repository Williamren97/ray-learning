"""
实验 4：理解资源声明（CPU/GPU）

目标：
- 理解如何为任务指定资源需求
- 理解 Ray 如何管理资源分配
- 观察资源不足时的行为

观察点：
- 资源声明会影响任务调度
- Ray 会自动管理资源分配
- 资源不足时任务会等待

思考题：
1. Ray 如何知道系统有多少 CPU/GPU？
2. 资源冲突时会发生什么？
"""

import ray
import time
import torch

def main():
    ray.init(ignore_reinit_error=True)
    
    print("=" * 60)
    print("实验 4：理解资源声明（CPU/GPU）")
    print("=" * 60)
    
    # 获取系统资源信息
    print("\n=== 系统资源信息 ===")
    cluster_resources = ray.cluster_resources()
    print(f"CPU 总数: {cluster_resources.get('CPU', 0)}")
    print(f"GPU 总数: {cluster_resources.get('GPU', 0)}")
    print(f"内存总数: {cluster_resources.get('memory', 0) / 1024**3:.2f} GB")
    
    # ========================================
    # 不指定资源（使用默认）
    # ========================================
    @ray.remote
    def cpu_task(task_id):
        """默认资源任务"""
        time.sleep(1)
        return f"Task {task_id} completed"
    
    # ========================================
    # 指定 CPU 资源
    # ========================================
    @ray.remote(num_cpus=2)
    def cpu_intensive_task(task_id):
        """需要 2 个 CPU 的任务"""
        time.sleep(1)
        return f"CPU-intensive task {task_id} completed"
    
    # ========================================
    # 指定 GPU 资源
    # ========================================
    @ray.remote(num_gpus=1)
    def gpu_task(task_id):
        """需要 1 个 GPU 的任务"""
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        time.sleep(0.5)
        return f"GPU task {task_id} on {device}"
    
    print("\n=== 默认资源任务 ===")
    start = time.time()
    futures1 = [cpu_task.remote(i) for i in range(4)]
    results1 = ray.get(futures1)
    elapsed1 = time.time() - start
    print(f"耗时: {elapsed1:.2f}秒")
    print(f"结果: {results1}")
    
    print("\n=== 指定 CPU 资源 ===")
    print("注意：如果系统只有 4 个 CPU，最多同时运行 2 个任务")
    start = time.time()
    futures2 = [cpu_intensive_task.remote(i) for i in range(4)]
    results2 = ray.get(futures2)
    elapsed2 = time.time() - start
    print(f"耗时: {elapsed2:.2f}秒")
    print(f"结果: {results2}")
    
    print("\n=== 指定 GPU 资源 ===")
    if torch.cuda.is_available():
        start = time.time()
        futures3 = [gpu_task.remote(i) for i in range(2)]
        results3 = ray.get(futures3)
        elapsed3 = time.time() - start
        print(f"耗时: {elapsed3:.2f}秒")
        print(f"结果: {results3}")
    else:
        print("No GPU available, skipping GPU tasks")
    
    print("\n=== 观察点 ===")
    print("1. 资源声明会影响任务调度")
    print("2. Ray 会自动管理资源分配")
    print("3. 资源不足时任务会等待")
    
    print("\n=== 思考题 ===")
    print("1. Ray 如何知道系统有多少 CPU/GPU？")
    print("2. 资源冲突时会发生什么？")
    
    ray.shutdown()

if __name__ == "__main__":
    main()

