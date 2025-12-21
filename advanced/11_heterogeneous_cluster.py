"""
实验 11：异构集群 - Ray Data 提升 GPU 利用率的"秘密武器"

目标：
- 理解异构集群的概念和优势
- 理解如何将 CPU 节点用于数据处理，GPU 节点用于训练
- 理解 Ray Data 如何实现高效的数据流式传输

核心概念：
- 异构集群：由不同类型节点组成的集群
- CPU 节点：专门负责数据加载和预处理
- GPU 节点：专门负责模型训练
- Ray Data：流式数据传输，打破 CPU/GPU 节点间的依赖

观察点：
- 传统方式：GPU 必须等待同一节点上的 CPU 完成预处理
- 异构集群：CPU 节点和 GPU 节点可以独立扩展
- 资源利用率：异构集群可以显著提高 GPU 利用率

参考：Introduction.md 中的第 3 点
"""

import ray
import time
import numpy as np

def main():
    print("=" * 60)
    print("实验 11：异构集群 - Ray Data 提升 GPU 利用率的秘密武器")
    print("=" * 60)
    
    ray.init(ignore_reinit_error=True)
    
    # ========================================
    # 模拟传统方式：CPU 和 GPU 在同一节点
    # ========================================
    print("\n=== 传统方式：CPU 和 GPU 在同一节点 ===")
    
    @ray.remote(num_cpus=2)
    def cpu_preprocess(data_batch):
        """CPU 节点进行数据预处理"""
        # 模拟数据预处理（耗时操作）
        time.sleep(0.5)
        processed = [d * 2 for d in data_batch]  # 简单的预处理
        return processed
    
    @ray.remote(num_gpus=1)
    def gpu_train(processed_batch):
        """GPU 节点进行模型训练"""
        # 模拟 GPU 训练（相对快速）
        time.sleep(0.1)
        loss = np.random.rand()
        return loss
    
    print("场景：GPU 必须等待同一节点上的 CPU 完成预处理")
    print("问题：GPU 资源闲置，等待 CPU 处理")
    
    # 模拟传统方式：串行执行
    data_batches = [[i*10+j for j in range(5)] for i in range(4)]
    
    start = time.time()
    for batch in data_batches:
        # CPU 预处理
        processed = ray.get(cpu_preprocess.remote(batch))
        # GPU 训练
        loss = ray.get(gpu_train.remote(processed))
        print(f"  Batch processed, loss: {loss:.4f}")
    
    traditional_time = time.time() - start
    print(f"传统方式总耗时: {traditional_time:.2f}秒")
    print(f"GPU 利用率: 低（大部分时间在等待 CPU）")
    
    # ========================================
    # 异构集群方式：CPU 和 GPU 节点分离
    # ========================================
    print("\n=== 异构集群方式：CPU 和 GPU 节点分离 ===")
    
    @ray.remote(num_cpus=2)
    class CPUPreprocessor:
        """CPU 节点：专门负责数据预处理"""
        def __init__(self, worker_id):
            self.worker_id = worker_id
        
        def preprocess(self, data_batch):
            """数据预处理"""
            time.sleep(0.5)
            processed = [d * 2 for d in data_batch]
            return {
                "worker_id": self.worker_id,
                "processed": processed
            }
    
    @ray.remote(num_gpus=1)
    class GPUTrainer:
        """GPU 节点：专门负责模型训练"""
        def __init__(self, worker_id):
            self.worker_id = worker_id
        
        def train(self, processed_data):
            """模型训练"""
            time.sleep(0.1)
            loss = np.random.rand()
            return {
                "worker_id": self.worker_id,
                "loss": loss
            }
    
    print("场景：CPU 节点和 GPU 节点分离，可以独立扩展")
    print("优势：")
    print("  1. CPU 节点可以并行处理多个批次")
    print("  2. GPU 节点可以持续训练，不等待 CPU")
    print("  3. 可以独立扩展 CPU 或 GPU 节点")
    
    # 创建异构 Workers
    cpu_workers = [CPUPreprocessor.remote(i) for i in range(4)]  # 4 个 CPU Workers
    gpu_workers = [GPUTrainer.remote(i) for i in range(2)]      # 2 个 GPU Workers
    
    print(f"\n创建了 {len(cpu_workers)} 个 CPU Workers 和 {len(gpu_workers)} 个 GPU Workers")
    
    # 并行处理：CPU Workers 预处理，GPU Workers 训练
    start = time.time()
    
    # CPU Workers 并行预处理
    cpu_futures = [cpu_workers[i % len(cpu_workers)].preprocess.remote(batch) 
                   for i, batch in enumerate(data_batches)]
    
    # 流式处理：一旦有预处理完成的数据，立即交给 GPU 训练
    results = []
    while cpu_futures:
        # 等待至少一个 CPU 任务完成
        ready, cpu_futures = ray.wait(cpu_futures, num_returns=1, timeout=10.0)
        if ready:
            processed_data = ray.get(ready[0])
            # 立即交给 GPU 训练（不等待所有 CPU 任务完成）
            gpu_future = gpu_workers[len(results) % len(gpu_workers)].train.remote(processed_data["processed"])
            results.append(ray.get(gpu_future))
            print(f"  Processed by CPU Worker {processed_data['worker_id']}, "
                  f"trained by GPU Worker {results[-1]['worker_id']}, "
                  f"loss: {results[-1]['loss']:.4f}")
    
    heterogeneous_time = time.time() - start
    print(f"\n异构集群方式总耗时: {heterogeneous_time:.2f}秒")
    print(f"GPU 利用率: 高（持续训练，不等待 CPU）")
    
    # ========================================
    # 性能对比
    # ========================================
    print("\n=== 性能对比 ===")
    print(f"传统方式: {traditional_time:.2f}秒")
    print(f"异构集群: {heterogeneous_time:.2f}秒")
    print(f"性能提升: {(traditional_time / heterogeneous_time - 1) * 100:.1f}%")
    
    print("\n=== 异构集群的优势 ===")
    print("1. 资源利用率高：GPU 不等待 CPU，持续训练")
    print("2. 成本优化：可以使用便宜的 CPU 节点处理数据")
    print("3. 独立扩展：可以根据需求独立扩展 CPU 或 GPU 节点")
    print("4. 流式处理：数据可以流式传输，减少延迟")
    
    print("\n=== 实际应用场景 ===")
    print("在一个 16 节点的集群中：")
    print("  - 增加 4 个 CPU 节点，数据处理吞吐量提升 65%")
    print("  - GPU 节点可以持续训练，不因数据加载而闲置")
    print("  - 硬件成本降低（CPU 节点比 GPU 节点便宜）")
    
    # 清理
    for worker in cpu_workers + gpu_workers:
        ray.kill(worker)
    
    ray.shutdown()
    
    print("\n=== 思考题 ===")
    print("1. 为什么异构集群可以提高 GPU 利用率？")
    print("2. 在什么场景下异构集群的优势最明显？")
    print("3. 如何设计一个异构集群架构？")

if __name__ == "__main__":
    main()


