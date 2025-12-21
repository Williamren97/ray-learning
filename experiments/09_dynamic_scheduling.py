"""
实验 9：理解动态资源调度

目标：
- 理解如何动态创建和销毁 Actor
- 理解不同阶段可以使用不同的资源分配
- 理解资源自动回收

观察点：
- 可以动态创建和销毁 Actor
- 不同阶段可以使用不同的资源分配
- 资源会被自动回收

思考题：
1. 动态调度相比静态分配有什么优势？
2. 如何在实际项目中应用动态调度？
"""

import ray
import time

def main():
    ray.init(ignore_reinit_error=True)
    
    print("=" * 60)
    print("实验 9：理解动态资源调度")
    print("=" * 60)
    
    # ========================================
    # 训练 Worker（需要完整 GPU）
    # ========================================
    @ray.remote(num_gpus=1)
    class TrainingWorker:
        def __init__(self, worker_id):
            self.worker_id = worker_id
        
        def train(self, epoch):
            """模拟训练"""
            time.sleep(0.5)
            return f"Worker {self.worker_id}: Epoch {epoch} completed"
    
    # ========================================
    # 推理 Worker（可以共享 GPU）
    # ========================================
    @ray.remote(num_gpus=0.5)
    class InferenceWorker:
        def __init__(self, worker_id):
            self.worker_id = worker_id
        
        def infer(self, data):
            """模拟推理"""
            time.sleep(0.2)
            return f"Worker {self.worker_id}: Inference on {data} completed"
    
    print("\n=== 阶段1：训练阶段 ===")
    print("创建训练 Workers（每个需要 1 个 GPU）")
    # 创建训练 Workers（假设有 2 个 GPU）
    trainers = [TrainingWorker.remote(i) for i in range(2)]
    print(f"创建了 {len(trainers)} 个训练 Workers")
    
    # 执行训练
    train_results = ray.get([t.train.remote(i) for i, t in enumerate(trainers)])
    for result in train_results:
        print(f"  {result}")
    
    print("\n=== 阶段2：销毁训练 Workers ===")
    for trainer in trainers:
        ray.kill(trainer)
    print("训练 Workers 已销毁，资源已释放")
    time.sleep(1)  # 等待资源释放
    
    print("\n=== 阶段3：推理阶段 ===")
    print("创建推理 Workers（每个需要 0.5 个 GPU）")
    # 创建推理 Workers（可以创建更多，因为每个只需要 0.5 GPU）
    inferencers = [InferenceWorker.remote(i) for i in range(4)]
    print(f"创建了 {len(inferencers)} 个推理 Workers（需要 2 个 GPU）")
    
    # 执行推理
    infer_results = ray.get([inf.infer.remote(f"data_{i}") for i, inf in enumerate(inferencers)])
    for result in infer_results:
        print(f"  {result}")
    
    print("\n=== 阶段4：清理 ===")
    for inf in inferencers:
        ray.kill(inf)
    print("所有 Workers 已销毁，资源已释放")
    
    print("\n=== 资源使用对比 ===")
    print("训练阶段: 2 个 Workers × 1 GPU = 2 GPU")
    print("推理阶段: 4 个 Workers × 0.5 GPU = 2 GPU")
    print("同样的 GPU 资源，不同阶段运行不同类型的 Workers")
    
    print("\n=== 观察点 ===")
    print("1. 可以动态创建和销毁 Actor")
    print("2. 不同阶段可以使用不同的资源分配")
    print("3. 资源会被自动回收")
    
    print("\n=== 思考题 ===")
    print("1. 动态调度相比静态分配有什么优势？")
    print("2. 如何在实际项目中应用动态调度？")
    
    ray.shutdown()

if __name__ == "__main__":
    main()

