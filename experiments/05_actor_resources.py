"""
实验 5：理解 Actor 资源绑定

目标：
- 理解 Actor 如何绑定特定资源
- 理解多个 Actor 如何共享 GPU
- 理解 Actor 资源的生命周期

观察点：
- Actor 在创建时就绑定资源
- 多个 Actor 可以共享同一个 GPU（如果声明 num_gpus=0.5）
- Actor 的资源在整个生命周期内保持不变

思考题：
1. 如何让多个 Actor 共享一个 GPU？
2. Actor 销毁后资源会释放吗？
"""

import ray
import torch
import time

def main():
    ray.init(ignore_reinit_error=True)
    
    print("=" * 60)
    print("实验 5：理解 Actor 资源绑定")
    print("=" * 60)
    
    # ========================================
    # Actor 绑定 GPU
    # ========================================
    @ray.remote(num_gpus=1)
    class GPUWorker:
        def __init__(self, worker_id):
            self.worker_id = worker_id
            if torch.cuda.is_available():
                self.device = torch.device("cuda:0")
                print(f"Worker {worker_id} initialized on {self.device}")
            else:
                self.device = torch.device("cpu")
                print(f"Worker {worker_id} initialized on {self.device}")
        
        def get_device(self):
            return str(self.device)
        
        def compute(self, data):
            """在绑定的设备上计算"""
            if torch.cuda.is_available():
                tensor = torch.tensor(data, dtype=torch.float32).to(self.device)
                result = tensor.sum().item()
                return result
            return sum(data)
    
    # ========================================
    # Actor 共享 GPU（每个 Actor 使用 0.5 GPU）
    # ========================================
    @ray.remote(num_gpus=0.5)
    class SharedGPUWorker:
        def __init__(self, worker_id):
            self.worker_id = worker_id
            if torch.cuda.is_available():
                self.device = torch.device("cuda:0")
                print(f"Shared Worker {worker_id} initialized on {self.device}")
            else:
                self.device = torch.device("cpu")
        
        def get_device(self):
            return str(self.device)
        
        def compute(self, data):
            if torch.cuda.is_available():
                tensor = torch.tensor(data, dtype=torch.float32).to(self.device)
                return tensor.sum().item()
            return sum(data)
    
    print("\n=== 创建 GPU Workers（每个需要 1 个 GPU）===")
    if torch.cuda.is_available():
        num_gpus = torch.cuda.device_count()
        print(f"可用 GPU 数量: {num_gpus}")
        
        # 创建 GPU Workers（最多创建 num_gpus 个）
        num_workers = min(2, num_gpus)
        workers = [GPUWorker.remote(i) for i in range(num_workers)]
        print(f"创建了 {num_workers} 个 GPU Workers")
        
        # 检查每个 Worker 的设备
        devices = ray.get([w.get_device.remote() for w in workers])
        print(f"Worker devices: {devices}")
        
        # 执行计算
        results = ray.get([w.compute.remote([1, 2, 3, 4, 5]) for w in workers])
        print(f"Results: {results}")
        
        print("\n=== 创建共享 GPU Workers（每个需要 0.5 个 GPU）===")
        # 可以创建更多 Workers，因为每个只需要 0.5 GPU
        num_shared_workers = min(4, num_gpus * 2)
        shared_workers = [SharedGPUWorker.remote(i) for i in range(num_shared_workers)]
        print(f"创建了 {num_shared_workers} 个共享 GPU Workers")
        
        # 检查设备
        shared_devices = ray.get([w.get_device.remote() for w in shared_workers])
        print(f"Shared Worker devices: {shared_devices}")
        
        # 执行计算
        shared_results = ray.get([w.compute.remote([1, 2, 3, 4, 5]) for w in shared_workers])
        print(f"Shared Results: {shared_results}")
        
        print("\n=== 资源对比 ===")
        print(f"独占 GPU Workers: {num_workers} 个（需要 {num_workers} 个 GPU）")
        print(f"共享 GPU Workers: {num_shared_workers} 个（需要 {num_shared_workers * 0.5} 个 GPU）")
    else:
        print("No GPU available, using CPU only")
        workers = [GPUWorker.remote(i) for i in range(2)]
        devices = ray.get([w.get_device.remote() for w in workers])
        print(f"Worker devices: {devices}")
    
    print("\n=== 观察点 ===")
    print("1. Actor 在创建时就绑定资源")
    print("2. 多个 Actor 可以共享同一个 GPU（如果声明 num_gpus=0.5）")
    print("3. Actor 的资源在整个生命周期内保持不变")
    
    print("\n=== 思考题 ===")
    print("1. 如何让多个 Actor 共享一个 GPU？")
    print("2. Actor 销毁后资源会释放吗？")
    
    ray.shutdown()

if __name__ == "__main__":
    main()

