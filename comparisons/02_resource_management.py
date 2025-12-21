"""
对比实验 2：资源管理对比（静态 vs 动态）

目标：
- 对比 torchrun 的静态资源分配和 Ray 的动态资源管理
- 理解动态调度的优势
- 理解资源利用率的差异

核心差异：
- torchrun: 资源在启动时固定，无法动态调整
- Ray: 可以在运行时动态创建/销毁 Workers，灵活分配资源

观察点：
- torchrun 的资源分配是静态的
- Ray 可以动态调整资源分配
- Ray 的资源利用率更高
"""

import ray
import time

def main():
    print("=" * 60)
    print("对比实验 2：资源管理对比（静态 vs 动态）")
    print("=" * 60)
    
    # ========================================
    # Ray 方式：动态资源管理
    # ========================================
    print("\n=== Ray 方式：动态资源管理 ===")
    ray.init(ignore_reinit_error=True)
    
    @ray.remote(num_gpus=1)
    class TrainingWorker:
        def train(self, epoch):
            time.sleep(0.5)
            return f"Training epoch {epoch}"
    
    @ray.remote(num_gpus=0.5)
    class InferenceWorker:
        def infer(self, data):
            time.sleep(0.2)
            return f"Inference on {data}"
    
    print("\n阶段 1：训练阶段")
    print("创建训练 Workers（每个需要 1 GPU）")
    trainers = [TrainingWorker.remote(i) for i in range(2)]
    train_results = ray.get([t.train.remote(i) for i, t in enumerate(trainers)])
    print(f"训练完成: {len(train_results)} 个结果")
    
    print("\n阶段 2：销毁训练 Workers，释放资源")
    for trainer in trainers:
        ray.kill(trainer)
    print("资源已释放")
    time.sleep(1)
    
    print("\n阶段 3：推理阶段")
    print("创建推理 Workers（每个需要 0.5 GPU）")
    inferencers = [InferenceWorker.remote(i) for i in range(4)]
    infer_results = ray.get([inf.infer.remote(f"data_{i}") for i, inf in enumerate(inferencers)])
    print(f"推理完成: {len(infer_results)} 个结果")
    
    print("\n阶段 4：清理")
    for inf in inferencers:
        ray.kill(inf)
    print("所有资源已释放")
    
    print("\n=== Ray 资源管理特点 ===")
    print("✅ 可以在运行时动态创建/销毁 Workers")
    print("✅ 不同阶段可以使用不同的资源分配")
    print("✅ 资源会被自动回收")
    print("✅ 支持细粒度资源分配（如 0.5 GPU）")
    
    ray.shutdown()
    
    # ========================================
    # torchrun 方式：静态资源分配
    # ========================================
    print("\n=== torchrun 方式：静态资源分配 ===")
    print("""
启动命令:
  torchrun --nproc_per_node=8 --nnodes=2 train.py

特点：
  ❌ 资源在启动时固定（8 个进程 × 2 节点 = 16 个 GPU）
  ❌ 无法在运行时调整资源分配
  ❌ 所有进程必须运行相同代码
  ❌ 无法让不同进程使用不同数量的 GPU
  ❌ 要改变资源分配，必须重启整个训练
    """)
    
    print("\n=== 资源利用率对比 ===")
    print("""
场景：有 16 个 GPU，需要交替进行训练和推理

torchrun 方式：
  - 启动时分配：16 个 GPU 全部用于训练
  - 训练阶段：16 个 GPU 全部使用 ✅
  - 推理阶段：16 个 GPU 全部使用，但推理只需要 8 个 GPU ❌
  - 资源利用率：50%（推理阶段浪费 8 个 GPU）

Ray 方式：
  - 训练阶段：创建 16 个训练 Workers（16 GPU）✅
  - 销毁训练 Workers，释放资源
  - 推理阶段：创建 32 个推理 Workers（16 GPU，每个 0.5 GPU）✅
  - 资源利用率：100%（所有阶段都充分利用）
    """)
    
    print("\n=== 关键差异总结 ===")
    print("""
┌─────────────────────────────────────────────────────────┐
│ 维度              │ torchrun            │ Ray             │
├─────────────────────────────────────────────────────────┤
│ 资源分配时机      │ 启动时固定          │ 运行时动态       │
│ 资源调整          │ 需要重启            │ 无需重启         │
│ 细粒度控制        │ 不支持（进程级）    │ 支持（GPU 级）   │
│ GPU 共享          │ 不支持              │ 支持（如 0.5 GPU）│
│ 资源利用率        │ 中等                │ 高               │
│ 适用场景          │ 单一任务类型        │ 多阶段任务       │
└─────────────────────────────────────────────────────────┘
    """)
    
    print("\n=== 实际应用场景 ===")
    print("""
强化学习训练场景：
  - 训练阶段：需要多个 Actor Workers（每个 1 GPU）
  - 推理阶段：需要多个 Rollout Workers（每个 0.5 GPU）
  - 评估阶段：需要多个 Eval Workers（每个 0.25 GPU）

torchrun 的问题：
  ❌ 无法在不同阶段使用不同的资源分配
  ❌ 必须为每个阶段启动不同的进程组
  ❌ 资源浪费严重

Ray 的优势：
  ✅ 可以在不同阶段动态调整资源分配
  ✅ 统一管理所有 Workers
  ✅ 资源利用率高
    """)
    
    print("\n=== 总结 ===")
    print("1. torchrun 的资源管理简单但不够灵活")
    print("2. Ray 的动态资源管理更灵活，资源利用率更高")
    print("3. 对于需要多阶段、多任务类型的场景，Ray 有明显优势")

if __name__ == "__main__":
    main()

