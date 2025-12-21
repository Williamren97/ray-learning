"""
对比实验 1：编程模型对比（SPMD vs Actor）

目标：
- 对比 torchrun 的 SPMD 模型和 Ray 的 Actor 模型
- 理解两种编程模型的差异
- 理解各自的适用场景

核心差异：
- torchrun: 所有进程运行相同代码（SPMD）
- Ray: 不同 Actor 可以运行不同代码（Actor 模型）

观察点：
- torchrun 需要所有进程执行相同逻辑
- Ray 可以定义不同类型的 Worker
- Ray 更灵活，但 torchrun 更简单
"""

import ray
import torch
import torch.distributed as dist
import os
import time

def main():
    print("=" * 60)
    print("对比实验 1：编程模型对比（SPMD vs Actor）")
    print("=" * 60)
    
    # ========================================
    # Ray 方式：Actor 模型
    # ========================================
    print("\n=== Ray 方式：Actor 模型 ===")
    ray.init(ignore_reinit_error=True)
    
    # 定义不同类型的 Workers
    @ray.remote(num_gpus=1)
    class ActorWorker:
        """策略训练 Worker"""
        def train(self, batch):
            time.sleep(0.1)
            return f"Actor trained on batch: {batch}"
    
    @ray.remote(num_gpus=1)
    class CriticWorker:
        """价值估计 Worker"""
        def estimate(self, state):
            time.sleep(0.1)
            return f"Critic estimated value for state: {state}"
    
    @ray.remote(num_gpus=0.5)
    class RolloutWorker:
        """样本生成 Worker"""
        def generate(self, prompt):
            time.sleep(0.1)
            return f"Rollout generated samples for prompt: {prompt}"
    
    # 创建不同类型的 Workers
    print("创建异构 Workers:")
    actors = [ActorWorker.remote() for _ in range(2)]
    critics = [CriticWorker.remote() for _ in range(2)]
    rollouts = [RolloutWorker.remote() for _ in range(4)]
    
    print(f"  - {len(actors)} 个 Actor Workers")
    print(f"  - {len(critics)} 个 Critic Workers")
    print(f"  - {len(rollouts)} 个 Rollout Workers")
    
    # 并行执行不同类型的任务
    start = time.time()
    actor_futures = [a.train.remote(f"batch_{i}") for i, a in enumerate(actors)]
    critic_futures = [c.estimate.remote(f"state_{i}") for i, c in enumerate(critics)]
    rollout_futures = [r.generate.remote(f"prompt_{i}") for i, r in enumerate(rollouts)]
    
    all_results = ray.get(actor_futures + critic_futures + rollout_futures)
    elapsed_ray = time.time() - start
    
    print(f"\nRay 执行完成，耗时: {elapsed_ray:.2f}秒")
    print(f"结果数量: {len(all_results)}")
    
    # 清理
    for worker in actors + critics + rollouts:
        ray.kill(worker)
    ray.shutdown()
    
    # ========================================
    # torchrun 方式：SPMD 模型
    # ========================================
    print("\n=== torchrun 方式：SPMD 模型 ===")
    print("注意：torchrun 需要多进程启动，这里仅展示代码结构")
    print("\n启动命令:")
    print("  torchrun --nproc_per_node=8 train.py")
    
    print("\n代码结构 (train.py):")
    print("""
import torch
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel as DDP

def main():
    # 1. 初始化进程组（所有进程执行相同代码）
    dist.init_process_group(backend="nccl")
    rank = dist.get_rank()
    
    # 2. 创建模型（所有进程创建相同模型）
    model = MyModel().to(rank)
    model = DDP(model, device_ids=[rank])
    
    # 3. 训练循环（所有进程执行相同逻辑）
    for epoch in range(10):
        for batch in dataloader:
            loss = model(batch)
            loss.backward()
            optimizer.step()  # DDP 自动同步梯度

if __name__ == "__main__":
    main()
    """)
    
    print("\n=== 关键差异对比 ===")
    print("""
┌─────────────────────────────────────────────────────────┐
│ 维度              │ torchrun (SPMD)    │ Ray (Actor)      │
├─────────────────────────────────────────────────────────┤
│ 编程模型          │ 所有进程相同代码   │ 不同 Actor 不同代码│
│ Worker 类型       │ 必须同构          │ 可以异构          │
│ 代码灵活性        │ 低                │ 高                │
│ 适用场景          │ 数据并行训练       │ 复杂工作流        │
│ 资源分配          │ 静态（启动时确定）│ 动态（运行时调整）│
│ 学习曲线          │ 简单              │ 中等              │
└─────────────────────────────────────────────────────────┘
    """)
    
    print("\n=== 适用场景分析 ===")
    print("""
torchrun 适合：
  ✅ 纯数据并行训练（所有 GPU 做相同工作）
  ✅ 简单的分布式训练场景
  ✅ 对性能要求极高的场景（通信开销最小）

Ray 适合：
  ✅ 异构任务（不同类型的 Worker）
  ✅ 复杂工作流（多阶段、多任务）
  ✅ 需要动态资源调度的场景
  ✅ 强化学习训练（Actor、Critic、Rollout 等）
    """)
    
    print("\n=== 总结 ===")
    print("1. torchrun 的 SPMD 模型简单直接，适合同构任务")
    print("2. Ray 的 Actor 模型灵活强大，适合异构任务和复杂工作流")
    print("3. 选择哪个取决于具体需求：简单训练用 torchrun，复杂场景用 Ray")

if __name__ == "__main__":
    main()

