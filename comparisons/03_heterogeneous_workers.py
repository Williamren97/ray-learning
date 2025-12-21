"""
对比实验 3：异构 Worker 对比

目标：
- 对比 torchrun 的同构 Worker 和 Ray 的异构 Worker
- 理解异构 Worker 系统的设计
- 理解实际应用场景

核心差异：
- torchrun: 所有进程必须运行相同代码（同构）
- Ray: 不同 Actor 可以运行不同代码（异构）

观察点：
- torchrun 无法轻松实现异构 Worker
- Ray 可以轻松定义和管理异构 Worker
- 异构 Worker 在复杂场景中非常重要
"""

import ray
import time

def main():
    print("=" * 60)
    print("对比实验 3：异构 Worker 对比")
    print("=" * 60)
    
    # ========================================
    # Ray 方式：异构 Worker
    # ========================================
    print("\n=== Ray 方式：异构 Worker ===")
    ray.init(ignore_reinit_error=True)
    
    # 定义不同类型的 Workers
    @ray.remote(num_gpus=1)
    class ActorRolloutWorker:
        """Actor Rollout Worker：需要训练和推理"""
        def __init__(self, worker_id):
            self.worker_id = worker_id
            self.iteration = 0
        
        def train(self, batch):
            """训练逻辑"""
            self.iteration += 1
            time.sleep(0.1)
            return f"Actor {self.worker_id}: trained iteration {self.iteration}"
        
        def generate(self, prompt):
            """推理逻辑"""
            time.sleep(0.2)
            return f"Actor {self.worker_id}: generated samples for {prompt}"
    
    @ray.remote(num_gpus=1)
    class CriticWorker:
        """Critic Worker：只需要训练"""
        def __init__(self, worker_id):
            self.worker_id = worker_id
        
        def train(self, batch):
            """训练逻辑（与 Actor 不同）"""
            time.sleep(0.1)
            return f"Critic {self.worker_id}: estimated value for {batch}"
    
    @ray.remote(num_gpus=0.5)
    class RewardModelWorker:
        """Reward Model Worker：只需要推理"""
        def __init__(self, worker_id):
            self.worker_id = worker_id
        
        def compute_reward(self, samples):
            """推理逻辑"""
            time.sleep(0.15)
            return f"RM {self.worker_id}: computed reward for {len(samples)} samples"
    
    @ray.remote(num_gpus=1)
    class RefPolicyWorker:
        """Reference Policy Worker：只读推理"""
        def __init__(self, worker_id):
            self.worker_id = worker_id
        
        def get_log_prob(self, samples):
            """只读推理"""
            time.sleep(0.1)
            return f"RefPolicy {self.worker_id}: log prob for {len(samples)} samples"
    
    print("创建异构 Workers:")
    actors = [ActorRolloutWorker.remote(i) for i in range(4)]
    critics = [CriticWorker.remote(i) for i in range(2)]
    reward_models = [RewardModelWorker.remote(i) for i in range(4)]
    ref_policies = [RefPolicyWorker.remote(i) for i in range(2)]
    
    print(f"  - {len(actors)} 个 ActorRollout Workers (训练+推理)")
    print(f"  - {len(critics)} 个 Critic Workers (只训练)")
    print(f"  - {len(reward_models)} 个 RewardModel Workers (只推理, 0.5 GPU)")
    print(f"  - {len(ref_policies)} 个 RefPolicy Workers (只读推理)")
    
    # 并行执行不同类型的任务
    print("\n并行执行异构任务:")
    start = time.time()
    
    # Actor 训练
    actor_train_futures = [a.train.remote(f"batch_{i}") for i, a in enumerate(actors)]
    # Critic 训练
    critic_train_futures = [c.train.remote(f"batch_{i}") for i, c in enumerate(critics)]
    # Actor 推理
    actor_gen_futures = [a.generate.remote(f"prompt_{i}") for i, a in enumerate(actors)]
    # Reward 计算
    reward_futures = [rm.compute_reward.remote([f"s{i}" for i in range(3)]) for rm in reward_models]
    # RefPolicy 计算
    ref_futures = [rp.get_log_prob.remote([f"s{i}" for i in range(3)]) for rp in ref_policies]
    
    all_results = ray.get(
        actor_train_futures + critic_train_futures + actor_gen_futures + 
        reward_futures + ref_futures
    )
    elapsed = time.time() - start
    
    print(f"所有异构任务完成，耗时: {elapsed:.2f}秒")
    print(f"总任务数: {len(all_results)}")
    
    # 清理
    all_workers = actors + critics + reward_models + ref_policies
    for worker in all_workers:
        ray.kill(worker)
    ray.shutdown()
    
    # ========================================
    # torchrun 方式：同构 Worker
    # ========================================
    print("\n=== torchrun 方式：同构 Worker ===")
    print("""
问题：如何用 torchrun 实现异构 Worker？

方案 1：启动多个独立的进程组
  ❌ 需要手动管理多个进程组
  ❌ 进程组间通信复杂
  ❌ 资源分配不灵活
  ❌ 容错困难

方案 2：所有进程运行相同代码，通过 rank 区分
  ❌ 代码复杂，需要大量 if-else
  ❌ 资源浪费（所有进程都需要完整 GPU）
  ❌ 难以维护

示例代码（复杂且不优雅）:
    """)
    print("""
import torch.distributed as dist

def main():
    dist.init_process_group(backend="nccl")
    rank = dist.get_rank()
    world_size = dist.get_world_size()
    
    # 通过 rank 区分不同类型的 Worker
    if rank < 4:
        # Actor Workers
        if rank % 2 == 0:
            train_actor()
        else:
            generate_samples()
    elif rank < 6:
        # Critic Workers
        train_critic()
    elif rank < 10:
        # Reward Model Workers
        compute_reward()
    else:
        # RefPolicy Workers
        get_log_prob()
    """)
    
    print("\n=== 关键差异对比 ===")
    print("""
┌─────────────────────────────────────────────────────────┐
│ 维度              │ torchrun            │ Ray             │
├─────────────────────────────────────────────────────────┤
│ Worker 类型       │ 必须同构            │ 可以异构        │
│ 代码组织          │ 复杂（if-else）     │ 清晰（类定义）  │
│ 资源分配          │ 不灵活              │ 灵活            │
│ 管理复杂度        │ 高                  │ 低              │
│ 可维护性          │ 低                  │ 高              │
│ 适用场景          │ 简单同构任务        │ 复杂异构任务    │
└─────────────────────────────────────────────────────────┘
    """)
    
    print("\n=== 实际应用场景：RLHF 训练 ===")
    print("""
RLHF 训练需要多种类型的 Workers：

1. ActorRollout Worker
   - 需要训练（梯度计算）
   - 需要推理（KV Cache）
   - 资源：1 GPU, 高内存

2. Critic Worker
   - 只需要训练
   - 较小的模型
   - 资源：1 GPU, 中等内存

3. Reward Model Worker
   - 只需要推理
   - 可以批处理
   - 资源：0.5 GPU, 低内存（可共享）

4. RefPolicy Worker
   - 只读推理
   - 模型固定不变
   - 资源：1 GPU, 只读权重

torchrun 的问题：
  ❌ 无法轻松实现这种异构系统
  ❌ 需要复杂的代码和手动管理
  ❌ 资源利用率低

Ray 的优势：
  ✅ 可以轻松定义和管理异构 Workers
  ✅ 代码清晰，易于维护
  ✅ 资源利用率高
    """)
    
    print("\n=== 总结 ===")
    print("1. torchrun 适合同构任务，异构任务实现困难")
    print("2. Ray 天然支持异构 Worker，代码清晰易维护")
    print("3. 对于需要多种类型 Worker 的复杂场景，Ray 有明显优势")

if __name__ == "__main__":
    main()

