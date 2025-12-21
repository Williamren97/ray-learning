"""
实验 10：模拟强化学习训练场景

目标：
- 综合运用 Ray 的各种特性
- 理解异构 Worker 系统的设计
- 理解实际应用场景

观察点：
- 不同类型的 Worker 可以同时运行
- 资源分配灵活（可以共享 GPU）
- 异步执行提高效率

思考题：
1. 这个场景如何映射到真实的 RLHF 训练？
2. 如何优化资源利用率？
"""

import ray
import numpy as np
import time

def main():
    ray.init(ignore_reinit_error=True)
    
    print("=" * 60)
    print("实验 10：模拟强化学习训练场景")
    print("=" * 60)
    
    # ========================================
    # Actor Worker（策略训练）
    # ========================================
    @ray.remote(num_gpus=1)
    class ActorWorker:
        def __init__(self, worker_id):
            self.worker_id = worker_id
            self.iteration = 0
            # 模拟模型权重
            self.weights = np.random.rand(10)
            print(f"Actor Worker {worker_id} initialized")
        
        def train_step(self, batch):
            """训练一步"""
            self.iteration += 1
            # 模拟训练
            time.sleep(0.1)
            loss = np.random.rand()
            return {
                "worker_id": self.worker_id,
                "iteration": self.iteration,
                "loss": loss
            }
        
        def get_weights(self):
            return self.weights.tolist()
    
    # ========================================
    # Critic Worker（价值估计）
    # ========================================
    @ray.remote(num_gpus=1)
    class CriticWorker:
        def __init__(self, worker_id):
            self.worker_id = worker_id
            self.weights = np.random.rand(5)
            print(f"Critic Worker {worker_id} initialized")
        
        def estimate_value(self, state):
            """估计状态价值"""
            time.sleep(0.05)
            value = np.dot(self.weights, state[:5])
            return {
                "worker_id": self.worker_id,
                "value": float(value)
            }
    
    # ========================================
    # Rollout Worker（样本生成，可以共享 GPU）
    # ========================================
    @ray.remote(num_gpus=0.5)
    class RolloutWorker:
        def __init__(self, worker_id):
            self.worker_id = worker_id
            print(f"Rollout Worker {worker_id} initialized")
        
        def generate_samples(self, prompt):
            """生成样本"""
            time.sleep(0.2)
            return {
                "worker_id": self.worker_id,
                "samples": [f"sample_{i}" for i in range(3)]
            }
    
    print("\n=== 创建异构 Workers ===")
    actors = [ActorWorker.remote(i) for i in range(2)]
    critics = [CriticWorker.remote(i) for i in range(2)]
    rollouts = [RolloutWorker.remote(i) for i in range(4)]
    
    print(f"创建了:")
    print(f"  - {len(actors)} 个 Actor Workers (每个需要 1 GPU)")
    print(f"  - {len(critics)} 个 Critic Workers (每个需要 1 GPU)")
    print(f"  - {len(rollouts)} 个 Rollout Workers (每个需要 0.5 GPU)")
    print(f"总 GPU 需求: {len(actors) + len(critics) + len(rollouts) * 0.5} GPU")
    
    print("\n=== 阶段1：生成样本 ===")
    start = time.time()
    rollout_futures = [r.generate_samples.remote(f"prompt_{i}") for i, r in enumerate(rollouts)]
    rollout_results = ray.get(rollout_futures)
    elapsed = time.time() - start
    print(f"生成了 {len(rollout_results)} 个 rollout 结果 (耗时 {elapsed:.2f}秒)")
    for result in rollout_results[:2]:  # 显示前 2 个
        print(f"  Worker {result['worker_id']}: {len(result['samples'])} samples")
    
    print("\n=== 阶段2：训练 Actor ===")
    start = time.time()
    actor_futures = [a.train_step.remote(f"batch_{i}") for i, a in enumerate(actors)]
    actor_results = ray.get(actor_futures)
    elapsed = time.time() - start
    print(f"训练完成 (耗时 {elapsed:.2f}秒):")
    for result in actor_results:
        print(f"  Worker {result['worker_id']}: iteration {result['iteration']}, loss {result['loss']:.4f}")
    
    print("\n=== 阶段3：估计价值 ===")
    state = np.random.rand(10)
    start = time.time()
    critic_futures = [c.estimate_value.remote(state) for c in critics]
    critic_results = ray.get(critic_futures)
    elapsed = time.time() - start
    print(f"价值估计完成 (耗时 {elapsed:.2f}秒):")
    for result in critic_results:
        print(f"  Worker {result['worker_id']}: value {result['value']:.4f}")
    
    print("\n=== 阶段4：并行执行（模拟真实场景）===")
    print("同时执行 Actor 训练和 Critic 估计")
    start = time.time()
    actor_futures = [a.train_step.remote(f"batch_{i}") for i, a in enumerate(actors)]
    critic_futures = [c.estimate_value.remote(state) for c in critics]
    # 等待所有完成
    all_results = ray.get(actor_futures + critic_futures)
    elapsed = time.time() - start
    print(f"并行执行完成 (耗时 {elapsed:.2f}秒)")
    
    print("\n=== 清理资源 ===")
    all_workers = actors + critics + rollouts
    for worker in all_workers:
        ray.kill(worker)
    print("所有 Workers 已清理")
    
    print("\n=== 观察点 ===")
    print("1. 不同类型的 Worker 可以同时运行")
    print("2. 资源分配灵活（可以共享 GPU）")
    print("3. 异步执行提高效率")
    
    print("\n=== 思考题 ===")
    print("1. 这个场景如何映射到真实的 RLHF 训练？")
    print("2. 如何优化资源利用率？")
    
    ray.shutdown()

if __name__ == "__main__":
    main()

