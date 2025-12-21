"""
对比实验 5：动态调度对比（综合场景）

目标：
- 综合对比 Ray 和 torchrun 在实际复杂场景中的表现
- 理解动态调度的优势
- 理解资源利用率的差异

核心差异：
- torchrun: 静态调度，资源固定
- Ray: 动态调度，资源灵活

观察点：
- torchrun 无法适应动态变化的工作负载
- Ray 可以动态调整资源分配
- Ray 的资源利用率更高
"""

import ray
import time

def main():
    print("=" * 60)
    print("对比实验 5：动态调度对比（综合场景）")
    print("=" * 60)
    
    # ========================================
    # Ray 方式：动态调度
    # ========================================
    print("\n=== Ray 方式：动态调度 ===")
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
    
    @ray.remote(num_gpus=0.25)
    class EvalWorker:
        def eval(self, data):
            time.sleep(0.1)
            return f"Eval on {data}"
    
    print("\n=== 场景：多阶段工作流 ===")
    
    # 阶段 1：训练
    print("\n阶段 1：训练（需要 8 个 GPU）")
    trainers = [TrainingWorker.remote(i) for i in range(8)]
    train_results = ray.get([t.train.remote(i) for i, t in enumerate(trainers)])
    print(f"训练完成: {len(train_results)} 个结果")
    
    # 销毁训练 Workers
    for trainer in trainers:
        ray.kill(trainer)
    print("训练 Workers 已销毁，资源释放")
    time.sleep(1)
    
    # 阶段 2：推理
    print("\n阶段 2：推理（需要 4 个 GPU，每个 0.5 GPU）")
    inferencers = [InferenceWorker.remote(i) for i in range(8)]
    infer_results = ray.get([inf.infer.remote(f"data_{i}") for i, inf in enumerate(inferencers)])
    print(f"推理完成: {len(infer_results)} 个结果")
    
    # 销毁推理 Workers
    for inf in inferencers:
        ray.kill(inf)
    print("推理 Workers 已销毁，资源释放")
    time.sleep(1)
    
    # 阶段 3：评估
    print("\n阶段 3：评估（需要 2 个 GPU，每个 0.25 GPU）")
    evals = [EvalWorker.remote(i) for i in range(8)]
    eval_results = ray.get([e.eval.remote(f"data_{i}") for i, e in enumerate(evals)])
    print(f"评估完成: {len(eval_results)} 个结果")
    
    # 清理
    for e in evals:
        ray.kill(e)
    print("所有 Workers 已清理")
    
    print("\n=== Ray 动态调度特点 ===")
    print("✅ 不同阶段可以使用不同的资源分配")
    print("✅ 资源利用率高（每个阶段都充分利用）")
    print("✅ 无需重启，动态调整")
    print("✅ 支持细粒度资源分配")
    
    ray.shutdown()
    
    # ========================================
    # torchrun 方式：静态调度
    # ========================================
    print("\n=== torchrun 方式：静态调度 ===")
    print("""
问题：如何用 torchrun 实现多阶段工作流？

方案 1：为每个阶段启动不同的进程组
  ❌ 需要手动管理多个进程组
  ❌ 进程组间切换复杂
  ❌ 资源分配不灵活

方案 2：所有阶段使用相同的进程组
  ❌ 资源浪费（某些阶段不需要那么多 GPU）
  ❌ 无法动态调整

示例场景：
  - 训练阶段：需要 8 个 GPU（每个 1 GPU）
  - 推理阶段：需要 8 个 Workers（每个 0.5 GPU = 4 GPU）
  - 评估阶段：需要 8 个 Workers（每个 0.25 GPU = 2 GPU）

torchrun 的问题：
  ❌ 如果启动 8 个进程（8 GPU），推理和评估阶段浪费资源
  ❌ 如果启动 4 个进程（4 GPU），训练阶段资源不足
  ❌ 无法在不同阶段使用不同的资源分配
    """)
    
    print("\n=== 资源利用率对比 ===")
    print("""
场景：16 个 GPU，多阶段工作流

阶段 1：训练（需要 16 个 GPU）
阶段 2：推理（需要 8 个 GPU）
阶段 3：评估（需要 4 个 GPU）

torchrun 方式：
  启动：16 个进程（16 GPU）
  阶段 1：16 GPU 全部使用 ✅
  阶段 2：16 GPU 全部使用，但只需要 8 GPU ❌（浪费 50%）
  阶段 3：16 GPU 全部使用，但只需要 4 GPU ❌（浪费 75%）
  平均资源利用率：约 60%

Ray 方式：
  阶段 1：创建 16 个训练 Workers（16 GPU）✅
  阶段 2：销毁训练 Workers，创建 16 个推理 Workers（8 GPU）✅
  阶段 3：销毁推理 Workers，创建 16 个评估 Workers（4 GPU）✅
  平均资源利用率：100%（每个阶段都充分利用）
    """)
    
    print("\n=== 关键差异总结 ===")
    print("""
┌─────────────────────────────────────────────────────────┐
│ 维度              │ torchrun            │ Ray             │
├─────────────────────────────────────────────────────────┤
│ 调度方式          │ 静态（启动时确定）   │ 动态（运行时调整）│
│ 资源调整          │ 需要重启            │ 无需重启         │
│ 多阶段支持        │ 困难                │ 简单             │
│ 资源利用率        │ 中等（60-80%）      │ 高（90-100%）    │
│ 适用场景          │ 单一任务类型        │ 多阶段工作流     │
│ 灵活性            │ 低                  │ 高               │
└─────────────────────────────────────────────────────────┘
    """)
    
    print("\n=== 实际应用场景 ===")
    print("""
强化学习训练（RLHF）：
  1. 样本生成阶段：需要多个 Rollout Workers
  2. 训练阶段：需要多个 Actor 和 Critic Workers
  3. 评估阶段：需要多个 Eval Workers
  4. 不同阶段需要不同的资源分配

torchrun 的问题：
  ❌ 无法适应这种动态变化的工作负载
  ❌ 资源利用率低
  ❌ 需要为每个阶段启动不同的进程组

Ray 的优势：
  ✅ 可以动态调整资源分配
  ✅ 资源利用率高
  ✅ 统一管理所有阶段
    """)
    
    print("\n=== 总结 ===")
    print("1. torchrun 的静态调度简单但不够灵活")
    print("2. Ray 的动态调度灵活且资源利用率高")
    print("3. 对于需要多阶段、动态工作负载的场景，Ray 有明显优势")
    print("4. 选择哪个取决于具体需求：简单固定场景用 torchrun，复杂动态场景用 Ray")

if __name__ == "__main__":
    main()

