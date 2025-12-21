"""
对比实验 4：容错能力对比

目标：
- 对比 torchrun 和 Ray 的容错机制
- 理解自动重试和重启的优势
- 理解容错对系统稳定性的影响

核心差异：
- torchrun: 容错需要手动实现，一个进程失败影响所有进程
- Ray: 自动容错，单个 Actor 失败不影响其他 Actors

观察点：
- torchrun 的容错复杂且影响全局
- Ray 的容错简单且影响局部
- Ray 的容错机制更适合生产环境
"""

import ray
import random
import time

def main():
    print("=" * 60)
    print("对比实验 4：容错能力对比")
    print("=" * 60)
    
    # ========================================
    # Ray 方式：自动容错
    # ========================================
    print("\n=== Ray 方式：自动容错 ===")
    ray.init(ignore_reinit_error=True)
    
    # 可能失败的任务（自动重试）
    @ray.remote(max_retries=3)
    def unreliable_task(task_id):
        if random.random() < 0.3:  # 30% 失败率
            raise RuntimeError(f"Task {task_id} failed!")
        return f"Task {task_id} succeeded"
    
    # 可能失败的 Actor（自动重启）
    @ray.remote(max_restarts=2)
    class UnreliableActor:
        def __init__(self, actor_id):
            self.actor_id = actor_id
            self.call_count = 0
        
        def do_work(self):
            self.call_count += 1
            if random.random() < 0.2:  # 20% 失败率
                raise RuntimeError(f"Actor {self.actor_id} crashed!")
            return f"Actor {self.actor_id} call {self.call_count}"
    
    print("\n测试任务容错（自动重试）:")
    futures = [unreliable_task.remote(i) for i in range(10)]
    try:
        results = ray.get(futures)
        print(f"✅ 所有任务成功: {len(results)} 个")
    except Exception as e:
        print(f"⚠️ 部分任务失败: {e}")
    
    print("\n测试 Actor 容错（自动重启）:")
    actor = UnreliableActor.remote(1)
    for i in range(5):
        try:
            result = ray.get(actor.do_work.remote())
            print(f"  调用 {i+1}: {result}")
        except Exception as e:
            print(f"  调用 {i+1} 失败，Actor 会自动重启: {e}")
    
    print("\n=== Ray 容错特点 ===")
    print("✅ 任务失败自动重试（可配置次数）")
    print("✅ Actor 崩溃自动重启（可配置次数）")
    print("✅ 单个 Actor 失败不影响其他 Actors")
    print("✅ 容错配置简单（装饰器参数）")
    
    ray.shutdown()
    
    # ========================================
    # torchrun 方式：手动容错
    # ========================================
    print("\n=== torchrun 方式：手动容错 ===")
    print("""
问题：torchrun 如何处理进程失败？

情况 1：单个进程失败
  ❌ 默认行为：所有进程都会失败
  ❌ 需要从检查点恢复整个训练
  ❌ 浪费大量计算资源

情况 2：需要容错
  ❌ 需要手动实现检查点保存/恢复
  ❌ 需要手动实现进程监控和重启
  ❌ 需要处理进程间同步问题
  ❌ 容错逻辑复杂

示例代码（需要大量手动工作）:
    """)
    print("""
import torch.distributed as dist
import signal
import os

checkpoint_dir = "./checkpoints"

def save_checkpoint(model, optimizer, epoch):
    # 手动保存检查点
    if dist.get_rank() == 0:
        torch.save({
            'epoch': epoch,
            'model_state_dict': model.state_dict(),
            'optimizer_state_dict': optimizer.state_dict(),
        }, f"{checkpoint_dir}/checkpoint_{epoch}.pt")

def load_checkpoint(model, optimizer, epoch):
    # 手动加载检查点
    checkpoint = torch.load(f"{checkpoint_dir}/checkpoint_{epoch}.pt")
    model.load_state_dict(checkpoint['model_state_dict'])
    optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
    return checkpoint['epoch']

def signal_handler(sig, frame):
    # 手动处理信号
    print("Received signal, saving checkpoint...")
    save_checkpoint(model, optimizer, current_epoch)
    dist.destroy_process_group()
    exit(0)

signal.signal(signal.SIGTERM, signal_handler)

def main():
    dist.init_process_group(backend="nccl")
    rank = dist.get_rank()
    
    model = MyModel().to(rank)
    optimizer = torch.optim.Adam(model.parameters())
    
    # 尝试从检查点恢复
    start_epoch = 0
    if os.path.exists(f"{checkpoint_dir}/latest.pt"):
        start_epoch = load_checkpoint(model, optimizer, "latest")
    
    try:
        for epoch in range(start_epoch, 10):
            for batch in dataloader:
                # 训练逻辑
                loss = model(batch)
                loss.backward()
                optimizer.step()
            
            # 定期保存检查点
            if epoch % 5 == 0:
                save_checkpoint(model, optimizer, epoch)
    except Exception as e:
        # 手动处理异常
        print(f"Error: {e}, saving checkpoint...")
        save_checkpoint(model, optimizer, current_epoch)
        raise
    """)
    
    print("\n=== 关键差异对比 ===")
    print("""
┌─────────────────────────────────────────────────────────┐
│ 维度              │ torchrun            │ Ray             │
├─────────────────────────────────────────────────────────┤
│ 容错机制          │ 手动实现             │ 自动处理         │
│ 失败影响范围      │ 全局（所有进程）     │ 局部（单个Actor） │
│ 检查点管理        │ 手动保存/恢复        │ 可选（自动重试） │
│ 恢复时间          │ 慢（分钟级）         │ 快（秒级）       │
│ 代码复杂度        │ 高                  │ 低               │
│ 资源浪费          │ 高（重启所有进程）   │ 低（只重启失败者）│
└─────────────────────────────────────────────────────────┘
    """)
    
    print("\n=== 实际场景对比 ===")
    print("""
场景：16 个 GPU 训练，1 个 GPU 进程崩溃

torchrun 方式：
  1. 检测到进程失败（需要手动实现监控）
  2. 所有 16 个进程停止
  3. 从检查点恢复（需要手动实现）
  4. 重新初始化所有 16 个进程
  5. 恢复训练
  恢复时间：分钟级
  资源浪费：所有进程都需要重启

Ray 方式：
  1. Ray 自动检测 Actor 失败
  2. 自动重启失败的 Actor（其他 15 个继续运行）
  3. 从对象存储恢复状态（如果需要）
  4. 继续训练
  恢复时间：秒级
  资源浪费：只重启失败的 Actor
    """)
    
    print("\n=== 容错配置对比 ===")
    print("""
Ray 容错配置（简单）:
  @ray.remote(max_retries=3, max_restarts=2)
  class MyWorker:
      pass

torchrun 容错配置（复杂）:
  - 需要实现检查点保存逻辑
  - 需要实现检查点加载逻辑
  - 需要实现进程监控
  - 需要实现信号处理
  - 需要实现异常处理
  - 需要处理进程间同步
    """)
    
    print("\n=== 总结 ===")
    print("1. torchrun 的容错需要大量手动工作，复杂且容易出错")
    print("2. Ray 的容错是自动的，简单且高效")
    print("3. 对于生产环境，Ray 的容错机制更有优势")

if __name__ == "__main__":
    main()

