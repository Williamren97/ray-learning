"""
实验 12：弹性训练 - 从容重启的务实现实

目标：
- 理解弹性训练的概念和实现方式
- 理解基于重启的弹性训练方案
- 理解检查点保存和恢复机制

核心概念：
- 弹性训练：训练作业能够动态适应集群资源变化
- 基于重启的弹性：通过重启实现弹性，而非在线重构
- 检查点机制：保存训练状态，支持恢复
- 优雅退出：在节点变化时保存状态

观察点：
- 完美的"在线重构"极其困难且脆弱
- 基于重启的弹性方案更健壮和实用
- 短暂的停机换取系统的稳定性

参考：Introduction.md 中的第 5 点
"""

import ray
import time
import signal
import os
import json

def main():
    print("=" * 60)
    print("实验 12：弹性训练 - 从容重启的务实现实")
    print("=" * 60)
    
    ray.init(ignore_reinit_error=True)
    
    # ========================================
    # 模拟检查点机制
    # ========================================
    checkpoint_dir = "./checkpoints"
    os.makedirs(checkpoint_dir, exist_ok=True)
    
    def save_checkpoint(epoch, model_state, optimizer_state):
        """保存检查点"""
        checkpoint = {
            "epoch": epoch,
            "model_state": model_state,
            "optimizer_state": optimizer_state,
            "timestamp": time.time()
        }
        checkpoint_path = os.path.join(checkpoint_dir, f"checkpoint_epoch_{epoch}.json")
        with open(checkpoint_path, 'w') as f:
            json.dump(checkpoint, f)
        
        # 保存最新检查点链接
        latest_path = os.path.join(checkpoint_dir, "latest.json")
        with open(latest_path, 'w') as f:
            json.dump(checkpoint, f)
        
        print(f"  ✓ 检查点已保存: epoch {epoch}")
        return checkpoint_path
    
    def load_checkpoint():
        """加载最新检查点"""
        latest_path = os.path.join(checkpoint_dir, "latest.json")
        if os.path.exists(latest_path):
            with open(latest_path, 'r') as f:
                checkpoint = json.load(f)
            print(f"  ✓ 检查点已加载: epoch {checkpoint['epoch']}")
            return checkpoint
        return None
    
    # ========================================
    # 模拟训练 Worker（支持检查点）
    # ========================================
    @ray.remote
    class ElasticTrainer:
        """支持弹性训练的训练器"""
        def __init__(self, worker_id):
            self.worker_id = worker_id
            self.epoch = 0
            self.model_state = {"weights": [1.0, 2.0, 3.0]}
            self.optimizer_state = {"lr": 0.001}
            print(f"Trainer {worker_id} initialized")
        
        def train_epoch(self, epoch):
            """训练一个 epoch"""
            self.epoch = epoch
            # 模拟训练
            time.sleep(0.5)
            # 更新模型状态
            self.model_state["weights"] = [w + 0.1 for w in self.model_state["weights"]]
            return {
                "worker_id": self.worker_id,
                "epoch": epoch,
                "loss": 0.5 - epoch * 0.01
            }
        
        def get_state(self):
            """获取当前状态（用于检查点）"""
            return {
                "epoch": self.epoch,
                "model_state": self.model_state,
                "optimizer_state": self.optimizer_state
            }
        
        def load_state(self, state):
            """加载状态（从检查点恢复）"""
            self.epoch = state["epoch"]
            self.model_state = state["model_state"]
            self.optimizer_state = state["optimizer_state"]
            print(f"  Trainer {self.worker_id} 状态已恢复: epoch {self.epoch}")
    
    # ========================================
    # 模拟弹性训练流程
    # ========================================
    print("\n=== 弹性训练流程演示 ===")
    
    # 步骤 1：启动训练
    print("\n步骤 1：启动训练")
    trainers = [ElasticTrainer.remote(i) for i in range(2)]
    print(f"创建了 {len(trainers)} 个训练器")
    
    # 步骤 2：训练几个 epoch
    print("\n步骤 2：训练 epoch 0-2")
    for epoch in range(3):
        results = ray.get([t.train_epoch.remote(epoch) for t in trainers])
        for result in results:
            print(f"  Trainer {result['worker_id']}: epoch {result['epoch']}, loss {result['loss']:.4f}")
        
        # 定期保存检查点
        if epoch % 2 == 0:
            state = ray.get(trainers[0].get_state.remote())
            save_checkpoint(epoch, state["model_state"], state["optimizer_state"])
    
    # 步骤 3：模拟节点变化（优雅退出）
    print("\n步骤 3：模拟节点变化（收到 SIGTERM 信号）")
    print("  在实际场景中，Kubernetes 或其他调度器会发送 SIGTERM")
    print("  训练器有约 30 秒的时间窗口保存状态")
    
    # 保存当前状态
    state = ray.get(trainers[0].get_state.remote())
    save_checkpoint(state["epoch"], state["model_state"], state["optimizer_state"])
    print("  ✓ 状态已保存，准备优雅退出")
    
    # 步骤 4：清理旧训练器（模拟节点被移除）
    print("\n步骤 4：清理旧训练器（模拟节点被移除）")
    for trainer in trainers:
        ray.kill(trainer)
    print("  旧训练器已清理")
    time.sleep(1)
    
    # 步骤 5：在新节点上恢复训练
    print("\n步骤 5：在新节点上恢复训练（弹性重生）")
    print("  在实际场景中，torchrun 或其他工具会触发弹性重生")
    
    # 加载检查点
    checkpoint = load_checkpoint()
    if checkpoint:
        start_epoch = checkpoint["epoch"] + 1
        print(f"  从 epoch {start_epoch} 继续训练")
        
        # 创建新的训练器（可能在不同的节点上）
        new_trainers = [ElasticTrainer.remote(i) for i in range(2)]
        
        # 恢复状态
        for trainer in new_trainers:
            trainer.load_state.remote({
                "epoch": checkpoint["epoch"],
                "model_state": checkpoint["model_state"],
                "optimizer_state": checkpoint["optimizer_state"]
            })
        
        # 继续训练
        print(f"\n步骤 6：继续训练 epoch {start_epoch}-{start_epoch+2}")
        for epoch in range(start_epoch, start_epoch + 3):
            results = ray.get([t.train_epoch.remote(epoch) for t in new_trainers])
            for result in results:
                print(f"  Trainer {result['worker_id']}: epoch {result['epoch']}, loss {result['loss']:.4f}")
        
        # 清理
        for trainer in new_trainers:
            ray.kill(trainer)
    
    # ========================================
    # 弹性训练的优势
    # ========================================
    print("\n=== 弹性训练的优势 ===")
    print("1. 健壮性：基于重启的方案比在线重构更稳定")
    print("2. 兼容性：与现有批处理调度器（如 Kubernetes）兼容")
    print("3. 简单性：实现相对简单，不容易出错")
    print("4. 实用性：短暂的停机换取系统的稳定性")
    
    print("\n=== 弹性训练的流程 ===")
    print("1. 捕捉信号：系统捕捉到集群节点变化的信号（如 SIGTERM）")
    print("2. 优雅退出：在被强制终止前，保存当前训练状态为检查点")
    print("3. 自动重生：进程干净地退出，触发弹性重生机制")
    print("4. 恢复与适应：新训练作业在新节点上启动，加载检查点，继续训练")
    
    print("\n=== 与在线重构的对比 ===")
    print("在线重构（理论上完美但脆弱）：")
    print("  ❌ 实现极其困难")
    print("  ❌ 容易出错和崩溃")
    print("  ❌ 需要复杂的状态同步机制")
    print("\n基于重启的弹性（务实现实）：")
    print("  ✅ 实现相对简单")
    print("  ✅ 健壮稳定")
    print("  ✅ 与现有系统兼容")
    print("  ✅ 短暂的停机换取稳定性")
    
    # 清理检查点
    import shutil
    if os.path.exists(checkpoint_dir):
        shutil.rmtree(checkpoint_dir)
    
    ray.shutdown()
    
    print("\n=== 思考题 ===")
    print("1. 为什么基于重启的弹性方案比在线重构更实用？")
    print("2. 如何设计一个健壮的检查点机制？")
    print("3. 在什么场景下弹性训练最重要？")

if __name__ == "__main__":
    main()


