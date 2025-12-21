"""
实验 2：理解 Actor 模型（有状态计算）

目标：
- 理解 Actor 如何保持状态
- 理解 Actor 与普通类的区别
- 理解多个 Actor 实例的独立性

观察点：
- 普通类每次创建新实例，状态不共享
- Actor 保持状态，多次调用会累积
- 多个 Actor 实例有独立状态

思考题：
1. Actor 的状态存储在哪里？
2. 为什么需要 Actor 而不是普通函数？
"""

import ray
import time

def main():
    ray.init(ignore_reinit_error=True)
    
    print("=" * 60)
    print("实验 2：理解 Actor 模型（有状态计算）")
    print("=" * 60)
    
    # ========================================
    # 普通类（无状态，每次调用都是新实例）
    # ========================================
    class NormalCounter:
        def __init__(self):
            self.count = 0
        
        def increment(self):
            self.count += 1
            return self.count
    
    # ========================================
    # Ray Actor（有状态，保持实例）
    # ========================================
    @ray.remote
    class ActorCounter:
        def __init__(self):
            self.count = 0
        
        def increment(self):
            self.count += 1
            return self.count
        
        def get_count(self):
            return self.count
    
    print("\n=== 普通类（无状态）===")
    counter1 = NormalCounter()
    counter2 = NormalCounter()
    print(f"counter1.increment(): {counter1.increment()}")  # 1
    print(f"counter2.increment(): {counter2.increment()}")  # 1 (独立实例)
    print(f"counter1.increment(): {counter1.increment()}")  # 2
    print(f"counter2.get_count(): {counter2.count}")  # 1 (独立状态)
    
    print("\n=== Ray Actor（有状态）===")
    actor = ActorCounter.remote()
    print(f"第一次 increment: {ray.get(actor.increment.remote())}")  # 1
    print(f"第二次 increment: {ray.get(actor.increment.remote())}")    # 2
    print(f"第三次 increment: {ray.get(actor.increment.remote())}")    # 3
    print(f"当前值 get_count: {ray.get(actor.get_count.remote())}")   # 3
    
    # 创建多个 Actor 实例
    print("\n=== 多个 Actor 实例（独立状态）===")
    actors = [ActorCounter.remote() for _ in range(3)]
    results = [a.increment.remote() for a in actors]
    print(f"多个 Actor increment: {ray.get(results)}")  # [1, 1, 1] (每个 Actor 独立状态)
    
    # 再次调用同一个 Actor
    print("\n=== 同一个 Actor 多次调用（状态累积）===")
    results2 = [actors[0].increment.remote() for _ in range(3)]
    print(f"同一个 Actor 多次 increment: {ray.get(results2)}")  # [2, 3, 4]
    
    print("\n=== 观察点 ===")
    print("1. 普通类每次创建新实例，状态不共享")
    print("2. Actor 保持状态，多次调用会累积")
    print("3. 多个 Actor 实例有独立状态")
    
    print("\n=== 思考题 ===")
    print("1. Actor 的状态存储在哪里？")
    print("2. 为什么需要 Actor 而不是普通函数？")
    
    ray.shutdown()

if __name__ == "__main__":
    main()

