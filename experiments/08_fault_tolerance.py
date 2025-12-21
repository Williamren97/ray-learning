"""
实验 8：理解容错机制

目标：
- 理解 Ray 如何处理任务失败
- 理解自动重试机制
- 理解 Actor 自动重启机制

观察点：
- 任务失败会自动重试
- Actor 崩溃会自动重启
- 可以配置重试/重启次数

思考题：
1. 重试和重启的区别是什么？
2. 如何区分临时故障和永久故障？
"""

import ray
import random
import time

def main():
    ray.init(ignore_reinit_error=True)
    
    print("=" * 60)
    print("实验 8：理解容错机制")
    print("=" * 60)
    
    # ========================================
    # 任务容错（自动重试）
    # ========================================
    @ray.remote(max_retries=3)  # 最多重试 3 次
    def unreliable_task(task_id):
        """可能失败的任务"""
        if random.random() < 0.3:  # 30% 失败率
            raise RuntimeError(f"Task {task_id} failed!")
        return f"Task {task_id} succeeded"
    
    print("\n=== 任务容错（自动重试）===")
    print("创建 10 个可能失败的任务（30% 失败率，最多重试 3 次）")
    futures = [unreliable_task.remote(i) for i in range(10)]
    
    success_count = 0
    fail_count = 0
    
    try:
        results = ray.get(futures)
        success_count = len(results)
        print(f"成功: {success_count} 个任务")
        for result in results[:5]:  # 显示前 5 个结果
            print(f"  {result}")
    except Exception as e:
        print(f"部分任务失败: {e}")
        # 检查哪些任务成功了
        done, pending = ray.wait(futures, num_returns=len(futures), timeout=5.0)
        success_count = len(done)
        fail_count = len(pending)
        print(f"成功: {success_count}, 失败: {fail_count}")
    
    # ========================================
    # Actor 容错（自动重启）
    # ========================================
    @ray.remote(max_restarts=2)  # 最多重启 2 次
    class UnreliableActor:
        def __init__(self, actor_id):
            self.actor_id = actor_id
            self.call_count = 0
            print(f"Actor {actor_id} initialized (call_count={self.call_count})")
        
        def do_work(self):
            self.call_count += 1
            if random.random() < 0.2:  # 20% 失败率
                raise RuntimeError(f"Actor {self.actor_id} crashed on call {self.call_count}!")
            return f"Actor {self.actor_id} call {self.call_count}"
        
        def get_call_count(self):
            return self.call_count
    
    print("\n=== Actor 容错（自动重启）===")
    print("创建一个可能崩溃的 Actor（20% 失败率，最多重启 2 次）")
    actor = UnreliableActor.remote(1)
    
    for i in range(5):
        try:
            result = ray.get(actor.do_work.remote())
            print(f"  调用 {i+1}: {result}")
            time.sleep(0.1)
        except Exception as e:
            print(f"  调用 {i+1} 失败: {e}")
            time.sleep(0.5)  # 等待 Actor 重启
    
    # 检查 Actor 状态
    try:
        call_count = ray.get(actor.get_call_count.remote())
        print(f"\nActor 最终 call_count: {call_count}")
    except Exception as e:
        print(f"\n无法获取 Actor 状态: {e}")
    
    print("\n=== 容错配置说明 ===")
    print("1. max_retries: 任务失败后最多重试次数")
    print("2. max_restarts: Actor 崩溃后最多重启次数")
    print("3. retry_exceptions: 指定哪些异常需要重试")
    
    print("\n=== 观察点 ===")
    print("1. 任务失败会自动重试")
    print("2. Actor 崩溃会自动重启")
    print("3. 可以配置重试/重启次数")
    
    print("\n=== 思考题 ===")
    print("1. 重试和重启的区别是什么？")
    print("2. 如何区分临时故障和永久故障？")
    
    ray.shutdown()

if __name__ == "__main__":
    main()

