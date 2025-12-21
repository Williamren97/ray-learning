"""
实验 6：理解任务调度和负载均衡

目标：
- 观察 Ray 如何调度任务到不同节点
- 理解负载均衡机制
- 理解分布式执行

注意：此实验需要多节点 Ray 集群
如果只有单机，任务会在同一节点执行

观察点：
- Ray 会自动将任务分配到不同节点
- 负载均衡是自动的
- 可以通过 Dashboard 观察任务分布

思考题：
1. Ray 如何决定任务放在哪个节点？
2. 如何手动指定任务运行的节点？
"""

import ray
import time
import os

def main():
    # 尝试连接到集群，如果失败则使用单机模式
    try:
        ray.init(address="auto", ignore_reinit_error=True)
        print("已连接到 Ray 集群")
    except:
        ray.init(ignore_reinit_error=True)
        print("使用单机模式（无法观察多节点调度）")
    
    print("=" * 60)
    print("实验 6：理解任务调度和负载均衡")
    print("=" * 60)
    
    @ray.remote
    def get_node_info():
        """获取任务执行的节点信息"""
        import socket
        import os
        return {
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "time": time.time()
        }
    
    # 创建多个任务
    print("\n=== 任务调度到不同节点 ===")
    num_tasks = 10
    futures = [get_node_info.remote() for _ in range(num_tasks)]
    results = ray.get(futures)
    
    # 统计每个节点的任务数
    node_counts = {}
    for result in results:
        hostname = result["hostname"]
        node_counts[hostname] = node_counts.get(hostname, 0) + 1
    
    print(f"\n任务分布:")
    for hostname, count in node_counts.items():
        print(f"  {hostname}: {count} 个任务")
    print(f"\n总节点数: {len(node_counts)}")
    print(f"总任务数: {num_tasks}")
    
    # 显示详细信息
    print("\n=== 任务详细信息 ===")
    for i, result in enumerate(results[:5]):  # 只显示前 5 个
        print(f"任务 {i}: hostname={result['hostname']}, pid={result['pid']}")
    
    print("\n=== 观察点 ===")
    if len(node_counts) > 1:
        print("1. Ray 会自动将任务分配到不同节点")
        print("2. 负载均衡是自动的")
        print("3. 可以通过 Dashboard 观察任务分布")
    else:
        print("1. 单机模式下，所有任务在同一节点执行")
        print("2. 要观察多节点调度，需要启动 Ray 集群")
        print("3. 启动集群命令: ray start --head (head节点)")
        print("4. 然后: ray start --address='<head-ip>:6379' (worker节点)")
    
    print("\n=== 思考题 ===")
    print("1. Ray 如何决定任务放在哪个节点？")
    print("2. 如何手动指定任务运行的节点？")
    
    ray.shutdown()

if __name__ == "__main__":
    main()

