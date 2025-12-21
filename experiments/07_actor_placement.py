"""
实验 7：理解 Actor 放置策略

目标：
- 理解如何控制 Actor 的放置位置
- 理解 Placement Groups 的使用
- 理解不同放置策略的区别

注意：此实验需要多节点 Ray 集群才能观察到效果

观察点：
- 默认情况下 Ray 自动选择节点
- Placement Groups 可以控制 Actor 的放置策略
- 不同策略（PACK、SPREAD、STRICT_SPREAD）有不同的行为

思考题：
1. 什么时候需要手动控制 Actor 放置？
2. 不同放置策略的适用场景是什么？
"""

import ray
import socket

def main():
    # 尝试连接到集群
    try:
        ray.init(address="auto", ignore_reinit_error=True)
        print("已连接到 Ray 集群")
    except:
        ray.init(ignore_reinit_error=True)
        print("使用单机模式（无法观察多节点放置）")
    
    print("=" * 60)
    print("实验 7：理解 Actor 放置策略")
    print("=" * 60)
    
    @ray.remote
    class StatelessWorker:
        def get_location(self):
            """获取 Actor 所在的节点"""
            return socket.gethostname()
    
    # ========================================
    # 默认放置（Ray 自动选择）
    # ========================================
    print("\n=== 默认 Actor 放置 ===")
    actors1 = [StatelessWorker.remote() for _ in range(4)]
    locations1 = ray.get([a.get_location.remote() for a in actors1])
    print(f"Actor locations: {locations1}")
    
    # 统计分布
    location_counts = {}
    for loc in locations1:
        location_counts[loc] = location_counts.get(loc, 0) + 1
    print(f"节点分布: {location_counts}")
    
    # ========================================
    # 使用 Placement Groups 控制放置
    # ========================================
    print("\n=== 使用 Placement Groups ===")
    try:
        # 创建一个 Placement Group，要求 Actor 分散在不同节点
        # STRICT_SPREAD: 强制分散到不同节点
        pg = ray.util.placement_group(
            [{"CPU": 1}] * 4, 
            strategy="STRICT_SPREAD"
        )
        
        # 等待资源就绪
        ray.get(pg.ready())
        print("Placement Group 已就绪")
        
        # 在 Placement Group 中创建 Actor
        actors2 = [
            StatelessWorker.options(placement_group=pg).remote() 
            for _ in range(4)
        ]
        locations2 = ray.get([a.get_location.remote() for a in actors2])
        print(f"Actor locations (STRICT_SPREAD): {locations2}")
        
        # 统计分布
        location_counts2 = {}
        for loc in locations2:
            location_counts2[loc] = location_counts2.get(loc, 0) + 1
        print(f"节点分布: {location_counts2}")
        
        # 清理
        ray.util.remove_placement_group(pg)
        
    except Exception as e:
        print(f"Placement Group 创建失败（可能是单机模式）: {e}")
        print("提示：需要多节点集群才能使用 STRICT_SPREAD 策略")
    
    print("\n=== Placement Group 策略说明 ===")
    print("1. PACK: 尽量将资源打包到同一节点")
    print("2. SPREAD: 尽量分散到不同节点（如果可能）")
    print("3. STRICT_SPREAD: 强制分散到不同节点（需要足够节点）")
    
    print("\n=== 观察点 ===")
    print("1. 默认情况下 Ray 自动选择节点")
    print("2. Placement Groups 可以控制 Actor 的放置策略")
    print("3. 不同策略有不同的行为")
    
    print("\n=== 思考题 ===")
    print("1. 什么时候需要手动控制 Actor 放置？")
    print("2. 不同放置策略的适用场景是什么？")
    
    ray.shutdown()

if __name__ == "__main__":
    main()

