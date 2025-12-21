"""
实验 3：理解对象存储（ray.put() 和 ray.get()）

目标：
- 理解 Ray 如何高效传输数据
- 理解对象存储的零拷贝机制
- 理解多个任务共享对象的优势

观察点：
- 对象存储可以避免重复序列化
- 多个任务可以共享同一个对象引用
- 同节点内可以实现零拷贝

思考题：
1. 对象存储在哪里？内存还是磁盘？
2. 跨节点时数据如何传输？
"""

import ray
import numpy as np
import time

def main():
    ray.init(ignore_reinit_error=True)
    
    print("=" * 60)
    print("实验 3：理解对象存储（ray.put() 和 ray.get()）")
    print("=" * 60)
    
    # 创建大数组
    large_array = np.random.rand(10000, 10000)  # 约 800MB
    print(f"\n数组大小: {large_array.nbytes / 1024**2:.2f} MB")
    
    # ========================================
    # 方法1：直接传递（序列化）
    # ========================================
    @ray.remote
    def process_data_direct(data):
        """直接传递数据，会序列化"""
        return np.sum(data)
    
    # ========================================
    # 方法2：使用对象存储（共享引用）
    # ========================================
    @ray.remote
    def process_data_shared(data_ref):
        """使用对象存储引用，避免重复序列化"""
        data = ray.get(data_ref)  # 从对象存储获取
        return np.sum(data)
    
    print("\n=== 直接传递（序列化）===")
    start = time.time()
    ref1 = process_data_direct.remote(large_array)
    result1 = ray.get(ref1)
    elapsed1 = time.time() - start
    print(f"耗时: {elapsed1:.2f}秒")
    print(f"结果: {result1:.2f}")
    
    print("\n=== 对象存储（共享引用）===")
    # 先放入对象存储
    obj_ref = ray.put(large_array)
    print(f"对象引用类型: {type(obj_ref)}")
    
    start = time.time()
    ref2 = process_data_shared.remote(obj_ref)
    result2 = ray.get(ref2)
    elapsed2 = time.time() - start
    print(f"耗时: {elapsed2:.2f}秒")
    print(f"结果: {result2:.2f}")
    
    # 多个任务共享同一个对象
    print("\n=== 多个任务共享对象 ===")
    start = time.time()
    refs = [process_data_shared.remote(obj_ref) for _ in range(5)]
    results = ray.get(refs)
    elapsed3 = time.time() - start
    print(f"5个任务耗时: {elapsed3:.2f}秒")
    print(f"平均每个任务: {elapsed3/5:.2f}秒")
    print(f"结果: {[f'{r:.2f}' for r in results]}")
    
    print("\n=== 性能对比 ===")
    print(f"直接传递: {elapsed1:.2f}秒")
    print(f"对象存储（单次）: {elapsed2:.2f}秒")
    print(f"对象存储（5次共享）: {elapsed3:.2f}秒")
    print(f"节省时间: {elapsed1*5 - elapsed3:.2f}秒")
    
    print("\n=== 观察点 ===")
    print("1. 对象存储可以避免重复序列化")
    print("2. 多个任务可以共享同一个对象引用")
    print("3. 同节点内可以实现零拷贝")
    
    print("\n=== 思考题 ===")
    print("1. 对象存储在哪里？内存还是磁盘？")
    print("2. 跨节点时数据如何传输？")
    
    ray.shutdown()

if __name__ == "__main__":
    main()

