import ray
import time

def main():
    ray.init(ignore_reinit_error=True)

    print("=" * 60)
    print("实验 10：Ray Data —— 大数据流水线")
    print("=" * 60)

    # 1. 生成数据 (假设这是 10000 个文件路径)
    # .range() 会把数据自动切片，分布到集群的不同节点上
    ds = ray.data.range(10000)
    print(f"数据集已定义: {ds}")
    
    # 2. 定义转换逻辑
    def heavy_processing(row):
        # 模拟耗时计算 (比如读取图片、增强图片)
        time.sleep(0.001) 
        return {"id": row["id"], "square": row["id"] ** 2}

    print("\n>>> 定义 Map 操作 (注意：此刻不会立即执行！)...")
    # map 是并行的。如果有 10 个 CPU，Ray 会启动 10 个 Worker 同时跑
    transformed_ds = ds.map(heavy_processing)
    
    print(">>> 只有当你'消费'数据时，计算才真正开始...")
    
    # 3. 消费数据 (Trigger Execution)
    start = time.time()
    
    # take_all() 会触发计算并将结果拉回主节点 (慎用在大数据上！)
    # 在真实训练中，我们会用 .iter_batches()
    results = transformed_ds.take(5) 
    
    print(f"前 5 个结果: {results}")
    print("正在处理所有数据...")
    
    # 统计一下总数，这会强制流式处理完所有数据
    count = transformed_ds.count()
    
    duration = time.time() - start
    print(f"处理完成！总条数: {count}, 耗时: {duration:.2f}s")
    print(f"吞吐量: {count / duration:.0f} 条/秒")

    print("\n=== Ray Data 的核心优势 ===")
    print("1. 惰性执行：定义管道不消耗资源，iter() 时才跑。")
    print("2. 自动批处理：自动利用 Pandas/Arrow 进行向量化加速。")
    print("3. 异构计算：可以指定 ds.map(fn, num_gpus=1) 轻松调用 GPU 处理数据。")

    ray.shutdown()

if __name__ == "__main__":
    main()