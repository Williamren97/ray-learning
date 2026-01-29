既然你已经实践过 **Parquet -> Ray Data -> vLLM** 这条链路，说明你已经处于**离线批量推理（Offline Batch Inference）**的实战前沿了。

这条链路看似简单，但要在 **TB 级数据**上把 vLLM 的性能压榨到极致，还有很多**“深水区”**的门道。

针对你提到的场景，Ray Data 还有以下 **5 个核心进阶点** 值得细说：

---

### 1. 核心模式：Actor-based Map Batches（避免重复加载模型）

这是最关键的一点。vLLM 引擎启动非常重（加载几十 GB 的权重到显存），你绝对不能在普通的函数里调用它。

**错误写法（Function Mode）：**

```python
# ❌ 错误：每次处理一个 batch 都要加载一次模型，极其慢
def process_batch(batch):
    llm = LLM(model="meta-llama/Llama-2-7b-hf") # 每一批数据都重新加载模型！疯了！
    return llm.generate(batch["text"])

ds.map_batches(process_batch, batch_size=32, num_gpus=1)

```

**正确写法（Class Mode / Actor Mode）：**
必须使用 **类（Class）** 来封装 vLLM，利用 Ray 的 Actor 机制让模型常驻显存。

```python
# ✅ 正确：模型只在 __init__ 加载一次，之后一直复用
class VLLMPredictor:
    def __init__(self):
        from vllm import LLM, SamplingParams
        # 初始化 vLLM 引擎，占用显存
        self.llm = LLM(model="meta-llama/Llama-2-7b-hf", tensor_parallel_size=1)
        self.sampling_params = SamplingParams(temperature=0.8, top_p=0.95)

    def __call__(self, batch):
        # batch 是一个字典，比如 {"text": np.array(["prompt1", "prompt2"])}
        prompts = batch["text"].tolist()
        outputs = self.llm.generate(prompts, self.sampling_params)
        
        # 解析 vLLM 的输出对象，转回普通文本
        generated_text = [o.outputs[0].text for o in outputs]
        return {"input": prompts, "output": generated_text}

# concurrency=4 表示启动 4 个 Actor（占用 4 张卡）并行推理
ds.map_batches(
    VLLMPredictor, 
    concurrency=4, 
    num_gpus=1,   # 每个 Actor 分配 1 张卡
    batch_size=128 # 这里的 batch_size 要够大，喂饱 vLLM
)

```

---

### 2. 进阶：多卡推理（Tensor Parallelism）

如果你的模型很大（比如 Llama-3-70B），单张 A100 放不下，需要 4 张卡才能跑起来（TP=4）。Ray Data 怎么配合？

**这就需要在 Ray Data 里申请“大份”的资源：**

```python
class LargeModelPredictor:
    def __init__(self):
        # vLLM 内部会自动处理多卡通信
        self.llm = LLM(model="llama-70b", tensor_parallel_size=4)

# 关键点：告诉 Ray 每个 Actor 需要 4 张卡
ds.map_batches(
    LargeModelPredictor,
    num_gpus=4,      # Ray 会把 4 张卡作为一个原子资源组分配给这个 Actor
    concurrency=2    # 如果你有 8 张卡，就可以同时跑 2 个这样的 Actor
)

```

---

### 3. 性能杀手：Token Length 倾斜（Stragglers）

**问题：** Parquet 里的数据通常是随机排列的。如果一个 batch 里，有 31 条数据长度是 10，有 1 条数据长度是 8000（长文本）。

* **结果：** 整个 batch 的推理时间会被那条 8000 的数据拖死。显存占用也会飙升。

**Ray Data 的解法：局部排序（Local Sort）**
在送给 vLLM 之前，先按长度排个序。

```python
# 1. 读取 Parquet
ds = ray.data.read_parquet("s3://bucket/data")

# 2. 计算长度（非常快的 CPU 操作）
ds = ds.map(lambda row: {"text": row["text"], "length": len(row["text"])})

# 3. 局部排序 (Sort within batch/window)
# 这一步会让长度相近的数据聚在一起，极大提升 vLLM 的 PagedAttention 效率
ds = ds.sort_each_window("length") 

# 4. 送入 vLLM
ds.map_batches(VLLMPredictor, ...)

```

---

### 4. 内存管理：流式落盘（Streaming Output）

当你处理 TB 级数据时，推理结果（Generated Text）也是巨大的。你不能等所有推理都做完才保存。

Ray Data 天生支持**流式写入**：

```python
# 读取 -> 推理 -> 写入
# 整个过程像流水线一样，内存里永远只有少量正在处理的数据
ray.data.read_parquet("input_path") \
    .map_batches(VLLMPredictor, num_gpus=1) \
    .write_parquet("output_path") # 边算边写

```

**注意陷阱：** 如果你的写入速度（写 S3）慢于推理速度，内存会积压。
**优化：** 增加写入的并发度（虽然 `write_parquet` 默认利用多核，但有时受限于网络带宽）。

---

### 5. 终极形态：计算与 IO 分离（CPU/GPU 比例）

在 Parquet -> vLLM 这个场景中，你需要平衡三种资源：

1. **S3/Disk IO**: 读取 Parquet。
2. **CPU**: Tokenizer（分词）、Pre-processing。
3. **GPU**: vLLM 推理。

Ray Data 允许你独立设置它们的并发：

```python
ds = ray.data.read_parquet(..., parallelism=100) # 100 个 CPU 线程读文件

# 复杂的预处理，用 CPU Actor 或 Task
ds = ds.map(heavy_preprocessing, num_cpus=1) 

# GPU 推理，显存打满
ds = ds.map_batches(VLLMPredictor, num_gpus=1, concurrency=8)

```


```python
import ray
import numpy as np

# 1. 定义一个类来封装 GPU 推理逻辑（关键点：让模型常驻显存）
class Predictor:
    def __init__(self):
        # 在 Actor 启动时加载模型，避免每次推理都重新加载
        from vllm import LLM
        self.model = LLM(model="meta-llama/Llama-2-7b-hf")

    def __call__(self, batch):
        # batch 是一个字典，包含从 Parquet 读出来的原始文本
        prompts = batch["text"]
        outputs = self.model.generate(prompts)
        return {"text": prompts, "generated": [o.outputs[0].text for o in outputs]}

# 2. 构建流水线
ds = ray.data.read_parquet("s3://bucket/data.parquet")  # (A) 流式读取

# 3. CPU 预处理 (Tokenizer)
# Ray 会自动启动 CPU Workers 来做这件事，与 GPU 推理并行
ds = ds.map(
    lambda row: {"text": tokenizer(row["raw_text"])},  # (B) CPU Tokenizer
    num_cpus=1  # 指定使用 CPU 资源
)

# 4. GPU 推理 (Inference)
# map_batches 会自动把数据积攒成 Batch，发送给 GPU Actor
ds = ds.map_batches(
    Predictor,            # 使用上面的 Actor 类
    concurrency=4,        # 启动 4 个 GPU Worker 并行
    num_gpus=1,           # 每个 Worker 占用 1 张卡
    batch_size=128        # 批大小，喂饱 GPU
)

# 5. 触发执行（流式落盘）
ds.write_parquet("s3://bucket/output_results")
```

这种做法的精髓（Pipeline Parallelism）
CPU/GPU 异步并行：当 GPU 在计算 Batch N 时，CPU 已经在读取和 Tokenize Batch N+1 和 Batch N+2。GPU 永远不会因为等数据而空转（Starvation）。

对象存储传输：CPU 处理好的 Token ID 会存入 Ray Object Store，GPU Worker 直接通过共享内存读取，极快。

---

*(这里的图解可以帮助你分析：如果 GPU 利用率不到 90%，说明前面的 Read 或 Preprocessing 慢了，需要增加 CPU 资源。)*

---

### 总结

对于你的 **Parquet + vLLM** 场景，检查一下这几点，性能可能还能提升 2-5 倍：

1. **用了 Class 模式吗？** (确保模型常驻)
2. **Batch Size 够大吗？** (vLLM 吞吐量随 Batch Size 增加而显著增加)
3. **做了长度排序吗？** (减少 Padding 浪费)
4. **用了 TP (Tensor Parallel) 吗？** (大模型必须切分)
5. **观察 Dashboard 了吗？** (看是 GPU 在等 CPU，还是 CPU 在等 GPU)

这就是 Ray Data 在这个特定垂直领域的深水区知识。它不只是一个“读取工具”，它是**喂饱 GPU 的高压泵**。