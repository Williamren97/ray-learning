# Ray 学习快速开始指南

## 5 分钟快速体验

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 运行第一个实验

```bash
# 实验 1：理解 ray.remote() 装饰器
python experiments/01_remote_decorator.py
```

**预期输出**：
- 普通函数串行执行，耗时约 5 秒
- Ray 远程函数并行执行，耗时约 1 秒

### 3. 运行第二个实验

```bash
# 实验 2：理解 Actor 模型
python experiments/02_actor_state.py
```

**预期输出**：
- 普通类每次创建新实例，状态不共享
- Actor 保持状态，多次调用会累积

### 4. 运行对比实验

```bash
# 对比实验 1：编程模型对比
python comparisons/01_programming_model.py
```

**预期输出**：
- Ray 可以定义不同类型的 Workers
- torchrun 所有进程必须运行相同代码

## 完整学习路径

### 第一天：基础概念（1-2 小时）

1. **实验 1**：理解 `ray.remote()` 装饰器
2. **实验 2**：理解 Actor 模型
3. **实验 3**：理解对象存储

**目标**：理解 Ray 的核心概念

### 第二天：资源管理（1-2 小时）

4. **实验 4**：理解资源声明
5. **实验 5**：理解 Actor 资源绑定

**目标**：理解 Ray 的资源管理机制

### 第三天：分布式特性（2-3 小时）

6. **实验 6**：理解任务调度
7. **实验 7**：理解 Actor 放置策略
8. **实验 8**：理解容错机制

**目标**：理解 Ray 的分布式特性

### 第四天：高级应用（2-3 小时）

9. **实验 9**：理解动态资源调度
10. **实验 10**：模拟强化学习训练场景

**目标**：综合运用 Ray 的各种特性

### 第五天：对比分析（2-3 小时）

- **对比实验 1-5**：Ray vs torchrun 深度对比

**目标**：理解 Ray 和 torchrun 的差异和适用场景

## 常见问题

### Q: 实验运行失败怎么办？

**A**: 检查以下几点：
1. Ray 是否正确安装：`pip install ray`
2. 是否有足够的资源（CPU/GPU）
3. 查看错误信息，根据提示修复

### Q: 如何查看 Ray Dashboard？

**A**: 
```bash
# 启动 Ray 时自动启动 Dashboard
ray start --head

# 访问 http://localhost:8265
```

### Q: 如何运行多节点实验？

**A**: 
```bash
# 在 head 节点
ray start --head

# 在 worker 节点
ray start --address='<head-node-ip>:6379'

# 运行实验（会自动连接到集群）
python experiments/06_task_scheduling.py
```

### Q: 实验代码可以修改吗？

**A**: 当然可以！建议：
1. 先运行原始代码，理解基本概念
2. 然后修改参数，观察不同行为
3. 尝试添加新功能，加深理解

## 下一步

完成所有实验后，建议：

1. **阅读源码**：深入理解 Ray 的实现
2. **研究实际项目**：查看 verl 等项目的 Ray 使用
3. **性能优化**：学习如何优化 Ray 应用的性能
4. **参与社区**：贡献代码或帮助他人

## 获取帮助

- **Ray 官方文档**：https://docs.ray.io/
- **Ray GitHub**：https://github.com/ray-project/ray
- **Ray 社区**：https://discuss.ray.io/

