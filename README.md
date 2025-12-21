# Ray 框架渐进式学习指南

> 通过实验一步步深入理解 Ray 分布式计算框架

## 📚 目录

- [简介](#简介)
- [学习路径](#学习路径)
- [实验列表](#实验列表)
- [快速开始](#快速开始)
- [Ray vs torchrun 对比](#ray-vs-torchrun-对比)
- [进阶学习](#进阶学习)

## 简介

本仓库旨在通过**渐进式实验**的方式，帮助开发者深入理解 Ray 框架的核心概念和设计思想。

### 为什么学习 Ray？

- **灵活性**：支持异构任务和动态资源分配
- **易用性**：统一的编程模型，降低分布式开发门槛
- **可扩展性**：从单机到数千节点的扩展能力
- **企业级特性**：容错、监控、多租户支持

### 适用人群

- 想要理解分布式计算框架的开发者
- 需要处理异构任务和复杂工作流的工程师
- 准备使用 Ray 进行 AI/ML 训练的开发者
- 希望对比 Ray 和 torchrun 的开发者

## 学习路径

### 阶段一：基础概念理解（单机实验）
- 实验 1-3：理解核心概念（remote、Actor、对象存储）

### 阶段二：资源管理（单机多 GPU）
- 实验 4-5：理解资源声明和 Actor 资源绑定

### 阶段三：分布式场景（多节点）
- 实验 6-7：理解任务调度和 Actor 放置策略

### 阶段四：高级特性
- 实验 8-9：理解容错机制和动态资源调度

### 阶段五：实际应用场景
- 实验 10：综合应用（模拟强化学习训练）

### 阶段六：对比分析
- 对比实验：Ray vs torchrun 深度对比

### 阶段七：高级主题（生产环境最佳实践）
- 实验 11：异构集群 - Ray Data 提升 GPU 利用率
- 实验 12：弹性训练 - 从容重启的务实现实
- [最佳实践文档](BEST_PRACTICES.md) - 生产环境最佳实践

## 实验列表

### 基础实验

| 实验 | 名称 | 核心概念 | 难度 |
|------|------|----------|------|
| [实验 1](experiments/01_remote_decorator.py) | `ray.remote()` 装饰器 | 任务并行化 | ⭐ |
| [实验 2](experiments/02_actor_state.py) | Actor 模型 | 有状态计算 | ⭐⭐ |
| [实验 3](experiments/03_object_store.py) | 对象存储 | 高效数据传输 | ⭐⭐ |
| [实验 4](experiments/04_resource_management.py) | 资源管理 | CPU/GPU 声明 | ⭐⭐ |
| [实验 5](experiments/05_actor_resources.py) | Actor 资源绑定 | 资源绑定 | ⭐⭐ |

### 高级实验

| 实验 | 名称 | 核心概念 | 难度 |
|------|------|----------|------|
| [实验 6](experiments/06_task_scheduling.py) | 任务调度 | 负载均衡 | ⭐⭐⭐ |
| [实验 7](experiments/07_actor_placement.py) | Actor 放置策略 | Placement Groups | ⭐⭐⭐ |
| [实验 8](experiments/08_fault_tolerance.py) | 容错机制 | 自动重试/重启 | ⭐⭐⭐ |
| [实验 9](experiments/09_dynamic_scheduling.py) | 动态资源调度 | 动态创建/销毁 | ⭐⭐⭐ |
| [实验 10](experiments/10_rl_training.py) | RL 训练场景 | 综合应用 | ⭐⭐⭐⭐ |

### 高级实验（生产环境最佳实践）

| 实验 | 名称 | 核心概念 | 难度 |
|------|------|----------|------|
| [实验 11](advanced/11_heterogeneous_cluster.py) | 异构集群 | Ray Data + GPU 利用率 | ⭐⭐⭐⭐ |
| [实验 12](advanced/12_elastic_training.py) | 弹性训练 | 检查点 + 重启机制 | ⭐⭐⭐⭐ |
| [最佳实践](BEST_PRACTICES.md) | 生产环境最佳实践 | 5 个核心原则 | - |

### 对比实验

| 实验 | 名称 | 对比维度 | 难度 |
|------|------|----------|------|
| [对比 1](comparisons/01_programming_model.py) | 编程模型对比 | SPMD vs Actor | ⭐⭐ |
| [对比 2](comparisons/02_resource_management.py) | 资源管理对比 | 静态 vs 动态 | ⭐⭐⭐ |
| [对比 3](comparisons/03_heterogeneous_workers.py) | 异构 Worker 对比 | 同构 vs 异构 | ⭐⭐⭐ |
| [对比 4](comparisons/04_fault_tolerance.py) | 容错能力对比 | 手动 vs 自动 | ⭐⭐⭐ |
| [对比 5](comparisons/05_dynamic_scheduling.py) | 动态调度对比 | 固定 vs 灵活 | ⭐⭐⭐⭐ |

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 运行基础实验

```bash
# 实验 1：理解 ray.remote()
python experiments/01_remote_decorator.py

# 实验 2：理解 Actor 模型
python experiments/02_actor_state.py

# 实验 3：理解对象存储
python experiments/03_object_store.py
```

### 3. 运行对比实验

```bash
# 对比实验 1：编程模型对比
python comparisons/01_programming_model.py

# 对比实验 2：资源管理对比
python comparisons/02_resource_management.py
```

### 4. 查看 Dashboard

启动 Ray 后，访问 Dashboard：
```bash
ray start --head
# 访问 http://localhost:8265
```

## Ray vs torchrun 对比

### 核心差异总结

| 维度 | Ray | torchrun (DDP) |
|------|-----|----------------|
| **设计目标** | 通用分布式框架 | 专注数据并行训练 |
| **适用场景** | 异构任务、复杂工作流 | 同构训练任务 |
| **编程模型** | Actor + Task | SPMD |
| **资源管理** | 动态、细粒度 | 静态、进程级 |
| **容错能力** | 自动重试、Actor 重建 | 需要手动实现 |
| **可扩展性** | 数千节点 | 数百节点 |

### 详细对比实验

所有对比实验位于 `comparisons/` 目录，每个实验都包含：
- Ray 实现
- torchrun 实现
- 性能对比
- 适用场景分析

## 进阶学习

### 1. 阅读源码

- Ray 源码：https://github.com/ray-project/ray
- 关键文件：
  - `python/ray/__init__.py` - 核心 API
  - `python/ray/actor.py` - Actor 实现
  - `python/ray/object.py` - 对象存储

### 2. 研究实际项目

- verl：https://github.com/volcengine/verl
- 查看 verl 中 Ray 的使用方式

### 3. 性能优化

- 学习如何减少通信开销
- 理解对象存储的优化策略
- 掌握 Placement Groups 的使用

## 实验设计思路

每个实验都遵循以下设计原则：

1. **问题驱动**：从实际问题出发
2. **概念递进**：从简单到复杂
3. **观察明确**：每个实验都有明确的观察点
4. **思考引导**：通过思考题引导深入理解

## 贡献指南

欢迎提交：
- 新的实验案例
- 实验改进建议
- 文档完善
- Bug 修复

## 参考资料

- [Ray 官方文档](https://docs.ray.io/)
- [Ray GitHub](https://github.com/ray-project/ray)
- [Ray 论文](https://arxiv.org/abs/1712.05889)
- [最佳实践文档](BEST_PRACTICES.md) - 生产环境最佳实践
- [Introduction.md](Introduction.md) - 关于 Ray 的 5 个惊人真相

## 许可证

MIT License

