# Changelog

本分支基于 Hadoop 3.1.1

## 20260908 Yarn

| JIRA      | FROM        | 作用                                  |
|-----------|-------------|---------------------------------------|
| YARN-8629 | 450c791ecf5 | 修复容器清理时删除 Cgroups 失败的问题 |

## 20260910 HDFS RPC 相关优化

| JIRA         | FROM        | 备注                                                     |
|--------------|-------------|----------------------------------------------------------|
| HDFS-13658   | c0ac0a53370 | 暴露 HighestPriorityLowRedundancy 统计，便于监控重建队列 |
| HDFS-13831   | 2cbc3c7d43c | 将 block 批量删除增量改为可配置                          |
| HDFS-13051   | 2dd27c999b2 | 修复 edit queue 写满时异步 editlog rolling 死锁          |
| HADOOP-16307 | cf0d5a0e6ee | 对 FileStatus 的 user/group 名做 intern，降低内存占用    |
| HADOOP-16248 | 55cc35c0e14 | 修复高负载下 MutableQuantiles 内存泄漏                   |
| HDFS-13977   | 9dc921f5e5d | QJM 输出流覆写 shouldForceSync，修正自动 sync 行为       |
| HDFS-14523   | 41c0757a1f5 | 移除 NetworkTopology 多余读锁，降低 NN 锁竞争            |
| HDFS-15622   | 7a3085d552c | 迭代重建队列时清理已删除块，避免其滞留并反复占用调度预算 |


