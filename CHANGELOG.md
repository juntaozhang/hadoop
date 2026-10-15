# Changelog

本分支基于 Hadoop 3.1.1

## 20260908 Yarn

| JIRA      | CHERRY PICK FROM | 作用                                  |
|-----------|------------------|---------------------------------------|
| YARN-8629 | 450c791ecf5      | 修复容器清理时删除 Cgroups 失败的问题 |

## 20260910 HDFS RPC 相关优化

| JIRA         | CHERRY PICK FROM | 备注                                                     |
|--------------|------------------|----------------------------------------------------------|
| HDFS-13658   | c0ac0a53370      | 暴露 HighestPriorityLowRedundancy 统计，便于监控重建队列 |
| HDFS-13831   | 2cbc3c7d43c      | 将 block 批量删除增量改为可配置                          |
| HDFS-13051   | 2dd27c999b2      | 修复 edit queue 写满时异步 editlog rolling 死锁          |
| HADOOP-16307 | cf0d5a0e6ee      | 对 FileStatus 的 user/group 名做 intern，降低内存占用    |
| HADOOP-16248 | 55cc35c0e14      | 修复高负载下 MutableQuantiles 内存泄漏                   |
| HDFS-13977   | 9dc921f5e5d      | QJM 输出流覆写 shouldForceSync，修正自动 sync 行为       |
| HDFS-14523   | 41c0757a1f5      | 移除 NetworkTopology 多余读锁，降低 NN 锁竞争            |
| HDFS-15622   | 7a3085d552c      | 迭代重建队列时清理已删除块，避免其滞留并反复占用调度预算 |

## 20260914 HDFS-15553 实现

| JIRA       | 备注                                                                       |
|------------|----------------------------------------------------------------------------|
| HDFS-15553 | 引入 ReadWriteRpcCallQueue，读 RPC 批量合并以提升 NameNode RPC 吞吐        |
| HDFS-13578 | ClientProtocol 方法加 ReadOnly 注解（SBN 读优化，cherry-pick a109f2b32f0） |

## 20261014 Yarn Queue

| JIRA       | 作用                                                                                                                                                         |
|------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| YARN-11828 | 新增 `yarn.scheduler.capacity.non-exclusive-label.queue-limit-enable` 配置，开启后非独占分区的 DEFAULT 请求借用会受队列在该 label 上 maximum-capacity 的限制 |
| YARN-11926 | 修复 `TestRMWebServicesReservation` 因时间戳过期导致的测试失败                                                                                               |