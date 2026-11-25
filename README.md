# data-qd-222 · 配置发布与回滚

配置发布者暂存一份不可变版本，随后用代数比较原子切换当前配置。服务实例通过轮询只接收更新；回滚指向旧内容时仍产生新代数，因此实例能识别这一变更。过时的发布请求会被拒绝。当前是单通道和整份配置替换，未提供实例确认或分批策略。

Python 3.9+，仅标准库。示例使用本仓合成配置和临时数据库。

```sh
python3 -B -m unittest -v
python3 -B demo.py
```

实例依次读到 `(1, 1, ...)`、`(2, 2, ...)`、`(3, 1, ...)`；过时切换会输出 `stale write rejected`。

## 带流量上限的代表性试发

全量切换前可以先走一轮试发：每个可用区恰好一台实例试用候选，同时覆盖清单里出现过的所有配置运行时，且所选实例负载之和不超过预算。择优先最小化总负载，再按区名升序的实例 ID 元组做字典序决胜；同库同时最多一个活动试发，试发期间普通 `poll` 仍只返回上一份全量配置。

```python
trial_id, selected = publisher.begin_canary(
    revision, expected_generation=generation,
    roster=[(instance_id, zone, runtime, load), ...], max_load=budget)
subscriber.poll_canary(trial_id, instance_id)   # None 或 (trial_id, revision, config)
subscriber.ack_canary(trial_id, instance_id)    # 必须先取到候选，重试幂等
publisher.finish_canary(trial_id)               # 集齐确认才原子切全量，返回新代数
publisher.abort_canary(trial_id)                # 只关闭试发，不动全量指针
```

试发名单在开启时快照进数据库（不保存原始清单），活动期间旧 `promote` 会抛 `ConflictingPromotion`；`finish_canary` 在同一事务里关闭试发并推进代数，跨连接竞争时最多一方成功、代数只前进一次。试发 ID 由 `AUTOINCREMENT` 分配，关闭后也不复用；新建指向同一数据库的对象即可继续参与活动试发。试发表仅通过 `CREATE TABLE IF NOT EXISTS` 追加，旧数据库可直接打开。
