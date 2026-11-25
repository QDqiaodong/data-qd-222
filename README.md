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

## 配置组：相互依赖组件的原子发布

一条网关可能同时依赖路由、上游、限流等组件，且某个路由版本会精确要求某一版上游。配置组路径让发布者一次提交相互依赖的整批变更，订阅者只安装自己根组件及其递归依赖闭包内的那部分。配置组有独立的组代数（空组为代数 0），与单通道的 `generation` 互不推进；组绑定只引用 `stage` 产生的同一批不可变 revision，不复制配置正文。所有新表均以 `CREATE TABLE IF NOT EXISTS` 追加，已有数据库无需重建即可使用。

```python
epoch = publisher.publish_group(
    {"route": (revision, {"shared": shared_revision}),   # 整体替换绑定
     "shared": (shared_revision, {}),
     "obsolete": None},                                   # 删除（不存在也算有效变更）
    expected_epoch=epoch)
reply = subscriber.poll_group(("route",))               # 首拉：mode="full"
reply = subscriber.poll_group(("route",), known_epoch=reply["epoch"])  # 无变化时为 None
```

发布对整批生效后的组做校验，而非逐条变更的中间状态：每条 `needs` 必须指向组内同名且版本完全相等的组件（精确版本，不是下限），组内不得有依赖环，未改动的组件也不能因本批失去依赖。一次成功恰好推进一个组代数并返回它，即使提交的绑定与之前完全相同；非法参数、悬空或错版本依赖、环及整组超过 2048 个组件抛 `ValueError`，直接绑定未暂存的 revision 抛 `KeyError`，组代数不符抛 `ConflictingPromotion`，且任何拒绝都不改动组内容、代数与历史。不同连接以同一旧代数竞争时只有一方成功。

`poll_group` 接受 1–16 个互不相同的根组件名（元组，顺序无关），暂未发布的根视为空，允许提前订阅；实例换根时调用方以 `known_epoch=None` 重新整体拉取。回复是同一组代数下的一致快照：

- `full`：首拉，或 `known_epoch` 已早于保留窗口；`put` 为当前完整订阅范围，`remove` 为空，订阅者整体替换这组根管理的本地配置。
- `delta`：按实例实际安装的旧依赖范围与当前范围做差，只下发新增或 revision 变化的组件，`remove`（升序元组）列出已不在范围内的组件。组件仍在组内但已离开本实例范围时也会卸下；只要任一根仍依赖它就不会卸下。即使变化发生在其他订阅范围，也返回空增量和新代数。
- 已报告当前代数时返回 `None`；负数、布尔值、未来代数及非法根参数抛 `ValueError`。轮询与发布重叠时，回复固定在发布前或发布后的单一数据库时点，不会拼接混代内容。

重开数据库后仍可精确追赶最近 256 个组代数（含当期）。窗口装满时，掉出窗口的最早代数被折叠进移动基线（`group_base`），其事件日志被删除——每代只存相对上一代的变更行，从不保存全组副本，因此长期切换的空间开销与代数无关。
