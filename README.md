# data-qd-222 · 配置发布与回滚

配置发布者暂存一份不可变版本，随后用代数比较原子切换当前配置。服务实例通过轮询只接收更新；回滚指向旧内容时仍产生新代数，因此实例能识别这一变更。过时的发布请求会被拒绝。当前是单通道和整份配置替换，未提供实例确认或分批策略。

Python 3.9+，仅标准库。示例使用本仓合成配置和临时数据库。

```sh
python3 -B -m unittest -v                 # 全部验收（含 test_reclaim.py）
python3 -B -m unittest -v test_reclaim    # 仅暂存版本回收
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

## 暂存版本的安全回收

下线服务或结束试发后，废弃版本的正文不会自动消失；但也只有**显式登记**过的版本才可能被回收。登记不删正文，也不撤销任何既有或未来的合法引用：在真正回收前，该版本仍可用原接口发布。

```python
publisher.retire_revisions((revision_a, revision_b))  # 登记待回收，可重复提交
pin_id = publisher.pin_revisions((revision_a,))       # 值班保留单，返回正整数 ID
publisher.read_pin(pin_id)                            # {revision: config}，未知/已释放为 None
publisher.unpin_revisions(pin_id)                     # 首次释放 True，其后/未知 False
publisher.collect_retired(after=0, scan_limit=64)
# {"deleted": (...), "retained": (...), "next_after": ...}
```

- 批量接口的 `revisions` 是 1–32 个互不相同的版本 ID 元组，顺序无关；ID 须为 1 到 2⁶³−1。形状非法、重复、越界或布尔值抛 `ValueError`；批次中含格式合法但不存在的版本抛 `KeyError`，且整批不生效（不会部分登记或部分保留）。
- 保留单不复制正文、不能改成员，重开数据库后仍有效；同一批可以有多个保留单，`unpin` 只释放指定的那一张，只要还有另一张持有该正文就仍受保护。释放不会自动删除版本，也不会取消其待回收登记。
- `collect_retired` 按版本 ID 升序检查严格大于 `after` 的待回收版本，最多 `scan_limit`（1–64）个。受保护项占用检查名额但不删除。若本批之后还有更大的待回收 ID，`next_after` 为本批最后检查的 ID，否则为 `None`；从 `0` 重新开始下一轮即可。合法 `after` 为 0 到 2⁶³−1。
- 保护来源在同一个原子事务里判断并删除：当前全量指针、活动试发候选、任何未释放保留单，以及配置组 256 代保留窗口内任一代（含当期与窗口最老一代）仍绑定的版本。落在窗口外的旧绑定不再保护。并发下先取得引用者保留正文；回收先成功时，随后引用该版本会得到原有的“未知版本”错误。
- 回收不推进任何发布代数、不缩短组窗口、不关闭试发、不影响轮询。删除腾出的空间留在 SQLite 页中供后续复用，不要求数据库文件立即缩小；已删除版本不留永久墓碑。
- `stage` 的版本 ID 高水位单独持久化，即使回收删空库存、重开数据库后再暂存，也会发放比历史上任何已发放 ID 都大的 ID，旧发布请求或旧保留单不会误用新正文。

已有数据库原地升级：新表均以 `CREATE TABLE IF NOT EXISTS` 追加，旧数据及原有调用方式保持不变；升级前暂存、从未登记的版本不会被自动回收。
