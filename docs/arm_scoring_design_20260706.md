# Arm 打分机制设计:方向感知的三层 constraint-first score(2026-07-06)

Last materially modified: 2026-07-27

> 2026-07-27 status: 本文保留参数 racing 与 `paired_daily_selection()` 的历史设计。新 paired 研究统一使用 `build_paired_daily_evidence -> paired_screen_v2`；新 action/OPE family 统一接入 `models.audit.experiment_scorecard`。`paired_screen_v1` 冻结保留复现，v2 是唯一 screening 排名权限，但仍无 panel/live promotion 权限。

本文档整理 2026-07-06 会话中对 arm 评分机制的升级设计。下文第 1--7 节描述的是当时针对 legacy `constraint_first_score` 提出的修复设计，不应解读为 2026-07-27 当前正式打分路径仍然存在相同缺陷。当前权限边界与实现状态见第 8 节。

目标:对每个 arm 测出的 PnL、toxic risk、markout、inventory 等结果, 产出一个**可审计的加权分数**,用于 racing 排序;晋级仍需独立 OOS 确认。

关联:

- 历史问题实现:`research_01_fixed_parameter_racing/parameter_selection.py::constraint_score_rollup`
- 攻防分类与不对称 gate:[offensive_defensive_arm_taxonomy_20260706.md](../research/families/f05_fill_quality_quote_ev/docs/offensive_defensive_arm_taxonomy_20260706.md)
- 数据来源:`research_01_fixed_parameter_racing/campaign_outcome_replay_audit.py` 的 `.daily.csv` / partial daily rows

---

## 1. Legacy constraint_first_score 的六个历史弱点与当前处置

以下六点描述的是 2026-07-06 当时的 legacy `constraint_first_score`，不是当前 `paired_screen_v2` / `experiment_scorecard` 的现状：

1. **量纲曾经混乱**:美元 delta 与比率 delta 直接相加,靠 `120.0 * pause_delta` 这类魔法常数平衡;权重的实际含义随回测窗口长度漂移。
2. **曾经没有噪声归一**:+$5(每天稳定 +$0.06)和 +$5(一天 +$40 其余全负)得分相同。
3. **曾经没有攻防方向感知**:所有 arm 用同一组 gate 与权重,与攻防分类文档矛盾。
4. **曾经缺少 toxic risk / markout 证据**。
5. **曾经没有小样本收缩**:10 天和 89 天的结论等权。
6. **曾经只使用 rollup 聚合行**:丢掉 daily 配对信息,无法度量稳定性。

截至 2026-07-27，这六项在具有当前权限的研究路径中已经处置：

| 历史弱点 | 当前处置 | 当前权威入口 |
|---|---|---|
| 不同量纲直接相加 | paired screen 使用 5/95 winsorized 日度配对 t-stat；action/OPE scorecard 使用带冻结经济尺度的有界无量纲分量，不再用窗口总美元与比例的魔法常数直接相加 | `build_paired_daily_evidence()`；`experiment_scorecard` |
| 忽略日间噪声 | 保留 daily 配对分布、t-stat、median、p10、min、win rate；action/OPE 使用 day-clustered 区间和 lower bound | `paired_screen_v2`；`experiment_scorecard` |
| 无攻防方向感知 | 参数 screen 报告由 fills ratio 与 spread delta 得到实际行为分类；action 研究使用 `action_alpha_v1`、`action_defense_v1`、`action_execution_v1` 及 selective execution profile 的不同 gate/权重 | `paired_screen_v2`；scorecard profiles |
| 缺 toxic/markout | action panel 以 net fill/action value 和 campaign outcome 计入成交经济性；selective execution 另以 toxic-fill log ratio 与 toxic-reduction surplus 的 day-clustered lower bound 做不可补偿 gate。paired screen 若没有可识别 action propensity，仍只拥有 screening 权限，不能用聚合 PnL 冒充 toxic action uplift | `randomized_action_contrast`；`toxic_fill_selectivity`；selective scorecard profiles |
| 无小样本收缩 | 当前统一使用 `sqrt(n_days / (n_days + 8))`，并同时执行 minimum-days/support gate | `experiment_scorecard` |
| 只读 rollup | 当前先构造同 UTC day、同 baseline 的纯 daily paired evidence，再独立筛选和排名 | `build_paired_daily_evidence -> paired_screen_v2` |

旧 `constraint_score_rollup()` 本身没有被改写成新方法，因为旧报告需要可复现；它仅是 legacy fallback。它的存在不表示上述六个缺陷仍属于当前正式机制，也不得据此产生新的正式候选、打开新 panel 或获得 live promotion。

---

## 2. 三层结构总览

```text
Layer 0  有效性检查        → invalid (不打分)
Layer 1  方向感知硬 gate   → fail 则 score = -inf, 记录 gate_notes
Layer 2  加权 t-stat 软分数 → 仅对 gate 通过者, 输出分量分解
Meta     排序≠晋级: 胜率 + 多重检验门槛 + OOS 确认
```

---

## 3. Layer 0:有效性

```text
n_days >= 20        (quick 阶段可放宽到 12, 但标 low_support)
baseline 行存在且同窗口
replay error / 缺日 = 0
```

不满足 → `valid=false`,不进入排序。

---

## 4. Layer 1:方向感知硬 gate

每个 arm 从参数注册表读取 `direction ∈ {offensive, defensive, mixed}` (`ParameterSpec` 新增字段,一次性人工标注),套用不同 gate:

| gate | offensive | defensive | mixed |
|---|---|---|---|
| fills_retention | ≥ 0.95(进攻不该掉量) | ≥ 0.80 | 0.90–1.10 |
| tail_campaign_delta | **≤ 0(严格)** | ≤ +0.5pp | ≤ 0 |
| worst-side markout t | ≥ −1.0 | ≥ −0.5 | ≥ −0.5 |
| inv_time_ratio | ≤ 1.10 | ≤ 1.20 | ≤ 1.10 |
| action mix L1 drift | ≤ 0.06 | ≤ 0.10 | ≤ 0.04 |
| side split min share | ≥ 0.35 | ≥ 0.30 | ≥ 0.35 |
| 必须证明的收益 | —(收益自动可见) | tail/MAE/toxic 至少一项 t ≥ +0.5,否则记 `defense_without_benefit` | 双侧各查 |

设计原理(见攻防分类文档):进攻的成本藏在尾部 → gate 盯尾部; 防守的成本立即可见(掉量)、收益藏在尾部 → gate 盯误杀,并强制证明防守收益存在。

任一 gate 失败 → `hard_gate_pass=false`,`score=-1e9`,原因写入 `gate_notes`。

---

## 5. Layer 2:加权 t-stat 软分数

### 5.1 分量统一为日度配对 t 统计量

对每个指标 $m$,取 arm 与 baseline 的**日度配对差** $d_1..d_n$ (同一 UTC day、同一窗口,来自 campaign audit daily rows):

$$
\tilde t_m = \mathrm{clip}\left(
\frac{\mathrm{mean}(d^{w})}{\mathrm{std}(d^{w})/\sqrt{n}},\ -3,\ +3
\right)
$$

- $d^w$:5/95 分位 winsorize 后的日度差(防单日爆表);
- clip ±3:**任何单一指标都买不通总分**;
- 退化处理:全零差 → $\tilde t = 0$;std=0 且 mean≠0 → ±3。

t-stat 一次性解决量纲和稳定性:同样的均值改善,日度方差越大分数越低, 稳定性内生于分数,无需另加胜率项。

### 5.2 总分

$$
\mathrm{score} = \underbrace{\sqrt{\frac{n}{n+8}}}_{\text{小样本收缩}}
\times \sum_m w_m(\mathrm{direction}) \cdot \tilde t_m
$$

收缩因子:20 天 ×0.85、40 天 ×0.91、89 天 ×0.96——短窗结论自动降权。

### 5.3 分量与方向权重($\sum |w| = 1$)

| 分量(日度配对 delta) | 方向 | offensive | defensive | mixed |
|---|---|---:|---:|---:|
| terminal_campaign_pnl | + | .20 | .20 | .20 |
| raw_pnl | + | .10 | .08 | .10 |
| inv_adj | + | .05 | .05 | .05 |
| worst-side 30s markout(取 BUY/SELL 较差侧) | + | .15 | .10 | .12 |
| toxic fill share(高毒桶成交占比) | − | .12 | .07 | .10 |
| tail_campaign_rate | − | .15 | .08 | .12 |
| campaign MAE | − | .08 | .05 | .06 |
| abs inventory time | − | .10 | .05 | .08 |
| fills retention 偏离方向目标 | − | .05 | .20 | .12 |
| false-block positives(被挡正 markout fill) | − | 0 | .12 | .05 |

关键取舍:

- **toxic risk 走两个通道**:高毒桶成交占比(连续、日度可配对)+ tail campaign rate(离散尾部);
- **markout 取较差侧**而非平均——防止一侧改善掩盖另一侧恶化;
- **InvAdj 只给 0.05**:它是路径分解不是风险调整(项目既有口径), 现行 0.25 权重过高;
- **fills retention 在防守臂权重最高(0.20)**——防守臂最大的骗分方式是少成交。

---

## 6. Meta 规则(防自欺)

1. **score 只用于排序**。晋级另需: gate 通过 + score > 0 + 主指标(terminal campaign PnL)日度胜率 ≥ 55% + OOS 确认;
2. **多重检验门槛**:一轮 racing $N$ 个 arm 时,候选晋级要求主指标 $\tilde t \ge \sqrt{2\ln N}$(N=30 → 约 2.6)。把「跑得多」自动折算成「要求高」,同时衔接假设检验全局记账缺口;
3. **输出必须带分量分解**:分数永远可审计,不输出孤立数字。

---

## 7. 输出 schema

```text
arm, direction, n_days, valid, hard_gate_pass, gate_notes,
t_terminal, t_raw, t_inv_adj, t_markout_worst_side, t_toxic_share,
t_tail_rate, t_mae, t_inv_time, t_fills_dev, t_false_block,
contrib_*(=w×t), shrink_factor, score, terminal_daily_win_rate,
promotion_eligible, promotion_notes
```

---

## 8. 当前实现

2026-07-10 实现的第一版选择器现作为历史兼容层保留：

- `research_01_fixed_parameter_racing/parameter_selection.py::paired_daily_selection()`：将任意 campaign `.daily.csv` 重新配对到指定 rolling live baseline；该入口已 deprecated，tier 只供旧报告复现；
- 新入口为 `research_01_fixed_parameter_racing/parameter_selection.py::build_paired_daily_evidence()` 与 `research_01_fixed_parameter_racing/audit/paired_screening.py::screen_paired_daily_arms()`；前者只生成证据，后者通过 `paired_screen_v2` 执行唯一正式筛选和排名；
- `research_01_fixed_parameter_racing/parameter_racing_sweep.py --rescore-daily-csv ... --selection-baseline-arm ...`：不重跑 replay，直接输出新的 paired selection 表；
- 输出将候选分为 `strict_candidate`、`unit_quality_candidate`、`risk_budget_candidate`、`exploratory_pareto`、`mechanism_only` 和 `reject`，不再用一个分数掩盖风险交换；
- 行为方向目前由实际 `fills_ratio + spread_delta` 判为 offensive / defensive / mixed。参数注册表中的人工方向元数据仍是后续增强，不是当前事实。

选择器同时输出：

```text
raw/terminal/InvAdj paired daily delta
5/95 winsorized paired t-stat
daily median / p10 / min / win rate
fills/campaign/inventory-time ratio
pause/keep/place-replace/spread/side-split drift
campaign early-20m MAE ratio / duration ratio
tail-better days / tail-worse days
Pareto front / multiple-test threshold
```

`constraint_score_rollup()` 仍保留给只有 rollup 的旧结果；正式候选选择应优先使用 daily paired selector。

---

## 9. 待确认的两个取舍

1. **权重表数字是起点值**:第一轮跑完后,用「已知好/坏 arm 能否被正确排序」做一次校验再固化;不通过则调权重,而不是调结论。
2. **多重检验门槛**:$\sqrt{2\ln N}$(随 racing 规模自适应)vs 固定 2.0 (更简单),取决于保守程度偏好。

---

## 10. 结论

1. 打分从「美元 + 魔法常数」升级为「日度配对 t-stat + 方向感知权重 + 小样本收缩」,量纲、稳定性、方向性三个问题一次解决。
2. 硬 gate 与软分数分离:gate 回答「机制是否失真、风险是否越界」, 分数只在合格者中排序。
3. 分数永远不等于晋级:胜率、多重检验门槛、OOS 确认是独立关卡。

---

## 11. Rolling Baseline 与“少做少亏”分解

baseline 不是仓库里的静态默认值，而是当前 EC2 live config 的参数快照。live 更新后，下一轮选择必须同步滚动 baseline；旧 CSV 可以重算，但必须显式指定新 baseline arm。

防守 arm 最容易产生一个假象：成交量下降后，总亏损自然变小。为拆开这部分，选择器新增两个活动量归一指标：

$$
\Delta P_{\text{selection}}
=P_{\text{arm}}-P_{\text{base}}
\frac{N_{\text{fill,arm}}}{N_{\text{fill,base}}}
$$

$$
\Delta C_{\text{selection}}
=C_{\text{arm}}-C_{\text{base}}
\frac{N_{\text{campaign,arm}}}{N_{\text{campaign,base}}}
$$

前者回答“保留下来的 fill 是否有更好的单位经济性”，后者回答“保留下来的 campaign 是否有更好的 terminal outcome”。总 PnL 改善但这两个指标都为负的 arm，只能解释为降频风险控制，不能解释为 alpha。

正式流程是：

```text
current live baseline snapshot
-> discovery smoke / retained daily paired evidence
-> activity-adjusted selection quality
-> multiple-test correction
-> fixed blocked historical OOS
-> late holdout
-> live shadow / small exposure
```

任何一层失败就停止；不得回到同一日期集继续调阈值后重新称为 OOS。

---

## 12. Unit-quality hard gate

Historical compatibility output from `paired_daily_selection()` exposes `unit_quality_candidate` instead of requiring old research scripts to rebuild an ad-hoc mask. It has no ranking or promotion authority in v2. The legacy gate is exactly:

```text
activity_adjusted_raw_delta > 0
campaign_adjusted_terminal_delta > 0
fills_ratio >= 0.85
tail_campaign_delta <= 0
campaign_mae_ratio <= 1.25
campaign_duration_ratio <= 1.55
inventory_time_ratio <= 1.25
enough paired days and coverage
```

The selector also emits `unit_quality_notes`, so a failed arm says whether it lost unit economics, killed fills, increased tail, or exceeded a campaign risk budget. A unit-quality pass only earns a blocked-OOS run; it is not a live promotion label.

Historical search-family winners and their exact deltas were removed after the event-clock, feature-ready and empirical-P3 corrections. The gate definition remains useful, but every candidate must be evaluated against the current corrected baseline; old retained/blocked/late rankings cannot be rescored into current evidence.

Operational safeguard: `campaign_outcome_replay_audit.py` now rejects `--trace-fills-max 0`. Campaign labels require fill traces; silently returning zero campaigns would make terminal and tail gates meaningless.

---

## 13. Selective toxicity reduction

For keep/cancel or defensive execution actions, a lower fill count is not an automatic failure. The action must instead show that toxic fills fall faster than all fills on one common randomized decision denominator:

$$
r_F=1-F_1/F_0,\quad r_T=1-T_1/T_0,
$$

$$
S_T=r_T-r_F,\quad
R_T=\log\frac{T_0/F_0}{T_1/F_1}.
$$

The ratio $r_T/r_F$ remains a diagnostic only because it is unstable near zero activity change. Formal scorecards use the day-clustered lower bounds of $S_T$ and $R_T$, plus the conditional-net-value lower bound. A cancel-all policy has $S_T\approx0$ and cannot pass merely by reporting zero toxic fills.

`action_execution_selective_v2` therefore permits large volume loss without a fixed fill-retention hard floor. Candidate-rate, overlap, ESS, native path, tail, and reward gates remain non-compensable.
