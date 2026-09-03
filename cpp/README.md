# NarrowGate C++ Extension Migration Plan

Date: 2026-06-16

Last materially modified: 2026-09-03

Status: Maintained architecture guide. Benchmark numbers and migration snapshots remain historical unless a dated current parity artifact explicitly supersedes them.

这份文档说明 `cpp/` 目录里的每个文件应该写什么，以及应该从现有 Python 代码迁移哪一块逻辑。目标不是把整个项目改成 C++，而是把最热、最容易成为回测和运行时瓶颈的纯数值路径迁出去，同时保留 Python 负责数据 IO、模型训练、实验编排和报表。

## 当前权威边界

- 正式 Python→C++ tick replay 只允许 `simulate_tick_arrays_ext_policy_v3`；`bindings_tick_replay.cpp` 中较低层的 `simulate_tick_arrays` 只属于 binding/benchmark/test surface，不能作为正式回测入口。
- direct quote-EV action executor 已从正式 replay ABI 删除。Quote-EV 模型、训练和 shadow evidence 仍保留在 Python/离线研究层。
- 正式策略证据仍由 `models/backtest_tick.py` 负责数据、身份、因果时钟和 fail-fast 校验；C++ 负责同一契约下的热循环。Python/C++ 必须共享输入、artifact、事件顺序和随机路径，不能各自产生一套研究口径。
- 本文 2026-06-16 的 benchmark/golden 数值是迁移历史，不是当前 baseline、live parity 或 promotion authority。

## 当前迁移状态

已完成并有 parity/benchmark 覆盖的部分：

- `strategy/quote_core.py` 的核心 quote math 已迁到 `narrowgate_cpp.compute_quote_core`，并额外提供 `compute_quote_core_batch` 数组批处理接口。
- `strategy/quote_core.py::compute_quote_core` 默认仍走 Python 精确路径；设置 `NARROWGATE_CPP_QUOTE_CORE=1` 时才切到 C++ wrapper，设置 `NARROWGATE_CPP_STRICT=1` 可让 C++ 异常直接抛出。
- `cpp/narrowgate_cpp/tick_replay.cpp` 已接入 `models/backtest_tick.py` 的显式 `--engine cpp` 单次回测入口，覆盖 raw trades、rolling variance/TI/ret²、BBO/L2、exact-level queue、new/cancel latency、maker fill gate、dynamic RQ、BER、fill cooldown、adaptive cooldown、10s markout EMA、replay-side widening、local extreme、fragile/adaptive TTL、position timeout / emergency taker close hooks、库存时间、InvAdj summary 和 C++ trace/fill trace 字段。
- `tests/test_cpp_quote_core_parity.py` 和 `tests/test_cpp_tick_replay_parity.py` 已从 placeholder 改为真实 parity tests。
- `bench/bench_quote_core.py` 和 `bench/bench_tick_replay.py` 已可直接跑速度测试。

当前本机基准结果：

```text
quote core, 100k decisions:
  python scalar:      45,637 quotes/s
  c++ scalar binding: 72,145 quotes/s, 1.58x
  c++ batch:       3,266,808 quotes/s, 71.58x

simplified tick replay, 200k trades:
  python replay: 349,756 trades/s
  c++ replay:  8,372,010 trades/s, 23.94x

data_quality, 500k timestamps:
  python horizon mask: 26,253,207 rows/s
  c++ horizon mask:     4,308,570 rows/s
```

因此后续真正接入完整回测时，优先使用 `compute_quote_core_batch` 或把完整 replay 状态机整体搬进 C++；不要在 Python 主循环里逐笔跨语言调用 scalar binding，那样收益有限。

## 完整 tick 回测双引擎入口

`models/backtest_tick.py` 现在有显式双引擎参数：

```bash
.venv/bin/python models/backtest_tick.py --engine python ...
PYTHONPATH=/tmp/narrowgate_btcusdc_cpp_build:. \
  .venv/bin/python models/backtest_tick.py --engine cpp ...
```

默认仍是 `--engine python`，所以现有训练、A/B、sweep 和报告行为不变。`--engine cpp` 当前只支持单次 backtest；`--sweep`、`--cap-ab`、`--sweep-cooldown` 仍强制走 Python，避免混合口径被误解成完整 parity。

C++ replay 已接入真实回测数据输入和一批核心 policy 状态：

- raw aggTrade time/price/quantity/aggressor side
- rolling variance lookup
- ML prediction lookup
- historical BBO best bid/ask/qty
- historical L2 levels
- exact-level / through-level queue ahead
- new/cancel latency
- maker fill gate
- dynamic requote interval 的快/慢波动 EMA
- BER guard 的快/慢 trade-intensity EMA
- fill cooldown 的连续同侧成交状态
- 10s maker-signed markout pending queue 与 `mo_ema_bid/ask/all`
- replay-side policy widening 后的真实建单价格
- quote EV widen/tighten/pause executor，输入为外部预计算的 per-quote EV/toxic/fill 概率数组
- local extreme guard、fragile TTL、adaptive TTL、adaptive fill cooldown
- position timeout taker close；emergency taker close 为显式 opt-in hook
- quote trace / fill trace 的主要 Python 字段
- inventory-time summary
- final spread cap, adverse guard, defense guard 的 quote-core flags
- inventory-adjusted PnL 拆解

注意：direct quote-EV policy executor 已归档并从 replay ABI 删除。quote-EV 模型、训练和 shadow evidence 仍保留在 Python/离线研究层；正式 C++ replay 只暴露 `simulate_tick_arrays_ext_policy_v3`，不再接受旧 EV action 数组。

### Golden 小窗

当前固定的真实数据 smoke/parity 窗口：

```bash
MM_DATA_ROOT=${NARROWGATE_DATA_ROOT} \
.venv/bin/python models/backtest_tick.py \
  --day 2026-05-01 \
  --start-time '2026-05-01 00:00' \
  --end-time '2026-05-01 00:30' \
  --engine python \
  --min-historical-book-coverage 0

MM_DATA_ROOT=${NARROWGATE_DATA_ROOT} \
PYTHONPATH=/tmp/narrowgate_btcusdc_cpp_build:. \
.venv/bin/python models/backtest_tick.py \
  --day 2026-05-01 \
  --start-time '2026-05-01 00:00' \
  --end-time '2026-05-01 00:30' \
  --engine cpp \
  --min-historical-book-coverage 0
```

2026-06-16 本机结果：

| engine | trades | raw PnL | InvAdj | fills/day | fill rate | avg final spread | avg abs inventory | inv hours | max inventory |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| python | 6,103 | -0.43 | -0.08 | 500.0 | 0.023 | 52.31 | 0.0025 | 0.0014 | 0.003 |
| cpp | 6,103 | -0.44 | -0.08 | 500.0 | 0.022 | 50.87 | 0.0027 | 0.0014 | 0.003 |

解释：这是 2026-06-16 的迁移快照，说明当时 C++ engine 已能跑同一批 raw/BBO/L2 数据；它不再描述当前“剩余差异”。当前 parity 必须由现有 Python/C++ tests、完整日 golden 和冻结 formal-replay identity 给出，不能从这张历史表推断。

## 总原则

1. 先迁纯函数，再迁状态机，最后才迁复杂特征流。
2. Python 继续负责 parquet/csv 读取、pandas 拼接、LightGBM 训练、A/B 编排、报告写入。
3. C++ 负责重复调用很多次的数值计算、订单队列状态机、滚动窗口状态更新。
4. C++ 接口尽量接收标量、结构体、连续 NumPy array，不在热路径里传 Python dict/list。
5. 每一步都必须有 Python parity test；没有 parity 前，Python 旧路径保留为 fallback。
6. 长循环 binding 要释放 GIL；单个 quote 的小函数可以不急着释放，但 batch API 应该释放。

## 目录职责

| 文件 | 作用 | 主要迁移来源 |
| --- | --- | --- |
| `cpp/pyproject.toml` | Python extension 构建入口 | 新增构建配置 |
| `cpp/CMakeLists.txt` | CMake/pybind11 编译配置 | 新增构建配置 |
| `cpp/narrowgate_cpp/common.hpp` | 共用类型、枚举、array view、数值 helper | `strategy/quote_core.py` 的 dataclass 和基础 helper；`data_quality.py` 的连续段语义 |
| `cpp/narrowgate_cpp/quote_core.hpp/.cpp` | AS/GLFT quote core 纯计算 | `strategy/quote_core.py` |
| `cpp/narrowgate_cpp/tick_replay.hpp/.cpp` | tick replay 热循环、队列、成交、summary | `models/backtest_tick.py::simulate_tick` 的核心循环 |
| `cpp/narrowgate_cpp/streaming_features.hpp/.cpp` | rolling/streaming 特征状态更新 | `features/feature_engineer.py` 的 rolling 数值部分；`data_quality.py` 的 gap/horizon 规则 |
| `cpp/narrowgate_cpp/bindings_module.cpp` | 唯一 pybind11 模块入口和注册顺序 | C++ API 到 Python 的薄封装 |
| `cpp/narrowgate_cpp/bindings_*.cpp` | 按功能拆分的 pybind11 注册单元 | 原单一 binding 编译单元 |
| `tests/test_cpp_quote_core_parity.py` | quote core Python/C++ parity | `strategy/quote_core.py` 对照 |
| `tests/test_cpp_tick_replay_parity.py` | tick replay Python/C++ parity | `models/backtest_tick.py::simulate_tick` 对照 |
| `bench/bench_quote_core.py` | quote core 微基准 | Python quote core vs C++ quote core |
| `bench/bench_tick_replay.py` | tick replay 端到端基准 | Python replay vs C++ replay |

## `cpp/pyproject.toml`

这里放 extension 的构建元数据，当前选择 `scikit-build-core + pybind11`。后续一般不用写业务代码，只需要维护：

- Python 包名和版本。
- CMake source dir。
- 构建依赖版本。
- 如果以后要把 `.pyi` 或 Python helper 包进 wheel，再补 `wheel.packages` 或 package data。

建议开发命令：

```bash
.venv/bin/python -m pip install -e ./cpp
```

上述命令生成可移植开发构建。EC2 live release 必须显式选择已实测的
Cascade Lake/256-bit profile；不能把默认 portable wheel 当成生产制品：

```bash
.venv/bin/python -m pip wheel ./cpp \
  --config-settings=cmake.define.NARROWGATE_LIVE_CPU_PROFILE=ec2-cascadelake-avx2
.venv/bin/python -c \
  'import narrowgate_cpp as n; print(n.NATIVE_LIVE_BUILD_PROFILE, n.NATIVE_LIVE_BUILD_COMPILE_OPTIONS, n.NATIVE_LIVE_BUILD_IS_PRODUCTION)'
```

该 profile 只针对 EC2 Linux x86_64 生产环境，整个 native
extension（不只是 quote core）固定使用
`-O3 -march=haswell -mtune=cascadelake -mprefer-vector-width=256
-fno-fast-math -ffp-contract=off -fno-lto`。构建器固定为 EC2 当前的 GNU C++
11.5.0，并要求单配置 Release 构建。它不启用 fast-math 或 AVX-512；Mac arm64 仅使用 portable
构建做语义检查，Azure x86 只用于开发、编译和相对性能基准。
不会为 Mac 或 Azure 降低 EC2 的 ISA/缓存调优。正式 native build
receipt 会拒绝 portable wheel。

这里选择 `-march=haswell` 不是为了兼容旧机器：在生产机 Xeon Platinum
8259CL 上，同一工作负载的 AVX2/256-bit 构建实测快于允许 AVX-512 的
Cascade Lake 构建。`-mtune=cascadelake` 仍按生产微架构调度。CMake 还会
显式关闭 pybind11 默认注入的 LTO，因为同机基准中的 LTO 构建也慢于该非 LTO
profile。只有新的同机基准证明另一组 ISA、向量宽度或 LTO 选择更快时才更换
该 profile。

可在 EC2 生产型号上运行无网络 native 热路径基准。它分别测量订单 action
planner、融合报价决策、gateway 空轮询、SPSC 入队/出队和完整本地 gateway
lifecycle；输出 `ns/op`、x86 TSC `cycles/op`、吞吐和防止循环消除的 checksum：

```bash
cmake -S cpp -B /tmp/ng-native-bench \
  -DCMAKE_BUILD_TYPE=Release \
  -DNARROWGATE_BUILD_NATIVE_BENCHMARKS=ON \
  -DNARROWGATE_LIVE_CPU_PROFILE=ec2-cascadelake-avx2 \
  -DPython_EXECUTABLE="$PWD/.venv/bin/python" \
  -Dpybind11_DIR="$($PWD/.venv/bin/python -m pybind11 --cmakedir)"
cmake --build /tmp/ng-native-bench \
  --target narrowgate_live_native_hot_path_bench -j2
/tmp/ng-native-bench/narrowgate_live_native_hot_path_bench 5000000

# 只在 maker 停止的维护窗口采集 PMU；该 EC2 的两个 vCPU 共享一个物理核。
perf stat -r 7 \
  -e cycles,instructions,cache-references,cache-misses,branches,branch-misses \
  /tmp/ng-native-bench/narrowgate_live_native_hot_path_bench 5000000
```

`gateway_enqueue_begin_complete` 的一个 op 是一次 enqueue、一次 begin/dequeue
以及一次本地 confirmed-not-dispatched 完成，用于安全复位 active slot；它不
包含 TLS、HTTP、交易所 ACK 或网络时间。Mac 的 portable 构建只用于编译和
运行 smoke，不能替代 EC2 同机性能结果。程序内的 `tsc_cycles_per_op` 是
x86 TSC reference cycles；CPU core cycles、cache miss 和 branch miss 必须以
同一台 EC2 上的 `perf stat` 为准，不能拿 Azure 或 Mac 数字替代。

注意：两个 repo 目前都会生成同名 Python module `narrowgate_cpp`。如果同一个 venv 同时安装 BTCUSDC/BTCUSDT 两个 extension，会互相覆盖。建议每个 repo 用独立 venv，或者后续把 module 名改成带 symbol 的名字。

## `cpp/CMakeLists.txt`

这里维护 C++ 编译目标。需要逐步补：

- 编译标准，建议 C++17 起步。
- `pybind11_add_module(narrowgate_cpp ...)` 的源文件列表。
- 后续如果引入 OpenMP、absl、fmt、Eigen、nanobind、LightGBM C API，要在这里显式链接。
- 开发 wheel 使用默认 portable profile；EC2 release 只使用上面冻结并在同型号
  生产 CPU 上实测过的 `ec2-cascadelake-avx2` profile。不要用 portable、Azure
  的 `-march=native` 或 Mac 构建替代 EC2 release artifact。

## `common.hpp`

这里写所有模块共享的轻量类型和 helper。不要在这里放策略业务主逻辑。

建议先放这些类型：

```cpp
namespace narrowgate_cpp {

enum class Side : uint8_t { Buy = 0, Sell = 1 };

struct DepthLevel {
    double price = 0.0;
    double qty = 0.0;
};

struct DepthSnapshot {
    std::vector<DepthLevel> bids;
    std::vector<DepthLevel> asks;
    bool has_book() const;
};

struct QuotePrediction {
    double dir_10s = 0.5;
    double vol_10s = 0.0;
    double ret_10s = 0.0;
    double tox_bid = 0.5;
    double tox_ask = 0.5;
};

struct QuoteState {
    double mid = 0.0;
    double inventory = 0.0;
    double sigma_sq = 0.0;
    double trade_intensity = 0.0;
    double best_bid = 0.0;
    double best_ask = 0.0;
    bool ber_active = false;
    double mo_ema_all = 0.0;
    double mo_ema_bid = 0.0;
    double mo_ema_ask = 0.0;
    double mo_ref = 50.0;
    bool position_open = false;
    double hold_time_s = 0.0;
    double unrealized_pnl = 0.0;
};

double floor_tick(double price, double tick);
double ceil_tick(double price, double tick);
double safe_div(double numerator, double denominator, double fallback = 0.0);

}  // namespace narrowgate_cpp
```

数据连续段与 horizon mask 保持在更快的 NumPy 权威实现中。若 C++ 以后直接生成 labels，必须重新建立独立 parity contract 后再引入。

## `quote_core.hpp` / `quote_core.cpp`

这是第一优先级迁移对象，因为现有 Python 已经把 quote core 做成了纯函数模块。

迁移来源：

- `strategy/quote_core.py::QuotePrediction`
- `strategy/quote_core.py::DepthSnapshot`
- `strategy/quote_core.py::QuoteState`
- `strategy/quote_core.py::QuoteCoreConfig`
- `strategy/quote_core.py::QuoteCoreResult`
- `strategy/quote_core.py::_floor_tick`
- `strategy/quote_core.py::_ceil_tick`
- `strategy/quote_core.py::_microprice`
- `strategy/quote_core.py::_depth_imbalance`
- `strategy/quote_core.py::_estimate_depth_kappa`
- `strategy/quote_core.py::_depth_tox_mult`
- `strategy/quote_core.py::apply_final_spread_cap`
- `strategy/quote_core.py::_near_depth_total`
- `strategy/quote_core.py::_exposure_increasing`
- `strategy/quote_core.py::_side_adverse_state`
- `strategy/quote_core.py::_side_defense_state`
- `strategy/quote_core.py::compute_quote_core`

建议 C++ 公开接口：

```cpp
struct QuoteCoreConfig {
    double gamma = 0.0;
    double kappa = 0.0;
    double tick_size = 0.0;
    double lot_size = 0.0;
    double maker_fee = 0.0;
    double order_size = 0.0;
    double max_inventory = 0.0;
    // 后续字段一一映射 Python QuoteCoreConfig
};

struct SideQuoteContext {
    double raw_price = 0.0;
    double pre_guard_price = 0.0;
    double final_price = 0.0;
    double final_distance_to_mid = 0.0;
    double final_quote_delta_to_bbo = 0.0;
    double spread_mult = 1.0;
    bool side_adverse = false;
    bool side_adverse_pause = false;
    bool defense_guard = false;
    bool defense_pause = false;
    // 先覆盖 tick replay 和 quote EV 需要的字段，别急着做成 dict。
};

struct QuoteCoreResult {
    double bid_price = 0.0;
    double ask_price = 0.0;
    double spread = 0.0;
    double raw_half_spread = 0.0;
    double raw_mid_shift = 0.0;
    SideQuoteContext buy;
    SideQuoteContext sell;
    bool cap_hit = false;
    bool delta_cap_hit = false;
    bool final_compressed = false;
    bool mid_guard = false;
    bool post_only = false;
};

QuoteCoreResult compute_quote_core(
    const QuoteState& state,
    const QuoteCoreConfig& cfg,
    const QuotePrediction& pred,
    const DepthSnapshot& depth
);

std::tuple<double, double, bool, double> apply_final_spread_cap(
    double mid,
    double bid_price,
    double ask_price,
    double max_spread,
    double tick_size
);
```

哪些先不要迁：

- `quote_core_config_from_live_config` 和 `quote_core_config_from_params` 可以先留在 Python。Python 负责把 yaml/params 解析成 C++ `QuoteCoreConfig`。
- `gi_quoter` 是 Python 对象，不适合直接放进 C++ hot path。第一版可以保持 general intensity disabled，或者让 Python 预先计算 `gi_hd_bid/gi_hd_ask` 后作为纯数值输入。
- quote EV 模型预测先不要迁到 C++。它涉及 LightGBM bundle、feature columns、bucket expected value，先保持 Python 侧调用。

parity 标准：

- 对固定 fixtures，bid/ask 价格必须完全相同或在 `0.5 * tick_size` 内。
- flags 和 side context 的关键字段必须一致：`cap_hit`、`delta_cap_hit`、`mid_guard`、`post_only`、`side_adverse_pause`、`defense_pause`。
- 用随机 fuzz 覆盖 inventory、sigma、depth、toxicity、markout、cap 等组合。

## `tick_replay.hpp` / `tick_replay.cpp`

这是第二优先级迁移对象，也是最大收益来源。不要把整个 `models/backtest_tick.py` 一次搬完，先迁 `simulate_tick` 内部最热的 event loop。

迁移来源：

- `models/backtest_tick.py::simulate_tick`
- 内部 helper：
  - `_l2_visible_queue_ahead`
  - `_estimate_queue_ahead`
  - `_new_fill_eligible`
  - `_book_snapshot_at`
  - `_current_book_age_ms`
  - `_make_order`
  - `_request_cancel_all`
  - `_process_order_transitions`
  - `_append_fill_trace`
  - `_recent_local_rank`
  - `_apply_replay_side_policy_price`
  - `strategy.policy_guards.evaluate_common_side_policy`
  - `_refresh_final_quote_context`
- 主循环中的：
  - BBO/L2 指针推进
  - pending markout resolve
  - FIFO queue fill check
  - cash/inventory/PnL 更新
  - inventory time 积分
  - position timeout
  - dynamic requote / BER EMA
  - ML prediction pointer lookup
  - quote core 调用
  - order replace/cancel/new lifecycle
  - summary metrics 汇总

建议 C++ 数据输入以 array view 为主：

```cpp
struct TickReplayInput {
    ArrayView<int64_t> trade_ts_ms;
    ArrayView<double> trade_price;
    ArrayView<double> trade_qty;
    ArrayView<uint8_t> is_buyer_maker;

    ArrayView<int64_t> var_ts_ms;
    ArrayView<double> var_ssq;
    ArrayView<double> var_ti;
    ArrayView<double> var_retsq;

    ArrayView<int64_t> ml_ts_ms;
    ArrayView<double> ml_dir_10s;
    ArrayView<double> ml_vol_10s;
    ArrayView<double> ml_ret_10s;
    ArrayView<double> ml_tox_bid;
    ArrayView<double> ml_tox_ask;

    // Optional BBO/L2 arrays.
};

struct TickReplayParams {
    double gamma = 0.0;
    double kappa = 0.0;
    double order_size = 0.0;
    double max_inventory = 0.0;
    double requote_interval_s = 0.0;
    double maker_fee = 0.0;
    double taker_fee = 0.0;
    double queue_base = 0.0;
    double queue_decay = 0.0;
    double maker_fill_prob = 1.0;
    int64_t rng_seed = 42;
    // 后续逐字段映射 Python params。
};

struct TickReplaySummary {
    double pnl = 0.0;
    double inventory_adjusted_pnl = 0.0;
    int64_t fills_bid = 0;
    int64_t fills_ask = 0;
    int64_t n_requotes = 0;
    double signed_inventory_time_s = 0.0;
    double abs_inventory_time_s = 0.0;
    double sq_inventory_time_s = 0.0;
    double notional_inventory_time_s = 0.0;
    // 对齐 Python summary keys。
};

TickReplayResult simulate_tick_arrays(
    const TickReplayInput& input,
    const TickReplayParams& params
);
```

订单状态建议 C++ 内部建 struct，不要用 dict：

```cpp
enum class OrderState : uint8_t {
    PendingNew = 0,
    Open = 1,
    PendingCancel = 2,
};

struct ReplayOrder {
    int64_t trace_id = -1;
    Side side = Side::Buy;
    double price = 0.0;
    double quantity = 0.0;
    double remaining = 0.0;
    int64_t submit_ts = 0;
    int64_t activate_ts = 0;
    int64_t cancel_ts = -1;
    int64_t quote_ts = 0;
    double mid_at_quote = 0.0;
    double queue_init = 0.0;
    double queue_left = 0.0;
    bool fill_eligible = true;
    OrderState state = OrderState::PendingNew;
    SideQuoteContext quote_context;
};
```

哪些先不要迁：

- `models/backtest_tick.py` 顶层 CLI、参数 sweep、文件加载、LightGBM prediction 生成。
- pandas DataFrame 构造、结果 CSV/Markdown 输出。
- `QuoteEVModel.predict` 暂时留 Python。第一版 C++ replay 可以先禁用 quote EV，或者接受 Python 预计算好的 quote EV action arrays。

建议迁移顺序：

1. 先做 `simulate_tick_arrays` 的最小版本：无 ML、无 L2、无 latency、无 quote EV，只复现基础 quote/fill/PnL。
2. 加 BBO/L2、exact-level queue、new/cancel latency。
3. 加 dynamic RQ、BER、markout EMA、inventory-time summary。
4. 加 adverse/defense/local-extreme/adaptive TTL/cooldown。
5. 最后再考虑 quote EV action。

parity 标准：

- 小型 deterministic fixture：每次 fill、cash、inventory、order outcome 完全一致。
- UTC 日窗口：summary key 允许浮点微差，但 fill count、cap count、requote count 应一致。
- trace 模式：`_fill_trace` 和 `_quote_trace` 的关键字段按 order id 对齐。

## `streaming_features.hpp` / `streaming_features.cpp`

这是第三优先级。原因是 feature 工程目前大量依赖 pandas/resample/parquet，直接全迁收益不一定好。真正值得迁的是 streaming/rolling 状态更新，尤其后续如果要复用在运行时特征和快速离线生成。

迁移来源：

- `features/feature_engineer.py::compute_tick_momentum`
- `features/feature_engineer.py::add_microstructure_features`
- `features/feature_engineer.py::_load_l2_summary_1s` 中每个 L2 snapshot 的数值摘要逻辑
- `features/feature_engineer.py::add_execution_l2_features` 的 rolling/mean 部分
- `features/feature_engineer.py::_cross_market_feature_frame` 的 rolling return/vol/imbalance 部分
- `data_quality.py::continuous_segment_ids`
- `data_quality.py::mask_valid_horizon`

当前权威 streaming 结构：

```cpp
struct Bar1s {
    int64_t ts_ms = 0;
    double open = 0.0;
    double high = 0.0;
    double low = 0.0;
    double close = 0.0;
    double volume = 0.0;
    double buy_volume = 0.0;
    double sell_volume = 0.0;
    double trade_count = 0.0;
};

class TradeBarAggregator;   // raw trades -> causal 1s bars
class SignalFeatureEngine; // completed bars + history -> fixed feature vector
```

旧 `FeatureRow10s + RollingStats + StreamingFeatureState` 首版已删除，避免与生产使用的 fixed-array feature engine 形成两套口径。当前 rolling window：

- 用 ring buffer/deque 保存窗口内 `(timestamp, value)`。
- 每次 update 时弹出过期元素。
- 同时维护 sum/sum_sq/count，避免每次重扫窗口。
- 遇到 gap 大于 `max_gap_s` 必须 reset，和 Python `split_on_calendar_day_gaps`、`continuous_segment_ids` 保持一致。

哪些先不要迁：

- `load_bars`、`load_metrics`、`load_l2_summary_1s` 的文件发现和 parquet 读取。
- `resample_to_10s` 可以先保留 pandas 版本，等 streaming state 稳定后再替换。
- `add_labels` 可以先保留 Python，除非后续 C++ 已经拥有连续段和 horizon mask parity。
- train/val/test/daily_latest 切分继续留 Python。

## `bindings_module.cpp` 与 `bindings_*.cpp`

这里写 pybind11 封装。`bindings_module.cpp` 只保留唯一模块入口和冻结的
注册顺序，其他 `bindings_*.cpp` 按功能注册。它们应该薄，不要在 binding
里堆业务逻辑。

建议暴露三类 API：

```cpp
PYBIND11_MODULE(narrowgate_cpp, m) {
    bind_quote_core(m);
    bind_tick_replay(m);
    bind_streaming_features(m);
}
```

quote core 可以先暴露标量/类接口：

```cpp
m.def("compute_quote_core", &compute_quote_core);
```

tick replay 必须用 NumPy array 输入：

```cpp
m.def("simulate_tick_arrays", [](py::array_t<int64_t> trade_ts, py::array_t<double> price, ...) {
    py::gil_scoped_release release;
    return simulate_tick_arrays(input, params);
});
```

binding 注意事项：

- 对 array 使用 `py::array::c_style | py::array::forcecast`。
- 检查长度一致，不一致直接 throw `std::invalid_argument`。
- 长循环必须 `py::gil_scoped_release`。
- 返回 summary 用 `py::dict` 可以接受；trace 大量数据最好返回 NumPy arrays 或 list of dict 的可选 debug 模式。
- 不要在 hot path 内反复创建 Python 对象。

## `tests/test_cpp_quote_core_parity.py`

这个测试负责保证 C++ quote core 没有改策略含义。

应覆盖：

1. 最小 AS quote：无 ML、无 depth、无 cap。
2. tick rounding：bid floor、ask ceil、mid guard。
3. dynamic cap：`delta_cap_hit`、`final_compressed`。
4. depth microprice：L1/L3/L5 depth 改变 fair price。
5. depth kappa：thin/thick depth 下 half spread 变化。
6. inventory skew/asym/fade。
7. adverse guard：toxicity、markout、direction、ret、microprice、thin depth。
8. defense guard：reducing side、emergency inventory/loss。
9. fuzz：随机生成 1,000 组状态，和 Python `strategy.quote_core.compute_quote_core` 比较。

断言建议：

- price 类字段：`abs(py - cpp) <= tick_size * 0.5`
- spread/shift 类字段：`abs(py - cpp) <= 1e-9` 或按 tick 容忍。
- bool flags 必须完全一致。

## `tests/test_cpp_tick_replay_parity.py`

这个测试负责保证 C++ replay 没有制造研究/运行时漂移。

建议 fixture 分层：

1. 单笔 BUY fill：卖方主动成交打到 bid，queue 为 0，检查 inventory/cash/fills。
2. 单笔 SELL fill：买方主动成交打到 ask。
3. queue ahead：第一笔 trade 先吃 queue，第二笔才 fill。
4. pending new latency：submit 后未 active 前不能 fill。
5. pending cancel latency：cancel ack 前仍可 fill。
6. post-only/GTX reject：active 时穿过对手价则 reject。
7. stale book：book age 超阈值时 cancel/skip。
8. markout trace：1s/5s/30s markout 和 toxic flag。
9. inventory time：`abs_inventory_time_s`、`notional_inventory_time_s` 和 Python 一致。
10. 一个小型月内窗口：summary 关键字段与 Python `simulate_tick` 对齐。

第一版可以先 skip 大 fixture，只打开最小 fixture；每迁一块就打开对应测试。

## `bench/bench_quote_core.py`

这个 benchmark 比较 Python quote core 和 C++ quote core 的调用成本。

建议输出：

- 单次 quote core：Python 每秒调用数、C++ 每秒调用数。
- batch quote core：一次传 N 个状态进 C++，每秒处理数。
- Python->C++ 边界成本：空函数/轻量函数基准。

重点看两件事：

1. 单次调用是否已经足够快。
2. 如果单次收益不明显，batch API 是否显著更好。

## `bench/bench_tick_replay.py`

这个 benchmark 比较 Python `simulate_tick` 和 C++ `simulate_tick_arrays`。

建议输出：

- trades/sec。
- 总耗时。
- summary parity 摘要。
- trace disabled vs trace enabled 的差异。
- BBO/L2 disabled vs enabled 的差异。

不要只测一个很小 fixture；至少要有：

- 10 万 trades 的短窗。
- 一个完整日度文件。
- 一个 Jan/Feb/Apr/May 常用 A/B 窗口。

## Python 集成方式

建议在 Python 侧做可选 import：

```python
try:
    import narrowgate_cpp
except Exception:
    narrowgate_cpp = None
```

并用显式开关控制：

```python
use_cpp = bool(params.get("use_cpp_replay", False)) and narrowgate_cpp is not None
```

不要让 C++ extension 自动改变默认路径。推荐顺序：

1. 默认 Python。
2. `use_cpp_quote_core=true` 只替换 quote core。
3. `use_cpp_replay=true` 替换 tick replay。
4. parity + benchmark 稳定后，再考虑把默认切到 C++。

## 什么不应该迁到 C++

这些先保持 Python：

- parquet/csv/json/yaml IO。
- data audit 文件发现和坏日期过滤。
- pandas dataset 拼接、train/val/test 切分。
- LightGBM 训练。
- LightGBM Python bundle 加载和 quote EV 推理。
- A/B experiment registry。
- Markdown/CSV 报告写入。
- WebSocket、REST、日志、告警。

原因很简单：这些不是最热路径，迁到 C++ 会增加维护成本，但速度收益小。

## 推荐迁移里程碑

### Phase 1: quote core

- C++ 实现 `compute_quote_core`。
- Python binding 暴露 `compute_quote_core`；spread-cap helper 只在 C++ quote/replay 内部使用。
- quote core parity test 打开。
- `strategy/maker_engine.py` 和 `models/backtest_tick.py` 可选调用 C++ quote core。

### Phase 2: tick replay minimal

- C++ 实现基础 replay：trade loop、quote interval、simple fill、cash/inventory/PnL。
- 不开 ML、不用 L2、不做 latency。
- tick replay 最小 parity test 打开。

### Phase 3: replay parity

- 加 queue ahead、BBO/L2、new/cancel latency、pending cancel fill。
- 加 markout EMA、inventory-time 积分、summary keys。
- 对齐常用 A/B summary。

### Phase 4: feature streaming

- C++ 实现 rolling window state。
- 和 `feature_engineer.py` 对固定日度样本做 feature parity。
- 后续再决定是否替换离线 feature generation。

### Phase 5: optional model-side acceleration

- 如果 quote EV 或主模型推理成为瓶颈，再考虑 C++/LightGBM C API。
- 在此之前，模型训练和推理保持 Python 更稳。
