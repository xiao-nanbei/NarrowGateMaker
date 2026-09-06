# Replay Studio 前端

Last materially modified: 2026-09-07
Last materially synchronized: 2026-09-07

[English（权威版本）](README.md)

用于 Python 安装包内置 Studio 界面的 React + TypeScript 工作目录。公共默认 runner 是 `replay-demo`，使用内置 `synthetic-demo` 数据集。可选 owner 登记离线适配器可排队执行已准备的研究、训练或数据处理计划，浏览器不暴露命令、路径或参数；公共 clone 不预启用真实计划。独立默认页展示通过 [owner 本地 CLI](../docs/plans/remote_replay_studio.zh-CN.md#只导入已完成-b0不重跑) 导入的已有私有 B0 摘要，查看不会启动新 baseline。界面不执行任意 shell，也不创建云资源。

真实结果使用只读 `/api/results` 接口，与合成任务、报告及比较保持分离。浏览器展示已保存金额和连续段覆盖，不制造每日 PnL 或 Sharpe；交易 PnL 已扣的手续费不再次扣除，资金费只加一次。Local/Azure 来源及既有跨主机核验描述历史出处，不代表当前云连接。队列缺失覆盖和模型证据限制仍明确显示。前端包不包含私有证据。

## 行情复盘与数据质量

行情复盘页选择已导入 B0、连续分段、UTC 日期，以及 1s / 5s / 1m / 5m K 线周期（默认 1m）。它复用现有 React / SVG，提供键盘和按钮缩放、有界拖动；请求不跨所选 UTC 日、对齐完整 K 线边界，每个窗口最多显示 1,000 根 K 线。蜡烛只来自保留的市场 bar，不由策略成交构造。当前来源明确标注为**历史市场上下文，未验证为原回放精确输入**。某秒没有 bar 的原因未知，不能直接判定为源停流。

BUY / SELL 标记是模拟成交方向，不表示开仓 / 平仓。每笔成交保留精确发生与本地可见时间、执行价、数量、原始与分段内订单标识、库存、有符号费用及已记录 Campaign 字段。同根 K 线、同一毫秒的多笔成交保持独立；未读完的分页明确披露并可继续加载。缺失字段保持未知，提交时 Campaign 不改称最终归属。库存观测按本地可见回调时钟及原始顺序绘制，同一时刻的变化保留，但不在不同时刻间补造连续路径。库存缺可见时钟时不以成交发生时间代替。订单详情仅为部分成交快照，不制造有效挂单带或连续 PnL。

独立 UTC 质量日历包含请求起止日期内的每一天，包括未下载的头尾日。可筛选来源、市场、币对、数据集、节点、待复核日及明确缺失的副本。原始／处理产物、当前 canonical 文件的检查关联、各用途结论与节点副本分别展示。当前用途卡片列出 K 线、特征输入、模型／严格队列回放和资金费的原因；历史检查仍保留原范围和时间。“未知”细分为尚未观察、未执行处理后检查、旧报告未关联或其他已记录原因。特征检查不等于模型训练准入，缺 native sequence 不否定所有用途。展开明细只展示已有记录数、字节数、覆盖、缺口和时间。登记大小匹配只关联已有报告，不称为重新验证内容，也不把 ffill 当作无丢包证明。

**刷新本机登记清单**仅向 `/api/data-quality/refresh` 提交日期范围、登记数据集 ID 和节点 ID，进行有界本机元数据观察，不读取原始内容、不做全盘 SHA、不启动下载、质量计算或回放。远端副本时间保持独立。**重新读取页面**只重新读取投影；导出仍仅生成有界 JSON 补数／复核清单，不执行清单。详见[质量证据与刷新范围](../docs/plans/remote_replay_studio.zh-CN.md#查看已有数据质量证据)。由日期跳转到复盘，不会把当前质量目录绑定为旧结果的原始输入证明。

## 开发与构建

计算资源页面使用 `/api/compute-resources` 显示 owner 配置的物理／云资源，不以 worker 数量代替主机数。友好别名、固定后台探测、外部作业观察与分配角色通过 `--resources-manifest` 配置，见[资源接入说明](../docs/plans/remote_replay_studio.zh-CN.md#真实计算资源与合成-worker-分开)。已缩容到零的池、陈旧观测和不可达主机与执行 worker 心跳分别展示。现有外部研究仍只读，不接管或重复启动。

控制端和 worker 配置 owner `--execution-manifest` 后，资源页读取 `/api/execution-plans`，只向 `/api/executions` 提交 `plan_id`、`resource_id`；相同计划／版本和目标重试保留同一幂等键。已有 attempt 的计划／版本显示原任务链接并禁用重复提交；换键提交会返回 HTTP 409，不会重跑失败或失联的执行。获准但未就绪／离线的 worker 会让任务留在队列，不开启 Azure 节点或静默回退至 Mac。训练优先配置的训练资源且不派 LAN；回放／数据处理遵循登记的资源顺序。具体步骤见[登记离线执行](../docs/plans/remote_replay_studio.zh-CN.md#排队执行-owner-登记的离线计划)。

任务队列明确区分合成任务与 owner 登记任务。只有合成任务进入 demo trace／订单／campaign 和合成比较；已完成离线任务使用 `registered_execution_report.v1`，显示登记的 JSON 摘要、输出元数据、有界终态日志和环境。空摘要保持空，不转换成 PnL；大型输出和完整日志保留在原 worker 持久盘。Worker 未就绪与资源离线不是同一个状态。

使用 Node.js 22.12+ 和固定的 pnpm 11.19.0。依赖变更需同时更新
`pnpm-lock.yaml`。`pnpm-workspace.yaml` 已显式批准 esbuild 的标准安装钩子，
CI 不需要交互式批准。

```bash
cd frontend
pnpm install --frozen-lockfile
pnpm run lint
pnpm test
pnpm run format:check
pnpm run build
```

`lint` 执行严格 TypeScript 检查，包括未使用变量和参数。生产产物直接写入
`narrowgate/studio_static/`，供 wheel 打包。使用已构建 Python wheel 的用户
不需要 Node.js。

构建只调用已安装的 TypeScript 和 Vite，不会自动安装或升级依赖。安装依赖后，
`npm run lint` 和 `npm run build` 也是等价入口；也可直接执行
`node scripts/build.mjs`，避开包管理器额外的环境检查。

开发时先在 `127.0.0.1:8080` 启动 API，再执行 `npm run dev`。Vite 将 `/api`
代理到这个本地服务；生产界面只访问同源 API。

执行 `pnpm run format` 使用标准 Prettier 格式化源码和文档；
`pnpm run format:check` 仅检查，不修改文件。

## 展示与执行边界

- SSE 推送之外，每四秒刷新一次任务状态。同一实验输入重试沿用同一个幂等键。
- 只有已完成的合成报告才能比较。浏览器直接展示 runner 原始指标，不重新计算
  PnL，不将缺失值补成零。界面没有预置任务或虚构收益。
- 订单和 Campaign 可联动到对应的原始 trace。概览小图只绘制 trace 中已经
  记录、同一字段至少两条有效观测的数值；不同字段口径不拼接、不重标记。
- 当前合成 fixture 有三条成交后库存观测，没有完整 PnL 序列，因此只画库存图。
  不把 equity 当作 PnL，也不补齐未知的中间状态；折线仅辅助连接已记录的点。
  点击图上的点会打开对应原始事件。
- 可在“访问凭据”中填写可选 Bearer token，仅保存在当前页面内存，不进入
  URL、localStorage 或文件。启用 token 时用认证轮询，不把 token 放进 SSE URL。
  刷新页面后 token 清除。
- 日志面板目前只显示已经发布的终态归档日志，不是运行中的 worker 日志流。
  任务运行中只能查看服务端任务状态；原始实时日志暂留在 worker。

## 建议的 CI 覆盖

1. 仅在 `frontend/**` 或 Studio API/静态资源打包变化时触发 Linux 前端作业，
   执行冻结依赖安装、类型检查、trace-series 测试、格式检查和一次构建。
2. 使用临时状态目录启动 loopback API 和一个合成 worker，浏览器验收创建、
   取消、已完成报告、订单/trace 联动、比较和窄屏布局。另用无害登记离线检查验证
   只按计划提交、不可用资源保持 queued、真实 worker 心跳和非 demo 报告；
   不为填充页面启动经济研究或云资源。
3. UI 构建后打包 Python wheel，验证安装后的 wheel 能直接提供 HTML 和
   带摘要文件名的 CSS/JS，不依赖前端开发服务器。

默认构建路径只维护一份依赖锁，不另加一套 pnpm 交互批准流程。
