# Replay Studio 前端

Last materially synchronized: 2026-09-06

[English（权威版本）](README.md)

用于 Python 安装包内置 Studio 界面的 React + TypeScript 工作目录。当前只接入
`replay-demo` 执行器和内置 `synthetic-demo` 数据集。界面不能提交真实 F01、
current B0 或 E/C 研究，不能执行任意 shell，也不会创建云资源。

## 开发与构建

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
   取消、已完成报告、订单/trace 联动、比较和窄屏布局。不要提交真实研究或云任务。
3. UI 构建后打包 Python wheel，验证安装后的 wheel 能直接提供 HTML 和
   带摘要文件名的 CSS/JS，不依赖前端开发服务器。

默认构建路径只维护一份依赖锁，不另加一套 pnpm 交互批准流程。
