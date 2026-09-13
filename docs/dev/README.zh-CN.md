# 开发者说明

<p><a href="README.md">English</a> | <a href="README.zh-CN.md">简体中文</a></p>

Last materially modified: 2026-08-30

Last materially synchronized: 2026-08-30

开发者文档涵盖本地安装、测试、lint、C++ 扩展构建和贡献工作流。

- 维护中的本地检查：[ci.md](ci.md)
- 托管分支保护只依赖一个根 `CI admission` 检查；各 worker job 只是按路径运行的内部实现。
- 包与可选依赖合同：仓库 `pyproject.toml`
