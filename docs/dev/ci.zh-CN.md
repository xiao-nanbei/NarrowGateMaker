# 开发检查

[English](ci.md) | [简体中文](ci.zh-CN.md)

Last materially modified: 2026-09-21

Last materially synchronized: 2026-09-21

`${NARROWGATE_*}` 是逻辑路径。个人数据与机器产物不随仓库分发，参见[公私文档合同](../public_private_documentation_contract.zh-CN.md)。

使用 Python 3.11 或更新版本；完整测试需要安装贡献者依赖。已有环境无需重建。

```bash
python3.11 -m venv .venv
PYTHON=.venv/bin/python
$PYTHON -m pip install -e ".[all]"
$PYTHON -m pytest -q
git ls-files -z '*.py' | xargs -0 "$PYTHON" -m ruff check --
$PYTHON -m py_compile narrowgate/cli.py
$PYTHON scripts/audit_public_documentation.py --repo-root .
git diff --check
```

完整测试导入离线研究和只读公共市场连接器，因此需要 `all` 依赖，即使不发网络请求或实盘订单。独立的基础安装检查只安装基础包，验证无数据命令、回放示例、订单示例与新用户测试，防止可选依赖泄入基础安装。[分支保护](#分支保护)规定必需检查；只有 `CI admission` 是汇总入口，各按路径执行的作业不是单独必需检查。

无法公开分发精确历史源码、部署目录或配置的历史复现测试，列在 `tests/fixtures/public_clone_historical_test_availability.json`。默认不收集相应模块或取消选择节点，不代表授予研究或实盘权限。恢复全部绑定证据后，可以设置 `NARROWGATE_RUN_HISTORICAL_REPRODUCTION_TESTS=1`；证据不全时仍应拒绝。

```bash
$PYTHON -m pip install -e cpp
$PYTHON -c "import narrowgate_cpp; print(narrowgate_cpp.__file__)"
RUN_NARROWGATE_GOLDEN=1 $PYTHON -m pytest tests/test_cpp_tick_replay_golden_parity.py -q
```

普通 CI 用 Python 3.11 检查基础安装和命令兼容性，用 Python 3.12 执行完整公开测试和原生扩展一致性检查。手动运行默认只执行主版本完整测试；显式选择 `compatibility` 才追加 3.11 完整测试。夜间检查仅在 NarrowGateMaker 执行两个版本。相同事件类型和分支的新 push/PR 会取消旧运行；手动与夜间运行独立。

NarrowGateMaker 是主回归仓库。NarrowGateMaker-private 收到 push 时，通过 GitHub API 比较当前树与主仓库 main；只有完全相同才将重复的全量回归交由主仓库执行。树不同、身份获取失败或 API 不可用时，仍执行完整回归。镜像仓库的 PR 和手动运行也保留完整回归。两个仓库各自保留适用的正确性、风格、文档、前端和基础打包检查。镜像绿灯不证明主回归已通过，仍须检查匹配的主仓库运行；汇总检查不会把上游失败改成成功。

托管 CI 没有真实行情 golden 测试所需的 `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` 输入。跳过是明确的数据可用性限制，不是通过；发布证据必须另附显式运行的 golden 结果和输入身份。源码修改按当前 alpha 约定在本地 amend，只有用户明确要求本次推送时才推送。

## 分支保护

以下是维护建议，不声称 GitHub 已启用这些设置；实际托管规则需单独核验，本次未修改。

### 必需检查

main 的必需 GitHub Actions 检查名称应为 `.github/workflows/ci.yml` 中的 `CI admission`。界面可能显示 `CI / CI admission`；不要把内部步骤或按路径执行的作业设成必需检查。它在分类和适用检查之后汇总，非跳过的依赖失败或取消时即失败。

普通代码 PR 中，基础作业用 Python 3.11 仅安装 `-e .`，质量作业执行全仓库 F/B 正确性检查，Python 3.12 作业构建一次 C++ 扩展并执行完整公开测试与原生一致性检查。纯文档变更只执行文档审计，不安装研究或原生依赖。主仓库夜间运行和显式选择 `compatibility` 的手动运行才追加 Python 3.11 完整套件及对应原生构建，普通手动运行不追加。完全相同代码树的镜像 push 将全量回归交由主仓库执行，同时保留自身适用的打包和公开边界检查；镜像汇总通过不证明主回归通过。回退与取消策略见[开发检查](ci.zh-CN.md)。

### 建议规则集

建议要求 PR、至少一次批准、解决审查讨论、`CI admission` 以及合并前更新分支；禁止强制推送和删除分支。实质代码变更后撤销旧批准；除非另有紧急流程说明，管理员也遵守这些规则。

必需检查属于仓库托管设置，修改工作流文件不会自动创建。迁移时先让托管仓库成功执行 `CI admission`，再添加该必需检查并移除旧的 `Base install smoke`、`Python tests and lint (3.11)`、`Python tests and lint (3.12)` 和 `C++ extension build smoke` 检查名。新汇总检查出现之前，不移除旧上下文。
