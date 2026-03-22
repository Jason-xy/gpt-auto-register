# Auto CPA Register

## Automation Status

[![register-automation](https://github.com/linuxdoo/gpt-auto-register/actions/workflows/check_and_register.yml/badge.svg)](https://github.com/linuxdoo/gpt-auto-register/actions/workflows/check_and_register.yml)

如果仓库是私有仓库，这个 badge 和对应的 Actions 页面也只对有仓库访问权限的成员可见。

## Where To Check

- Actions 页面：查看 `register-automation` workflow 的最近一次运行结果、日志和步骤耗时
- Job Summary：每次运行结束后，会在 Actions 的 step summary 中显示
  - 当前 Sub2Api 数量
  - 触发阈值
  - 本次是否跳过
  - 本次计划注册数量

## Workflow

- 自动检查并按需注册：[.github/workflows/check_and_register.yml](./.github/workflows/check_and_register.yml)
- 检查脚本入口：[scripts/check_and_register.py](./scripts/check_and_register.py)
- 示例配置模板：[config.example.json](./config.example.json)
- 本地私密配置文件不入库：[`config.json`](./config.json) 已被 `.gitignore` 忽略

## GitHub Setup

首次推送到 `linuxdoo/gpt-auto-register` 之前，先确认以下内容：

- 提交 [config.example.json](./config.example.json)，不要提交本地真实 [config.json](./config.json)
- 提交 [zhuce5_cfmail_accounts.json](./zhuce5_cfmail_accounts.json) 前确认里面不含真实密钥；当前文件是分享安全示例
- 在仓库 `Settings -> Secrets and variables -> Actions` 中补齐需要的 `Secrets` / `Variables`

常用 `Secrets`：

- `DUCKMAIL_BEARER`
- `SUB2API_BASE_URL`
- `SUB2API_BEARER` 或 `SUB2API_EMAIL` + `SUB2API_PASSWORD`
- `CPA_BASE_URL`
- `CPA_MANAGEMENT_KEY`

常用 `Variables`：

- `SUB2API_MIN_COUNT`
- `TOPUP_BATCH_SIZE`
- `TOPUP_MAX_COUNT`
- `REGISTER_MAX_WORKERS`
- `REGISTER_CPA_CLEANUP`

## Notes

- 手动触发 workflow 时，可以填写 `manual_total_accounts` 强制指定本次注册数量
- 定时触发时，会先检查 `sub2api` 数量，低于阈值才运行注册
- 当前 GitHub Actions 运行模式固定为 `MODE=github`，不会使用本地代理或代理池配置
