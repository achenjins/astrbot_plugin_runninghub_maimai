# 更新日志

## v2.3.3

- README 计数徽章修正，体验工作流说明默认展开

## v2.3.2

- Pages 打开后自动查询国内外 RunningHub 余额，无需手动点「刷新余额」
- 新增插件更新日志文件（WebUI 插件详情 → 更新日志）

## v2.3.1

- 将两个 Pages 合并为一个页面：打开 `workflow-editor` 后，用顶部「工作流配置 / 任务记录」标签切换
- 删除独立的 `task-history` 页面，避免 AstrBot 卡片快捷入口默认打开任务记录页的问题
- 顶部「刷新」按钮按当前标签页刷新对应内容；配置页按钮在任务记录标签下自动隐藏

## v2.3.0

- 配置页顶部新增 RunningHub 余额面板：海外 / 国内分别显示 RH 币、运行中任务数、钱包余额和 API 类型
- 新增最近任务记录：只保留任务 ID、工作流名称、消耗 RH 币三个字段
- 成功任务完成后自动写入 `data/plugin_data/<插件目录>/task_history.json`，最多保留 200 条，页面支持刷新、复制任务 ID、清空
- 后端新增 `POST /uc/openapi/accountStatus` 账户查询，任务消耗取自 `/openapi/v2/query` 返回的 `usage.consumeCoins`
- 新增 `task_history.json` 读取、去重、上限裁剪与原子写入

## v2.2.6

- 精简 README 并补充 Pages 使用说明
- 更新插件描述与标签（生图 / 工作流 / 生视频 / AI）
