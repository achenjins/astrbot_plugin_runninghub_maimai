# 麦麦画师 · RunningHub

让 AstrBot 调用 RunningHub 工作流出图、出视频，结果自动发回聊天。

- 文生图 / 图生图 / 文生视频 / 多参考文件工作流
- 图片、语音、视频交互式上传，可只传部分或「跳过剩余」
- LLM 提示词扩写（支持模板上传/预览），命令 + 自然语言触发
- 白名单、限频、管理员中断、可选自动撤回
- AstrBot Pages 可视化管理工作流、余额与最近任务记录

> [!WARNING]
> RunningHub 按次扣费，建议装好后先配置「访问控制」。自动撤回仅 NapCat / OneBot v11 生效。

---

## 安装

```bash
cd <AstrBot目录>/data/plugins
git clone https://github.com/achenjins/astrbot_plugin_runninghub_maimai.git astrobt-runninghub
cd astrobt-runninghub
pip install -r requirements.txt
```

装好后在 AstrBot WebUI 启用 / 重载插件。

---

## 快速开始

### 1. 填写 API Key

- [国内站注册](https://www.runninghub.cn?inviteCode=8cq8uhl8) / [海外站注册](https://www.runninghub.ai?inviteCode=bvhsaqdr)（链接已带邀请码）
- 海外 API 页面：<https://www.runninghub.ai/enterprise-api/consumerApi?inviteCode=bvhsaqdr>
- 国内 API 页面：<https://www.runninghub.cn/enterprise-api/consumerApi?inviteCode=8cq8uhl8>
- 在插件配置中填「国内 API Key」或「国外 API Key」，两边 Key 不通用。

### 2. 识别工作流

```text
/wf国外工作流 <工作流ID> <名称>
/wf国内工作流 <工作流ID> <名称>
```

看到「识别成功」后，在配置页或 Pages 里删掉不需要的节点。

### 3. 运行

```text
/wf运行 <工作流名> <描述>
```

例如：

```text
/wf运行 文生图 原神刻晴
```

需要文件时机器人会引导上传；不想传就回「跳过剩余」。

<details>
<summary>体验工作流（可选）</summary>

这是为第一次试运行准备的文生图工作流，已删除高成本「豆包提示词优化」节点。

- 海外版：<https://www.runninghub.ai/zh-cn/workflow/2087492768787685378?inviteCode=bvhsaqdr>
- 国内版：<https://www.runninghub.cn/workflow/2087939838371786753?inviteCode=8cq8uhl8>

> [!NOTE]
> 工作流魔改自 B 站视频 [BV1arGt6wExL](https://www.bilibili.com/video/BV1arGt6wExL)，感谢 UP 主 [@每日提钢小助手5号](https://space.bilibili.com/3690999272442168)，如侵权可联系删除。

> [!IMPORTANT]
> 打开并保存后，工作流 ID 会变成你账号专属 ID，识别时请换用新 ID。

导入：

```text
/wf国外工作流 2087492768787685378 文生图
/wf国内工作流 2087939838371786753 文生图
```

导入后只保留提示词节点（海外版为 353 号），开启 LLM 扩写并选择 `prompt/anima3_prompt_template.txt`。

费用参考：Standard 设备约 2 分钟 / 约 25 RH 币；Ultra 仅会员可用且更贵。

</details>

---

## Pages 页面怎么用

需要 AstrBot 4.24.2+：**插件详情 → Pages → workflow-editor**（页面路径在插件包里，打开后会连接插件）。
插件提供两个 Pages：`workflow-editor`（配置 + 余额）和 `task-history`（最近任务记录）。

页面能做什么：

- 新建 / 编辑 / 删除工作流与输入节点；
- 节点拖动排序，支持搜索和国内外筛选；
- 「识别」后按复选框挑选要添加的节点，支持全选；
- LLM 扩写模板上传 / 预览 / 选择，保存在 `data/plugin_data/<插件目录>/prompt/`，更新插件不丢失；
- 所有修改点「保存配置」才生效，保存后插件热更新。
- 页面顶部显示国内外 RunningHub 余额（RH 币 / 运行中任务 / 钱包），可手动刷新；
- `task-history` 页面显示最近任务记录，只保留任务 ID、工作流名称和消耗的 RH 币，支持刷新 / 复制任务 ID / 清空；成功任务会在运行结束后自动写入。

旧版 AstrBot 没有 Pages 入口时，仍可用插件配置页里的「工作流列表 + 输入节点列表」动态表单。

---

## 常用命令

| 命令 | 作用 |
| --- | --- |
| `/wf运行 <工作流名> [描述]` | 运行工作流 |
| `/wf中断` | 中断输入会话或按编号取消任务 |
| `/wf工作流` | 列出已配置工作流 |
| `/wf国外工作流 <ID> [名称]` | 识别海外站工作流 |
| `/wf国内工作流 <ID> [名称]` | 识别国内站工作流 |
| `/wf详细国外工作流 <ID> [名称]` | LLM 详细识别海外工作流 |
| `/wf详细国内工作流 <ID> [名称]` | LLM 详细识别国内工作流 |

---

## 自然语言触发

工作流只有一个提示词节点时，直接说：

```text
帮我画一只甘雨，蓝色长发，全身立绘
```

新增 / 改名工作流后重载插件，刷新可用名称。

---

## FAQ

- **识别报 API Key 未填写？** 国内工作流用 `api_key_cn`，海外用 `api_key`。
- **自然语言不触发？** 带文件 / 参数节点的工作流请用 `/wf运行`。
- **自动撤回没生效？** 仅 NapCat / OneBot v11 平台支持。

---

## 许可证

MIT
