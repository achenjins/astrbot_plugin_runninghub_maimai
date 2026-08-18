<div align="center">

# 麦麦画师 · RunningHub

**让 AstrBot 调用 RunningHub 工作流出图 / 出视频，结果自动发回聊天**

[![AstrBot](https://img.shields.io/badge/AstrBot-%E2%89%A54.13.0-7b3ff2)](https://github.com/achenjins/astrbot_plugin_runninghub_maimai)
[![Pages](https://img.shields.io/badge/Pages-4.24.2%2B-2ea44f)](#pages-可视化管理)
[![License](https://img.shields.io/badge/license-MIT-blue)](#许可证)

![running](https://count.kjchmc.cn/get/@astraajinse?theme=gelbooru)

</div>

---

## 目录

- [环境要求](#环境要求)
- [安装](#安装)
- [快速开始](#快速开始)
- [Pages 可视化管理](#pages-可视化管理)
- [常用命令](#常用命令)
- [自然语言触发](#自然语言触发)
- [FAQ](#faq)

---

## 环境要求

| 项目 | 要求 |
| --- | --- |
| AstrBot | ≥ 4.13.0；Pages 管理页需 ≥ 4.24.2 |
| 机器人平台 | NapCat / OneBot v11 |
| RunningHub 账号 | 国内站或海外站均可，两边 Key 不通用 |
| Python 依赖 | `requests`（安装步骤会一并处理） |

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

| 站点 | 注册 | API Key 页面 |
| --- | --- | --- |
| 国内站 | [注册（带邀请码）](https://www.runninghub.cn?inviteCode=8cq8uhl8) | [打开 API Key 页](https://www.runninghub.cn/enterprise-api/consumerApi?inviteCode=8cq8uhl8) |
| 海外站 | [注册（带邀请码）](https://www.runninghub.ai?inviteCode=bvhsaqdr) | [打开 API Key 页](https://www.runninghub.ai/enterprise-api/consumerApi?inviteCode=bvhsaqdr) |

在插件配置中填「国内 API Key」或「国外 API Key」。**两边 Key 不通用。**

### 2. 识别工作流

```text
/wf国外工作流 <工作流ID> <名称>
/wf国内工作流 <工作流ID> <名称>
```

看到「识别成功」后，在配置页或 Pages 里删掉不需要的节点（避免误触发无关能力，也少绕弯路）。

### 3. 运行

```text
/wf运行 <工作流名> <描述>
```

例如：

```text
/wf运行 文生图 原神刻晴
```

需要文件时机器人会引导上传；不想传就回「跳过剩余」。

一次完整的对话大致是这样（示意）：

```text
你：/wf运行 文生图 原神刻晴
机器人：请上传参考图，或回复「跳过剩余」
你：跳过剩余
机器人：任务已提交，完成后自动发回结果
```

<details open>
<summary>🚀 体验工作流（可选，第一次试运行推荐）</summary>

这是为第一次试运行准备的文生图工作流，已删除高成本「豆包提示词优化」节点。

**① 复制工作流**

| 站点 | 工作流链接 |
| --- | --- |
| 海外版 | [打开工作流](https://www.runninghub.ai/zh-cn/workflow/2087492768787685378?inviteCode=bvhsaqdr) |
| 国内版 | [打开工作流](https://www.runninghub.cn/workflow/2087939838371786753?inviteCode=8cq8uhl8) |

> [!IMPORTANT]
> 如果先打开并保存，工作流 ID 会变成你账号专属 ID，下面导入命令请换成新 ID；也可以不保存，直接用命令导入作者发布的原 ID。

**② 导入**

```text
/wf国外工作流 2087492768787685378 文生图
/wf国内工作流 2087939838371786753 文生图
```

**③ 配置节点**

插件支持可视化page页面，建议使用，配置完记得右上角保存。导入后只保留提示词节点（海外版为 353 号），开启 LLM 扩写并打开插件的page页面选择 `prompt/anima3_prompt_template.txt`。

**④ 费用参考**

| 设备 | 参考耗时 | 参考费用 |
| --- | --- | --- |
| Standard | 约 2 分钟 | 约 25 RH 币 |
| Ultra | 仅会员可用 | 更贵 |

> [!NOTE]
> 工作流魔改自 B 站视频 [BV1arGt6wExL](https://www.bilibili.com/video/BV1arGt6wExL)，感谢 UP 主 [@每日提钢小助手5号](https://space.bilibili.com/3690999272442168)，如侵权可联系删除。

</details>

---

## Pages 可视化管理

> [!NOTE]
> 需要 AstrBot 4.24.2+。旧版没有 Pages 入口时，可用插件配置页里的「工作流列表 + 输入节点列表」动态表单。

**入口**：插件详情 → Pages → `workflow-editor`

页面顶部有两个标签页：「工作流配置」和「任务记录」，点标签即可切换，不用开两个 Pages。

**工作流配置**

- 新建 / 编辑 / 删除工作流与输入节点；
- 节点拖动排序，支持搜索和国内外筛选；
- 「识别」后按复选框挑选要添加的节点，支持全选；
- LLM 扩写模板上传 / 预览 / 选择，保存在 `data/plugin_data/<插件目录>/prompt/`，更新插件不丢失；
- 所有修改点「保存配置」才生效，保存后插件热更新。

**任务记录**

- 只保留任务 ID、工作流名称和消耗的 RH 币；
- 支持刷新 / 复制任务 ID / 清空；
- 成功任务会在运行结束后自动写入。

**余额面板**

- 页面顶部显示国内外 RunningHub 余额（RH 币 / 运行中任务 / 钱包），可手动刷新。

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

- `<参数>` 必填，`[参数]` 可选。
- 命令后可用空格、中英文冒号或逗号分隔，例如 `/wf运行：文生图 一只猫`。
- 普通识别只取输入节点；「详细识别」会调用 LLM 补全输入与配置节点，更全但更慢。

---

## 自然语言触发

某一个工作流只有一个提示词节点时，工作流会进入llm的工具列表，支持自然语言触发，或在llm自己想触发时触发，直接说：

```text
帮我画一只甘雨，蓝色长发，全身立绘
```

- 带文件或图片参数节点的工作流请用 `/wf运行`。不适配自然语言触发
- 新增工作流后重载插件，刷新可用名称。

---

## FAQ

- **识别报 API Key 未填写？** 国内工作流用 `api_key_cn`，海外用 `api_key`。
- **自然语言不触发？** 带文件 / 参数节点的工作流请用 `/wf运行`；新增 / 改名后先重载插件。
- **自动撤回没生效？** 仅 NapCat / OneBot v11 平台支持。
- **找不到 Pages 入口？** Pages 需要 AstrBot 4.24.2+，旧版请用插件配置页的动态表单。
- **更新插件后模板和任务记录会丢吗？** 不会，它们存放在 `data/plugin_data/<插件目录>/` 下。
- **普通识别和详细识别有什么区别？** 普通识别用 LLM 只取输入节点，详细识别用 LLM 补全配置节点，更全但更慢，还可能识别到没用的节点。

---

## 许可证

[MIT](./LICENSE)
