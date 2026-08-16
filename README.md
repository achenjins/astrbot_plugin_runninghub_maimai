# 麦麦画师 · RunningHub（AstrBot 版）

从 maibot 插件 [runninghub-workflow-adapter](https://github.com/achenjins/runninghub-workflow-adapter) 迁移到
AstrBot 的通用工作流适配插件。让机器人调用 RunningHub 工作流完成文生图、多参考图生成、视频生成，
并把成品自动发回聊天。

## 平台支持

- **首选通道**：AstrBot 通用发送通道（`context.send_message`），理论上支持 AstrBot 已接入的所有平台。
- **撤回能力**：AstrBot 没有统一的消息撤回 API；本插件通过 AstrBot aiocqhttp 适配器暴露的
  OneBot v11 客户端直接调用 `delete_msg` 实现撤回。因此自动撤回仅在 NapCat / OneBot v11 QQ 平台生效。
- 图片/视频直发也遵循同样策略：**默认走 AstrBot 通用通道**；仅当开启自动撤回、需要拿到
  `message_id` 时，才临时使用 OneBot 直发，失败自动回退通用通道。

## 功能

- 多工作流配置：名称、工作流 ID、设备类型（Standard / Plus / Ultra）、区域（overseas / domestic）
- 输入节点类型：`prompt` / `text` / `default` / `image` / `audio` / `video`，留空自动推断
- 交互式收集：图片 / 语音 / 视频按顺序上传，支持「跳过剩余」；`text` 配置节点上传后询问确认
- LLM 提示词扩写：支持自定义模板文件
- LLM 节点识别：`/识别国外工作流`、`/识别国内工作流`、`/详细识别*`，LLM 失败自动回退启发式
- 访问控制：用户 / 群白名单、每用户每小时限频、管理员中断
- 自动撤回（仅 OneBot v11）：可在配置中开启并设置延迟秒数

## 命令

| 命令 | 说明 |
| --- | --- |
| `/rh运行 <工作流名> [描述文本]` | 运行工作流 |
| `/工作流` | 列出已配置工作流 |
| `/识别国外工作流 <工作流ID> [名称]` | 识别 runninghub.ai 工作流关键节点 |
| `/识别国内工作流 <工作流ID> [名称]` | 识别 runninghub.cn 工作流关键节点 |
| `/详细识别国外工作流 <工作流ID> [名称]` | LLM 详细识别全部输入/配置节点 |
| `/详细识别国内工作流 <工作流ID> [名称]` | LLM 详细识别全部输入/配置节点 |
| `/rh中断` | 中断输入会话或按编号取消运行中的任务 |

## LLM 工具

插件注册了 `run_workflow` 工具。**仅包含一个提示词节点、没有图片/音频/视频/配置输入的工作流**
支持自然语言调用；可用工作流名称会动态注入工具描述。

## Web API

插件启动时注册：

```
POST /api/plug/runninghub-workflow-adapter/run_workflow_api
Content-Type: application/json

{
  "workflow_name": "动漫生图",
  "prompt": "一只猫",
  "stream_id": "",       // AstrBot unified_msg_origin，主动发送结果时必填
  "user_id": "",
  "group_id": "",
  "platform_id": ""
}
```

## 配置

AstrBot 插件配置在 WebUI 中可视化编辑，Schema 位于 `_conf_schema.json`。

### maibot → AstrBot 配置映射

| maibot `config.toml` | AstrBot 配置项 | 说明 |
| --- | --- | --- |
| `[server] base_url / api_key / base_url_cn / api_key_cn` | `server.*` | 原样对应 |
| `[generation] poll_interval / max_wait / max_concurrent / download_timeout` | `generation.*` | 原样对应 |
| `[feature] enable / recall_seconds` | `feature.enable / recall_seconds` | 原样对应 |
| `[feature] use_llm` | `feature.use_llm` | 原样对应 |
| `[feature] model / enhance_model` | `feature.model / enhance_model` | MaiBot 模型槽位名改为 AstrBot 模型提供商 ID，留空使用默认/当前会话模型 |
| `[access] allow_users / allow_groups / max_per_user_per_hour / admin_users` | `access.*` | 原样对应 |
| `[[workflows.items]] ...` | `workflows`（template_list） | 工作流列表；`input_nodes` 在 WebUI 中为 JSON 文本，推荐用 `/识别*` 命令自动生成 |

> 迁移说明：MaiBot 的 `config.toml` 不会自动导入 AstrBot。你可以按 README 中的映射在 WebUI
> 中填写，或使用 `/识别*` 命令重新生成工作流节点。

## 代码结构

```
astrobt-runninghub/
├── main.py                        # AstrBot Star 插件入口：命令 / 工具 / Web API / 业务编排
├── metadata.yaml                  # 插件元数据
├── _conf_schema.json              # WebUI 配置 Schema
├── requirements.txt
├── tests/
│   └── test_plugin.py             # 配置 / 命令 / 投递通道测试
└── rh_generic_lib/
    ├── config.py                  # 配置模型 + AstrBot 配置归一化 / 序列化
    ├── delivery.py                # 消息投递与撤回通道抽象（通用通道 / OneBot 通道）
    ├── legacy_config.py           # 旧 config.toml 自动迁移
    ├── runninghub_client.py       # RunningHub OpenAPI 客户端（纯业务，无框架依赖）
    └── workflow_detect.py         # 工作流节点识别 / LLM 输出解析（无框架依赖纯函数）
├── tools/
│   └── convert_config.py          # config.toml → AstrBot JSON 独立转换工具
```

### 发送与撤回如何解耦

`delivery.py` 定义 `DeliveryTarget`（消息目标）和通道抽象：

- `GenericAstrBotChannel`：AstrBot 通用通道，**所有平台的首选发送方式**。
- `OneBotChannel`：OneBot v11 直发并返回 `message_id`，提供 `delete_msg` 撤回。
- `Delivery` 门面统一选择通道：
  - 发文本：AstrBot 通用通道优先。
  - 发图片/视频：默认 AstrBot 通用通道；`need_message_id=True` 时先 OneBot 直发，失败回退通用通道。
  - 撤回：仅 OneBot 通道。

后续想支持其他有撤回能力的平台，只需新增一个 Channel 实现并在 `Delivery.channel_for()` 中注册判断，
不需要改 `main.py` 的业务逻辑。

## 与 maibot 原版的差异

1. **消息发送**：优先 AstrBot 通用通道，不再是 NapCat API 命名空间硬编码直发。
2. **自动撤回**：maibot 无撤回接口，原版靠 `_ACTION_API_CANDIDATES` 猜 API 并直发拿 message_id；
   AstrBot 版收敛到 `delivery.py`，用 OneBot `delete_msg` 实现。
3. **LLM 上下文 / 主动回复**：AstrBot 没有 `maisaka.context.append` / `maisaka.proactive.trigger`。
   迁移版不再追加内部记忆；命令路径完成后由插件直接发送一句「生成完成」确认
   （可关闭 `feature.result_notice`），LLM 工具路径由 AstrBot Agent 自然续写回复。
4. **配置**：`config.toml` → AstrBot `_conf_schema.json`；模型槽位名改为模型提供商 ID。
5. **插件间 API**：MaiBot 的 `@API("run_workflow_api")` 改为 AstrBot Web API
   `POST /api/plug/runninghub-workflow-adapter/run_workflow_api`。

## 安装

1. 把整个 `astrobt-runninghub` 目录复制到 AstrBot 的 `data/plugins/` 下。
2. AstrBot 会自动安装 `requirements.txt` 依赖。
3. 在 WebUI 插件管理中启用插件，进入配置页填写 RunningHub API Key。
4. 使用 `/识别国外工作流 <工作流ID> <名称>` 自动识别节点，或手动添加工作流。
5. `/rh运行 <名称> 一只猫` 开始生图。

## 致谢

本项目从 [Mai-with-u](https://github.com/Mai-with-u) / maibot 生态插件
[runninghub-workflow-adapter](https://github.com/achenjins/runninghub-workflow-adapter) 迁移，
保留 MIT 许可。
