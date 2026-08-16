# 麦麦画师 · RunningHub

让机器人调用 RunningHub 工作流帮你出图、出视频：一句话文生图、多参考图 / 视频生成，结果自动发回聊天。

- 平台入口：[RunningHub 国外](https://www.runninghub.ai?inviteCode=bvhsaqdr) / [RunningHub 国内](https://www.runninghub.cn?inviteCode=8cq8uhl8)

> [!WARNING]
> RunningHub 是付费平台，每次运行都会消耗余额 / 积分。建议装好后先配置「访问控制」。

> [!NOTE]
> 发送兼容 AstrBot 已接入的平台；自动撤回依赖 OneBot v11（NapCat / QQ），其他平台会自动跳过撤回。

---

## 能做什么

- 文生图 / 图生图 / 文生视频 / 多参考生视频，可配置多个工作流
- 图片 / 语音 / 视频参考文件交互式上传，可只传部分或「跳过剩余」
- 可编辑参数（分辨率、步数、CFG 等）运行前询问确认
- 可选 LLM 提示词扩写，一句话也能出高质量图
- 命令触发 + 自然语言触发
- 用户 / 群白名单、每用户每小时限频、管理员中断
- 结果自动发送，可选发后自动撤回

---

## 安装

```bash
cd <AstrBot目录>/data/plugins
git clone https://github.com/achenjins/astrbot_plugin_runninghub_maimai.git astrobt-runninghub
cd astrobt-runninghub
pip install -r requirements.txt
```

装好后在 AstrBot WebUI 的插件管理里启用 / 重载插件。

---

## 三步跑通第一张图

### 1. 注册并填写 API Key

1. 打开 [国内站](https://www.runninghub.cn?inviteCode=8cq8uhl8) 或 [海外站](https://www.runninghub.ai?inviteCode=bvhsaqdr) 注册。
2. 在对应平台的 API 页面复制 API Key。
3. 打开 AstrBot 插件配置页，填入「国内 API Key」或「国外 API Key」。

> [!TIP]
> 国内 / 国外账号和 Key 不通用，工作流在哪个站就填哪个 Key。

### 2. 识别一个工作流

在聊天里发：

```
/识别国外工作流 2087492768787685378 文生图
```

或：

```
/识别国内工作流 <工作流ID> <名称>
```

看到「识别成功」后，去插件配置页的工作流列表里检查输入节点，按需删除多余节点。

### 3. 跑一张图

```
/rh运行 文生图 原神刻晴
```

如果工作流需要参考图 / 音频 / 视频，机器人会引导你上传；不想传就回「跳过剩余」。完成后结果会自动发回聊天。

---

## 常用命令

| 命令 | 作用 |
| --- | --- |
| `/rh运行 <工作流名> [描述]` | 运行工作流 |
| `/rh中断` | 中断输入会话或按编号取消任务 |
| `/工作流` | 列出已配置工作流 |
| `/识别国外工作流 <ID> [名称]` | 识别 runninghub.ai 工作流关键节点 |
| `/识别国内工作流 <ID> [名称]` | 识别 runninghub.cn 工作流关键节点 |
| `/详细识别国外工作流 <ID> [名称]` | LLM 详细识别全部参数 |
| `/详细识别国内工作流 <ID> [名称]` | LLM 详细识别全部参数 |

---

## 自然语言触发

工作流只有一个「提示词（prompt）」输入节点时，不用敲命令，直接说：

```
帮我画一只甘雨，蓝色长发，全身立绘
```

机器人会自动调用工作流并把结果发回。新增 / 改名工作流后，重载插件刷新可用列表。

---

## 配置项速查

### RunningHub 服务

| 配置项 | 说明 |
| --- | --- |
| `api_key` | 国外 API Key |
| `api_key_cn` | 国内 API Key |

### 生成参数

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `poll_interval` | 15 | 任务轮询间隔（秒） |
| `max_wait` | 1800 | 任务最大等待时间（秒） |
| `max_concurrent` | 2 | 同时进行任务数上限 |
| `download_timeout` | 120 | 下载结果超时（秒） |

### 功能设置

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `enable` | false | 自动撤回开关 |
| `recall_seconds` | 90 | 发送后多少秒撤回，0 表示不撤回 |
| `result_notice` | true | 完成后是否追加「生成完成」消息 |
| `use_llm` | true | 使用 LLM 识别节点，失败自动回退规则 |
| `model` / `enhance_model` | 空 | 识别 / 扩写使用的模型提供商，留空用默认模型 |

### 访问控制

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `allow_users` | [] | 用户 ID 白名单，留空不限 |
| `allow_groups` | [] | 群号白名单，留空不限群 |
| `max_per_user_per_hour` | 0 | 每用户每小时上限，0 不限 |
| `admin_users` | [] | 管理员 ID，可中断所有人任务 |

> [!WARNING]
> 默认全部放行，任何人都能触发任务。建议至少配置白名单或限频。

### 工作流列表

| 配置项 | 说明 |
| --- | --- |
| `name` | 工作流名称，用于 /rh运行 |
| `workflow_id` | RunningHub 工作流 ID |
| `instance_type` | Standard / Plus / Ultra |
| `region` | overseas=国外，domestic=国内 |
| `llm_enhance` / `llm_template_path` | 提示词扩写开关与模板路径 |

### 输入节点列表

输入节点是独立的「添加条目」列表，想加几个就加几个：

| 配置项 | 说明 |
| --- | --- |
| `workflow_name` | 所属工作流，需与上面工作流的 `name` 完全一致 |
| `node_id` | RunningHub 节点 ID |
| `field_name` | 要控制的字段名，如 prompt / image / audio / width |
| `field_value` | 默认值，留空运行时询问 |
| `value_type` | 节点类型：prompt / text / default / image / audio / video |
| `label` | 等待上传时显示的中文说明 |

---

## FAQ

**Q：识别报「对应 API Key 未填写」？**
A：检查工作流区域和已填的 Key 是否对应：国内工作流填 `api_key_cn`，国外填 `api_key`。

**Q：识别不出节点？**
A：普通识别只认关键节点，改用 `/详细识别*`，或去配置页手动添加。

**Q：上传文件后没反应？**
A：确认插件已重载，并查看日志中是否有「已接收输入 / 任务已提交」。

**Q：为什么自然语言没触发生成？**
A：只有单个「提示词」节点的工作流支持自然语言；需要传文件或改参数时请用 `/rh运行`。

**Q：自动撤回没生效？**
A：自动撤回仅 NapCat / OneBot v11 平台可用；其他平台会自动跳过。

---

## 许可证

MIT
