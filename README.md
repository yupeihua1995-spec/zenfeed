# Zenfeed

[English](README-en.md)

Zenfeed 是一个可自托管的 AI 信息阅读后端。它从 RSS 和 RSSHub 抓取内容，通过可配置的 LLM 管道生成摘要、分类和标签，并向 Web 阅读器、查询 API、RSS、MCP 和通知渠道提供统一数据。

本仓库包含后端服务。配套 Web 客户端位于 [zenfeed-web](https://github.com/yupeihua1995-spec/zenfeed-web)。

## 当前能力

- RSS、RSSHub 和自定义来源抓取，支持来源启用、停用、状态检查和手动刷新。
- 可配置抓取回溯窗口、数据保留期和基于游标的分页查询。
- LLM 重写管道，可生成中文 HTML 摘要、受控主题标签、评分和自定义标签。
- 语义搜索、来源/类型/标签统计和查询结果摘要。
- 定时任务、邮件/Webhook 通知、RSS 输出和 MCP 接口。
- 配置 revision 校验，避免多个管理端互相覆盖修改。
- 组件生命周期、并发关闭、存储恢复和索引一致性保护。
- 本地 API 默认只绑定 loopback，适合在反向代理后部署。

## 数据处理流程

```text
RSS / RSSHub
    -> 来源级过滤与元数据
    -> LLM 重写规则（标签、摘要等）
    -> Feed 分块存储与向量索引
    -> Query / RSS / MCP / Notify
    -> zenfeed-web
```

当前默认配置会使用 `Qwen/Qwen3-8B` 生成内容，并使用 `Qwen/Qwen3-Embedding-4B` 建立向量索引。模型、供应商和提示词都可以通过 YAML 配置替换。

## 标签体系

默认配置会为新文章生成 1-3 个受控中文标签。标签只描述主题，不重复内容类型，也不使用公司名、人物名和模型名，避免筛选项无限增长。

标签分为三层：

- 核心主题：大语言模型、智能体、多模态、生成式媒体、计算机视觉、语音音频、机器人与具身、数据与检索、AI 编程、AI 基础设施。
- 研发阶段：训练与微调、推理与部署、评测与基准、安全与对齐、网络与隐私。
- 应用语境：产品发布、开源生态、企业应用、行业与商业、政策治理、科学与教育、自然与影像、观点评论、人物动态。

完整规则见 [文章标签体系](docs/tag-taxonomy-zh.md)。历史文章可以通过 `POST /update_feed_labels` 写入持久化标签覆盖；覆盖数据保存在 Feed 数据目录中，并参与查询统计和前端筛选。

## 快速启动

### 使用预构建镜像

下载 `docker-compose.yml` 后执行：

```bash
API_KEY="your-api-key" docker compose up -d
```

Web UI：http://127.0.0.1:1400

HTTP API：http://127.0.0.1:1300

API Key 只用于首次初始化配置。之后配置保存在 Docker 的 `config` 卷中。

### 从当前源码构建

后端和前端目录需要互为同级目录：

```text
Workspace/
├── zenfeed/
└── zenfeed-web/
```

```bash
git clone https://github.com/yupeihua1995-spec/zenfeed.git
git clone https://github.com/yupeihua1995-spec/zenfeed-web.git
cd zenfeed

API_KEY="your-api-key" docker compose \
  -f docker-compose.yml \
  -f docker-compose.local.yml \
  up -d --build
```

`docker-compose.local.yml` 将两个仓库构建为 `zenfeed:local` 和 `zenfeed-web:local`。三个服务均使用 `restart: unless-stopped`，Docker 或 OrbStack 启动后会自动恢复。

### 常用命令

```bash
# 查看状态
docker compose -f docker-compose.yml -f docker-compose.local.yml ps

# 查看日志
docker compose -f docker-compose.yml -f docker-compose.local.yml logs -f

# 重新构建并启动
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d --build

# 停止服务但保留数据
docker compose -f docker-compose.yml -f docker-compose.local.yml down
```

不要使用 `down -v`，除非确定要删除配置和历史文章。

## 配置

主要配置区域：

| 配置           | 用途                               |
| -------------- | ---------------------------------- |
| `llms`         | 模型供应商、模型名称和凭据         |
| `scrape`       | 抓取窗口、RSSHub 和订阅源          |
| `storage.feed` | 保留期、分块、Embedding 和重写规则 |
| `scheduls`     | 定时查询与监控任务                 |
| `notify`       | 邮件和 Webhook 通知                |
| `api`          | HTTP、MCP 和 RSS 服务              |

完整字段见 [配置参考](docs/config-zh.md)，重写机制见 [Rewrite 设计](docs/tech/rewrite-zh.md)。

> 配置文件和 API Key 位于 Docker `config` 卷，不应提交到 Git。`query_config` 返回的凭据会被替换为 `<redacted>`。

## API

常用 HTTP 接口：

| 接口                          | 说明                       |
| ----------------------------- | -------------------------- |
| `POST /query`                 | 查询、分页、语义搜索和统计 |
| `POST /query_config`          | 获取脱敏配置与 revision    |
| `POST /apply_config`          | 使用 revision 更新配置     |
| `POST /query_source_statuses` | 查看订阅源运行状态         |
| `POST /refresh_source`        | 手动刷新指定订阅源         |
| `POST /update_feed_labels`    | 为已有文章持久化标签覆盖   |

参见 [Query API](docs/query-api-zh.md)、[RSS API](docs/rss-api-zh.md) 和 [Webhook](docs/webhook-zh.md)。

## 本地开发

需要 Go 1.23.4 或兼容版本。

```bash
go test ./...
go run . --config /path/to/config.yaml
```

前端开发命令见 `zenfeed-web` 仓库。

## 安全边界

- 默认 Compose 仅将 Web UI 和 HTTP API 绑定到 `127.0.0.1`。
- Zenfeed 没有内置用户认证。公网部署必须使用带认证和 TLS 的反向代理。
- 不要把真实 API Key 写入 `docker-compose.yml`、README 或 Git 跟踪的文件。
- 浏览器可见的 `PUBLIC_*` 环境变量不能用于存放秘密。

## 数据与升级

- `data` 卷保存 Feed、索引和历史标签覆盖。
- `config` 卷保存实际运行配置和密钥。
- 更新镜像不会删除数据卷。
- 1.0 之前 API 和配置格式仍可能调整，升级前建议备份两个卷。

## 上游与许可

本项目基于 [glidea/zenfeed](https://github.com/glidea/zenfeed) 持续开发，采用 [AGPL-3.0](LICENSE) 许可证。第三方文章、模型服务和数据源仍受各自条款约束。
