# Zenfeed

[简体中文](README.md)

Zenfeed is a self-hosted backend for AI-assisted reading. It collects content from RSS and RSSHub, runs configurable LLM rewrite pipelines for summaries, classification, and tagging, and exposes the resulting data to the Web reader, query API, RSS, MCP, and notification channels.

This repository contains the backend service. The companion Web client is maintained in [zenfeed-web](https://github.com/yupeihua1995-spec/zenfeed-web).

## Capabilities

- RSS, RSSHub, and custom source ingestion with source enablement, status reporting, and manual refresh.
- Configurable scrape lookback, retention, and cursor-based query pagination.
- LLM rewrite pipelines for Chinese HTML summaries, controlled topic tags, scores, and custom labels.
- Semantic search plus source, content-type, and tag statistics.
- Scheduled monitoring, email and webhook notifications, RSS output, and MCP access.
- Revision-aware configuration updates that prevent concurrent editors from overwriting each other.
- Lifecycle, concurrent shutdown, storage recovery, and index consistency safeguards.
- Loopback-only API bindings in the default Compose deployment.

## Data Flow

```text
RSS / RSSHub
    -> source filters and metadata
    -> LLM rewrite rules (tags, summaries, and more)
    -> block storage and vector indexes
    -> Query / RSS / MCP / Notify
    -> zenfeed-web
```

The default configuration uses `Qwen/Qwen3-8B` for generation and `Qwen/Qwen3-Embedding-4B` for vector indexing. Providers, models, and prompts are replaceable through YAML configuration.

## Tag Taxonomy

The default pipeline assigns one to three controlled Chinese tags to each new article. Tags describe subject matter rather than repeating content types, and company, person, model, and product names are deliberately excluded to keep the filter set stable.

The taxonomy has three layers:

- Core subjects: language models, agents, multimodality, generative media, computer vision, speech and audio, embodied AI, retrieval, AI coding, and AI infrastructure.
- Development stages: training and fine-tuning, inference and deployment, evaluation, safety and alignment, and network security and privacy.
- Context: product releases, open source, enterprise adoption, business, policy, science and education, nature and imagery, commentary, and personal updates.

See the [Chinese taxonomy reference](docs/tag-taxonomy-zh.md) for the canonical labels. Existing articles can receive persistent overrides through `POST /update_feed_labels`; these overrides participate in query statistics and Web filtering.

## Quick Start

### Prebuilt images

Download `docker-compose.yml`, then run:

```bash
API_KEY="your-api-key" docker compose up -d
```

Web UI: http://127.0.0.1:1400

HTTP API: http://127.0.0.1:1300

The API key initializes the configuration on the first run. The effective configuration is then stored in the Docker `config` volume.

### Build the current source

Keep the backend and frontend as sibling directories:

```text
Workspace/
  zenfeed/
  zenfeed-web/
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

`docker-compose.local.yml` builds `zenfeed:local` and `zenfeed-web:local`. All three services use `restart: unless-stopped`, so they resume when Docker or OrbStack starts.

### Common commands

```bash
# Status
docker compose -f docker-compose.yml -f docker-compose.local.yml ps

# Logs
docker compose -f docker-compose.yml -f docker-compose.local.yml logs -f

# Rebuild and start
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d --build

# Stop while preserving data
docker compose -f docker-compose.yml -f docker-compose.local.yml down
```

Do not run `down -v` unless you intend to remove the configuration and stored articles.

## Configuration

The main configuration sections are:

| Section        | Purpose                                          |
| -------------- | ------------------------------------------------ |
| `llms`         | Model providers, model names, and credentials    |
| `scrape`       | Lookback window, RSSHub, and sources             |
| `storage.feed` | Retention, blocks, embeddings, and rewrite rules |
| `scheduls`     | Scheduled queries and monitoring                 |
| `notify`       | Email and webhook delivery                       |
| `api`          | HTTP, MCP, and RSS services                      |

See the [configuration reference](docs/config.md) and the [rewrite design reference](docs/tech/rewrite-zh.md) (Chinese).

> The effective configuration and API keys live in the Docker `config` volume and must not be committed. `query_config` replaces credentials with `<redacted>`.

## API

Common HTTP endpoints:

| Endpoint                      | Purpose                                            |
| ----------------------------- | -------------------------------------------------- |
| `POST /query`                 | Query, pagination, semantic search, and statistics |
| `POST /query_config`          | Read redacted configuration and its revision       |
| `POST /apply_config`          | Update configuration with revision checking        |
| `POST /query_source_statuses` | Read source runtime status                         |
| `POST /refresh_source`        | Refresh one source                                 |
| `POST /update_feed_labels`    | Persist label overrides for stored articles        |

See the [Query API](docs/query-api-zh.md), [RSS API](docs/rss-api-zh.md), and [Webhook documentation](docs/webhook-zh.md). These endpoint references are currently available in Chinese.

## Development

Go 1.23.4 or a compatible version is required.

```bash
go test ./...
go run . --config /path/to/config.yaml
```

Frontend development commands live in the `zenfeed-web` repository.

## Security

- The default Compose file binds the Web UI and HTTP API to `127.0.0.1`.
- Zenfeed has no built-in user authentication. Public deployments require an authenticated TLS reverse proxy.
- Never place real API keys in `docker-compose.yml`, README files, or Git-tracked files.
- Browser-visible `PUBLIC_*` variables are not suitable for secrets.

## Data and Upgrades

- The `data` volume stores feeds, indexes, and persistent label overrides.
- The `config` volume stores the effective configuration and credentials.
- Rebuilding images does not delete these volumes.
- API and configuration compatibility may still change before 1.0. Back up both volumes before upgrading.

## Upstream and License

This project builds on [glidea/zenfeed](https://github.com/glidea/zenfeed) and is licensed under [AGPL-3.0](LICENSE). Third-party articles, model services, and data sources remain subject to their respective terms.
