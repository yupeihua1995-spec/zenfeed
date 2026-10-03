[中文](README.md)

<p align="center">
  <img src="docs/images/crad.png" alt="zenfeed cover image">
</p>

<p align="center">
  <a href="https://app.codacy.com/gh/glidea/zenfeed/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade"><img src="https://app.codacy.com/project/badge/Grade/1b51f1087558402d85496fbe7bddde89"/></a>
  <a href="https://sonarcloud.io/summary/new_code?id=glidea_zenfeed"><img src="https://sonarcloud.io/api/project_badges/measure?project=glidea_zenfeed&metric=sqale_rating"/></a>
  <a href="https://goreportcard.com/badge/github.com/glidea/zenfeed"><img src="https://goreportcard.com/badge/github.com/glidea/zenfeed"/></a>
  <a href="https://deepwiki.com/glidea/zenfeed"><img src="https://deepwiki.com/badge.svg"/></a>
</p>

<h3 align="center">In the torrent of information (Feed), may you maintain your Zen.</h3>

<p align="center">
zenfeed is your <strong>AI information hub</strong>. It's an intelligent RSS reader, a real-time "news" knowledge base, and a personal secretary that helps you monitor "specific events" and delivers analysis reports.
</p>

<p align="center">
  <a href="https://zenfeed.xyz"><b>Live Demo (RSS Reading Only)</b></a>
  &nbsp;&nbsp;&nbsp;|&nbsp;&nbsp;&nbsp;
  <a href="docs/tech/hld-en.md"><b>Technical Documentation</b></a>
  &nbsp;&nbsp;&nbsp;|&nbsp;&nbsp;&nbsp;
  <a href="#-installation-and-usage"><b>Quick Start</b></a>
</p>

> [!NOTE]
> The description on DeepWiki is not entirely accurate (and I cannot correct it), but the Q&A quality is decent.

---

**epub2rss**: Convert epub ebooks into RSS feeds that update with a chapter every day, [join waitlist](https://epub2rss.pages.dev/)

---

## 💡 Introduction

RSS (Really Simple Syndication) was born in the Web 1.0 era to solve the problem of information fragmentation, allowing users to aggregate and track updates from multiple websites in one place without frequent visits. It pushes website updates to subscribers in summary form for quick information retrieval.

However, with the rise of Web 2.0, social media, and algorithmic recommendations, RSS never became mainstream. The shutdown of Google Reader in 2013 was a landmark event. As Zhang Yiming (founder of ByteDance) pointed out at the time, RSS demands a lot from its users: strong information filtering skills and self-discipline to manage subscription sources, otherwise it's easy to get drowned in information noise. He believed that for most users, easier "personalized recommendations" were a better solution, which led to the creation of Toutiao and Douyin (TikTok).

Algorithmic recommendations have indeed lowered the barrier to accessing information, but their tendency to over-cater to human weaknesses often leads to filter bubbles and entertainment addiction. If you want to get truly valuable content from your information stream, you need even greater self-control to resist the algorithm's "feed."

So, is pure RSS subscription the answer? Not entirely. Information overload and the difficulty of filtering (information noise) are still major pain points for RSS users.

Confucius spoke of the "Doctrine of the Mean" in all things. Can we find a middle ground that allows us to enjoy the sense of control and high-quality sources from active RSS subscriptions while using technology to overcome the drawback of information overload?

Give zenfeed a try! **AI + RSS** might be a better way to consume information in this era. zenfeed aims to leverage the power of AI to help you automatically filter and summarize the information you care about, allowing you to maintain your Zen in the torrent of information (Feed).

> Reference Article: [AI Revives RSS? - sspai.com (Chinese)](https://sspai.com/post/89494)

---

## ✨ Features

![Zenfeed Architecture](docs/images/arch.png)

**For [RSS](https://en.wikipedia.org/wiki/RSS) Power Users** 🚗
* Your AI-powered RSS reader (use with [zenfeed-web](https://github.com/glidea/zenfeed-web))
* Can act as an [MCP](https://mcp.so/) Server for [RSSHub](https://github.com/DIYgod/RSSHub)
* Customize trusted RSS sources to build a lightning-fast personal AI search engine
* Similar in functionality to [Feedly AI](https://feedly.com/ai)
<details>
  <summary><b>Preview</b></summary>
  <br>
  <img src="docs/images/feed-list-with-web.png" alt="Feed list" width="600">
  <img src="docs/images/chat-with-feeds.png" alt="Chat with feeds" width="500">
</details>

**For Those Seeking an [Everything Tracker](https://www.wwzzai.com/) Alternative** 🔍
* Possesses powerful [information tracking capabilities](https://github.com/glidea/zenfeed/blob/main/docs/config.md#schedule-configuration-schedules) and emphasizes high-quality, customizable data sources.
* Can serve as an RSS version of [AI Chief Intelligence Officer](https://github.com/TeamWiseFlow/wiseflow?tab=readme-ov-file), but more flexible and closer to an engine.
<details>
  <summary><b>Preview</b></summary>
  <br>
  <img src="docs/images/monitoring.png" alt="Monitoring setup" width="500">
  <img src="docs/images/notification-with-web.png" alt="Notification example" width="500">
</details>

**For Those with Information Anxiety (like me)** 😌
* If you're tired of endlessly scrolling through feeds, try the briefing feature. Receive AI-powered briefings at a scheduled time each day for a comprehensive and efficient overview, eliminating the hidden costs of context switching. A bit of a renaissance feel, don't you think? ✨
* "zenfeed" is a combination of "zen" and "feed," meaning: in the torrent of information (feed), may you maintain your zen.
<details>
  <summary><b>Preview</b></summary>
  <br>
  <img src="docs/images/daily-brief.png" alt="Daily brief example" width="500">
</details>

**For Developers** 🔬
* **Pipelined Processing**: Similar to Prometheus's [Relabeling](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#relabel_config), zenfeed abstracts each piece of content into a set of labels. At each stage of the pipeline, you can use custom prompts to process these labels (e.g., scoring, classifying, summarizing, filtering).
* **Flexible Orchestration**: Based on the processed labels, you can freely query, filter, [route](https://github.com/glidea/zenfeed/blob/main/docs/config.md#notification-routing-configuration-notifyroute-and-notifyroutesub_routes), and [send notifications](https://github.com/glidea/zenfeed/blob/main/docs/config.md#notification-channel-email-configuration-notifychannelsemail), giving zenfeed a highly tool-oriented and customizable nature. For details, see [Rewrite Rules](docs/tech/rewrite-en.md).
* **Open APIs**:
  * [Query API](/docs/query-api-en.md)
  * [RSS Exported API](/docs/rss-api-en.md)
  * [Notify Webhook](/docs/webhook-en.md)
  * [Extensive Declarative YAML Configuration](/docs/config.md)
<details>
  <summary><b>Preview</b></summary>
  <br>
  <img src="docs/images/update-config-with-web.png" alt="Update config via web" width="500">
</details>

<p align="center">
  <a href="docs/preview.md"><b>➡️ See More Previews</b></a>
</p>

---

## 🚀 Installation and Usage

### 1. Prerequisites

> [!IMPORTANT]
> zenfeed uses model services from [SiliconFlow](https://cloud.siliconflow.cn/en) by default.
> *   Models: `Qwen/Qwen3-8B` (Free) and `Qwen/Qwen3-Embedding-4B`.
> *   If you don't have a SiliconFlow account yet, use this [**invitation link**](https://cloud.siliconflow.cn/i/U2VS0Q5A) to get a **¥14** credit.
> *   If you need to use other providers or models, or for more detailed custom deployments, please refer to the [Configuration Documentation](https://github.com/glidea/zenfeed/blob/main/docs/config.md) to edit `docker-compose.yml`.

### 2. One-Click Deployment

> Get the service up and running in as little as one minute.

#### Mac / Linux

```bash
# Download the configuration file
curl -L -O https://raw.githubusercontent.com/glidea/zenfeed/main/docker-compose.yml

# Start the service (replace with your API_KEY)
API_KEY="sk-..." docker-compose -p zenfeed up -d
```

#### Windows (PowerShell)

```powershell
# Download the configuration file
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/glidea/zenfeed/main/docker-compose.yml" -OutFile "docker-compose.yml"

# Start the service (replace with your API_KEY)
$env:API_KEY = "sk-..."; docker-compose -p zenfeed up -d
```

🎉 **Deployment Complete!**
Access it at http://localhost:1400

> [!WARNING]
> *   If you deploy zenfeed on a public server such as a VPS, do not expose ports `1400`, `1300`, `1301`, or `9090` directly. Put the Web UI behind an authenticated TLS reverse proxy; the default Compose configuration binds the Web UI and HTTP API to the host loopback interface only.
> *   **Security Notice:** zenfeed does not provide built-in user authentication. Treat every reachable client as trusted and keep the management API and telemetry endpoints private.

### 3. Getting Started

#### Add RSS Subscription Feeds

<img src="docs/images/web-add-source.png" alt="Add RSS source via web" width="400">

> *   To migrate from Follow, please refer to [migrate-from-follow-en.md](docs/migrate-from-follow-en.md).
> *   After adding a source, zenfeed needs to access the origin site, so ensure your network is connected.
> *   Please wait a few minutes after adding for content to be fetched and processed, especially if the model has strict rate limits.

#### Configure Daily Briefings, Monitoring, etc.

<img src="docs/images/notification-with-web.png" alt="Configure notifications via web" width="400">

#### Configure MCP (Optional)
For example, to configure MCP and connect to Zenfeed with Cherry Studio, see [Cherry Studio MCP](docs/cherry-studio-mcp-en.md).
> Default address `http://localhost:1301/sse`

#### More...
The web UI doesn't fully capture zenfeed's powerful flexibility. For more ways to play, please check the [Configuration Documentation](docs/config.md)

---

## 🗺️ Roadmap

We have some cool features planned. Check out our [Roadmap](/docs/roadmap-en.md) and feel free to share your suggestions!

---

## 💬 Community and Support

> **For usage questions, please prioritize opening an [Issue](https://github.com/glidea/zenfeed/issues).** This helps others with similar problems and allows for better tracking and resolution.

<table>
  <tr>
    <td align="center">
      <img src="docs/images/wechat.png" alt="Wechat QR Code" width="150">
      <br>
      <strong>Join WeChat Group</strong>
    </td>
    <td align="center">
      <img src="docs/images/sponsor.png" alt="Sponsor QR Code" width="150">
      <br>
      <strong>Buy Me a Coffee 🧋</strong>
    </td>
  </tr>
</table>

Since you've read this far, how about giving us a **Star ⭐️**? It's the biggest motivation for me to keep maintaining this project!

If you have any interesting AI job opportunities, please contact me!

---

## 🧩 Ecosystem Projects

### [Ruhang365 Daily](https://daily.ruhang365.com)
Founded in 2017, Ruhang365 aims to build a community for sharing expertise and growing together, starting with industry information exchange. It is dedicated to providing comprehensive career consulting, training, niche community interactions, and resource collaboration services for internet professionals.

*Experimental Content Sources (Updates Paused)*
*   [V2EX](https://v2ex.analysis.zenfeed.xyz/)
*   [LinuxDO](https://linuxdo.analysis.zenfeed.xyz/)

---

## 📝 Notes and Disclaimer

### Notes
*   **Version Compatibility:** Backward compatibility for APIs and configurations is not guaranteed before version 1.0.
*   **Open Source License:** The project uses the AGPLv3 license. Any forks or distributions must also remain open source.
*   **Commercial Use:** Please contact the author to register for commercial use. Support can be provided within reasonable limits. We welcome legitimate commercial applications but discourage using this project for illicit activities.
*   **Data Storage:** Data is not stored permanently; the default retention period is 8 days.

### Acknowledgements
*   Thanks to [eryajf](https://github.com/eryajf) for the [Compose Inline Config](https://github.com/glidea/zenfeed/issues/1) suggestion, which makes deployment easier to understand.
*   [![Powered by DartNode](https://dartnode.com/branding/DN-Open-Source-sm.png)](https://dartnode.com "Powered by DartNode - Free VPS for Open Source")

### Contributing
*   The contribution guidelines are still a work in progress, but we adhere to one core principle: "Code Style Consistency."

### Disclaimer

<details>
<summary><strong>Click to expand for the full disclaimer</strong></summary>

**Before using the `zenfeed` software (hereinafter "the Software"), please read and understand this disclaimer carefully. By downloading, installing, using the Software or any related services, you acknowledge that you have read, understood, and agree to be bound by all the terms of this disclaimer. If you do not agree with any part of this disclaimer, please cease using the Software immediately.**

1.  **"AS IS" BASIS:** The Software is provided on an "as is" and "as available" basis, without any warranties of any kind, either express or implied. The project authors and contributors make no representations or warranties regarding the Software's merchantability, fitness for a particular purpose, non-infringement, accuracy, completeness, reliability, security, timeliness, or performance.

2.  **USER RESPONSIBILITY:** You are solely responsible for all your activities conducted through the Software. This includes, but is not limited to:
    *   **Data Source Selection:** You are responsible for selecting and configuring the data sources (e.g., RSS feeds, future potential Email sources) to be connected. You must ensure that you have the right to access and process the content from these sources and comply with their respective terms of service, copyright policies, and applicable laws and regulations.
    *   **Content Compliance:** You must not use the Software to process, store, or distribute any illegal, infringing, defamatory, obscene, or otherwise objectionable content.
    *   **API Key and Credential Security:** You are responsible for safeguarding any API keys, passwords, or other credentials you configure within the Software. The project authors and contributors are not liable for any loss or damage arising from your failure to do so.
    *   **Configuration and Use:** You are responsible for the correct configuration and use of the Software's features, including content processing pipelines, filtering rules, notification settings, etc.

3.  **THIRD-PARTY CONTENT AND SERVICES:** The Software may integrate with or rely on third-party data sources and services (e.g., RSSHub, LLM providers, SMTP services). The project authors and contributors are not responsible for the availability, accuracy, legality, security, or terms of service of such third-party content or services. Your interactions with these third parties are governed by their respective terms and policies. The copyright of third-party content accessed or processed through the Software (including original articles, summaries, classifications, scores, etc.) belongs to the original rights holders. You are solely responsible for any legal liabilities that may arise from your use of such content.

4.  **NO GUARANTEE OF PROCESSING ACCURACY:** The Software uses technologies like Large Language Models (LLMs) to process content (e.g., for summaries, classifications, scoring, filtering). These results may be inaccurate, incomplete, or biased. The project authors and contributors are not responsible for any decisions or actions taken based on these processing results. The accuracy of semantic search results is also affected by multiple factors and is not guaranteed.

5.  **LIMITATION OF LIABILITY:** In no event shall the project authors or contributors be liable for any direct, indirect, incidental, special, exemplary, or consequential damages (including, but not limited to, procurement of substitute goods or services; loss of use, data, or profits; or business interruption) however caused and on any theory of liability, whether in contract, strict liability, or tort (including negligence or otherwise) arising in any way out of the use of this software, even if advised of the possibility of such damage.

6.  **OPEN SOURCE SOFTWARE:** The Software is licensed under the AGPLv3 license. You are responsible for understanding and complying with the terms of this license.

7.  **NOT LEGAL ADVICE:** This disclaimer does not constitute legal advice. If you have any questions about the legal implications of using the Software, you should consult with a qualified legal professional.

8.  **MODIFICATION AND ACCEPTANCE:** The project authors reserve the right to modify this disclaimer at any time. Your continued use of the Software will be deemed acceptance of the modified terms.

**Please be aware: Crawling, processing, and distributing copyrighted content using the Software may carry legal risks. Users are responsible for ensuring that their use complies with all applicable laws, regulations, and third-party terms of service. The project authors and contributors assume no liability for any legal disputes or losses arising from the user's misuse or improper use of the Software.**

</details>
