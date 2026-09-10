import { defineConfig } from 'vitepress'
import { withMermaid } from 'vitepress-plugin-mermaid'

export default withMermaid(
  defineConfig({
  title: 'OxMQ',
  description: 'The Distributed Message Queue Engine for Java 21 Loom (100% BullMQ Parity)',
  base: process.env.VITEPRESS_BASE || '/',
  head: [
    ['link', { rel: 'icon', type: 'image/png', href: '/assets/oxmq-icon.png' }]
  ],
  themeConfig: {
    logo: '/assets/oxmq-icon.png',
    siteTitle: 'OxMQ',
    nav: [
      { text: 'Guide', link: '/guide/what-is-oxmq' },
      { text: 'Concepts', link: '/concepts/queues' },
      { text: 'Patterns', link: '/patterns/rate-limiting' },
      { text: 'Operations', link: '/operations/redis-topology' },
      { text: 'Bull-Board', link: '/interop/bull-board' },
      {
        text: 'v1.0.0',
        items: [
          { text: 'GitHub Release', link: 'https://github.com/gaurav10610/oxmq/releases/tag/v1.0.0' },
          { text: 'GitHub Repository', link: 'https://github.com/gaurav10610/oxmq' }
        ]
      }
    ],
    sidebar: [
      {
        text: '🚀 Getting Started',
        collapsed: false,
        items: [
          { text: 'What is OxMQ?', link: '/guide/what-is-oxmq' },
          { text: 'Quickstart (Core)', link: '/guide/quickstart' },
          { text: 'Spring Boot 3+ Starter', link: '/guide/spring-boot' },
          { text: 'Virtual Threads (Loom)', link: '/guide/virtual-threads' },
          { text: 'Configuration Reference', link: '/guide/configuration' }
        ]
      },
      {
        text: '⚙️ Core Concepts',
        collapsed: false,
        items: [
          { text: 'Queues & Batching', link: '/concepts/queues' },
          { text: 'Jobs & Lifecycle', link: '/concepts/jobs' },
          { text: 'Workers & Concurrency', link: '/concepts/workers' },
          { text: 'FlowProducer (DAG Workflows)', link: '/concepts/flows' },
          { text: 'QueueEvents & Streams', link: '/concepts/events' }
        ]
      },
      {
        text: '💡 Patterns & Recipes',
        collapsed: false,
        items: [
          { text: 'Rate Limiting & Group Keys', link: '/patterns/rate-limiting' },
          { text: 'Scheduling & Cron Jobs', link: '/patterns/scheduling-cron' },
          { text: 'Retries & Backoff Policies', link: '/patterns/retries-backoff' },
          { text: 'Debounce & Deduplication', link: '/patterns/debounce-deduplication' },
          { text: 'Batch Ingestion Architecture', link: '/patterns/batch-dequeue' }
        ]
      },
      {
        text: '🌐 Interoperability & UI',
        collapsed: false,
        items: [
          { text: 'BullMQ Wire Compatibility', link: '/interop/bullmq-wire-protocol' },
          { text: 'Bull-Board Dashboard Setup', link: '/interop/bull-board' }
        ]
      },
      {
        text: '🛠️ Operations & Internals',
        collapsed: false,
        items: [
          { text: 'Redis Key Topology', link: '/operations/redis-topology' },
          { text: '49 Lua Scripts Catalog', link: '/operations/lua-scripts-catalog' },
          { text: 'JMH Benchmarks & Tuning', link: '/operations/benchmarks' },
          { text: 'Production Troubleshooting', link: '/operations/troubleshooting' },
          { text: 'Attribution & License', link: '/attribution' }
        ]
      }
    ],
    socialLinks: [
      { icon: 'github', link: 'https://github.com/gaurav10610/oxmq' }
    ],
    search: {
      provider: 'local'
    },
    footer: {
      message: 'Released under the Apache 2.0 / MIT Open-Source Licenses.',
      copyright: 'Copyright © 2026 Gaurav Kumar Yadav & OxMQ Contributors'
    }
  }
}))
