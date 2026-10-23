import { defineConfig } from 'vitepress'
import { withMermaid } from 'vitepress-plugin-mermaid'

const base = (process.env.VITEPRESS_BASE || '/oxmq/').replace(/\/+$/, '') + '/'

export default withMermaid(
  defineConfig({
    title: 'OxMQ',
    description: 'The Distributed Message Queue Engine for Java 17+ & Java 21 Loom (100% BullMQ Parity)',
    base,
    head: [
      ['link', { rel: 'icon', type: 'image/png', href: `${base}assets/oxmq-icon.png` }],
      ['link', { rel: 'icon', type: 'image/png', sizes: '32x32', href: `${base}assets/oxmq-icon.png` }],
      ['link', { rel: 'icon', type: 'image/png', sizes: '16x16', href: `${base}assets/oxmq-icon.png` }],
      ['link', { rel: 'shortcut icon', href: `${base}favicon.ico` }],
      ['link', { rel: 'apple-touch-icon', href: `${base}assets/oxmq-icon.png` }]
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
        { text: 'Roadmap', link: '/ROADMAP' },
        {
          text: 'v1.7.0',
          items: [
            { text: 'GitHub Release', link: 'https://github.com/gaurav10610/oxmq/releases/tag/v1.7.0' },
            { text: 'GitHub Repository', link: 'https://github.com/gaurav10610/oxmq' }
          ]
        }
      ],
      sidebar: [
        {
          text: 'Getting Started',
          collapsed: false,
          items: [
            { text: 'What is OxMQ?', link: '/guide/what-is-oxmq' },
            { text: 'Quickstart Guide', link: '/guide/quickstart' },
            { text: 'Spring Boot 3+ Starter', link: '/guide/spring-boot' },
            { text: 'Virtual Threads (Project Loom)', link: '/guide/virtual-threads' },
            { text: 'Context Isolation & Namespacing', link: '/guide/namespacing' },
            { text: 'Configuration Reference', link: '/guide/configuration' },
            { text: 'Full Manual (Getting Started)', link: '/GETTING_STARTED' }
          ]
        },
        {
          text: 'Core Concepts',
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
          text: 'Design Patterns',
          collapsed: false,
          items: [
            { text: 'Request-Reply Synchronization', link: '/patterns/request-reply' },
            { text: 'Rate Limiting & Concurrency', link: '/patterns/rate-limiting' },
            { text: 'Retries & Backoff Policies', link: '/patterns/retries-backoff' },
            { text: 'Debounce & Deduplication', link: '/patterns/debounce-deduplication' },
            { text: 'Scheduling & Cron Jobs', link: '/patterns/scheduling-cron' },
            { text: 'High-Throughput Batch Dequeue', link: '/patterns/batch-dequeue' }
          ]
        },
        {
          text: 'Plugins & Enterprise',
          collapsed: false,
          items: [
            { text: 'Plugins & Interceptors Guide', link: '/guide/plugins' },
            { text: 'OxDoc 365 Enterprise Showcase', link: '/guide/oxdoc-365' },
            { text: 'Multi-Tenant Scaling Strategy', link: '/operations/oxdoc-365-multi-tenant-strategy' }
          ]
        },
        {
          text: 'Architecture & Under the Hood',
          collapsed: false,
          items: [
            { text: 'Architecture & Engine Internals', link: '/ARCHITECTURE' },
            { text: '49 Lua Scripts Catalog', link: '/operations/lua-scripts-catalog' },
            { text: 'Redis Key Topology & Wire Format', link: '/operations/redis-topology' },
            { text: 'Memory Optimization & Tuning', link: '/operations/memory-optimization' },
            { text: 'BullMQ Wire Compatibility', link: '/interop/bullmq-wire-protocol' },
            { text: 'Architectural Comparison (OxMQ vs BullMQ)', link: '/COMPARISON' }
          ]
        },
        {
          text: 'Operations & Observability',
          collapsed: false,
          items: [
            { text: 'Bull-Board Web UI Dashboard', link: '/interop/bull-board' },
            { text: 'Scale & DAG Benchmarks', link: '/operations/benchmarks' },
            { text: 'Benchmarking Architecture', link: '/benchmarks/benchmarks-architecture' },
            { text: 'Disaster Recovery & Chaos Resilience', link: '/operations/disaster-recovery' },
            { text: 'Production Troubleshooting', link: '/operations/troubleshooting' }
          ]
        },
        {
          text: 'Project & Community',
          collapsed: false,
          items: [
            { text: 'Strategic Roadmap', link: '/ROADMAP' },
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
