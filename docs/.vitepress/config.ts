import { defineConfig } from 'vitepress'
import { withMermaid } from 'vitepress-plugin-mermaid'

const base = (process.env.VITEPRESS_BASE || '/oxmq/').replace(/\/+$/, '') + '/'

export default withMermaid(
  defineConfig({
    title: 'OxMQ',
    description: 'The Distributed Message Queue Engine for Java 17+ & Java 21 Loom (100% BullMQ Parity)',
    base,
    sitemap: {
      hostname: 'https://gaurav10610.github.io/oxmq/'
    },
    head: [
      ['link', { rel: 'icon', type: 'image/png', href: `${base}assets/oxmq-icon.png` }],
      ['link', { rel: 'icon', type: 'image/png', sizes: '32x32', href: `${base}assets/oxmq-icon.png` }],
      ['link', { rel: 'icon', type: 'image/png', sizes: '16x16', href: `${base}assets/oxmq-icon.png` }],
      ['link', { rel: 'shortcut icon', href: `${base}favicon.ico` }],
      ['link', { rel: 'apple-touch-icon', href: `${base}assets/oxmq-icon.png` }],
      ['meta', { name: 'author', content: 'Gaurav Kumar Yadav' }],
      ['meta', { name: 'keywords', content: 'OxMQ, BullMQ, Java, Spring Boot, Redis, Distributed Queue, Project Loom, Virtual Threads, Message Queue, Job Queue, Bull-Board, Redisson alternative, Java 21' }],
      ['meta', { property: 'og:type', content: 'website' }],
      ['meta', { property: 'og:site_name', content: 'OxMQ' }],
      ['meta', { property: 'og:title', content: 'OxMQ - Virtual Thread-Native Distributed Queue for Java 17+ & 21' }],
      ['meta', { property: 'og:description', content: 'The modern Java alternative to BullMQ. Powered by Project Loom Virtual Threads, official BullMQ Lua scripts, native Context Isolation, and Request-Reply workflows.' }],
      ['meta', { property: 'og:image', content: 'https://gaurav10610.github.io/oxmq/assets/oxmq-icon.png' }],
      ['meta', { property: 'og:url', content: 'https://gaurav10610.github.io/oxmq/' }],
      ['meta', { name: 'twitter:card', content: 'summary' }],
      ['meta', { name: 'twitter:title', content: 'OxMQ - Virtual Thread-Native Distributed Queue for Java 17+ & 21' }],
      ['meta', { name: 'twitter:description', content: 'The modern Java alternative to BullMQ. Powered by Project Loom Virtual Threads, official BullMQ Lua scripts, and Spring Boot 3+.' }],
      ['meta', { name: 'twitter:image', content: 'https://gaurav10610.github.io/oxmq/assets/oxmq-icon.png' }],
      ['script', { type: 'application/ld+json' }, JSON.stringify({
        '@context': 'https://schema.org',
        '@type': 'SoftwareApplication',
        'name': 'OxMQ',
        'operatingSystem': 'Cross-platform (JVM)',
        'applicationCategory': 'DeveloperApplication',
        'description': 'Virtual Thread-native distributed message queue engine for Java 17+ and 21 with 100% BullMQ wire protocol parity.',
        'softwareVersion': '1.8.0',
        'license': 'https://github.com/gaurav10610/oxmq/blob/main/LICENSE',
        'url': 'https://gaurav10610.github.io/oxmq/',
        'author': {
          '@type': 'Person',
          'name': 'Gaurav Kumar Yadav',
          'url': 'https://github.com/gaurav10610'
        }
      })]
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
          text: 'v1.8.0',
          items: [
            { text: 'GitHub Release', link: 'https://github.com/gaurav10610/oxmq/releases/tag/v1.8.0' },
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
            { text: 'Virtual Queue Groups & Multi-Tenancy', link: '/patterns/virtual-queue-groups' },
            { text: 'Transactional Outbox Pattern', link: '/patterns/transactional-outbox' },
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
      { icon: 'github', link: 'https://github.com/gaurav10610/oxmq' },
      { icon: 'linkedin', link: 'https://www.linkedin.com/in/gaurav-kumar-yadav-6125817a/' }
    ],
    search: {
      provider: 'local'
    },
    footer: {
      message: 'Released under the Apache 2.0 / MIT Open-Source Licenses.',
      copyright: 'Copyright © 2026 <a href="https://www.linkedin.com/in/gaurav-kumar-yadav-6125817a/" target="_blank" rel="noopener">Gaurav Kumar Yadav</a> & OxMQ Contributors'
    }
  }
}))
