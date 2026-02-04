import { defineConfig } from 'vitepress'
import { withMermaid } from 'vitepress-plugin-mermaid'
import llmstxt from 'vitepress-plugin-llms'

export default withMermaid(
  defineConfig({
    title: 'Foundatio Repositories',
    description: 'Production-grade repository pattern for .NET with Elasticsearch',
    lang: 'en-US',

    lastUpdated: true,
    cleanUrls: true,

    head: [
      ['link', { rel: 'icon', type: 'image/svg+xml', href: 'https://raw.githubusercontent.com/FoundatioFx/Foundatio/master/media/foundatio.svg' }],
    ],

    vite: {
      plugins: [
        llmstxt(),
      ],
    },

    themeConfig: {
      logo: {
        light: 'https://raw.githubusercontent.com/FoundatioFx/Foundatio/master/media/foundatio.svg',
        dark: 'https://raw.githubusercontent.com/FoundatioFx/Foundatio/master/media/foundatio-dark-bg.svg',
      },
      siteTitle: 'Repositories',

      nav: [
        { text: 'Guide', link: '/guide/what-is-foundatio-repositories' },
        { text: 'GitHub', link: 'https://github.com/FoundatioFx/Foundatio.Repositories' },
      ],

      sidebar: [
        {
          text: 'Introduction',
          items: [
            { text: 'What is Foundatio.Repositories?', link: '/guide/what-is-foundatio-repositories' },
            { text: 'Getting Started', link: '/guide/getting-started' },
          ],
        },
        {
          text: 'Core Concepts',
          items: [
            { text: 'Repository Pattern', link: '/guide/repository-pattern' },
            { text: 'Elasticsearch Setup', link: '/guide/elasticsearch-setup' },
            { text: 'CRUD Operations', link: '/guide/crud-operations' },
            { text: 'Querying', link: '/guide/querying' },
          ],
        },
        {
          text: 'Configuration',
          items: [
            { text: 'Configuration Options', link: '/guide/configuration' },
            { text: 'Validation', link: '/guide/validation' },
          ],
        },
        {
          text: 'Features',
          items: [
            { text: 'Caching', link: '/guide/caching' },
            { text: 'Message Bus', link: '/guide/message-bus' },
            { text: 'Patch Operations', link: '/guide/patch-operations' },
            { text: 'Soft Deletes', link: '/guide/soft-deletes' },
            { text: 'Versioning', link: '/guide/versioning' },
          ],
        },
        {
          text: 'Advanced Topics',
          items: [
            { text: 'Index Management', link: '/guide/index-management' },
            { text: 'Migrations', link: '/guide/migrations' },
            { text: 'Jobs', link: '/guide/jobs' },
            { text: 'Custom Fields', link: '/guide/custom-fields' },
            { text: 'Troubleshooting', link: '/guide/troubleshooting' },
          ],
        },
      ],

      socialLinks: [
        { icon: 'github', link: 'https://github.com/FoundatioFx/Foundatio.Repositories' },
        { icon: 'discord', link: 'https://discord.gg/6HxgFCx' },
      ],

      editLink: {
        pattern: 'https://github.com/FoundatioFx/Foundatio.Repositories/edit/main/docs/:path',
        text: 'Edit this page on GitHub',
      },

      search: {
        provider: 'local',
      },

      footer: {
        message: 'Released under the Apache 2.0 License.',
        copyright: 'Copyright © 2015-present Foundatio',
      },
    },

    mermaid: {
      // Mermaid configuration
    },
  })
)
