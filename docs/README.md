# Foundatio.Repositories Documentation

This folder contains the VitePress documentation site for Foundatio.Repositories.

## Development

```bash
# Install dependencies
npm install

# Start development server
npm run dev

# Build for production
npm run build

# Preview production build
npm run preview
```

## Structure

- `.vitepress/config.ts` - VitePress configuration
- `guide/` - Documentation pages
- `public/` - Static assets
- `index.md` - Homepage

## Deployment

Documentation is automatically deployed to GitHub Pages when changes are pushed to the `main` branch. See `.github/workflows/docs.yml` for the deployment workflow.
