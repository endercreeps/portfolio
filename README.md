# Portfolio Angular One-Page

## Prerequis

- Node.js 20+
- npm 10+

## Installation

```bash
npm install
```

## Lancer en local

```bash
npm start
```

Application disponible sur `http://localhost:4200`.

## Build production

```bash
npm run build
```

## Structure principale

- `src/app/core/services`: services globaux (theme, i18n, seo)
- `src/app/data`: contenu editable FR/EN + config site
- `src/app/models`: types de contenu
- `src/app/sections`: sections one-page (hero, projets, etc.)
- `src/app/shared`: composants/directives reutilisables

## Personnaliser le contenu

- FR: `src/app/data/content.fr.ts`
- EN: `src/app/data/content.en.ts`
- CV: `public/cv/cv.pdf`
- Config globale (SEO/CV/langue par defaut): `src/app/data/site.config.ts`

## Ajouter un projet, experience, etc..

- `src/app/data/content.fr.ts`
- `src/app/data/content.en.ts`

