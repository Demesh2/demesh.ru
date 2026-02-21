# CLAUDE.md — AI Assistant Guide for demesh.ru

## Project Overview

This repository contains a **Telegram Mini App** — a mobile-first e-commerce catalog called **ROYAL MARKET**, built as a single-file React application served as a static site.

The app allows users to browse product categories (nuts, dried fruits, oils, spices, etc.), manage a shopping cart, and submit orders directly via Telegram.

---

## Repository Structure

```
demesh.ru/
├── index.html          # Main application (active, 659 lines)
├── 222index.html       # UI iteration / archived version (929 lines)
├── 3333index.html      # UI iteration / archived version (978 lines)
├── CNAME               # GitHub Pages domain config: demesh.ru
├── README.md           # Minimal placeholder
└── Inclusion & Diversity_files/   # External CSS/font assets
    ├── ac-globalfooter.built.css
    ├── ac-localnav.built.css
    ├── globalheader.css
    ├── main.built.css
    ├── overview.built.css
    └── fonts/
```

**The only file that matters for development is `index.html`.**

---

## Technology Stack

| Layer | Technology |
|---|---|
| Markup | HTML5 |
| UI framework | React 18 (via CDN, UMD build) |
| Language | JavaScript (JSX, transpiled by Babel Standalone in browser) |
| Styling | CSS3 with CSS custom properties |
| Platform | Telegram Mini Apps API |
| Hosting | GitHub Pages (static, push-to-deploy) |

**No build tool, no bundler, no package manager.** All dependencies are loaded via CDN at runtime:

```html
<script src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
<script src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
<script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
```

---

## Application Architecture

The app is a monolithic single-file React application with two main components:

### `App` (main component)

State:
- `view` — `'catalog'` | `'cart'` (two-screen navigation)
- `cat` — active category filter (string)
- `cart` — array of `{ id, name, weightType, priceStr, priceValue, quantity }`
- `name`, `phone`, `address` — delivery form fields

### `PricePill` (memoized sub-component)

Renders a single weight-variant button for a product. Wrapped in `React.memo` to avoid re-renders on cart updates.

### Data shape

```javascript
const data = [
  {
    cat: "Орехи",           // Category name (Russian)
    items: [
      {
        name: "Арахис жареный",   // Product name
        kg: "15р",                // Price per 1 kg
        g500: "8р",               // Price per 500g
        g250: "4,5р",             // Price per 250g (optional)
      },
      // ...
    ]
  },
  // ... 10 more categories
]
```

### Key utility functions

- `parsePrice(s)` — extracts numeric value from Russian-formatted price string (`"4,5р"` → `4.5`)
- `fmt(n)` — formats a number back to Russian rubles string
- `addToCart(item)` — deduplicates by `id` (name + weight combo), increments quantity
- `updateQty(id, delta)` — modifies cart item quantity, removes at 0
- `sendOrder()` — builds a formatted Russian-language order message, opens `tg://` link to @OrEx_zakaz

---

## Telegram Mini App Integration

The app uses the Telegram WebApp API extensively:

```javascript
const tg = window.Telegram?.WebApp;
```

Key integrations:
- **`tg.MainButton`** — "Оформить заказ" (checkout), visible on cart screen
- **`tg.BackButton`** — navigates from cart back to catalog
- **`tg.SecondaryButton`** — shown conditionally
- **`tg.HapticFeedback`** — wrapped in a `haptic` helper object for all touch interactions
- **`tg.CloudStorage`** — user data persistence (falls back to `localStorage`)
- **`tg.themeParams`** — CSS variables (`--tg-theme-*`) for native color theming

All `tg.*` calls are guarded with optional chaining (`?.`) so the app degrades gracefully in a plain browser.

---

## Styling Conventions

- **Telegram theme variables** — colors come from `var(--tg-theme-bg-color)`, `var(--tg-theme-text-color)`, etc., so the UI automatically matches the user's Telegram theme.
- **CSS class naming** — kebab-case (e.g., `cart-open-btn`, `price-pill-added`, `section-label`).
- **Layout** — fixed-positioned header and footer with scrollable content area in between. No CSS framework.
- **Mobile-first** — touch targets are minimum 44px, tap highlight is disabled, smooth scroll is enabled.
- **Animations** — use `transform` and `opacity` only (GPU-accelerated, no layout thrashing).

---

## Code Conventions

- **Language** — all UI text, comments, and order messages are in **Russian**.
- **JavaScript style** — arrow functions, optional chaining (`?.`), template literals, array methods (`map`, `filter`, `find`, `reduce`).
- **React hooks** — `useState`, `useEffect`, `useCallback`, `useMemo`, `useRef`. No Context API or external state management.
- **Performance** — `React.memo` on `PricePill`, `useMemo` for filtered data and category list, `useCallback` for handlers passed as props.
- **Error handling** — `try/catch` around `localStorage` access; `?.` guards on all Telegram API calls.

---

## Development Workflow

### Local testing

1. Open `index.html` directly in a browser (no server required for basic UI review).
2. For Telegram-specific features (haptics, native buttons, CloudStorage), use the [Telegram Mini App test environment](https://core.telegram.org/bots/webapps#testing-mini-apps) or BotFather's bot with a web app configured to point at `demesh.ru`.

### Making changes

All product data, logic, and styles live in `index.html`. Edit it directly.

```
# Edit the main file
vim index.html  # or your editor of choice

# Commit and push
git add index.html
git commit -m "Describe your change"
git push -u origin <branch>
```

### Deployment

Pushing to `main` deploys automatically via GitHub Pages to `https://demesh.ru` (configured via CNAME).

---

## Git Branches

| Branch | Purpose |
|---|---|
| `main` | Production (GitHub Pages) |
| `master` | Legacy / local default |
| `claude/*` | AI-assisted feature/documentation branches |

---

## No Tests, No Build Step

There are no automated tests, no linter config, and no build pipeline. Changes go directly from editor to production.

If adding tests or a linter in the future, recommended tools for this stack:
- **Linting**: ESLint with `eslint-plugin-react`
- **Formatting**: Prettier
- **Testing**: For a CDN-based React app, consider moving to Vite + Vitest if a build step is introduced

---

## Common Tasks

### Add a new product

Find the relevant category in the `data` array in `index.html` and add an entry:

```javascript
{ name: "Новый продукт", kg: "20р", g500: "11р", g250: "6р" }
```

### Add a new category

Append an object to the `data` array:

```javascript
{ cat: "Новая категория", items: [ /* products */ ] }
```

### Change the Telegram order recipient

Search for `@OrEx_zakaz` in `index.html` and replace with the target username.

### Update a price

Prices are plain strings in the `data` array. The `parsePrice()` function handles both `"15р"` and `"4,5р"` formats (comma as decimal separator).

---

## Key Constraints

- **No npm / node_modules** — do not introduce a package.json or build step without refactoring the entire project.
- **Single file** — keep all logic and styles in `index.html` unless explicitly asked to split the project.
- **Russian text** — all user-facing strings must be in Russian.
- **Telegram-first** — design decisions should prioritize the Telegram Mini App UX (mobile, limited screen space, native buttons over custom ones where possible).
