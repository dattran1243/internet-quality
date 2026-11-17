# VIQ-Full frontend

React Dashboard and BigData Probe Explorer, with MapLibre and Recharts.
See the [project README](../README.md) for dataset scope and API prerequisites.

From this directory, run npm ci, then npm run dev in your own configured lab.
Dashboard: /. Explorer: /bigdata-map.html.
React reads VITE_API_BASE_URL; see [.env.example](.env.example).
Explorer currently uses the local API on port 8000 directly.
The current Vite build has a single index.html entry; Explorer is not included in dist.
No UI screenshots or same-run frontend E2E acceptance are claimed.
