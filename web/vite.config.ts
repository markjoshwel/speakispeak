import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { execSync } from 'child_process'

const commitCount = (() => {
  try { return execSync('git rev-list --count HEAD').toString().trim() }
  catch { return '?' }
})()

export default defineConfig({
  define: { __COMMIT_COUNT__: JSON.stringify(commitCount) },
  plugins: [react()],
  server: {
    proxy: {
      '/ws': {
        target: 'ws://localhost:6782',
        ws: true,
      },
    },
  },
  build: {
    outDir: 'dist',
  },
})
