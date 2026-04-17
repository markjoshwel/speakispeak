import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { execSync } from 'child_process'

const gitHash = (() => {
  try { return execSync('git rev-parse --short HEAD').toString().trim() }
  catch { return 'unknown' }
})()

export default defineConfig({
  define: { __GIT_HASH__: JSON.stringify(gitHash) },
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
