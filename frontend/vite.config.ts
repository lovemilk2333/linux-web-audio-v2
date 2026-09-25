import vue from '@vitejs/plugin-vue'
import { defineConfig } from 'vite'

// https://vite.dev/config/
export default defineConfig(({ command }) => ({
  base: command === 'build' ? process.env.VITE_BASE_PATH || './' : '/',
  plugins: [vue()],
  build: {
    target: 'chrome96',
    sourcemap: false,
  },
  server: {
    proxy: {
      '/backend': {
        target: 'http://127.0.0.1:8643',
        changeOrigin: true,
        ws: true
      }
    }
  }
}))
