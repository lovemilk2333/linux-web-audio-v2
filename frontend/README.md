# Linux Web Audio 前端验证页

前端使用 Vue 3、WebSocket、`libopus-wasm` 和 AudioWorklet 测试服务端的音频流协议。

```sh
# 在 frontend/ 目录中
pnpm install
pnpm dev
```

浏览器访问 Vite 输出的本地地址，点击“开始播放”；之后按钮切换为“停止播放”，停止时会关闭 WebSocket、释放播放器资源，下次开始会重新建立连接。握手完成后会持续播放。Vite 已将 `/backend` 的 WebSocket 请求代理到 `127.0.0.1:8643`；先在另一终端运行 `go run ./cmd/server`（从项目根目录执行）。页面会显示准备、连接、握手、等待音频、收到音频或错误状态，异常音频包会作为警告显示并继续等待后续数据。浏览器需要允许音频播放与 AudioWorklet（使用 localhost 或 HTTPS）。

验证命令：`pnpm build`；协议和播放器单元测试：`node --test tests/*.test.mjs`（需要支持直接运行 TypeScript 的 Node 版本，例如 Node 26）。后端回归测试：在项目根目录执行 `go test ./...`；要从默认输出设备监视源接收并校验实时 Opus 流，可运行 `LIVE_AUDIO_TEST=1 go test ./core -run TestLiveAudioStream -count=1 -v`。页面支持 `none`、`gzip`、`lz4`、`zstd:1` 和 `zstd:3` pocket 压缩，zstd/lz4 通过 Go WASM bridge 实现；压缩 pocket 通过类型字段最高位自动识别。

服务端按 2.5 ms/帧发送双声道 48 kHz Opus；握手的 `tb: 40` 表示约 100 ms 的初始帧请求，播放器等待至少 40 ms 数据才开始输出。页面显示播放欠载和溢出丢包计数；若仍有断续或爆音，请对照计数与服务端日志定位。

服务端可通过 `--audio-threshold` 设置发送音频包前至少需要的 Opus 帧数，默认值为 `4`；没有可发送的音频帧时不会发送空 pocket。例如：`go run ./cmd/server --buffer-rate 400 --audio-threshold 4`。
