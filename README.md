# linux-web-audio-v2

通过 PipeWire/PulseAudio 捕获桌面音频，使用 Opus 编码后通过 WebSocket 推送到浏览器。
浏览器端使用 Web Audio、AudioWorklet 和 `libopus-wasm` 解码播放。

## 依赖

运行服务端需要 Linux 音频会话和 Opus/PulseAudio 开发文件：

```sh
# Arch Linux
sudo pacman -S base-devel go nodejs pnpm opus pipewire pipewire-pulse
```

服务端使用 CGO 访问 PulseAudio/PipeWire 兼容接口，因此发布构建默认使用
`CGO_ENABLED=1`，应在目标 Linux 环境或兼容的交叉编译环境中构建。

## 构建

生成 Go 和前端发布产物：

```sh
make release
```

产物位于：

```text
build/
├── bin/
│   └── webaudiod       # 服务端 release ELF
└── web/                # 前端静态文件
```

Go 发布二进制默认使用 `GOAMD64=v2`，并使用 `-trimpath -ldflags="-s -w"`，移除调试信息和符号表。
Makefile 默认使用串行 Go 编译（`GOMAXPROCS=1`、`-p=1`），并关闭实验特性、固定使用本地工具链，
以兼容部分设备上的 Go 编译器和仅支持 x86-64-v2 的设备；这些参数都可以通过同名变量覆盖。
前端发布构建使用 Vite，按 Chrome 96 可运行的 ES2022 语法输出，关闭 sourcemap，并启用 JS/CSS
压缩和标识符混淆。前端发布构建默认使用相对资源路径 `./`，不会把 `/audio/` 或
其他部署前缀固定写入静态文件，外部访问路径由 Caddy 配置处理。

常用命令：

```sh
make test   # Go 和前端协议/Worklet 测试
make clean  # 删除 build/
```

也可以只构建某一部分：

```sh
make go-release
make frontend-release
```

Makefile 支持覆盖常用构建参数：

```sh
make release GOARCH=arm64
make release GOOS=linux GOARCH=amd64 CGO_ENABLED=1
make frontend-release FRONTEND_BASE=./
```

由于服务端依赖本机音频库，跨平台交叉编译时需要准备对应的 CGO 编译器和
`pkg-config` 配置；最简单可靠的方式是在目标 Linux 系统上执行构建。

## 本地开发

先启动服务端：

```sh
go run ./cmd/server
```

再启动 Vite：

```sh
pnpm --dir frontend install
pnpm --dir frontend dev
```

开发页面默认通过 `/backend` 将 WebSocket 代理到 `127.0.0.1:8643`。
浏览器访问 Vite 地址后点击“开始播放”。浏览器需要支持 AudioWorklet，并允许当前页面播放音频。

服务端参数示例：

```sh
go run ./cmd/server \
  --listen :8643 \
  --base /backend/v2/ \
  --buffer-rate 400 \
  --audio-threshold 4
```

`buffer-rate` 的单位是 10 ms/帧；`audio-threshold` 表示服务端发送音频 pocket
前至少需要的 Opus 帧数。没有足够音频数据时不会发送空 pocket。

## 部署

### systemd user service

服务端必须作为当前音频用户运行，不能作为没有音频会话的 system service：

```sh
make release
install -Dm755 build/bin/webaudiod ~/.local/bin/webaudiod
install -Dm644 deploy/webaudiod.service ~/.config/systemd/user/webaudiod.service
systemctl --user daemon-reload
systemctl --user enable --now webaudiod
```

如果用户退出登录后仍需要保持服务运行：

```sh
loginctl enable-linger "$USER"
```

`deploy/webaudiod.service` 默认启动 `webaudiod`，并通过当前用户的
`XDG_RUNTIME_DIR` 访问 PipeWire/PulseAudio 会话。

### Caddy

`deploy/Caddyfile` 假设：

- 前端静态文件安装到 `/path/to/website`；示例 Caddy 路由将其映射到 `/audio/`；
- Go 服务端监听 `:8643`；
- 其他网站请求由 `:59090` 提供。

先部署前端文件，再按实际域名和路径修改 Caddy 配置：

```sh
make release
sudo install -d /path/to/website
sudo cp -a build/web/. /path/to/website/
sudo cp deploy/Caddyfile /etc/caddy/Caddyfile
sudo systemctl reload caddy
```

反向代理中的 `flush_interval -1` 用于尽快刷新音频 WebSocket 数据，避免代理层额外聚合延迟。

### 前端路径说明

`/audio/` 只是 `deploy/Caddyfile` 中的路由示例，不是前端的默认前缀。
发布前端默认使用相对资源路径 `./`，因此 Caddy 可以将静态文件挂载到任意路径，
无需把 `/audio/` 写入 `VITE_BASE_PATH`。

## 音频与压缩

协议使用 48 kHz、双声道、每帧 10 ms 的 Opus 音频。前端支持：

- `none`
- `gzip`
- `lz4`
- `zstd:1`
- `zstd:3`

zstd/lz4 的浏览器端压缩通过 Go WASM bridge 实现。页面会显示编码、解码、AudioBuffer
写入、WebSocket 调用和客户端缓冲等诊断数据。

完整回归测试：

```sh
make test
```

需要真实音频设备时，可额外运行：

```sh
LIVE_AUDIO_TEST=1 go test ./core -run TestLiveAudioStream -count=1 -v
```
