<script setup lang="ts">
import { computed, onUnmounted, ref } from 'vue'
import { createDecoder } from 'libopus-wasm'
import playerWorkletUrl from '/player.js?url'
import {
  PocketType,
  decodeClose,
  decodeHandshakeInfo,
  decodeOpusFrames,
  decodePocketWithCompression,
  encodeBufferWithCompression,
  encodeClose,
  encodeHandshakeWithCompression,
  isNewerSequence,
  type Compression,
} from './protocol'

const status = ref<'idle' | 'preparing' | 'connecting' | 'handshaking' | 'waiting' | 'receiving' | 'error'>('idle')
const playing = ref(false)
const message = ref('点击开始以连接音频服务')
const warning = ref('')
const receivedFrames = ref(0)
const underruns = ref(0)
const droppedPackets = ref(0)
const bufferRate = ref(4)
const selectedCompression = ref<Compression>('zstd:1')
const streamAddress = ref('')
const negotiatedBuffer = ref<number | null>(null)
const sessionActive = ref(false)

const bufferDuration = computed(() => `${(bufferRate.value * 2.5).toFixed(1)} ms`)
const negotiatedDuration = computed(() => negotiatedBuffer.value === null ? '—' : `${(negotiatedBuffer.value * 2.5).toFixed(1)} ms`)
const sliderFill = computed(() => `${((bufferRate.value - 1) / 399) * 100}%`)
const statusLabel = computed(() => ({
  idle: '未连接', preparing: '准备中', connecting: '连接中', handshaking: '握手中',
  waiting: '缓冲中', receiving: '播放中', error: '连接异常',
}[status.value]))
const compressionLabel = computed(() => selectedCompression.value === 'none' ? '不压缩' : selectedCompression.value)
const actionLabel = computed(() => sessionActive.value ? '停止播放' : '开始播放')

type Decoder = Awaited<ReturnType<typeof createDecoder>>
type Session = {
  context: AudioContext
  decoder: Decoder | null
  node: AudioWorkletNode | null
  socket: WebSocket | null
  timeout: ReturnType<typeof setTimeout> | null
  lastSequence: number | null
  receivedFrames: number
  lastReport: number
  lastDisplay: number
  handshakeComplete: boolean
  resuming: boolean
  compression: Compression
  receiveChain: Promise<void>
}

let active: Session | null = null
let sharedContext: AudioContext | null = null
let workletModulePromise: Promise<void> | null = null

function getStreamUrl() {
  const address = streamAddress.value.trim() || '/backend/v2/stream'
  const url = new URL(address, location.href)
  if (url.protocol === 'http:') url.protocol = 'ws:'
  else if (url.protocol === 'https:') url.protocol = 'wss:'
  else if (url.protocol !== 'ws:' && url.protocol !== 'wss:') {
    throw new Error('流地址必须使用 http(s) 或 ws(s) 协议')
  }
  return url
}

function loadWorklet(context: AudioContext) {
  if (!workletModulePromise) {
    workletModulePromise = context.audioWorklet.addModule(playerWorkletUrl).catch((error: unknown) => {
      workletModulePromise = null
      throw error
    })
  }
  return workletModulePromise
}

function closeSession(session: Session) {
  if (active === session) {
    active = null
    sessionActive.value = false
    playing.value = false
  }
  if (session.timeout !== null) clearTimeout(session.timeout)
  session.timeout = null

  const socket = session.socket
  if (socket) {
    socket.onopen = null
    socket.onmessage = null
    socket.onerror = null
    socket.onclose = null
    try {
      if (socket.readyState === WebSocket.OPEN && session.handshakeComplete) socket.send(encodeClose())
      if (socket.readyState === WebSocket.CONNECTING || socket.readyState === WebSocket.OPEN) socket.close()
    } catch (err) {
      console.error('Failed to close WebSocket:', err)
    }
  }
  session.socket = null

  if (session.node) {
    session.node.port.onmessage = null
    session.node.port.close()
    session.node.disconnect()
    session.node = null
  }
  void session.context.suspend().catch((err: unknown) => console.error('Failed to suspend audio context:', err))
  session.decoder?.free()
  session.decoder = null
}

function fail(session: Session, error: unknown) {
  if (active !== session) return
  message.value = error instanceof Error ? error.message : String(error)
  warning.value = ''
  status.value = 'error'
  closeSession(session)
}

function handleAudio(session: Session, payload: Uint8Array) {
  const frames = decodeOpusFrames(payload)
  for (const frame of frames) {
    if (!isNewerSequence(frame.sequence, session.lastSequence)) continue
    session.lastSequence = frame.sequence
    if (!playing.value || session.resuming) continue
    if (!session.decoder || !session.node) throw new Error('音频播放器尚未准备就绪')
    const interleaved = session.decoder.decodeFloat(frame.data)
    if (interleaved.length === 0) continue
    session.node.port.postMessage({ type: 'PCM_DATA', interleaved, sequence: frame.sequence }, [interleaved.buffer])
    session.receivedFrames++
  }

  if (session.receivedFrames > 0 && playing.value && performance.now() - session.lastDisplay >= 250) {
    receivedFrames.value = session.receivedFrames
    status.value = 'receiving'
    message.value = `正在接收音频帧（${session.receivedFrames} 帧）`
    session.lastDisplay = performance.now()
  }
}

async function start() {
  if (active) return

  status.value = 'preparing'
  message.value = '正在准备音频播放器…'
  receivedFrames.value = 0
  underruns.value = 0
  droppedPackets.value = 0
  warning.value = ''

  let context: AudioContext
  try {
    // Resume the shared context while this click still counts as a user gesture.
    context = sharedContext ?? new AudioContext({ sampleRate: 48000 })
    sharedContext = context
  } catch (err) {
    status.value = 'error'
    message.value = err instanceof Error ? err.message : String(err)
    return
  }

  const session: Session = {
    context,
    decoder: null,
    node: null,
    socket: null,
    timeout: null,
    lastSequence: null,
    receivedFrames: 0,
    lastReport: 0,
    lastDisplay: 0,
    handshakeComplete: false,
    resuming: false,
    compression: selectedCompression.value,
    receiveChain: Promise.resolve(),
  }
  active = session
  sessionActive.value = true
  playing.value = true

  try {
    await context.resume()
    if (active !== session) return

    const decoder = await createDecoder({ sampleRate: 48000, channels: 2 })
    if (active !== session) {
      decoder.free()
      return
    }
    session.decoder = decoder

    await loadWorklet(context)
    if (active !== session) return

    const node = new AudioWorkletNode(context, 'pcm-player-processor', {
      numberOfInputs: 0,
      numberOfOutputs: 1,
      outputChannelCount: [2],
    })
    session.node = node
    node.port.onmessage = (event: MessageEvent<{ type: string; bufferedPackets: number; sequence: number | null; underruns: number; droppedPackets: number }>) => {
      if (active !== session || event.data.type !== 'BUFFER_STATUS') return
      underruns.value = event.data.underruns
      droppedPackets.value = event.data.droppedPackets
      if (event.data.sequence === null) return
      const socket = session.socket
      if (!socket || socket.readyState !== WebSocket.OPEN || !session.handshakeComplete) return
      const now = performance.now()
      if (now - session.lastReport < 100) return
      session.receiveChain = session.receiveChain.then(async () => {
        if (active !== session || socket.readyState !== WebSocket.OPEN || !session.handshakeComplete) return
        socket.send(await encodeBufferWithCompression(
          Math.min(event.data.bufferedPackets, 0xffff),
          event.data.sequence!,
          session.compression,
        ))
        session.lastReport = now
      }).catch((err: unknown) => fail(session, err))
    }
    node.connect(context.destination)

    const socket = new WebSocket(getStreamUrl())
    session.socket = socket
    socket.binaryType = 'arraybuffer'
    if (playing.value) {
      status.value = 'connecting'
      message.value = '正在连接音频服务…'
    }

    session.timeout = setTimeout(() => fail(session, new Error('连接或握手超时')), 6000)
    socket.onopen = () => {
      if (active !== session) return
      try {
        socket.send(encodeHandshakeWithCompression(bufferRate.value, session.compression))
        if (playing.value) {
          status.value = 'handshaking'
          message.value = '已连接，等待服务端握手…'
        }
      } catch (err) {
        fail(session, err)
      }
    }
    socket.onmessage = (event: MessageEvent) => {
      session.receiveChain = session.receiveChain.then(async () => {
        if (active !== session) return
        if (!(event.data instanceof ArrayBuffer)) throw new Error('收到非二进制消息')
        const { type, payload } = await decodePocketWithCompression(event.data, session.compression)
        switch (type) {
          case PocketType.HandshakeResponse: {
            if (session.handshakeComplete) throw new Error('收到意外的握手响应')
            const handshake = decodeHandshakeInfo(payload)
            session.compression = handshake.compression
            negotiatedBuffer.value = handshake.targetBuffer
            session.handshakeComplete = true
            if (session.timeout !== null) clearTimeout(session.timeout)
            session.timeout = null
            if (playing.value) {
              status.value = 'waiting'
              message.value = `握手完成，等待音频（服务端目标缓冲 ${handshake.targetBuffer} 帧）`
            }
            break
          }
          case PocketType.Opus:
            if (!session.handshakeComplete) throw new Error('握手前收到音频帧')
            try {
              handleAudio(session, payload)
              warning.value = ''
            } catch (err) {
              warning.value = `音频包无效，仍在等待后续数据：${err instanceof Error ? err.message : String(err)}`
            }
            break
          case PocketType.Close:
            fail(session, new Error(`服务端关闭连接：${decodeClose(payload)}`))
            break
          default:
            throw new Error(`不支持的服务端消息类型：${type}`)
        }
      }).catch((err: unknown) => fail(session, err))
    }
    socket.onerror = () => fail(session, new Error('WebSocket 连接错误'))
    socket.onclose = (event) => fail(session, new Error(`连接已断开（${event.code}）`))
  } catch (err) {
    fail(session, err)
  }
}

function stop() {
  const session = active
  if (!session) return
  closeSession(session)
  warning.value = ''
  negotiatedBuffer.value = null
  status.value = 'idle'
  message.value = '播放已停止，点击开始以重新连接'
}

function toggle() {
  if (active) stop()
  else void start()
}

onUnmounted(() => {
  if (active) closeSession(active)
  if (sharedContext) {
    void sharedContext.close().catch((err: unknown) => console.error('Failed to close audio context:', err))
    sharedContext = null
    workletModulePromise = null
  }
})
</script>

<template>
  <main class="page-shell">
    <header class="topbar">
      <div class="brand-lockup">
        <span class="brand-mark" aria-hidden="true">◌</span>
        <div>
          <p class="eyebrow">LINUX WEB AUDIO</p>
          <h1>音频流控制台</h1>
        </div>
      </div>
      <div class="status-chip" :class="`status-${status}`">
        <span class="status-dot" aria-hidden="true"></span>
        {{ statusLabel }}
      </div>
    </header>

    <section class="hero-card">
      <div class="hero-copy">
        <p class="eyebrow">实时音频链路</p>
        <h2>稳定、低延迟地播放系统音频</h2>
        <p class="hero-description">通过 WebSocket 接收 Opus 音频，在浏览器中解码并交给 AudioWorklet 播放。</p>
      </div>
      <div class="signal-visual" aria-hidden="true">
        <i v-for="bar in 18" :key="bar" :style="{ '--bar-height': `${24 + ((bar * 17) % 52)}%` }"></i>
      </div>
    </section>

    <div class="dashboard-grid">
      <section class="panel buffer-panel">
        <div class="panel-heading">
          <div>
            <p class="eyebrow">BUFFER RATE</p>
            <h3>目标缓冲</h3>
          </div>
          <div class="buffer-value">
            <strong>{{ bufferRate }}</strong>
            <span>帧</span>
          </div>
        </div>
        <div class="duration-readout">
          <span>约 {{ bufferDuration }}</span>
          <span class="muted">每帧 2.5 ms</span>
        </div>
        <input
          v-model.number="bufferRate"
          class="range-input"
          type="range"
          min="1"
          max="400"
          step="1"
          :style="{ '--range-fill': sliderFill }"
          :disabled="sessionActive"
          aria-label="目标缓冲帧数"
        >
        <div class="range-labels"><span>1 帧</span><span>400 帧</span></div>
        <p class="field-note">连接建立后锁定设置；重新连接即可应用新的缓冲目标。</p>
      </section>

      <section class="panel control-panel">
        <div class="panel-heading">
          <div>
            <p class="eyebrow">PLAYBACK</p>
            <h3>播放控制</h3>
          </div>
          <span class="connection-icon" :class="{ active: sessionActive }" aria-hidden="true">↗</span>
        </div>
        <p class="message" role="status">{{ message }}</p>
        <button class="primary-button" type="button" @click="toggle">
          <span class="button-icon">{{ sessionActive ? '■' : '▶' }}</span>
          {{ actionLabel }}
        </button>
        <div class="setting-row address-row" style="margin-top: 2rem;">
          <label for="stream-address">流地址</label>
          <input
            id="stream-address" 
            v-model="streamAddress"
            class="setting-input"
            type="text"
            placeholder="默认：/backend/v2/stream"
            :disabled="sessionActive"
            autocomplete="url"
            spellcheck="false"
          >
        </div>
        <p class="field-note address-note">支持相对路径或完整 URL；留空使用默认地址。</p>
        <div class="setting-row">
          <label for="compression">传输压缩</label>
          <select id="compression" v-model="selectedCompression" :disabled="sessionActive">
            <option value="zstd:1">zstd:1（推荐）</option>
            <option value="zstd:3">zstd:3（高压缩）</option>
            <option value="lz4">lz4（低延迟）</option>
            <option value="gzip">gzip</option>
            <option value="none">不压缩</option>
          </select>
        </div>
      </section>
    </div>

    <section class="metrics-grid" aria-label="实时指标">
      <div class="metric-card accent-blue">
        <span class="metric-label">已接收音频帧</span>
        <strong>{{ receivedFrames.toLocaleString() }}</strong>
        <span class="metric-unit">frames</span>
      </div>
      <div class="metric-card accent-orange">
        <span class="metric-label">播放欠载</span>
        <strong>{{ underruns.toLocaleString() }}</strong>
        <span class="metric-unit">次数</span>
      </div>
      <div class="metric-card accent-red">
        <span class="metric-label">溢出丢包</span>
        <strong>{{ droppedPackets.toLocaleString() }}</strong>
        <span class="metric-unit">packets</span>
      </div>
      <div class="metric-card accent-green">
        <span class="metric-label">服务端目标</span>
        <strong>{{ negotiatedBuffer ?? '—' }}</strong>
        <span class="metric-unit">{{ negotiatedBuffer === null ? '等待握手' : negotiatedDuration }}</span>
      </div>
    </section>

    <p v-if="warning" class="notice warning" role="alert">{{ warning }}</p>
    <p v-if="status === 'error'" class="notice error">请检查服务端日志、代理配置及浏览器权限后重试。</p>
    <footer class="footer-note">当前协议：Opus / 48 kHz / 双声道 · 压缩：{{ compressionLabel }}</footer>
  </main>
</template>
