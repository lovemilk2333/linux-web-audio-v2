<script setup lang="ts">
import { onUnmounted, ref } from 'vue'
import { createDecoder } from 'libopus-wasm'
import playerWorkletUrl from '/player.js?url'
import {
  PocketType,
  decodeClose,
  decodeHandshake,
  decodeOpusFrames,
  decodePocket,
  encodeBuffer,
  encodeClose,
  encodeHandshake,
  isNewerSequence,
} from './protocol'

const status = ref<'idle' | 'preparing' | 'connecting' | 'handshaking' | 'waiting' | 'receiving' | 'paused' | 'error'>('idle')
const playing = ref(false)
const message = ref('点击开始以连接音频服务')
const warning = ref('')
const receivedFrames = ref(0)
const underruns = ref(0)
const droppedPackets = ref(0)
const targetBuffer = 40

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
}

let active: Session | null = null

function closeSession(session: Session) {
  if (active === session) {
    active = null
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
  void session.context.close().catch((err: unknown) => console.error('Failed to close audio context:', err))
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
    // Start the context while this click still counts as a user gesture.
    context = new AudioContext({ sampleRate: 48000 })
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
  }
  active = session
  playing.value = true

  try {
    await context.resume()
    if (active !== session) return
    if (!playing.value) await context.suspend()
    if (active !== session) return
    if (playing.value && context.state === 'suspended') await context.resume()
    if (active !== session) return

    const decoder = await createDecoder({ sampleRate: 48000, channels: 2 })
    if (active !== session) {
      decoder.free()
      return
    }
    session.decoder = decoder

    await context.audioWorklet.addModule(playerWorkletUrl)
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
      try {
        socket.send(encodeBuffer(Math.min(event.data.bufferedPackets, 0xffff), event.data.sequence))
        session.lastReport = now
      } catch (err) {
        fail(session, err)
      }
    }
    node.connect(context.destination)

    const url = new URL('/backend/v2/stream', location.href)
    url.protocol = location.protocol === 'https:' ? 'wss:' : 'ws:'
    const socket = new WebSocket(url)
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
        socket.send(encodeHandshake(targetBuffer))
        if (playing.value) {
          status.value = 'handshaking'
          message.value = '已连接，等待服务端握手…'
        }
      } catch (err) {
        fail(session, err)
      }
    }
    socket.onmessage = (event: MessageEvent) => {
      if (active !== session) return
      try {
        if (!(event.data instanceof ArrayBuffer)) throw new Error('收到非二进制消息')
        const { type, payload } = decodePocket(event.data)
        switch (type) {
          case PocketType.HandshakeResponse: {
            if (session.handshakeComplete) throw new Error('收到意外的握手响应')
            const buffer = decodeHandshake(payload)
            session.handshakeComplete = true
            if (session.timeout !== null) clearTimeout(session.timeout)
            session.timeout = null
            if (playing.value) {
              status.value = 'waiting'
              message.value = `握手完成，等待音频（服务端目标缓冲 ${buffer} 帧）`
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
      } catch (err) {
        fail(session, err)
      }
    }
    socket.onerror = () => fail(session, new Error('WebSocket 连接错误'))
    socket.onclose = (event) => fail(session, new Error(`连接已断开（${event.code}）`))
  } catch (err) {
    fail(session, err)
  }
}

function pause() {
  const session = active
  if (!session || !playing.value) return
  playing.value = false
  warning.value = ''
  status.value = 'paused'
  message.value = '已暂停（连接保持中）'
  session.node?.port.postMessage({ type: 'CLEAR' })
  if (!session.resuming) {
    void session.context.suspend().catch((err: unknown) => fail(session, err))
  }
}

async function resume() {
  const session = active
  if (!session || playing.value || session.resuming) return
  session.resuming = true
  try {
    session.decoder?.free()
    session.decoder = null
    const decoder = await createDecoder({ sampleRate: 48000, channels: 2 })
    if (active !== session) {
      decoder.free()
      return
    }
    session.decoder = decoder
    await session.context.resume()
    if (active !== session) return
    playing.value = true
    warning.value = ''
    status.value = session.handshakeComplete ? 'waiting' : (session.socket ? 'handshaking' : 'preparing')
    message.value = session.handshakeComplete ? '已恢复，等待音频…' : '已恢复，正在连接音频服务…'
  } catch (err) {
    fail(session, err)
  } finally {
    session.resuming = false
  }
}

function toggle() {
  if (!active) void start()
  else if (playing.value) pause()
  else void resume()
}

onUnmounted(() => {
  if (active) closeSession(active)
})
</script>

<template>
  <main>
    <h1>Linux Web Audio 验证</h1>
    <button type="button" @click="toggle">
      {{ playing ? '暂停' : '开始' }}
    </button>
    <p role="status">{{ message }}</p>
    <p v-if="receivedFrames">播放欠载 {{ underruns }} 次；溢出丢包 {{ droppedPackets }} 个</p>
    <p v-if="warning" role="alert">{{ warning }}</p>
    <p v-if="status === 'error'">请检查服务端日志及连接状态。</p>
  </main>
</template>
