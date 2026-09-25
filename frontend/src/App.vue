<script setup lang="ts">
import { computed, onUnmounted, ref, watch } from 'vue'
import { createDecoder } from 'libopus-wasm'
import playerWorkletUrl from '/player.js?url'
import {
  PocketType,
  decodeClose,
  decodeHandshakeInfo,
  decodeLatency,
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
const receivedPackets = ref(0)
const sentPackets = ref(0)
const underruns = ref(0)
const droppedPackets = ref(0)
const resyncDroppedPackets = ref(0)
const bufferedFrames = ref(0)
const decodeLatencyMs = ref<number | null>(null)
const opusLatencyMs = ref<number | null>(null)
const audioBufferLatencyMs = ref<number | null>(null)
const wsSendLatencyMs = ref<number | null>(null)
const clientCompressionLatencyMs = ref<number | null>(null)
const clientDecompressionLatencyMs = ref<number | null>(null)
const serverCompressionLatencyMs = ref<number | null>(null)
const serverDecompressionLatencyMs = ref<number | null>(null)
const audioOutputLatencyMs = ref(0)
const bufferRate = ref(15)
const gainDb = ref(0)
const selectedCompression = ref<Compression>('zstd:1')
const streamAddress = ref('')
const negotiatedBuffer = ref<number | null>(null)
const sessionActive = ref(false)

const bufferDuration = computed(() => `${(bufferRate.value * 2.5).toFixed(1)} ms`)
const negotiatedDuration = computed(() => negotiatedBuffer.value === null ? '—' : `${(negotiatedBuffer.value * 2.5).toFixed(1)} ms`)
const currentBufferDuration = computed(() => `${(bufferedFrames.value * 2.5).toFixed(1)} ms`)
const playbackLatencyMs = computed(() => bufferedFrames.value * 2.5 + audioOutputLatencyMs.value)
const bufferWatermarkLabel = computed(() => {
  if (negotiatedBuffer.value === null) return '等待握手协商'
  const watermarks = getBufferWatermarks(negotiatedBuffer.value)
  return `余量 ${watermarks.lower} 帧时补发 · 上限 ${watermarks.upper} 帧`
})
const sliderFill = computed(() => `${((bufferRate.value - 1) / 399) * 100}%`)
const gainSliderFill = computed(() => `${((gainDb.value + 30) / 54) * 100}%`)
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
  gain: GainNode | null
  socket: WebSocket | null
  timeout: ReturnType<typeof setTimeout> | null
  lastSequence: number | null
  receivedFrames: number
  lastDisplay: number
  handshakeComplete: boolean
  resuming: boolean
  compression: Compression
  receiveChain: Promise<void>
  bufferReportChain: Promise<void>
  bufferReportTimer: ReturnType<typeof setTimeout> | null
  bufferReportQueued: boolean
  pendingBufferReport: { bufferedFrames: number; sequence: number; resync: boolean; requestBuffer: number } | null
  lastBufferReportAt: number
  lastPacketArrivalAt: number | null
  jitterMs: number
  baseTargetFrames: number
  upperTargetFrames: number
  adaptiveTargetFrames: number
  reconfigureTimer: ReturnType<typeof setTimeout> | null
  reconfiguring: boolean
}

let active: Session | null = null
let sharedContext: AudioContext | null = null
const workletModulePromises = new WeakMap<AudioContext, Promise<void>>()
const MAX_PLC_FRAMES = 8
const BUFFER_REPORT_INTERVAL_MS = 100
const debugEnabled = (() => {
  try {
    return new URLSearchParams(location.search).get('debug') === '1'
      || localStorage.getItem('linux-web-audio-debug') === '1'
  } catch {
    return false
  }
})()

function debugLog(event: string, details?: unknown) {
  if (!debugEnabled) return
  const prefix = `[audio-debug ${performance.now().toFixed(1)}ms] ${event}`
  if (details === undefined) console.debug(prefix)
  else console.debug(prefix, details)
}

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

function getBufferWatermarks(target: number) {
  const lower = target <= 2 ? 2 : Math.floor(target / 2)
  return {
    lower,
    upper: Math.min(0xffff, target + lower),
  }
}

function loadWorklet(context: AudioContext) {
  let promise = workletModulePromises.get(context)
  if (!promise) {
    promise = context.audioWorklet.addModule(playerWorkletUrl).catch((error: unknown) => {
      workletModulePromises.delete(context)
      throw error
    })
    workletModulePromises.set(context, promise)
  }
  return promise
}

function closeSession(session: Session) {
  debugLog('session-close', {
    receivedFrames: session.receivedFrames,
    lastSequence: session.lastSequence,
    handshakeComplete: session.handshakeComplete,
  })
  if (active === session) {
    active = null
    sessionActive.value = false
    playing.value = false
  }
  if (session.timeout !== null) clearTimeout(session.timeout)
  session.timeout = null
  if (session.reconfigureTimer !== null) clearTimeout(session.reconfigureTimer)
  session.reconfigureTimer = null
  if (session.bufferReportTimer !== null) clearTimeout(session.bufferReportTimer)
  session.bufferReportTimer = null
  session.pendingBufferReport = null

  const socket = session.socket
  if (socket) {
    socket.onopen = null
    socket.onmessage = null
    socket.onerror = null
    socket.onclose = null
    try {
      if (socket.readyState === WebSocket.OPEN && session.handshakeComplete) sendPacket(session, encodeClose())
      if (socket.readyState === WebSocket.CONNECTING || socket.readyState === WebSocket.OPEN) socket.close()
    } catch (err) {
      console.error('Failed to close WebSocket:', err)
    }
  }
  session.socket = null

  if (session.node) {
    session.node.port.postMessage({ type: 'CLEAR' })
    session.node.port.onmessage = null
    session.node.port.close()
    session.node.disconnect()
    session.node = null
  }
  session.gain?.disconnect()
  session.gain = null
  void session.context.suspend().catch((err: unknown) => console.error('Failed to suspend audio context:', err))
  session.decoder?.free()
  session.decoder = null
}

function fail(session: Session, error: unknown) {
  if (active !== session) return
  debugLog('session-error', error instanceof Error ? error.message : String(error))
  message.value = error instanceof Error ? error.message : String(error)
  warning.value = ''
  status.value = 'error'
  closeSession(session)
}

function handleAudio(session: Session, payload: Uint8Array) {
  const frames = decodeOpusFrames(payload)
  if (!session.decoder || !session.node) throw new Error('音频播放器尚未准备就绪')

  const arrivalAt = performance.now()
  if (session.lastPacketArrivalAt !== null) {
    const expectedInterval = frames.length * 2.5
    const arrivalInterval = arrivalAt - session.lastPacketArrivalAt
    const deviation = Math.abs(arrivalInterval - expectedInterval)
    session.jitterMs += (deviation - session.jitterMs) / 16
  }
  session.lastPacketArrivalAt = arrivalAt
  const jitterFrames = Math.ceil(session.jitterMs / 2.5)
  const adaptiveTargetFrames = Math.min(
    session.upperTargetFrames,
    session.baseTargetFrames + jitterFrames,
  )
  if (adaptiveTargetFrames !== session.adaptiveTargetFrames) {
    session.adaptiveTargetFrames = adaptiveTargetFrames
    const watermarks = getBufferWatermarks(session.baseTargetFrames)
    session.node.port.postMessage({
      type: 'SET_BUFFER_LIMIT',
      lowerFrames: watermarks.lower,
      upperFrames: watermarks.upper,
      targetFrames: adaptiveTargetFrames,
    })
    debugLog('adaptive-target', {
      targetFrames: adaptiveTargetFrames,
      baseTargetFrames: session.baseTargetFrames,
      upperTargetFrames: session.upperTargetFrames,
      jitterMs: Number(session.jitterMs.toFixed(2)),
    })
  }

  let pendingParts: Float32Array[] = []
  let pendingLength = 0
  let pendingSequence: number | null = null
  let pendingDiscontinuity = false

  const flushPending = () => {
    if (pendingParts.length === 0 || pendingSequence === null) return
    let interleaved: Float32Array
    if (pendingParts.length === 1) {
      interleaved = pendingParts[0]
    } else {
      interleaved = new Float32Array(pendingLength)
      let offset = 0
      for (const part of pendingParts) {
        interleaved.set(part, offset)
        offset += part.length
      }
    }
    session.node!.port.postMessage({
      type: 'PCM_DATA',
      interleaved,
      sequence: pendingSequence,
      discontinuity: pendingDiscontinuity,
    }, [interleaved.buffer])
    pendingParts = []
    pendingLength = 0
    pendingSequence = null
    pendingDiscontinuity = false
  }

  const appendPending = (interleaved: Float32Array, sequence: number, discontinuity = false) => {
    if (discontinuity) flushPending()
    pendingParts.push(interleaved)
    pendingLength += interleaved.length
    pendingSequence = sequence
    pendingDiscontinuity ||= discontinuity
  }

  for (const frame of frames) {
    if (!isNewerSequence(frame.sequence, session.lastSequence)) continue
    const previousSequence = session.lastSequence
    const missingFrames = previousSequence === null
      ? 0
      : ((frame.sequence - previousSequence + 0x10000) & 0xffff) - 1
    if (missingFrames > 0) {
      debugLog('sequence-gap', {
        previousSequence,
        sequence: frame.sequence,
        missingFrames,
        concealedFrames: Math.min(missingFrames, MAX_PLC_FRAMES),
      })
    }
    if (!playing.value || session.resuming || session.reconfiguring) continue
    session.lastSequence = frame.sequence
    const concealedFrames = Math.min(missingFrames, MAX_PLC_FRAMES)
    for (let missing = 0; missing < concealedFrames; missing++) {
      const decodeStart = performance.now()
      const concealed = session.decoder.decodePacketLossFloat(120)
      updateDecodeLatency(performance.now() - decodeStart)
      const concealedSequence = (previousSequence! + missing + 1) & 0xffff
      appendPending(concealed, concealedSequence)
      session.receivedFrames++
    }
    const discontinuity = missingFrames > MAX_PLC_FRAMES
    if (discontinuity) flushPending()
    const decodeStart = performance.now()
    const interleaved = session.decoder.decodeFloat(frame.data)
    const elapsed = performance.now() - decodeStart
    updateDecodeLatency(elapsed)
    if (interleaved.length === 0) continue
    appendPending(interleaved, frame.sequence, discontinuity)
    session.receivedFrames++
  }
  flushPending()

  if (session.receivedFrames > 0 && playing.value && performance.now() - session.lastDisplay >= 250) {
    receivedFrames.value = session.receivedFrames
    status.value = 'receiving'
    message.value = `正在接收音频帧（${session.receivedFrames} 帧）`
    session.lastDisplay = performance.now()
  }
}

function updateDecodeLatency(elapsed: number) {
  decodeLatencyMs.value = decodeLatencyMs.value === null ? elapsed : decodeLatencyMs.value * 0.8 + elapsed * 0.2
}

function sendPacket(session: Session, data: Uint8Array) {
  if (session.socket?.readyState !== WebSocket.OPEN) return false
  const packet = new Uint8Array(data.byteLength)
  packet.set(data)
  session.socket.send(packet.buffer)
  sentPackets.value++
  if (sentPackets.value <= 3 || sentPackets.value % 25 === 0) {
    debugLog('ws-send', { count: sentPackets.value, bytes: packet.byteLength })
  }
  return true
}

function queueBufferReport(session: Session, bufferedFrames: number, sequence: number, urgent = false, resync = false) {
  if (!session.handshakeComplete || session.reconfiguring || session.socket?.readyState !== WebSocket.OPEN) return
  session.pendingBufferReport = {
    bufferedFrames: Math.min(bufferedFrames, 0xffff),
    sequence,
    resync,
    requestBuffer: resync ? Math.min(0xffff, Math.ceil(session.baseTargetFrames * 1.5)) : 0,
  }
  debugLog('buffer-report-queued', {
    bufferedFrames,
    sequence,
    urgent,
    resync,
    requestBuffer: session.pendingBufferReport.requestBuffer,
  })
  if (urgent && session.bufferReportTimer !== null) {
    clearTimeout(session.bufferReportTimer)
    session.bufferReportTimer = null
  }
  if (session.bufferReportTimer !== null || session.bufferReportQueued) return

  const wait = urgent ? 0 : Math.max(0, session.lastBufferReportAt + BUFFER_REPORT_INTERVAL_MS - performance.now())
  session.bufferReportTimer = setTimeout(() => {
    session.bufferReportTimer = null
    const report = session.pendingBufferReport
    session.pendingBufferReport = null
    if (!report || active !== session || session.socket?.readyState !== WebSocket.OPEN || !session.handshakeComplete || session.reconfiguring) return

    session.bufferReportQueued = true
    session.bufferReportChain = session.bufferReportChain.then(async () => {
      if (active !== session || session.socket?.readyState !== WebSocket.OPEN || !session.handshakeComplete || session.reconfiguring) return
      const compressionStart = performance.now()
      const packet = await encodeBufferWithCompression(
        report.bufferedFrames,
        report.sequence,
        session.compression,
        report.resync,
        report.requestBuffer,
      )
      clientCompressionLatencyMs.value = performance.now() - compressionStart
      if (sendPacket(session, packet)) session.lastBufferReportAt = performance.now()
    }).catch((err: unknown) => fail(session, err)).finally(() => {
      session.bufferReportQueued = false
      const pending = session.pendingBufferReport
      if (pending) queueBufferReport(
        session,
        pending.bufferedFrames,
        pending.sequence,
        pending.resync,
        pending.resync,
      )
    })
  }, wait)
}

function scheduleReconfigure() {
  const session = active
  if (!session?.handshakeComplete) return
  if (session.reconfigureTimer !== null) clearTimeout(session.reconfigureTimer)
  session.reconfigureTimer = setTimeout(() => {
    session.reconfigureTimer = null
    if (active !== session || !session.handshakeComplete || !session.socket) return
    session.reconfiguring = true
    session.node?.port.postMessage({ type: 'CLEAR' })
    bufferedFrames.value = 0
    status.value = 'handshaking'
    message.value = '正在应用播放设置…'
    try {
      sendPacket(session, encodeHandshakeWithCompression(bufferRate.value, selectedCompression.value))
    } catch (error) {
      fail(session, error)
    }
  }, 500)
}

watch(selectedCompression, scheduleReconfigure)
watch(gainDb, (value) => {
  if (active?.gain) active.gain.gain.setTargetAtTime(10 ** (value / 20), active.context.currentTime, 0.01)
})

async function start() {
  if (active) return

  status.value = 'preparing'
  message.value = '正在准备音频播放器…'
  receivedFrames.value = 0
  receivedPackets.value = 0
  sentPackets.value = 0
  underruns.value = 0
  droppedPackets.value = 0
  resyncDroppedPackets.value = 0
  bufferedFrames.value = 0
  decodeLatencyMs.value = null
  opusLatencyMs.value = null
  audioBufferLatencyMs.value = null
  wsSendLatencyMs.value = null
  clientCompressionLatencyMs.value = null
  clientDecompressionLatencyMs.value = null
  serverCompressionLatencyMs.value = null
  serverDecompressionLatencyMs.value = null
  audioOutputLatencyMs.value = 0
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
    lastDisplay: 0,
    handshakeComplete: false,
    resuming: false,
    gain: null,
    compression: selectedCompression.value,
    receiveChain: Promise.resolve(),
    bufferReportChain: Promise.resolve(),
    bufferReportTimer: null,
    bufferReportQueued: false,
    pendingBufferReport: null,
    lastBufferReportAt: 0,
    lastPacketArrivalAt: null,
    jitterMs: 0,
    baseTargetFrames: 1,
    upperTargetFrames: 1,
    adaptiveTargetFrames: 1,
    reconfigureTimer: null,
    reconfiguring: false,
  }
  active = session
  debugLog('session-start', {
    targetFrames: bufferRate.value,
    compression: session.compression,
    streamAddress: streamAddress.value.trim() || '/backend/v2/stream',
  })
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
    const gain = context.createGain()
    gain.gain.value = 10 ** (gainDb.value / 20)
    session.gain = gain
    let lastWorkletUnderruns = 0
    let lastWorkletDroppedPackets = 0
    let lastWorkletResyncDroppedPackets = 0
    node.port.onmessage = (event: MessageEvent<{
      type: string
      bufferedFrames: number
      bufferedSamples: number
      sequence: number | null
      underruns: number
      droppedPackets: number
      resyncDroppedPackets: number
      lowWatermark: boolean
      criticalWatermark: boolean
      highWatermark: boolean
      primed: boolean
      resync: boolean
    }>) => {
      if (active !== session || event.data.type !== 'BUFFER_STATUS') return
      underruns.value = event.data.underruns
      droppedPackets.value = event.data.droppedPackets
      resyncDroppedPackets.value = event.data.resyncDroppedPackets
      bufferedFrames.value = event.data.bufferedFrames
      if (event.data.underruns !== lastWorkletUnderruns
        || event.data.droppedPackets !== lastWorkletDroppedPackets
        || event.data.resyncDroppedPackets !== lastWorkletResyncDroppedPackets
        || event.data.lowWatermark
        || event.data.criticalWatermark
        || event.data.highWatermark) {
        debugLog('worklet-buffer-status', {
          bufferedFrames: event.data.bufferedFrames,
          bufferedSamples: event.data.bufferedSamples,
          sequence: event.data.sequence,
          underruns: event.data.underruns,
          droppedPackets: event.data.droppedPackets,
          resyncDroppedPackets: event.data.resyncDroppedPackets,
          lowWatermark: event.data.lowWatermark,
          criticalWatermark: event.data.criticalWatermark,
          highWatermark: event.data.highWatermark,
          primed: event.data.primed,
          resync: event.data.resync,
        })
      }
      lastWorkletUnderruns = event.data.underruns
      lastWorkletDroppedPackets = event.data.droppedPackets
      lastWorkletResyncDroppedPackets = event.data.resyncDroppedPackets
      if ((event.data.lowWatermark || event.data.criticalWatermark || event.data.highWatermark || event.data.resync) && event.data.sequence !== null) {
        const resync = event.data.criticalWatermark || event.data.resync
        queueBufferReport(
          session,
          event.data.bufferedFrames,
          event.data.sequence,
          event.data.lowWatermark || event.data.criticalWatermark || event.data.resync || event.data.highWatermark,
          resync,
        )
      }
    }
    node.connect(gain)
    gain.connect(context.destination)

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
        debugLog('ws-open')
        sendPacket(session, encodeHandshakeWithCompression(bufferRate.value, session.compression))
        if (playing.value) {
          status.value = 'handshaking'
          message.value = '已连接，等待服务端握手…'
        }
      } catch (err) {
        fail(session, err)
      }
    }
    socket.onmessage = (event: MessageEvent) => {
      receivedPackets.value++
      if (receivedPackets.value <= 3 || receivedPackets.value % 25 === 0) {
        debugLog('ws-receive', { count: receivedPackets.value, bytes: event.data?.byteLength ?? null })
      }
      session.receiveChain = session.receiveChain.then(async () => {
        if (active !== session) return
        if (!(event.data instanceof ArrayBuffer)) throw new Error('收到非二进制消息')
        const decompressionStart = performance.now()
        const { type, payload } = await decodePocketWithCompression(event.data, session.compression)
        clientDecompressionLatencyMs.value = performance.now() - decompressionStart
        switch (type) {
          case PocketType.HandshakeResponse: {
            if (session.handshakeComplete && !session.reconfiguring) throw new Error('收到意外的握手响应')
            const handshake = decodeHandshakeInfo(payload)
            debugLog('handshake', {
              targetFrames: handshake.targetBuffer,
              compression: handshake.compression,
              reconfiguring: session.reconfiguring,
            })
            if (session.reconfiguring) {
              const decoder = await createDecoder({ sampleRate: 48000, channels: 2 })
              if (active !== session) {
                decoder.free()
                return
              }
              session.decoder?.free()
              session.decoder = decoder
              session.lastSequence = null
              session.node?.port.postMessage({ type: 'CLEAR' })
              session.reconfiguring = false
            }
            session.compression = handshake.compression
            negotiatedBuffer.value = handshake.targetBuffer
            const watermarks = getBufferWatermarks(handshake.targetBuffer)
            session.baseTargetFrames = handshake.targetBuffer
            session.upperTargetFrames = watermarks.upper
            session.adaptiveTargetFrames = handshake.targetBuffer
            session.lastPacketArrivalAt = null
            session.jitterMs = 0
            session.node?.port.postMessage({
              type: 'SET_BUFFER_LIMIT',
              lowerFrames: watermarks.lower,
              upperFrames: watermarks.upper,
              targetFrames: handshake.targetBuffer,
            })
            const outputLatency = 'outputLatency' in session.context
              ? (session.context as AudioContext & { outputLatency: number }).outputLatency
              : 0
            audioOutputLatencyMs.value = (session.context.baseLatency + outputLatency) * 1000
            session.handshakeComplete = true
            if (session.timeout !== null) clearTimeout(session.timeout)
            session.timeout = null
            if (playing.value) {
              status.value = 'waiting'
              message.value = `握手完成，等待音频（服务端目标缓冲 ${handshake.targetBuffer} 帧）`
            }
            if (handshake.targetBuffer !== bufferRate.value || handshake.compression !== selectedCompression.value) {
              scheduleReconfigure()
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
          case PocketType.Latency: {
            const latency = decodeLatency(payload)
            debugLog('server-latency', latency)
            opusLatencyMs.value = latency.opus / 1_000_000
            audioBufferLatencyMs.value = latency.audioBuffer / 1_000_000
            wsSendLatencyMs.value = latency.wsSend / 1_000_000
            serverCompressionLatencyMs.value = latency.compression / 1_000_000
            serverDecompressionLatencyMs.value = latency.decompression / 1_000_000
            break
          }
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
  debugLog('session-stop')
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
        <p class="field-note watermark-note">{{ bufferWatermarkLabel }}</p>
        <input
          v-model.number="bufferRate"
          class="range-input"
          type="range"
          min="1"
          max="400"
          step="1"
          @change="scheduleReconfigure"
          :style="{ '--range-fill': sliderFill }"
          aria-label="目标缓冲帧数"
        >
        <div class="range-labels"><span>1 帧</span><span>400 帧</span></div>
        <p class="field-note">播放前会等待目标帧数；运行中修改后自动重新协商。</p>
        <div class="gain-control">
          <div class="setting-row">
            <label for="gain-db">客户端增益</label>
            <strong>{{ gainDb.toFixed(1) }} dB</strong>
          </div>
          <input
            id="gain-db"
            v-model.number="gainDb"
            class="range-input gain-range"
            type="range"
            min="-30"
            max="24"
            step="0.5"
            :style="{ '--range-fill': gainSliderFill }"
            aria-label="客户端分贝增益"
          >
          <div class="range-labels"><span>-30 dB</span><span>+24 dB</span></div>
        </div>
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
          <select id="compression" v-model="selectedCompression">
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
      <div class="metric-card accent-orange">
        <span class="metric-label">欠载重同步丢包</span>
        <strong>{{ resyncDroppedPackets.toLocaleString() }}</strong>
        <span class="metric-unit">packets</span>
      </div>
      <div class="metric-card accent-green">
        <span class="metric-label">服务端目标</span>
        <strong>{{ negotiatedBuffer ?? '—' }}</strong>
        <span class="metric-unit">{{ negotiatedBuffer === null ? '等待握手' : negotiatedDuration }}</span>
      </div>
      <div class="metric-card accent-blue">
        <span class="metric-label">WebSocket 收包</span>
        <strong>{{ receivedPackets.toLocaleString() }}</strong>
        <span class="metric-unit">packets</span>
      </div>
      <div class="metric-card accent-orange">
        <span class="metric-label">WebSocket 发包</span>
        <strong>{{ sentPackets.toLocaleString() }}</strong>
        <span class="metric-unit">packets</span>
      </div>
    </section>

    <section class="metrics-grid latency-grid" aria-label="音频延迟指标">
      <div class="metric-card accent-blue">
        <span class="metric-label">Opus 编码</span>
        <strong>{{ opusLatencyMs === null ? '—' : opusLatencyMs.toFixed(3) }}</strong>
        <span class="metric-unit">ms / frame</span>
      </div>
      <div class="metric-card accent-orange">
        <span class="metric-label">AudioBuffer 写入</span>
        <strong>{{ audioBufferLatencyMs === null ? '—' : audioBufferLatencyMs.toFixed(3) }}</strong>
        <span class="metric-unit">ms</span>
      </div>
      <div class="metric-card accent-red">
        <span class="metric-label">WsSend 调用</span>
        <strong>{{ wsSendLatencyMs === null ? '—' : wsSendLatencyMs.toFixed(3) }}</strong>
        <span class="metric-unit">ms · 非网络延迟</span>
      </div>
      <div class="metric-card accent-green">
        <span class="metric-label">Opus 解码</span>
        <strong>{{ decodeLatencyMs === null ? '—' : decodeLatencyMs.toFixed(3) }}</strong>
        <span class="metric-unit">ms / frame</span>
      </div>
      <div class="metric-card accent-orange">
        <span class="metric-label">估算播放延迟</span>
        <strong>{{ playbackLatencyMs.toFixed(1) }}</strong>
        <span class="metric-unit">ms · 含队列与输出延迟</span>
      </div>
      <div class="metric-card accent-blue">
        <span class="metric-label">客户端缓冲</span>
        <strong>{{ bufferedFrames }}</strong>
        <span class="metric-unit">帧 · {{ currentBufferDuration }}</span>
      </div>
      <div class="metric-card accent-blue">
        <span class="metric-label">客户端压缩</span>
        <strong>{{ clientCompressionLatencyMs === null ? '—' : clientCompressionLatencyMs.toFixed(3) }}</strong>
        <span class="metric-unit">ms / pocket</span>
      </div>
      <div class="metric-card accent-orange">
        <span class="metric-label">客户端解压缩</span>
        <strong>{{ clientDecompressionLatencyMs === null ? '—' : clientDecompressionLatencyMs.toFixed(3) }}</strong>
        <span class="metric-unit">ms / pocket</span>
      </div>
      <div class="metric-card accent-red">
        <span class="metric-label">服务端压缩</span>
        <strong>{{ serverCompressionLatencyMs === null ? '—' : serverCompressionLatencyMs.toFixed(3) }}</strong>
        <span class="metric-unit">ms / pocket</span>
      </div>
      <div class="metric-card accent-green">
        <span class="metric-label">服务端解压缩</span>
        <strong>{{ serverDecompressionLatencyMs === null ? '—' : serverDecompressionLatencyMs.toFixed(3) }}</strong>
        <span class="metric-unit">ms / pocket</span>
      </div>
    </section>

    <p v-if="warning" class="notice warning" role="alert">{{ warning }}</p>
    <p v-if="status === 'error'" class="notice error">请检查服务端日志、代理配置及浏览器权限后重试。</p>
    <footer class="footer-note">当前协议：Opus / 48 kHz / 双声道 · 压缩：{{ compressionLabel }}</footer>
  </main>
</template>
