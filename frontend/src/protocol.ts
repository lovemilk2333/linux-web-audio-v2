import { deserialize, serialize } from 'bson'

export const PocketType = {
  Handshake: 0,
  HandshakeResponse: 1,
  Close: 2,
  Latency: 3,
  Opus: 4,
  Buffer: 5,
} as const

const rawMask = 0x8000

export type Compression = 'none' | 'gzip' | 'lz4' | 'zstd' | `zstd:${number}`

type WasmCompressor = (operation: 'compress' | 'decompress', algorithm: 'zstd' | 'lz4', data: Uint8Array, level: number) => Uint8Array

let wasmCompressor: Promise<WasmCompressor> | null = null

function compressionSpec(compression: Compression): { algorithm: 'zstd' | 'lz4'; level: number } | null {
  if (compression === 'lz4') return { algorithm: 'lz4', level: 0 }
  if (compression === 'zstd') return { algorithm: 'zstd', level: 3 }
  if (compression.startsWith('zstd:')) {
    const level = Number(compression.slice('zstd:'.length))
    if (Number.isInteger(level)) return { algorithm: 'zstd', level }
  }
  return null
}

async function loadWasmCompressor(): Promise<WasmCompressor> {
  if (wasmCompressor) return wasmCompressor
  wasmCompressor = new Promise<WasmCompressor>((resolve, reject) => {
    const global = globalThis as typeof globalThis & {
      Go?: new () => { importObject: WebAssembly.Imports; run(instance: WebAssembly.Instance): Promise<void> }
      linuxWebAudioCompress?: WasmCompressor
    }
    const finish = () => {
      if (global.linuxWebAudioCompress) {
        resolve(global.linuxWebAudioCompress)
      } else {
        reject(new Error('压缩 WASM 初始化失败'))
      }
    }
    const start = () => {
      if (!global.Go) {
        reject(new Error('浏览器缺少 Go WASM 运行时'))
        return
      }
      const go = new global.Go()
      void WebAssembly.instantiateStreaming(fetch('/compressor.wasm'), go.importObject)
        .catch(async () => WebAssembly.instantiate(await (await fetch('/compressor.wasm')).arrayBuffer(), go.importObject))
        .then(({ instance }) => {
          void go.run(instance)
          const waitForGlobal = () => global.linuxWebAudioCompress ? finish() : setTimeout(waitForGlobal, 0)
          waitForGlobal()
        })
        .catch(reject)
    }
    if (global.Go) {
      start()
      return
    }
    const script = document.createElement('script')
    script.src = '/wasm_exec.js'
    script.onload = start
    script.onerror = () => reject(new Error('无法加载 Go WASM 运行时'))
    document.head.appendChild(script)
  })
  try {
    return await wasmCompressor
  } catch (error) {
    wasmCompressor = null
    throw error
  }
}

async function transformWasm(data: Uint8Array, compression: Compression, operation: 'compress' | 'decompress'): Promise<Uint8Array<ArrayBuffer>> {
  const spec = compressionSpec(compression)
  if (!spec) throw new Error(`不支持的压缩方式：${compression}`)
  const transform = await loadWasmCompressor()
  const result = transform(operation, spec.algorithm, data, spec.level)
  const output = new Uint8Array(result.byteLength)
  output.set(result)
  return output
}

function parseCompression(value: unknown): Compression {
  if (value === 'none') return 'none'
  if (value === 'gzip' || typeof value === 'string' && value.startsWith('gzip:')) return 'gzip'
  if (value === 'lz4' || typeof value === 'string' && value.startsWith('lz4:')) return 'lz4'
  if (value === 'zstd') return 'zstd'
  if (typeof value === 'string' && /^zstd:-?\d+$/.test(value)) return value as `zstd:${number}`
  throw new Error('服务端握手参数无效或使用了不支持的压缩方式')
}

type CompressionStreamConstructor = new (format: 'gzip') => TransformStream

function getCompressionStream(kind: 'compress' | 'decompress'): CompressionStreamConstructor {
  const constructor = kind === 'compress' ? globalThis.CompressionStream : globalThis.DecompressionStream
  if (!constructor) throw new Error('当前浏览器不支持 gzip 压缩')
  return constructor as CompressionStreamConstructor
}

async function transformGzip(data: Uint8Array, kind: 'compress' | 'decompress'): Promise<Uint8Array<ArrayBuffer>> {
  const stream = new (getCompressionStream(kind))('gzip')
  const input = new ArrayBuffer(data.byteLength)
  new Uint8Array(input).set(data)
  const body = new Blob([input]).stream().pipeThrough(stream)
  const buffer = await new Response(body).arrayBuffer()
  return new Uint8Array(buffer)
}

async function compress(data: Uint8Array, compression: Compression): Promise<Uint8Array<ArrayBuffer>> {
  if (compression === 'none') return new Uint8Array(data)
  if (compression === 'gzip') return transformGzip(data, 'compress')
  return transformWasm(data, compression, 'compress')
}

async function decompress(data: Uint8Array, compression: Compression): Promise<Uint8Array<ArrayBuffer>> {
  if (compression === 'none') throw new Error('收到压缩 pocket，但当前握手未启用压缩')
  if (compression === 'gzip') return transformGzip(data, 'decompress')
  return transformWasm(data, compression, 'decompress')
}

function appendType(payload: Uint8Array, type: number, raw: boolean): Uint8Array<ArrayBuffer> {
  const packet = new Uint8Array(payload.length + 2)
  packet.set(payload)
  new DataView(packet.buffer).setUint16(payload.length, raw ? type | rawMask : type, false)
  return packet
}

export function encodePocket(type: number, data: Uint8Array): Uint8Array<ArrayBuffer> {
  return appendType(data, type, true)
}

export function encodeHandshake(targetBuffer: number, compression: Compression = 'none'): Uint8Array<ArrayBuffer> {
  return encodePocket(PocketType.Handshake, serialize({ c: compression, tb: targetBuffer }))
}

export function encodeBuffer(bufferedFrames: number, sequence: number, resync = false, requestBuffer = 0): Uint8Array<ArrayBuffer> {
  const value: { cb: number; cs: number; rs?: boolean; rb?: number } = { cb: bufferedFrames, cs: sequence }
  if (resync) value.rs = true
  if (requestBuffer > 0) value.rb = requestBuffer
  return encodePocket(PocketType.Buffer, serialize(value))
}

export function encodeClose(): Uint8Array<ArrayBuffer> {
  return encodePocket(PocketType.Close, serialize({ r: 'client stopped' }))
}

export async function encodePocketWithCompression(type: number, data: Uint8Array, compression: Compression): Promise<Uint8Array<ArrayBuffer>> {
  const compressed = await compress(data, compression)
  return appendType(compressed.length < data.length ? compressed : data, type, compressed.length >= data.length)
}

export function encodeHandshakeWithCompression(targetBuffer: number, compression: Compression): Uint8Array<ArrayBuffer> {
  // The first handshake is always raw: the server has not selected its compressor yet.
  return encodeHandshake(targetBuffer, compression)
}

export async function encodeBufferWithCompression(bufferedFrames: number, sequence: number, compression: Compression, resync = false, requestBuffer = 0): Promise<Uint8Array<ArrayBuffer>> {
  const value: { cb: number; cs: number; rs?: boolean; rb?: number } = { cb: bufferedFrames, cs: sequence }
  if (resync) value.rs = true
  if (requestBuffer > 0) value.rb = requestBuffer
  return encodePocketWithCompression(PocketType.Buffer, serialize(value), compression)
}

export async function encodeCloseWithCompression(compression: Compression): Promise<Uint8Array<ArrayBuffer>> {
  return encodePocketWithCompression(PocketType.Close, serialize({ r: 'client stopped' }), compression)
}

export function decodePocket(data: ArrayBuffer): { type: number; payload: Uint8Array } {
  if (data.byteLength < 2) throw new Error('消息长度不足')

  const view = new DataView(data)
  const wireType = view.getUint16(data.byteLength - 2, false)
  if ((wireType & rawMask) === 0) throw new Error('服务端发送了不支持的压缩消息')
  return {
    type: wireType & ~rawMask,
    payload: new Uint8Array(data, 0, data.byteLength - 2),
  }
}

export async function decodePocketWithCompression(data: ArrayBuffer, compression: Compression): Promise<{ type: number; payload: Uint8Array }> {
  if (data.byteLength < 2) throw new Error('消息长度不足')

  const view = new DataView(data)
  const wireType = view.getUint16(data.byteLength - 2, false)
  const raw = (wireType & rawMask) !== 0
  const type = wireType & ~rawMask
  const payload = new Uint8Array(data, 0, data.byteLength - 2)

  return {
    type,
    payload: raw ? payload : await decompress(payload, compression),
  }
}

export function decodeHandshakeInfo(payload: Uint8Array): { compression: Compression; targetBuffer: number } {
  const response = deserialize(payload)
  if (!Number.isInteger(response.tb) || response.tb < 0 || response.tb > 0xffff) {
    throw new Error('服务端握手参数无效或使用了不支持的压缩方式')
  }
  return { compression: parseCompression(response.c), targetBuffer: response.tb as number }
}

export function decodeHandshake(payload: Uint8Array): number {
  return decodeHandshakeInfo(payload).targetBuffer
}

export function decodeClose(payload: Uint8Array): string {
  const response = deserialize(payload)
  return typeof response.r === 'string' ? response.r : '未提供原因'
}

export type LatencyInfo = {
  opus: number
  audioBuffer: number
  wsSend: number
  compression: number
  decompression: number
}

export function decodeLatency(payload: Uint8Array): LatencyInfo {
  const response = deserialize(payload)
  const opus = response.o
  const audioBuffer = response.ab
  const wsSend = response.ws
  const compression = response.c ?? 0
  const decompression = response.d ?? 0
  if (![opus, audioBuffer, wsSend, compression, decompression].every(value => typeof value === 'number' && Number.isFinite(value) && value >= 0)) {
    throw new Error('服务端延迟数据无效')
  }
  return { opus, audioBuffer, wsSend, compression, decompression }
}

export type OpusFrame = { sequence: number; data: Uint8Array }

export function decodeOpusFrames(payload: Uint8Array): OpusFrame[] {
  const frames: OpusFrame[] = []
  let offset = 0
  while (offset < payload.length) {
    if (offset + 3 > payload.length) throw new Error('Opus 帧头不完整')
    const length = payload[offset]
    const sequence = (payload[offset + 1] << 8) | payload[offset + 2]
    offset += 3
    if (length === 0 || offset + length > payload.length) throw new Error('Opus 帧长度无效')
    frames.push({ sequence, data: payload.subarray(offset, offset + length) })
    offset += length
  }
  if (frames.length === 0) throw new Error('Opus 消息没有音频帧')
  return frames
}

export function isNewerSequence(sequence: number, previous: number | null): boolean {
  if (previous === null) return true
  const delta = (sequence - previous + 0x10000) & 0xffff
  return delta > 0 && delta < 0x8000
}
