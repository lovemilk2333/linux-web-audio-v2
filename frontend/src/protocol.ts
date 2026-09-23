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

export function encodePocket(type: number, data: Uint8Array): Uint8Array<ArrayBuffer> {
  const packet = new Uint8Array(data.length + 2)
  packet.set(data)
  new DataView(packet.buffer).setUint16(data.length, type | rawMask, false)
  return packet
}

export function encodeHandshake(targetBuffer: number): Uint8Array<ArrayBuffer> {
  return encodePocket(PocketType.Handshake, serialize({ c: 'none', tb: targetBuffer }))
}

export function encodeBuffer(bufferedFrames: number, sequence: number): Uint8Array<ArrayBuffer> {
  return encodePocket(PocketType.Buffer, serialize({ cb: bufferedFrames, cs: sequence }))
}

export function encodeClose(): Uint8Array<ArrayBuffer> {
  return encodePocket(PocketType.Close, serialize({ r: 'client stopped' }))
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

export function decodeHandshake(payload: Uint8Array): number {
  const response = deserialize(payload)
  if (response.c !== 'none' || !Number.isInteger(response.tb) || response.tb < 0 || response.tb > 0xffffffff) {
    throw new Error('服务端握手参数无效或使用了不支持的压缩方式')
  }
  return response.tb as number
}

export function decodeClose(payload: Uint8Array): string {
  const response = deserialize(payload)
  return typeof response.r === 'string' ? response.r : '未提供原因'
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
