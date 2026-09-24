import assert from 'node:assert/strict'
import test from 'node:test'
import { deserialize, serialize } from 'bson'
import {
  PocketType,
  decodeClose,
  decodeHandshake,
  decodePocketWithCompression,
  decodeOpusFrames,
  decodePocket,
  encodeBuffer,
  encodeBufferWithCompression,
  encodeClose,
  encodeHandshake,
  isNewerSequence,
} from '../src/protocol.ts'

test('sends a raw BSON handshake with the required fields', () => {
  const packet = decodePocket(encodeHandshake(40).buffer)
  assert.equal(packet.type, PocketType.Handshake)
  assert.deepEqual(deserialize(packet.payload), { c: 'none', tb: 40 })
})

test('sends buffer and close pockets with BSON payloads', () => {
  let packet = decodePocket(encodeBuffer(12, 65535).buffer)
  assert.equal(packet.type, PocketType.Buffer)
  assert.deepEqual(deserialize(packet.payload), { cb: 12, cs: 65535 })
  packet = decodePocket(encodeClose().buffer)
  assert.equal(packet.type, PocketType.Close)
  assert.equal(deserialize(packet.payload).r, 'client stopped')
})

test('decodes handshake and close response fields', () => {
  assert.equal(decodeHandshake(serialize({ c: 'none', tb: 40 })), 40)
  assert.equal(decodeClose(serialize({ r: 'closing' })), 'closing')
  assert.equal(decodeHandshake(serialize({ c: 'gzip:-1', tb: 40 })), 40)
  assert.throws(() => decodeHandshake(serialize({ c: 'brotli', tb: 40 })), /不支持/)
})

test('compresses and decompresses gzip pockets', async () => {
  const packet = await encodeBufferWithCompression(120, 65535, 'gzip')
  const decoded = await decodePocketWithCompression(packet.buffer, 'gzip')
  assert.equal(decoded.type, PocketType.Buffer)
  assert.deepEqual(deserialize(decoded.payload), { cb: 120, cs: 65535 })
})

test('rejects truncated and compressed pockets', () => {
  assert.throws(() => decodePocket(new ArrayBuffer(1)), /长度不足/)
  assert.throws(() => decodePocket(Uint8Array.from([0, 4]).buffer), /压缩/)
})

test('splits multiple Opus frames and rejects invalid lengths', () => {
  const frames = decodeOpusFrames(Uint8Array.from([2, 0, 1, 10, 11, 1, 0, 2, 12]))
  assert.deepEqual(frames.map(({ sequence, data }) => [sequence, [...data]]), [
    [1, [10, 11]], [2, [12]],
  ])
  assert.throws(() => decodeOpusFrames(Uint8Array.from([2, 0, 1, 10])), /长度无效/)
  assert.throws(() => decodeOpusFrames(Uint8Array.from([1, 0])), /帧头/)
  assert.throws(() => decodeOpusFrames(new Uint8Array()), /没有音频帧/)
})

test('deduplicates sequence numbers including wraparound', () => {
  assert.equal(isNewerSequence(0, null), true)
  assert.equal(isNewerSequence(0, 65535), true)
  assert.equal(isNewerSequence(65535, 0), false)
  assert.equal(isNewerSequence(8, 8), false)
  assert.equal(isNewerSequence(9, 8), true)
})
