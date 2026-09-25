import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import { runInNewContext } from 'node:vm'

const workletSource = readFileSync(new URL('../public/player.js', import.meta.url), 'utf8')

function createPlayer() {
  let Processor
  class AudioWorkletProcessor {
    constructor() {
      this.port = { onmessage: null, messages: [], postMessage(message) { this.messages.push(message) } }
    }
  }
  runInNewContext(workletSource, {
    AudioWorkletProcessor,
    registerProcessor(name, implementation) {
      assert.equal(name, 'pcm-player-processor')
      Processor = implementation
    },
  })
  return new Processor()
}

function feed(player, count) {
  for (let index = 0; index < count; index++) {
    const interleaved = new Float32Array(240)
    interleaved.fill(0.5)
    player.port.onmessage({ data: { type: 'PCM_DATA', interleaved, sequence: index } })
  }
}

function render(player) {
  const output = [new Float32Array(128), new Float32Array(128)]
  player.process([], [output])
  return output[0]
}

test('prefills before playing and fades around an underrun', () => {
  const player = createPlayer()
  feed(player, 15)
  assert.equal(render(player).every((sample) => sample === 0), true)
  feed(player, 1)

  const start = render(player)
  assert.equal(start[0] > 0, true)
  assert.equal(start[0] < 0.01, true)
  assert.equal(start[127] < 0.5, true)
  for (let index = 0; index < 14; index++) render(player)
  const end = render(player)
  assert.equal(end[0], 0.5 * 255 / 256)
  assert.equal(end[63], 0.5 * 192 / 256)
  assert.equal(end[127], 0.5 * 128 / 256)
  assert.equal(player.underruns, 1)

  feed(player, 8)
  const fadeOut = render(player)
  assert.equal(fadeOut[0] > 0, true)
  assert.equal(fadeOut[127], 0)
  assert.equal(render(player).every((sample) => sample === 0), true)
  feed(player, 8)
  const resumed = render(player)
  assert.equal(resumed[0] > 0, true)
  assert.equal(resumed[0] < 0.01, true)
})

test('counts dropped packets when the queue overflows', () => {
  const player = createPlayer()
  player.port.onmessage({ data: { type: 'SET_BUFFER_LIMIT', lowerFrames: 4, upperFrames: 20, targetFrames: 10 } })
  feed(player, 401)
  assert.equal(player.bufferedSamples, 24000)
  assert.equal(player.droppedPackets, 201)
  player.port.onmessage({ data: { type: 'CLEAR' } })
  assert.equal(player.bufferedSamples, 0)
  assert.equal(render(player).every((sample) => sample === 0), true)
})

test('crossfades after dropping a delayed burst instead of jumping', () => {
  const player = createPlayer()
  player.port.onmessage({ data: { type: 'SET_BUFFER_LIMIT', lowerFrames: 2, upperFrames: 4, targetFrames: 2 } })
  feed(player, 2)
  render(player)
  render(player)
  player.primed = true
  player.currentPacket = new Float32Array(240)
  player.currentPacket.fill(1)
  player.currentOffset = 238
  player.lastLeft = 1
  player.lastRight = 1
  player.resyncAfterUnderrun = false
  player.packetQueue = []
  player.bufferedSamples = 0
  for (let index = 0; index < 210; index++) {
    const packet = new Float32Array(480)
    packet.fill(-1)
    player.port.onmessage({ data: { type: 'PCM_DATA', interleaved: packet, sequence: index + 10 } })
  }
  const output = render(player)
  assert.equal(player.droppedPackets > 0, true)
  assert.equal(output[0] > -1, true)
  assert.equal(output[0] < 1, true)
})

test('keeps only the newest target buffer while recovering from an underrun', () => {
  const player = createPlayer()
  player.port.onmessage({ data: { type: 'SET_BUFFER_LIMIT', lowerFrames: 1, upperFrames: 4, targetFrames: 2 } })
  feed(player, 2)
  render(player)
  player.currentPacket = null
  player.packetQueue = []
  player.bufferedSamples = 0
  player.primed = true
  player.fadeOut = 1
  render(player)
  assert.equal(player.resyncAfterUnderrun, true)

  for (let value = 1; value <= 4; value++) {
    const packet = new Float32Array(240)
    packet.fill(value)
    player.port.onmessage({ data: { type: 'PCM_DATA', interleaved: packet, sequence: value } })
  }

  assert.equal(player.bufferedSamples, 240)
  assert.equal(player.packetQueue.length, 2)
  assert.equal(player.packetQueue[0][0], 3)
  assert.equal(player.packetQueue[1][0], 4)
  assert.equal(player.resyncAfterUnderrun, true)

  let output
  for (let index = 0; index < 2; index++) output = render(player)
  assert.equal(output.some((sample) => sample > 0), true)
  assert.equal(player.resyncAfterUnderrun, false)
})

test('recovers when the adaptive target is larger than one server packet', () => {
  const player = createPlayer()
  player.port.onmessage({ data: { type: 'SET_BUFFER_LIMIT', lowerFrames: 3, upperFrames: 8, targetFrames: 4 } })
  feed(player, 2)
  render(player)
  player.currentPacket = null
  player.packetQueue = []
  player.bufferedSamples = 0
  player.primed = true
  player.fadeOut = 1
  render(player)
  assert.equal(player.resyncAfterUnderrun, true)

  for (let value = 1; value <= 2; value++) {
    const packet = new Float32Array(480)
    packet.fill(value)
    player.port.onmessage({ data: { type: 'PCM_DATA', interleaved: packet, sequence: value } })
  }

  assert.equal(player.bufferedSamples, 480)
  assert.equal(player.resyncDroppedPackets, 0)
  assert.equal(player.packetQueue.length, 2)

  render(player)
  render(player)
  assert.equal(player.resyncAfterUnderrun, false)
})

test('reports low buffer once per downward crossing and rearms after refill', () => {
  const player = createPlayer()
  player.port.onmessage({ data: { type: 'SET_BUFFER_LIMIT', lowerFrames: 4, targetFrames: 8 } })
  feed(player, 8)
  player.port.messages.length = 0
  for (let index = 0; index < 8; index++) render(player)
  const reports = player.port.messages.filter((message) => message.lowWatermark)
  assert.equal(reports.length, 1)
  assert.equal(reports[0].bufferedFrames, 4)

  feed(player, 8)
  player.port.messages.length = 0
  for (let index = 0; index < 15; index++) render(player)
  assert.equal(player.port.messages.filter((message) => message.lowWatermark).length, 1)
})

test('reports a critical quarter-buffer resync once per downward crossing', () => {
  const player = createPlayer()
  player.port.onmessage({ data: { type: 'SET_BUFFER_LIMIT', lowerFrames: 4, upperFrames: 12, targetFrames: 8 } })
  feed(player, 8)
  player.port.messages.length = 0
  for (let index = 0; index < 12; index++) render(player)
  const critical = player.port.messages.filter((message) => message.criticalWatermark)
  assert.equal(critical.length, 1)
  assert.equal(critical[0].bufferedFrames, 2)

  feed(player, 8)
  player.port.messages.length = 0
  for (let index = 0; index < 24; index++) render(player)
  assert.equal(player.port.messages.filter((message) => message.criticalWatermark).length, 1)
})
