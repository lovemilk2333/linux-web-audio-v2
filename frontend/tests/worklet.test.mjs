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
  feed(player, 7)
  assert.equal(render(player).every((sample) => sample === 0), true)
  feed(player, 1)

  const start = render(player)
  assert.equal(start[0], 0.5 / 64)
  assert.equal(start[63], 0.5)
  for (let index = 0; index < 6; index++) render(player)
  const end = render(player)
  assert.equal(end[63], 0.5)
  assert.equal(end[64], 0.5 * 63 / 64)
  assert.equal(end[127], 0)
  assert.equal(player.underruns, 1)

  feed(player, 8)
  assert.equal(render(player)[0], 0.5 / 64)
})

test('counts dropped packets when the queue overflows', () => {
  const player = createPlayer()
  feed(player, 201)
  assert.equal(player.bufferedSamples, 24000)
  assert.equal(player.droppedPackets, 1)
  player.port.onmessage({ data: { type: 'CLEAR' } })
  assert.equal(player.bufferedSamples, 0)
  assert.equal(render(player).every((sample) => sample === 0), true)
})
