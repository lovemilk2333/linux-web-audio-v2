import assert from 'node:assert/strict'
import test from 'node:test'
import { configUrlFor, probeConfig, streamCandidates } from '../src/streamAddress.ts'

const PAGE = 'http://player.local/audio/'

test('uses a user address that already points at /stream', () => {
  const urls = streamCandidates('http://host:8643/backend/v2/stream', PAGE)
  assert.deepEqual(urls.map((url) => url.href), ['ws://host:8643/backend/v2/stream'])
})

test('completes a host or root address with /backend/v2/stream', () => {
  const urls = streamCandidates('http://192.168.1.8:8643', PAGE)
  assert.deepEqual(urls.map((url) => url.href), [
    'ws://192.168.1.8:8643/',
    'ws://192.168.1.8:8643/backend/v2/stream',
  ])
})

test('tries the user URL, its parent base, then the URL itself', () => {
  const urls = streamCandidates('http://host:8643/web/', PAGE)
  assert.deepEqual(urls.map((url) => url.href), [
    'ws://host:8643/web/',
    'ws://host:8643/backend/v2/stream',
    'ws://host:8643/web/backend/v2/stream',
  ])
})

test('resolves a relative address against the page and keeps its query', () => {
  const urls = streamCandidates('/custom/stream?token=1', PAGE)
  assert.deepEqual(urls.map((url) => url.href), ['ws://player.local/custom/stream?token=1'])
})

test('falls back to the default stream path when the field is empty', () => {
  const urls = streamCandidates('   ', PAGE)
  assert.deepEqual(urls.map((url) => url.href), ['ws://player.local/backend/v2/stream'])
})

test('maps a stream URL to the sibling config endpoint', () => {
  const [stream] = streamCandidates('wss://host/backend/v2/stream', PAGE)
  assert.equal(configUrlFor(stream).href, 'https://host/backend/v2/config')
})

test('treats a non-404 response as a hit and 404 as a miss', async () => {
  const url = new URL('http://host/config')
  const ok = await probeConfig(url, async () => new Response(null, { status: 500 }))
  const missing = await probeConfig(url, async () => new Response(null, { status: 404 }))
  const down = await probeConfig(url, async () => { throw new Error('offline') })
  assert.equal(ok, 'ok')
  assert.equal(missing, 'missing')
  assert.equal(down, 'unavailable')
})
