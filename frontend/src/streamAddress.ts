export const DEFAULT_STREAM_PATH = '/backend/v2/stream'
export const STREAM_SUFFIX = '/backend/v2/stream'

export type ProbeResult = 'ok' | 'missing' | 'unavailable'

/** Turn a user-entered stream address into an absolute http(s)/ws(s) URL. */
export function parseStreamAddress(address: string, baseHref = location.href): URL {
  const url = new URL(address.trim() || DEFAULT_STREAM_PATH, baseHref)
  if (url.protocol === 'http:') url.protocol = 'ws:'
  else if (url.protocol === 'https:') url.protocol = 'wss:'
  else if (url.protocol !== 'ws:' && url.protocol !== 'wss:') {
    throw new Error('流地址必须使用 http(s) 或 ws(s) 协议')
  }
  return url
}

function joinPath(base: string, suffix: string): string {
  const prefix = base.endsWith('/') ? base.slice(0, -1) : base
  return `${prefix}${suffix}`
}

/**
 * Candidate stream URLs, in order:
 * 1. the address the user entered
 * 2. `<user URL base>/backend/v2/stream`
 * 3. `<user URL>/backend/v2/stream`
 *
 * An address that already ends in `/stream` is used as-is.
 */
export function streamCandidates(address: string, baseHref = location.href): URL[] {
  const primary = parseStreamAddress(address, baseHref)
  const pathname = primary.pathname.replace(/\/+$/, '') || '/'
  if (pathname === '/stream' || pathname.endsWith('/stream')) return [primary]

  const underBase = new URL(primary.href)
  const parent = pathname === '/' ? '' : pathname.slice(0, pathname.lastIndexOf('/'))
  underBase.pathname = joinPath(parent || '/', STREAM_SUFFIX)

  const appended = new URL(primary.href)
  appended.pathname = joinPath(pathname, STREAM_SUFFIX)

  const seen = new Set<string>()
  return [primary, underBase, appended].filter((url) => {
    if (seen.has(url.href)) return false
    seen.add(url.href)
    return true
  })
}

/** Config endpoint that sits next to a stream endpoint. */
export function configUrlFor(stream: URL): URL {
  const url = new URL(stream.href)
  url.protocol = url.protocol === 'wss:' ? 'https:' : 'http:'
  const pathname = url.pathname.replace(/\/+$/, '') || '/'
  url.pathname = pathname.endsWith('/stream')
    ? `${pathname.slice(0, -'/stream'.length)}/config`
    : `${pathname === '/' ? '' : pathname}/config`
  return url
}

const PROBE_TIMEOUT_MS = 2500

/**
 * Ask a candidate's config endpoint whether a server is there.
 * 404 means "not this path"; anything else (including non-404 errors) means
 * the server answered, so the matching stream path is worth connecting to.
 */
export async function probeConfig(
  url: URL,
  fetchImpl: typeof fetch = fetch,
  timeoutMs = PROBE_TIMEOUT_MS,
): Promise<ProbeResult> {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  try {
    const response = await fetchImpl(url, { headers: { Accept: 'application/bson' }, signal: controller.signal })
    if (response.status === 404) return 'missing'
    return 'ok'
  } catch {
    return 'unavailable'
  } finally {
    clearTimeout(timer)
  }
}
