/**
 * Small shared helpers for the session-sync bundle. Everything here is plain
 * Node.js so the Host plugin needs no Harness package imports at runtime.
 */
import { createHash, randomBytes } from 'node:crypto'
import fs from 'node:fs/promises'
import path from 'node:path'

/** `<firstSeq>-<endExclusive>-<hash>.jsonl.gz` segment file name. */
export const SEGMENT_FILE = /^(\d+)-(\d+)-([0-9a-f]{16})\.jsonl\.gz$/

/** Session ids are used as directory names; keep them to a conservative set. */
export const SESSION_ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,150}$/

/**
 * Parse one segment file name.
 * @param {string} name - directory entry name.
 * @returns {{ start: number, end: number, hash: string } | undefined} parsed range, or undefined when the name is not a segment.
 */
export function parseSegmentFile(name) {
  const match = SEGMENT_FILE.exec(name)
  if (match === null) return undefined
  const start = Number(match[1])
  const end = Number(match[2])
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 || end <= start) return undefined
  return { start, end, hash: match[3] }
}

/**
 * Build one segment file name.
 * @param {number} start - first event seq.
 * @param {number} end - exclusive end seq.
 * @param {string} hash - content hash prefix.
 * @returns {string} the file name.
 */
export function segmentFile(start, end, hash) {
  return `${start}-${end}-${hash}.jsonl.gz`
}

/**
 * @param {string} id - candidate session id.
 * @returns {boolean} whether the id is safe as one path segment.
 */
export function isSessionId(id) {
  return typeof id === 'string' && SESSION_ID.test(id) && id !== '.' && id !== '..'
}

/**
 * Encode a contiguous event batch as newline-delimited JSON.
 * @param {readonly object[]} events - events to encode.
 * @returns {Buffer} UTF-8 JSONL without a header line.
 */
export function encodeEvents(events) {
  let text = ''
  for (const event of events) text += `${JSON.stringify(event)}\n`
  return Buffer.from(text, 'utf8')
}

/**
 * Decode newline-delimited JSON events.
 * @param {Buffer} buffer - payload bytes.
 * @returns {object[]} parsed events in file order.
 */
export function decodeEvents(buffer) {
  const text = buffer.toString('utf8')
  const events = []
  for (const line of text.split('\n')) {
    if (line.length === 0) continue
    events.push(JSON.parse(line))
  }
  return events
}

/**
 * Serialize a value with object keys sorted, so two equal values compare equal
 * regardless of construction order.
 * @param {unknown} value - JSON-compatible value.
 * @returns {string} canonical JSON text.
 */
export function canonicalJson(value) {
  return JSON.stringify(sortValue(value))
}

function sortValue(value) {
  if (Array.isArray(value)) return value.map(sortValue)
  if (value !== null && typeof value === 'object') {
    const sorted = {}
    for (const key of Object.keys(value).sort()) sorted[key] = sortValue(value[key])
    return sorted
  }
  return value
}

/**
 * @param {Buffer|string} value - bytes or text.
 * @returns {string} full lowercase SHA-256 hex digest.
 */
export function sha256(value) {
  return createHash('sha256').update(value).digest('hex')
}

/**
 * Split events into contiguous batches bounded by count and serialized bytes.
 * @param {readonly object[]} events - source batch.
 * @param {{ maxCount?: number, maxBytes?: number }} [limits] - batch bounds.
 * @returns {object[][]} contiguous batches.
 */
export function chunkEvents(events, limits = {}) {
  const maxCount = limits.maxCount ?? 400
  const maxBytes = limits.maxBytes ?? 4 * 1024 * 1024
  const chunks = []
  let current = []
  let bytes = 0
  for (const event of events) {
    const size = Buffer.byteLength(JSON.stringify(event)) + 1
    if (current.length > 0 && (current.length >= maxCount || bytes + size > maxBytes)) {
      chunks.push(current)
      current = []
      bytes = 0
    }
    current.push(event)
    bytes += size
  }
  if (current.length > 0) chunks.push(current)
  return chunks
}

/**
 * Write a file atomically through a sibling temporary file. With `createOnly`,
 * an existing target is preserved and reported instead of overwritten.
 * @param {string} file - absolute target path.
 * @param {Buffer|string} data - payload.
 * @param {{ createOnly?: boolean }} [options] - create-only behavior.
 * @returns {Promise<boolean>} true when written, false when create-only found an existing file.
 */
export async function writeFileAtomic(file, data, options = {}) {
  await fs.mkdir(path.dirname(file), { recursive: true })
  if (options.createOnly === true) {
    try {
      await fs.writeFile(file, data, { flag: 'wx', mode: 0o600 })
      return true
    } catch (error) {
      if (error?.code === 'EEXIST') return false
      throw error
    }
  }
  const temporary = `${file}.${process.pid}.${randomBytes(6).toString('hex')}.tmp`
  await fs.writeFile(temporary, data, { mode: 0o600 })
  try {
    await fs.rename(temporary, file)
  } catch (error) {
    await fs.rm(temporary, { force: true }).catch(() => {})
    throw error
  }
  return true
}

/**
 * Read and parse one JSON file.
 * @param {string} file - file path.
 * @returns {Promise<unknown | undefined>} parsed value, or undefined when absent or unreadable.
 */
export async function readJson(file) {
  try {
    return JSON.parse(await fs.readFile(file, 'utf8'))
  } catch (error) {
    if (error?.code === 'ENOENT') return undefined
    throw error
  }
}

/**
 * Compare paths the way the current platform treats them.
 * @param {string} value - path text.
 * @returns {string} normalized comparison key.
 */
export function pathKey(value) {
  const resolved = path.resolve(value)
  return process.platform === 'win32' ? resolved.toLowerCase() : resolved
}

/**
 * @param {string} value - candidate path.
 * @returns {boolean} whether the path currently names a directory.
 */
export function isDirectorySync(value) {
  try {
    return fs.statSync(value).isDirectory()
  } catch {
    return false
  }
}
