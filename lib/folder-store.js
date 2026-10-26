/**
 * The shared-folder remote store. One folder holds every account namespace;
 * each session is a directory of an immutable header plus content-addressed
 * event segments. Nothing in this store is ever deleted or rewritten.
 */
import fs from 'node:fs/promises'
import path from 'node:path'
import { gunzipSync, gzipSync } from 'node:zlib'
import {
  decodeEvents,
  encodeEvents,
  isSessionId,
  parseSegmentFile,
  readJson,
  segmentFile,
  sha256,
  writeFileAtomic,
} from './util.js'

/**
 * One remote session observation.
 * @typedef {object} RemoteSession
 * @property {string} id - session id.
 * @property {object} [header] - stored session header.
 * @property {number} [inheritedEventCount] - fork-inherited prefix length.
 * @property {{ start: number, end: number, hash: string, file: string }[]} segments - contiguous segments.
 * @property {number} max - exclusive end seq covered by the segments.
 * @property {string} [error] - why the session cannot be used.
 */

export class FolderStore {
  /**
   * @param {{ root: string, accountKey: string, deviceId: string, deviceName: string }} options - remote root and identity.
   */
  constructor(options) {
    this.root = path.resolve(options.root)
    this.accountKey = String(options.accountKey).replace(/[^A-Za-z0-9._-]/g, '_')
    this.deviceId = String(options.deviceId).replace(/[^A-Za-z0-9._-]/g, '_')
    this.deviceName = options.deviceName
    this.accountDir = path.join(this.root, 'accounts', this.accountKey)
    this.sessionsDir = path.join(this.accountDir, 'sessions')
    this.conflictsDir = path.join(this.accountDir, 'conflicts')
    this.devicesDir = path.join(this.accountDir, 'devices')
    this.headerCache = new Map()
  }

  /**
   * Create the layout when it is missing.
   * @returns {Promise<void>} resolution after the directories exist.
   */
  async ensure() {
    await fs.mkdir(this.sessionsDir, { recursive: true })
    await fs.mkdir(this.conflictsDir, { recursive: true })
    await fs.mkdir(this.devicesDir, { recursive: true })
  }

  /**
   * Record this device's last contact.
   * @param {Record<string, unknown>} details - extra device facts.
   * @returns {Promise<void>} resolution after the heartbeat is durable.
   */
  async heartbeat(details = {}) {
    const file = path.join(this.devicesDir, `${this.deviceId}.json`)
    const body = {
      schema: 1,
      deviceId: this.deviceId,
      deviceName: this.deviceName,
      lastSeen: new Date().toISOString(),
      ...details,
    }
    await writeFileAtomic(file, `${JSON.stringify(body, null, 2)}\n`)
  }

  /**
   * Read every remote session header and segment index.
   * @returns {Promise<Map<string, RemoteSession>>} sessions keyed by id.
   */
  async listSessions() {
    const entries = await fs.readdir(this.sessionsDir, { withFileTypes: true }).catch(() => [])
    const sessions = new Map()
    for (const entry of entries) {
      if (!entry.isDirectory() || !isSessionId(entry.name)) continue
      sessions.set(entry.name, await this.loadSession(entry.name))
    }
    return sessions
  }

  /**
   * Observe one remote session.
   * @param {string} id - session id.
   * @returns {Promise<RemoteSession>} header, segments, and coverage.
   */
  async loadSession(id) {
    const directory = path.join(this.sessionsDir, id)
    const headerFile = path.join(directory, 'header.json')
    const headerStat = await fs.stat(headerFile).catch(() => undefined)
    if (headerStat === undefined) {
      return { id, segments: [], max: 0, error: 'the remote session has no header.json' }
    }
    let cached = this.headerCache.get(id)
    if (cached === undefined || cached.mtimeMs !== headerStat.mtimeMs) {
      const stored = await readJson(headerFile)
      const header = stored?.header
      if (header === null || typeof header !== 'object' || header.id !== id || typeof header.version !== 'number') {
        return { id, segments: [], max: 0, error: 'the remote header.json is invalid for this session' }
      }
      const inheritedEventCount = Number.isSafeInteger(stored.inheritedEventCount) ? stored.inheritedEventCount : 0
      cached = { mtimeMs: headerStat.mtimeMs, header, inheritedEventCount }
      this.headerCache.set(id, cached)
    }

    const eventsDir = path.join(directory, 'events')
    const names = await fs.readdir(eventsDir).catch(() => [])
    const segments = []
    for (const name of names) {
      const parsed = parseSegmentFile(name)
      if (parsed === undefined) continue
      segments.push({ ...parsed, file: path.join(eventsDir, name) })
    }
    segments.sort((left, right) => left.start - right.start)

    let cursor = 0
    let error
    for (const segment of segments) {
      if (segment.start !== cursor) {
        error = `the remote segment chain has a gap at seq ${cursor} (next segment starts at ${segment.start})`
        break
      }
      cursor = segment.end
    }
    return {
      id,
      header: cached.header,
      inheritedEventCount: cached.inheritedEventCount,
      segments,
      max: cursor,
      ...(error === undefined ? {} : { error }),
    }
  }

  /**
   * Write the immutable header for a new remote session.
   * @param {string} id - session id.
   * @param {{ header: object, inheritedEventCount: number }} input - stored metadata.
   * @returns {Promise<object>} the header record now on disk.
   * @throws {Error} when an existing header belongs to another session or format.
   */
  async writeHeader(id, input) {
    const directory = path.join(this.sessionsDir, id)
    const file = path.join(directory, 'header.json')
    const body = {
      schema: 1,
      header: input.header,
      inheritedEventCount: input.inheritedEventCount ?? 0,
      writer: { deviceId: this.deviceId, deviceName: this.deviceName },
      writtenAt: new Date().toISOString(),
    }
    const written = await writeFileAtomic(file, `${JSON.stringify(body, null, 2)}\n`, { createOnly: true })
    if (written) {
      this.headerCache.delete(id)
      return body
    }
    const existing = await readJson(file)
    if (existing?.header?.id !== id) {
      throw new Error(`remote session "${id}" has a header for a different session`)
    }
    if (existing.header.version !== input.header.version) {
      throw new Error(`remote session "${id}" is format ${String(existing.header.version)}, local is ${String(input.header.version)}`)
    }
    return existing
  }

  /**
   * Append one immutable segment. The caller guarantees the range continues
   * the current remote chain.
   * @param {string} id - session id.
   * @param {readonly object[]} events - contiguous events starting at the chain end.
   * @returns {Promise<{ start: number, end: number, hash: string, file: string } | undefined>} the written segment.
   */
  async writeSegment(id, events) {
    if (events.length === 0) return undefined
    const start = events[0].seq
    const end = events[events.length - 1].seq + 1
    const payload = encodeEvents(events)
    const hash = sha256(payload).slice(0, 16)
    const directory = path.join(this.sessionsDir, id, 'events')
    const file = path.join(directory, segmentFile(start, end, hash))

    const existing = await this.loadSession(id)
    if (existing.error !== undefined) throw new Error(existing.error)
    for (const segment of existing.segments) {
      if (segment.start === start && segment.end === end && segment.hash === hash) return segment
      if (segment.start < end && start < segment.end) {
        throw new Error(`remote session "${id}" already has a different segment covering seq ${start}-${end}`)
      }
    }
    if (start !== existing.max) {
      throw new Error(`remote session "${id}" expects the next segment at seq ${existing.max}, got ${start}`)
    }

    await fs.mkdir(directory, { recursive: true })
    await writeFileAtomic(file, gzipSync(payload, { level: 6 }))
    this.headerCache.delete(id)
    return { start, end, hash, file }
  }

  /**
   * Read events in one closed range.
   * @param {string} id - session id.
   * @param {number} from - first seq to include.
   * @param {number} to - exclusive end seq.
   * @returns {Promise<object[]>} events ordered by seq.
   */
  async readEvents(id, from, to) {
    const session = await this.loadSession(id)
    if (session.error !== undefined) throw new Error(session.error)
    if (!Number.isSafeInteger(from) || !Number.isSafeInteger(to) || to <= from) return []
    const events = []
    for (const segment of session.segments) {
      if (segment.end <= from || segment.start >= to) continue
      const payload = gunzipSync(await fs.readFile(segment.file))
      for (const event of decodeEvents(payload)) {
        if (event.seq >= from && event.seq < to) events.push(event)
      }
    }
    events.sort((left, right) => left.seq - right.seq)
    return events
  }

  /**
   * Reject a whole event range with a stable explanation when the remote chain
   * cannot produce it.
   * @param {string} id - session id.
   * @param {number} from - first seq to include.
   * @param {number} to - exclusive end seq.
   * @returns {Promise<object[]>} contiguous events, or a rejection.
   */
  async requireEvents(id, from, to) {
    const events = await this.readEvents(id, from, to)
    if (events.length !== to - from) {
      throw new Error(`remote session "${id}" is missing events ${from}-${to}`)
    }
    for (let index = 0; index < events.length; index += 1) {
      if (events[index].seq !== from + index) {
        throw new Error(`remote session "${id}" returned an out-of-order event at seq ${events[index].seq}`)
      }
    }
    return events
  }

  /**
   * Copy a losing local suffix into the shared conflict area so it survives on
   * every device.
   * @param {string} id - session id.
   * @param {readonly object[]} events - losing suffix.
   * @param {Record<string, unknown>} details - conflict facts.
   * @returns {Promise<string>} the archive file name.
   */
  async archiveConflict(id, events, details) {
    const directory = path.join(this.conflictsDir, id)
    const stamp = new Date().toISOString().replace(/[:.]/g, '-')
    const name = `${this.deviceId}-${stamp}-local.jsonl.gz`
    await fs.mkdir(directory, { recursive: true })
    await writeFileAtomic(path.join(directory, name), gzipSync(encodeEvents(events), { level: 6 }))
    await writeFileAtomic(
      path.join(directory, `${this.deviceId}-${stamp}-conflict.json`),
      `${JSON.stringify({ sessionId: id, deviceId: this.deviceId, at: new Date().toISOString(), ...details }, null, 2)}\n`,
    )
    return name
  }
}
