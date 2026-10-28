/**
 * The replication engine. A session is a contiguous event log; each device
 * uploads the suffix it produced and appends the suffix another device
 * produced. Both sides can only append, so a device edits a session only while
 * its local log still ends where the shared copy ends. When two devices both
 * advanced, the engine either proves the overlap identical and finishes the
 * longer side, or freezes the session and archives the losing suffix.
 */
import { randomUUID } from 'node:crypto'
import os from 'node:os'
import path from 'node:path'
import { FolderStore } from './folder-store.js'
import { canonicalJson, chunkEvents, isDirectorySync, pathKey } from './util.js'

const PAGE_EVENTS = 400
const MAX_COMPARE_EVENTS = 50_000
const MAX_REPORTED_ERRORS = 20

/**
 * @typedef {object} SyncResult
 * @property {string} at - ISO start time.
 * @property {string} reason - what triggered the run.
 * @property {number} exported - sessions that pushed new events.
 * @property {number} imported - sessions that pulled new events.
 * @property {string[]} conflicts - sessions frozen by divergence.
 * @property {{ sessionId: string, message: string }[]} errors - bounded failure list.
 * @property {string[]} skipped - sessions postponed (for example, open locally).
 * @property {number} sessions - sessions considered.
 */

export class SyncEngine {
  /**
   * @param {{ ctx: object, config: object, state: import('./local-state.js').LocalState, logger?: object, folderFactory?: Function }} options - wiring.
   */
  constructor(options) {
    this.ctx = options.ctx
    this.config = options.config
    this.state = options.state
    this.logger = options.logger
    this.folderFactory = options.folderFactory ?? (folderOptions => new FolderStore(folderOptions))
    this.running = undefined
    this.queued = false
    this.lastResult = undefined
    this.lastErrorLog = new Map()
    this.remote = undefined
    this.localFormatVersion = undefined
    this.workspaces = []
    this.accountKey = undefined
    this.announced = new Set()
  }

  /**
   * Run one cycle, coalescing concurrent callers.
   * @param {string} reason - trigger name.
   * @returns {Promise<SyncResult>} the completed result.
   */
  syncNow(reason) {
    if (this.running !== undefined) {
      this.queued = true
      return this.running
    }
    this.running = this.run(reason).finally(() => {
      this.running = undefined
      if (this.queued) {
        this.queued = false
        queueMicrotask(() => { void this.syncNow('queued') })
      }
    })
    return this.running
  }

  /**
   * Run one full cycle and persist its report.
   * @param {string} reason - trigger name.
   * @returns {Promise<SyncResult>} result object; never rejects.
   */
  async run(reason) {
    /** @type {SyncResult} */
    const result = {
      at: new Date().toISOString(),
      reason,
      exported: 0,
      imported: 0,
      conflicts: [],
      errors: [],
      skipped: [],
      sessions: 0,
    }
    try {
      await this.state.load()
      const identity = await this.resolveIdentity()
      const accountKey = this.config.accountKey ?? (identity.userId === undefined ? 'local' : `user-${identity.userId}`)
      if (this.state.data.accountKey !== accountKey) {
        this.state.data.accountKey = accountKey
        this.state.data.sessions = {}
      }
      this.accountKey = accountKey
      const deviceId = identity.deviceId ?? this.state.data.deviceId ?? randomUUID()
      this.state.data.deviceId = deviceId
      const remoteDir = this.config.remoteDir
      if (typeof remoteDir !== 'string' || remoteDir.length === 0) {
        throw new Error('no shared folder is configured and no OneDrive folder was detected')
      }
      this.remote = this.folderFactory({ root: remoteDir, accountKey, deviceId, deviceName: this.config.deviceName })
      await this.remote.ensure()
      await this.remote.heartbeat({ platform: process.platform, osVersion: os.release(), plugin: 'session-sync' })

      const remoteSessions = await this.remote.listSessions()
      const localSnapshots = await this.ctx.sessionPersistence.list()
      this.localFormatVersion = localSnapshots[0]?.header?.version
      const localSessions = new Map(localSnapshots.map(snapshot => [snapshot.header.id, snapshot]))
      this.workspaces = this.readWorkspaces()
      this.announceUngrouped(localSnapshots)

      const ids = [...new Set([...localSessions.keys(), ...remoteSessions.keys()])]
      ids.sort((left, right) => {
        const leftHeader = localSessions.get(left)?.header ?? remoteSessions.get(left)?.header
        const rightHeader = localSessions.get(right)?.header ?? remoteSessions.get(right)?.header
        const delta = (leftHeader?.createdAt ?? 0) - (rightHeader?.createdAt ?? 0)
        return delta === 0 ? left.localeCompare(right) : delta
      })
      result.sessions = ids.length

      for (const id of ids) {
        try {
          await this.syncOne(id, localSessions.get(id), remoteSessions.get(id), result)
        } catch (error) {
          this.recordError(result, id, error)
        }
      }
    } catch (error) {
      this.recordError(result, '*', error)
    }

    this.lastResult = result
    try {
      await this.state.save()
      const conflicts = Object.entries(this.state.data.sessions ?? {})
        .filter(([, entry]) => entry.conflict !== undefined)
        .map(([id, entry]) => ({ sessionId: id, ...entry.conflict }))
      await this.state.writeStatus({
        schema: 1,
        updatedAt: new Date().toISOString(),
        remoteDir: this.config.remoteDir,
        accountKey: this.accountKey,
        deviceId: this.state.data.deviceId,
        deviceName: this.config.deviceName,
        lastRun: result,
        conflicts,
      })
    } catch (error) {
      this.recordError(result, '*', error)
    }
    return result
  }

  /**
   * Replicate one session in whichever direction its two logs allow.
   * @param {string} id - session id.
   * @param {object | undefined} localSnapshot - local persistence snapshot.
   * @param {import('./folder-store.js').RemoteSession | undefined} remote - remote observation.
   * @param {SyncResult} result - run report.
   * @returns {Promise<void>} resolution after the session is reconciled.
   */
  async syncOne(id, localSnapshot, remote, result) {
    const header = localSnapshot?.header ?? remote?.header
    if (!this.config.includeSubagents && header?.origin === 'subagent') return

    const entry = this.state.session(id)
    if (entry.conflict !== undefined) {
      result.skipped.push(id)
      return
    }
    if (entry.needsAttach === true && localSnapshot !== undefined) {
      if (await this.attachImported(id, localSnapshot.header)) delete entry.needsAttach
    }
    if (remote?.error !== undefined) throw new Error(remote.error)
    if (remote !== undefined && localSnapshot !== undefined && remote.header.version !== localSnapshot.header.version) {
      throw new Error(`format ${String(remote.header.version)} on the shared copy, ${String(localSnapshot.header.version)} locally`)
    }
    if (remote !== undefined && this.localFormatVersion !== undefined && remote.header.version !== this.localFormatVersion) {
      throw new Error(`format ${String(remote.header.version)} on the shared copy, this build stores format ${this.localFormatVersion}`)
    }

    const remoteMax = remote?.max ?? 0
    const synced = entry.synced ?? 0
    if (remoteMax < synced) {
      throw new Error(`the shared log ends at seq ${remoteMax}, below the locally recorded seq ${synced}`)
    }

    if (localSnapshot === undefined) {
      if (remote === undefined || remoteMax === 0) return
      await this.importSession(id, remote, entry, result)
      return
    }

    const localSize = localSnapshot.sizeBytes
    const knownSize = entry.sizeBytes
    if (localSize !== undefined && knownSize !== undefined && localSize < knownSize) {
      throw new Error(`the local log shrank from ${knownSize} to ${localSize} bytes`)
    }
    const changed = localSize === undefined || knownSize === undefined || localSize > knownSize

    if (!changed && remoteMax === synced) return
    if (changed && remoteMax > synced) {
      await this.reconcile(id, synced, remoteMax, localSnapshot, result)
      return
    }
    if (changed) {
      await this.pushFrom(id, synced, localSnapshot, remote, result)
      return
    }
    await this.pullInto(id, synced, remoteMax, localSnapshot, result)
  }

  /**
   * Both sides advanced. Compare the overlapping range; an identical overlap
   * means one side is a strict prefix and the remaining suffix is transferred.
   * A mismatch freezes the session and archives the local suffix remotely.
   * @param {string} id - session id.
   * @param {number} synced - last seq both sides agreed on.
   * @param {number} remoteMax - remote exclusive end seq.
   * @param {object} localSnapshot - local persistence snapshot.
   * @param {SyncResult} result - run report.
   * @returns {Promise<void>} resolution after reconciliation.
   */
  async reconcile(id, synced, remoteMax, localSnapshot, result) {
    const tail = await this.readLocalTail(id, synced)
    if (tail.length === 0) {
      await this.pullInto(id, synced, remoteMax, localSnapshot, result)
      return
    }
    const localMax = tail[tail.length - 1].seq + 1
    const overlapEnd = Math.min(remoteMax, localMax)
    const remoteEvents = await this.remote.requireEvents(id, synced, overlapEnd)
    for (let index = 0; index < remoteEvents.length; index += 1) {
      if (canonicalJson(remoteEvents[index]) !== canonicalJson(tail[index])) {
        await this.freeze(id, synced, tail, remoteMax, localMax, result, `the shared and local events disagree at seq ${synced + index}`)
        return
      }
    }
    if (localMax > remoteMax) {
      await this.pushFrom(id, remoteMax, localSnapshot, await this.remote.loadSession(id), result)
      return
    }
    if (remoteMax > localMax) {
      await this.pullInto(id, localMax, remoteMax, localSnapshot, result)
      return
    }
    this.state.session(id).synced = remoteMax
    this.state.session(id).sizeBytes = localSnapshot.sizeBytes
  }

  /**
   * Append the remote suffix to the local log, creating the session first when
   * this device has never seen it.
   * @param {string} id - session id.
   * @param {number} from - first seq to fetch.
   * @param {number} to - exclusive end seq.
   * @param {object | undefined} localSnapshot - local persistence snapshot.
   * @param {SyncResult} result - run report.
   * @returns {Promise<void>} resolution after the local log catches up.
   */
  async pullInto(id, from, to, localSnapshot, result) {
    if (this.ctx.sessions.get(id) !== undefined) {
      result.skipped.push(id)
      return
    }
    let handle
    try {
      handle = await this.ctx.sessionPersistence.open(id, 'write')
    } catch (error) {
      if (error?.name === 'SessionAlreadyOwnedError') {
        result.skipped.push(id)
        return
      }
      throw error
    }
    try {
      let cursor = from
      while (cursor < to) {
        const events = await this.remote.requireEvents(id, cursor, Math.min(to, cursor + PAGE_EVENTS))
        if (events[0].seq !== cursor) throw new Error(`expected seq ${cursor}, got ${events[0].seq}`)
        await handle.append(events)
        cursor = events[events.length - 1].seq + 1
      }
      this.state.session(id).synced = to
      this.state.session(id).sizeBytes = localSnapshot?.sizeBytes ?? 0
      result.imported += 1
    } finally {
      await handle.close()
    }
  }

  /**
   * Materialize a remote-only session locally and attach it to a workspace.
   * @param {string} id - session id.
   * @param {import('./folder-store.js').RemoteSession} remote - remote observation.
   * @param {object} entry - local state entry.
   * @param {SyncResult} result - run report.
   * @returns {Promise<void>} resolution after the session is locally durable.
   */
  async importSession(id, remote, entry, result) {
    if (this.localFormatVersion !== undefined && remote.header.version !== this.localFormatVersion) {
      throw new Error(`format ${String(remote.header.version)} on the shared copy, this build stores format ${this.localFormatVersion}`)
    }
    const header = { ...remote.header }
    const mapped = this.mapRemoteCwd(header.cwd)
    if (mapped !== undefined) header.cwd = mapped
    let handle
    try {
      handle = await this.ctx.sessionPersistence.create(
        header,
        header.isSeeded === true ? { inheritedEventCount: remote.inheritedEventCount } : undefined,
      )
    } catch (error) {
      if (error?.name === 'SessionAlreadyExistsError') {
        result.skipped.push(id)
        return
      }
      throw error
    }
    try {
      let cursor = 0
      while (cursor < remote.max) {
        const events = await this.remote.requireEvents(id, cursor, Math.min(remote.max, cursor + PAGE_EVENTS))
        if (events[0].seq !== cursor) throw new Error(`expected seq ${cursor}, got ${events[0].seq}`)
        await handle.append(events)
        cursor = events[events.length - 1].seq + 1
      }
    } finally {
      await handle.close()
    }
    entry.synced = remote.max
    entry.sizeBytes = 0
    result.imported += 1
    entry.needsAttach = !(await this.attachImported(id, header))
    this.announceSession(header, undefined, true)
  }

  /**
   * Upload the local suffix from `from` to the end of the local log.
   * @param {string} id - session id.
   * @param {number} from - first local seq to upload.
   * @param {object} localSnapshot - local persistence snapshot.
   * @param {import('./folder-store.js').RemoteSession | undefined} remote - remote observation.
   * @param {SyncResult} result - run report.
   * @returns {Promise<void>} resolution after the upload.
   */
  async pushFrom(id, from, localSnapshot, remote, result) {
    const handle = await this.ctx.sessionPersistence.open(id, 'read')
    let wrote = false
    try {
      if (remote === undefined) {
        await this.remote.writeHeader(id, {
          header: localSnapshot.header,
          inheritedEventCount: handle.inheritedEventCount,
        })
      } else if (remote.max !== from) {
        throw new Error(`the shared log ends at seq ${remote.max}, but this device wants to upload from ${from}`)
      }
      let cursor = from
      for (;;) {
        const page = await handle.read(cursor, PAGE_EVENTS)
        if (page.events.length === 0) break
        if (page.events[0].seq !== cursor) throw new Error(`the local log has a gap at seq ${cursor}`)
        for (const chunk of chunkEvents(page.events)) {
          await this.remote.writeSegment(id, chunk)
          cursor = chunk[chunk.length - 1].seq + 1
          this.state.session(id).synced = cursor
          wrote = true
        }
      }
      if (wrote) result.exported += 1
      this.state.session(id).sizeBytes = localSnapshot.sizeBytes
    } finally {
      await handle.close()
    }
  }

  /**
   * Read the local suffix from `from` to the end, bounded for comparison.
   * @param {string} id - session id.
   * @param {number} from - first seq to read.
   * @returns {Promise<object[]>} contiguous local events.
   */
  async readLocalTail(id, from) {
    const handle = await this.ctx.sessionPersistence.open(id, 'read')
    try {
      const events = []
      let cursor = from
      for (;;) {
        const page = await handle.read(cursor, PAGE_EVENTS)
        if (page.events.length === 0) break
        if (page.events[0].seq !== cursor) throw new Error(`the local log has a gap at seq ${cursor}`)
        for (const event of page.events) events.push(event)
        cursor = page.events[page.events.length - 1].seq + 1
        if (events.length > MAX_COMPARE_EVENTS) {
          throw new Error(`the local suffix exceeds ${MAX_COMPARE_EVENTS} events; refusing to compare it in memory`)
        }
      }
      return events
    } finally {
      await handle.close()
    }
  }

  /**
   * Freeze a divergent session and store the local suffix in the shared area.
   * @param {string} id - session id.
   * @param {number} synced - last agreed seq.
   * @param {object[]} tail - local suffix.
   * @param {number} remoteMax - remote exclusive end.
   * @param {number} localMax - local exclusive end.
   * @param {SyncResult} result - run report.
   * @param {string} reason - why the session diverged.
   * @returns {Promise<void>} resolution after the archive is written.
   */
  async freeze(id, synced, tail, remoteMax, localMax, result, reason) {
    const archived = await this.remote.archiveConflict(id, tail, {
      reason,
      synced,
      localMax,
      remoteMax,
      localSizeBytes: this.state.session(id).sizeBytes,
    }).catch(() => undefined)
    this.state.session(id).conflict = {
      at: new Date().toISOString(),
      reason,
      synced,
      localMax,
      remoteMax,
      archived,
    }
    result.conflicts.push(id)
    this.warn(`session-sync: session "${id}" diverged: ${reason}; frozen and archived locally${archived === undefined ? ' (archive failed)' : ''}`)
  }

  /**
   * Announce cold-imported Sessions that are not part of any Workspace, so a
   * connected Client adds the rows without a page reload. Sessions that
   * already have a projection cache are left alone: their rows already carry
   * titles and announcing a summary without projections would replace them.
   * @param {readonly object[]} snapshots - local persistence snapshots.
   * @returns {void}
   */
  announceUngrouped(snapshots) {
    for (const snapshot of snapshots) this.announceSession(snapshot.header, snapshot.sizeBytes)
  }

  /**
   * Forward one `api-session/added` row for a cold Session.
   * @param {object} header - stored Session header.
   * @param {number} [sizeBytes] - stored log size, when known.
   * @param {boolean} [force] - announce even when the Session is already accounted (a fresh import).
   * @returns {void}
   */
  announceSession(header, sizeBytes, force = false) {
    if (typeof this.ctx.emit !== 'function') return
    if (header.origin === 'subagent') return
    if (this.announced.has(header.id)) return
    if (sizeBytes === 0) return
    const workspaces = this.workspaces.length > 0 ? this.workspaces : this.readWorkspaces()
    if (!force && workspaces.some(workspace => workspace.sessionIds.includes(header.id))) return
    const cache = this.ctx.get('sessionProjectionCache')
    if (cache !== undefined) {
      try {
        if (cache.cachedSnapshot(header) !== undefined) return
      } catch {
        // An unavailable cache must not block the announcement.
      }
    }
    this.announced.add(header.id)
    this.ctx.emit('api-session/added', {
      agentAvailable: false,
      sessionId: header.id,
      updatedAt: header.createdAt,
      running: false,
      blank: false,
      ...(header.cwd === undefined ? {} : { cwd: header.cwd }),
      ...(header.parentSession === undefined ? {} : { parentSessionId: header.parentSession }),
    })
  }

  /**
   * Attach a freshly imported session to the local workspace with the mapped
   * directory, creating that workspace when the directory exists.
   * @param {string} id - session id.
   * @param {object} header - stored header.
   * @returns {Promise<boolean>} true when there is nothing left to retry.
   */
  async attachImported(id, header) {
    if (header.origin === 'subagent') return true
    const registry = this.ctx.get('workspaceRegistry')
    if (registry === undefined || typeof header.cwd !== 'string') return true
    const target = this.mapRemoteCwd(header.cwd)
    if (target === undefined || !isDirectorySync(target)) return true
    let workspace = this.workspaces.find(candidate => pathKey(candidate.path) === pathKey(target))
    if (workspace === undefined) {
      try {
        workspace = await registry.create(target)
      } catch (error) {
        this.warn(`session-sync: could not create workspace "${target}": ${messageOf(error)}`)
        return false
      }
      this.workspaces = this.readWorkspaces()
    }
    if (!workspace.sessionIds.includes(id)) {
      try {
        await workspace.attachSession(id)
      } catch (error) {
        this.warn(`session-sync: could not attach session "${id}" to "${target}": ${messageOf(error)}`)
        return false
      }
    }
    return true
  }

  /**
   * Map a recorded working directory onto this machine.
   * @param {string | undefined} cwd - directory recorded by the writing device.
   * @returns {string | undefined} an existing local directory, when one is known.
   */
  mapRemoteCwd(cwd) {
    if (typeof cwd !== 'string' || cwd.length === 0) return undefined
    const key = pathKey(cwd)
    for (const entry of this.config.workspaceMap) {
      if (pathKey(entry.remote) === key) return entry.local
    }
    if (isDirectorySync(cwd)) return cwd
    const base = path.basename(cwd)
    const matches = this.workspaces.filter(candidate => path.basename(candidate.path) === base)
    if (matches.length === 1) return matches[0].path
    return undefined
  }

  /**
   * @returns {Array<{ path: string, sessionIds: string[] }>} current local workspaces.
   */
  readWorkspaces() {
    const registry = this.ctx.get('workspaceRegistry')
    if (registry === undefined) return []
    try {
      return registry.list()
    } catch {
      return []
    }
  }

  /**
   * Resolve the platform identity without failing the run when the account
   * service or credential store is unavailable.
   * @returns {Promise<{ userId?: string, deviceId?: string }>} available identity.
   */
  async resolveIdentity() {
    const account = this.ctx.get('deepseekAccount')
    if (account === undefined) return {}
    try {
      return (await account.getDeviceIdentity()) ?? {}
    } catch (error) {
      this.warn(`session-sync: account identity unavailable: ${messageOf(error)}`)
      return {}
    }
  }

  /**
   * Record one bounded failure and log it when it is new.
   * @param {SyncResult} result - run report.
   * @param {string} sessionId - failing session, or '*'.
   * @param {unknown} error - thrown value.
   * @returns {void}
   */
  recordError(result, sessionId, error) {
    const message = messageOf(error)
    if (result.errors.length < MAX_REPORTED_ERRORS) result.errors.push({ sessionId, message })
    const key = `${sessionId}:${message}`
    const previous = this.lastErrorLog.get(key)
    const now = Date.now()
    if (previous !== undefined && now - previous < 30 * 60_000) return
    this.lastErrorLog.set(key, now)
    this.warn(`session-sync: ${sessionId}: ${message}`)
  }

  /**
   * @param {string} message - diagnostic text.
   * @returns {void}
   */
  warn(message) {
    this.logger?.warn?.(message)
  }

  /**
   * @returns {string} human-readable status for `/sync status`.
   */
  describe() {
    const result = this.lastResult
    const conflicts = Object.entries(this.state.data.sessions ?? {})
      .filter(([, entry]) => entry.conflict !== undefined)
      .map(([id]) => id)
    const lines = [
      'Session sync',
      `  shared folder: ${this.config.remoteDir ?? '(not configured)'}`,
      `  account: ${this.accountKey ?? '(not resolved yet)'}`,
      `  device: ${this.state.data.deviceId ?? '(not resolved yet)'}`,
      `  enabled: ${this.config.enabled ? 'yes' : 'no'}`,
    ]
    if (result === undefined) {
      lines.push('  last run: never')
    } else {
      lines.push(`  last run: ${result.at} (${result.reason})`)
      lines.push(`  sessions: ${result.sessions}, exported: ${result.exported}, imported: ${result.imported}`)
      if (result.errors.length > 0) {
        lines.push(`  errors: ${result.errors.length}`)
        for (const error of result.errors.slice(0, 3)) lines.push(`    ${error.sessionId}: ${error.message}`)
      }
      if (result.skipped.length > 0) lines.push(`  postponed: ${result.skipped.length} (session open locally)`)
    }
    if (conflicts.length > 0) {
      lines.push(`  frozen conflicts: ${conflicts.length}`)
      for (const id of conflicts.slice(0, 5)) lines.push(`    ${id}`)
    }
    return lines.join('\n')
  }

  /**
   * @param {SyncResult} result - completed run.
   * @returns {string} command-friendly summary.
   */
  formatRun(result) {
    const parts = [`同步完成：上传 ${result.exported} 个会话，下载 ${result.imported} 个会话`]
    if (result.conflicts.length > 0) parts.push(`冲突冻结 ${result.conflicts.length} 个：${result.conflicts.slice(0, 3).join(', ')}`)
    if (result.errors.length > 0) {
      parts.push(`错误 ${result.errors.length} 个`)
      for (const error of result.errors.slice(0, 3)) parts.push(`  ${error.sessionId}: ${error.message}`)
    }
    if (result.skipped.length > 0) parts.push(`推迟 ${result.skipped.length} 个（本机打开中）`)
    if (result.imported > 0) parts.push('新导入的会话需要刷新页面（Ctrl+R）后才会出现在侧边栏。')
    if (result.errors.length === 0 && result.conflicts.length === 0 && result.exported === 0 && result.imported === 0) {
      parts.push('没有需要同步的变化')
    }
    return parts.join('\n')
  }
}

/**
 * @param {unknown} error - thrown value.
 * @returns {string} one-line message.
 */
function messageOf(error) {
  if (error instanceof Error) return error.message
  try {
    return String(error)
  } catch {
    return 'unknown error'
  }
}
