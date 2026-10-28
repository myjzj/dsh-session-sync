import assert from 'node:assert/strict'
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import test from 'node:test'
import { FolderStore } from '../lib/folder-store.js'
import { SyncEngine } from '../lib/engine.js'
import { LocalState } from '../lib/local-state.js'

function event(seq, text) {
  return { type: 'user/message', seq, time: 1_700_000_000_000 + seq, data: { text } }
}

class MemoryHandle {
  constructor(session, access) {
    this.session = session
    this.access = access
  }

  get inheritedEventCount() {
    return this.session.inheritedEventCount
  }

  async read(offset = 0, length = Number.MAX_SAFE_INTEGER) {
    return { eventState: 'detached', events: this.session.events.slice(offset, offset + length) }
  }

  async append(events) {
    assert.equal(this.access, 'write')
    assert.equal(events[0].seq, this.session.events.length)
    for (const item of events) this.session.events.push(structuredClone(item))
  }

  async flush() {}

  async close() {}
}

class MemoryPersistence {
  constructor() {
    this.sessions = new Map()
  }

  seed(id, header, events) {
    this.sessions.set(id, { header, events, inheritedEventCount: header.isSeeded ? 0 : 0 })
  }

  snapshot(session) {
    return {
      header: session.header,
      sizeBytes: Buffer.byteLength(JSON.stringify(session.events)),
      eventCount: session.events.length,
    }
  }

  async list() {
    return [...this.sessions.values()].map(session => this.snapshot(session))
  }

  async open(id, access) {
    const session = this.sessions.get(id)
    if (session === undefined) {
      const error = new Error(`session "${id}" does not exist`)
      error.name = 'SessionPersistenceNotFoundError'
      throw error
    }
    return new MemoryHandle(session, access)
  }

  async create(header, options) {
    if (this.sessions.has(header.id)) {
      const error = new Error(`session "${header.id}" already exists`)
      error.name = 'SessionAlreadyExistsError'
      throw error
    }
    const session = { header, events: [], inheritedEventCount: options?.inheritedEventCount ?? 0 }
    this.sessions.set(header.id, session)
    return new MemoryHandle(session, 'write')
  }
}

class MemoryFolder {
  constructor() {
    this.headers = new Map()
    this.segments = new Map()
    this.archives = []
  }

  async ensure() {}

  async heartbeat() {}

  async loadSession(id) {
    const entry = this.headers.get(id)
    if (entry === undefined) return { id, segments: [], max: 0, error: 'missing header' }
    const segments = (this.segments.get(id) ?? []).slice()
    let cursor = 0
    for (const segment of segments) {
      if (segment.start !== cursor) {
        return { id, header: entry.header, inheritedEventCount: entry.inheritedEventCount, segments, max: cursor, error: 'gap' }
      }
      cursor = segment.end
    }
    return { id, header: entry.header, inheritedEventCount: entry.inheritedEventCount, segments, max: cursor }
  }

  async listSessions() {
    const sessions = new Map()
    for (const id of this.headers.keys()) sessions.set(id, await this.loadSession(id))
    return sessions
  }

  async writeHeader(id, { header, inheritedEventCount }) {
    const existing = this.headers.get(id)
    if (existing !== undefined) {
      assert.equal(existing.header.id, id)
      return existing
    }
    this.headers.set(id, { header, inheritedEventCount })
  }

  async writeSegment(id, events) {
    const current = await this.loadSession(id)
    if (current.error !== undefined) throw new Error(current.error)
    assert.equal(events[0].seq, current.max)
    const segments = this.segments.get(id) ?? []
    segments.push({
      start: events[0].seq,
      end: events[events.length - 1].seq + 1,
      hash: '0'.repeat(16),
      events: events.map(item => structuredClone(item)),
    })
    this.segments.set(id, segments)
  }

  async requireEvents(id, from, to) {
    const segments = this.segments.get(id) ?? []
    const events = []
    for (const segment of segments) {
      for (const item of segment.events) {
        if (item.seq >= from && item.seq < to) events.push(structuredClone(item))
      }
    }
    events.sort((left, right) => left.seq - right.seq)
    if (events.length !== to - from) throw new Error(`missing events ${from}-${to}`)
    return events
  }

  async archiveConflict(id, events, details) {
    this.archives.push({ id, events: events.map(item => structuredClone(item)), details })
    return 'archive.jsonl.gz'
  }
}

function makeEngine(persistence, folder, stateDir) {
  const ctx = {
    sessionPersistence: persistence,
    sessions: { get: () => undefined },
    get: () => undefined,
    logger: { warn: () => {} },
  }
  const config = {
    enabled: true,
    remoteDir: 'memory',
    intervalMs: 60_000,
    flushDebounceMs: 20_000,
    includeSubagents: true,
    workspaceMap: [],
    accountKey: 'test-account',
    deviceName: 'test-device',
    stateDir,
  }
  return new SyncEngine({
    ctx,
    config,
    state: new LocalState(stateDir),
    logger: ctx.logger,
    folderFactory: () => folder,
  })
}

function header(id, cwd = 'D:/work') {
  return { version: 4, id, createdAt: 1_700_000_000_000, cwd, isSeeded: false }
}

test('exports a new local session and imports it on another device', async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'dsh-sync-'))
  try {
    const folder = new MemoryFolder()
    const deviceA = new MemoryPersistence()
    deviceA.seed('session-1', header('session-1'), [event(0, 'hello'), event(1, 'world')])
    const engineA = makeEngine(deviceA, folder, path.join(root, 'a'))
    await engineA.syncNow('test')
    assert.equal(folder.headers.get('session-1').header.id, 'session-1')
    assert.equal((await folder.loadSession('session-1')).max, 2)

    const deviceB = new MemoryPersistence()
    const engineB = makeEngine(deviceB, folder, path.join(root, 'b'))
    await engineB.syncNow('test')
    assert.equal(deviceB.sessions.get('session-1').events.length, 2)
    assert.equal(deviceB.sessions.get('session-1').header.cwd, 'D:/work')
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test('continues a session from the other device in both directions', async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'dsh-sync-'))
  try {
    const folder = new MemoryFolder()
    const deviceA = new MemoryPersistence()
    deviceA.seed('session-1', header('session-1'), [event(0, 'one'), event(1, 'two')])
    const engineA = makeEngine(deviceA, folder, path.join(root, 'a'))
    await engineA.syncNow('test')

    const deviceB = new MemoryPersistence()
    const engineB = makeEngine(deviceB, folder, path.join(root, 'b'))
    await engineB.syncNow('test')
    await engineB.syncNow('stabilize')

    deviceB.sessions.get('session-1').events.push(event(2, 'from B'))
    await engineB.syncNow('test')
    assert.equal((await folder.loadSession('session-1')).max, 3)

    await engineA.syncNow('test')
    assert.equal(deviceA.sessions.get('session-1').events.length, 3)
    assert.equal(deviceA.sessions.get('session-1').events[2].data.text, 'from B')

    deviceA.sessions.get('session-1').events.push(event(3, 'from A'))
    await engineA.syncNow('test')
    await engineB.syncNow('test')
    assert.equal(deviceB.sessions.get('session-1').events.length, 4)
    assert.equal(deviceB.sessions.get('session-1').events[3].data.text, 'from A')
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test('freezes a session when both devices appended different events', async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'dsh-sync-'))
  try {
    const folder = new MemoryFolder()
    const deviceA = new MemoryPersistence()
    deviceA.seed('session-1', header('session-1'), [event(0, 'base')])
    const engineA = makeEngine(deviceA, folder, path.join(root, 'a'))
    await engineA.syncNow('test')

    const deviceB = new MemoryPersistence()
    const engineB = makeEngine(deviceB, folder, path.join(root, 'b'))
    await engineB.syncNow('test')
    await engineB.syncNow('stabilize')

    deviceA.sessions.get('session-1').events.push(event(1, 'branch A'))
    deviceB.sessions.get('session-1').events.push(event(1, 'branch B'))
    await engineA.syncNow('test')
    const result = await engineB.syncNow('test')

    assert.deepEqual(result.conflicts, ['session-1'])
    assert.equal(folder.archives.length, 1)
    assert.equal(folder.archives[0].events[0].data.text, 'branch B')
    assert.equal((await folder.loadSession('session-1')).max, 2)
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test('treats an identical overlap as a stale watermark, not a conflict', async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'dsh-sync-'))
  try {
    const folder = new MemoryFolder()
    const deviceA = new MemoryPersistence()
    deviceA.seed('session-1', header('session-1'), [event(0, 'a'), event(1, 'b')])
    const engineA = makeEngine(deviceA, folder, path.join(root, 'a'))
    await engineA.syncNow('test')

    const deviceB = new MemoryPersistence()
    deviceB.seed('session-1', header('session-1'), [event(0, 'a'), event(1, 'b')])
    const engineB = makeEngine(deviceB, folder, path.join(root, 'b'))
    await engineB.state.load()
    engineB.state.session('session-1').synced = 1
    engineB.state.session('session-1').sizeBytes = Buffer.byteLength(JSON.stringify(deviceB.sessions.get('session-1').events))
    await engineB.state.save()

    const result = await engineB.syncNow('test')
    assert.deepEqual(result.conflicts, [])
    assert.equal(engineB.state.session('session-1').synced, 2)
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test('maps a remote working directory through the configured table', async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'dsh-sync-'))
  try {
    const engine = makeEngine(new MemoryPersistence(), new MemoryFolder(), path.join(root, 'a'))
    engine.config.workspaceMap = [{ remote: 'C:/work/project', local: 'D:/work/project' }]
    assert.equal(engine.mapRemoteCwd('C:/work/project'), 'D:/work/project')
    assert.equal(engine.mapRemoteCwd('C:/other'), undefined)
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test('mirrors content-addressed attachment objects in both directions', async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'dsh-sync-'))
  try {
    const localRoot = path.join(root, 'local-attachments')
    const remoteRoot = path.join(root, 'remote')
    const hash = 'a'.repeat(64)
    const relative = path.join(hash.slice(0, 2), hash)
    const localObject = path.join(localRoot, 'v1', 'objects', relative)
    await mkdir(path.dirname(localObject), { recursive: true })
    await writeFile(localObject, 'image-bytes')

    const folder = new FolderStore({ root: remoteRoot, accountKey: 'test', deviceId: 'device', deviceName: 'device' })
    await folder.ensure()
    const engine = makeEngine(new MemoryPersistence(), folder, path.join(root, 'state'))
    engine.remote = folder
    engine.config.includeAttachments = true
    engine.config.attachmentsDir = localRoot

    const upload = {}
    await engine.syncAttachments(upload)
    assert.equal(upload.attachmentsUploaded, 1)
    const remoteObject = path.join(remoteRoot, 'attachments', 'v1', 'objects', relative)
    assert.equal(await readFile(remoteObject, 'utf8'), 'image-bytes')

    await rm(localObject, { force: true })
    const download = {}
    await engine.syncAttachments(download)
    assert.equal(download.attachmentsDownloaded, 1)
    assert.equal(await readFile(localObject, 'utf8'), 'image-bytes')
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})
