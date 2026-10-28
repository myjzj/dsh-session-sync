/**
 * dsh-session-sync: replicate Harness session histories between computers
 * through a shared folder (OneDrive by default).
 *
 * The plugin is Host-only. It reads and appends through `ctx.sessionPersistence`
 * so the local JSONL lock, format validation, and crash recovery stay in charge;
 * the shared folder only carries immutable logical-event segments.
 */
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { SyncEngine } from './lib/engine.js'
import { LocalState } from './lib/local-state.js'

export const name = 'session-sync'
export const inject = ['sessionPersistence', 'sessions', 'commands', 'timer']

/**
 * Mount the sync engine, the `/sync` command, and the automatic schedule.
 * @param {object} ctx - Host context.
 * @param {object} rawConfig - this entry's Loader config.
 * @returns {void}
 */
export function apply(ctx, rawConfig) {
  const config = normalizeConfig(rawConfig)
  const state = new LocalState(config.stateDir)
  const engine = new SyncEngine({ ctx, config, state, logger: ctx.logger })

  ctx.commands.register({
    name: 'sync',
    description: 'Synchronize session histories with your other devices',
    input: { hint: '[status]' },
    recordInput: false,
    handler: async (invocation) => {
      const argument = invocation.rawInput.trim()
      if (argument === 'status') return { kind: 'success', text: engine.describe() }
      if (argument.length > 0 && argument !== 'now') {
        return { kind: 'error', text: 'Usage: /sync [status]' }
      }
      const result = await engine.syncNow('command')
      const text = engine.formatRun(result)
      return { kind: result.errors.length > 0 ? 'error' : 'success', text }
    },
  })

  if (!config.enabled) {
    ctx.logger.info('session-sync: installed but disabled; /sync still runs one cycle on demand')
    return
  }

  ctx.logger.info(`session-sync: shared folder ${config.remoteDir}`)
  ctx.timeout(() => { void engine.syncNow('startup') }, 5000)
  ctx.interval(() => { void engine.syncNow('interval') }, config.intervalMs)
  const afterFlush = ctx.debounce(() => { void engine.syncNow('flush') }, config.flushDebounceMs)
  ctx.on('session/flush', () => { afterFlush() })
}

/**
 * Validate the Loader config and resolve defaults.
 * @param {unknown} raw - raw config object from cordis.patch.yml.
 * @returns {object} normalized configuration.
 * @throws {Error} when an enabled row has no usable shared folder.
 */
function normalizeConfig(raw) {
  const config = raw !== null && typeof raw === 'object' ? raw : {}
  const enabled = config.enabled !== false
  const interval = Number.isFinite(config.intervalMs) ? Math.trunc(config.intervalMs) : 60_000
  const flushDebounce = Number.isFinite(config.flushDebounceMs) ? Math.trunc(config.flushDebounceMs) : 20_000
  const remoteDir = typeof config.remoteDir === 'string' && config.remoteDir.trim().length > 0
    ? path.resolve(config.remoteDir.trim())
    : detectOneDrive()
  if (enabled && remoteDir === undefined) {
    throw new Error('session-sync: set config.remoteDir to a shared folder, or install OneDrive so the default folder resolves')
  }
  const dshHome = typeof process.env.DSH_HOME === 'string' && process.env.DSH_HOME.length > 0
    ? process.env.DSH_HOME
    : path.join(os.homedir(), '.dsh')
  const workspaceMap = Array.isArray(config.workspaceMap)
    ? config.workspaceMap
      .filter(entry => entry !== null && typeof entry === 'object' && typeof entry.remote === 'string' && typeof entry.local === 'string')
      .map(entry => ({ remote: entry.remote, local: entry.local }))
    : []
  return {
    enabled,
    remoteDir,
    intervalMs: Math.max(15_000, interval),
    flushDebounceMs: Math.max(5_000, flushDebounce),
    includeSubagents: config.includeSubagents === true,
    includeAttachments: config.includeAttachments !== false,
    attachmentsDir: typeof config.attachmentsDir === 'string' && config.attachmentsDir.trim().length > 0
      ? path.resolve(config.attachmentsDir.trim())
      : path.join(dshHome, 'attachments'),
    maxAttachmentFiles: Number.isFinite(config.maxAttachmentFiles) ? Math.max(1, Math.trunc(config.maxAttachmentFiles)) : 500,
    maxAttachmentBytes: Number.isFinite(config.maxAttachmentBytes) ? Math.max(1, Math.trunc(config.maxAttachmentBytes)) : 256 * 1024 * 1024,
    workspaceMap,
    accountKey: typeof config.accountKey === 'string' && config.accountKey.trim().length > 0 ? config.accountKey.trim() : undefined,
    deviceName: typeof config.deviceName === 'string' && config.deviceName.trim().length > 0 ? config.deviceName.trim() : os.hostname(),
    stateDir: typeof config.stateDir === 'string' && config.stateDir.trim().length > 0
      ? path.resolve(config.stateDir)
      : path.join(dshHome, 'storages', 'session-sync'),
  }
}

/**
 * Resolve the default shared folder inside OneDrive.
 * @returns {string | undefined} absolute folder, or undefined without OneDrive.
 */
function detectOneDrive() {
  for (const variable of ['OneDrive', 'OneDriveConsumer', 'OneDriveCommercial']) {
    const value = process.env[variable]
    if (typeof value === 'string' && value.length > 0) return path.join(value, 'dsh-session-sync')
  }
  const fallback = path.join(os.homedir(), 'OneDrive')
  try {
    if (fs.statSync(fallback).isDirectory()) return path.join(fallback, 'dsh-session-sync')
  } catch {
    return undefined
  }
  return undefined
}
