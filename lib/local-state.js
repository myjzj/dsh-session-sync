/**
 * Per-machine sync bookkeeping. The remote store stays append-only and
 * conflict-free; watermarks, conflicts, and the device identity live here.
 */
import path from 'node:path'
import { readJson, writeFileAtomic } from './util.js'

export class LocalState {
  /**
   * @param {string} directory - local state directory.
   */
  constructor(directory) {
    this.directory = directory
    this.file = path.join(directory, 'state.json')
    this.statusFile = path.join(directory, 'status.json')
    /** @type {{ schema?: number, deviceId?: string, accountKey?: string, sessions?: Record<string, { synced?: number, sizeBytes?: number, conflict?: object }> }} */
    this.data = { schema: 1, sessions: {} }
  }

  /**
   * Load the durable state, replacing the in-memory copy when valid.
   * @returns {Promise<void>} resolution after the read.
   */
  async load() {
    const stored = await readJson(this.file)
    if (stored !== null && typeof stored === 'object' && stored.schema === 1) {
      this.data = {
        schema: 1,
        ...stored,
        sessions: stored.sessions !== null && typeof stored.sessions === 'object' ? stored.sessions : {},
      }
    }
  }

  /**
   * Read one session's bookkeeping, creating the entry when absent.
   * @param {string} id - session id.
   * @returns {{ synced?: number, sizeBytes?: number, conflict?: object }} mutable entry.
   */
  session(id) {
    const sessions = (this.data.sessions ??= {})
    sessions[id] ??= { synced: 0 }
    return sessions[id]
  }

  /**
   * Persist the current state.
   * @returns {Promise<void>} resolution after the atomic write.
   */
  async save() {
    this.data.schema = 1
    await writeFileAtomic(this.file, `${JSON.stringify(this.data, null, 2)}\n`)
  }

  /**
   * Persist a human-readable last-run report next to the state.
   * @param {object} report - report fields.
   * @returns {Promise<void>} resolution after the write.
   */
  async writeStatus(report) {
    await writeFileAtomic(this.statusFile, `${JSON.stringify(report, null, 2)}\n`)
  }
}
