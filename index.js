const speedometer = require('speedometer')
const { Readable } = require('bare-stream')
const DriveAnalyzer = require('drive-analyzer')

module.exports = class Prefetcher extends Readable {
  constructor(drive, { interval = 250 } = {}) {
    super()

    this.drive = drive
    this.mirror = null
    this.finished = false

    this._interval = interval
    this._downloadedBlocks = 0
    this._downloadedBytes = 0
    this._downloadedBlocksEstimate = 0
    this._downloadSpeed = speedometer()

    this._uploadedBlocks = 0
    this._uploadedBytes = 0
    this._uploadSpeed = speedometer()

    this._startedResolve = null
    this._startedPromise = new Promise((resolve) => {
      this._startedResolve = resolve
    })

    this._interval = interval
    this._timer = null

    this.stats = null
    this.monitor = null

    this._ondownloadBound = this._ondownload.bind(this)
    this._onuploadBound = this._onupload.bind(this)
  }

  async *[Symbol.asyncIterator]() {
    await this._startedPromise
    if (!this.mirror) return
    yield* this.mirror[Symbol.asyncIterator]()
    await this.started
  }

  _onupload(index, byteLength) {
    this._uploadedBlocks++
    this._uploadedBytes += byteLength

    if (this.monitor) this.monitor.uploadSpeed(byteLength)
    else this._uploadSpeed(byteLength)
  }

  _ondownload(index, byteLength) {
    this._downloadedBlocks++
    this._downloadedBytes += byteLength

    if (this.mirror) this.mirror.downloadSpeed(byteLength)
    else this._downloadSpeed(byteLength)
  }

  update() {
    const mUploadedBytes = this.monitor ? this.monitor.stats.upload.bytes : 0
    const mUploadedBlocks = this.monitor ? this.monitor.stats.upload.blocks : 0
    const mDownloadedBytes = this.monitor ? this.monitor.stats.download.bytes : 0
    const mDownloadedBlocks = this.monitor ? this.monitor.stats.download.blocks : 0
    const mDownloadedBlocksEst = this.mirror ? this.mirror.downloadedBlocksEstimate : 0
    const mPeers = this.mirror ? this.mirror.peers.length : 0

    const blocks = mDownloadedBlocks + this._downloadedBlocks
    const est = mDownloadedBlocksEst + this._downloadedBlocksEstimate
    const progress = this.finished ? 1 : est === 0 ? 0 : Math.min(0.99, blocks / est)

    this.stats = {
      peers: Math.max(mPeers, this.drive.db.core.peers.length),
      download: {
        bytes: mDownloadedBytes + this._downloadedBytes,
        blocks,
        speed: this.mirror ? this.mirror.downloadSpeed() : this._downloadSpeed(),
        progress
      },
      upload: {
        bytes: mUploadedBytes,
        blocks: mUploadedBlocks,
        speed: this.mirror ? this.mirror.uploadSpeed() : this._uploadSpeed()
      }
    }

    this.emit('update', this.stats)
  }

  start(mirror) {
    if (this.started) return
    this.started = this._start(mirror)
    return this.started
  }

  _start(mirror) {
    const started = this._start(mirror)
    started.catch((err) => {
      this.emit('error', err)
    })
    return started
  }

  async _start(mirror) {
    this.mirror = mirror

    const [blobs, warmup] = await Promise.all([this.drive.getBlobs(), this.drive.db.get('warmup')])

    if (this.mirror) {
      this.monitor = this.mirror.monitor()
      if (!this.monitor.preloaded) {
        await new Promise((resolve) => this.monitor.on('preloaded', resolve))
      }
    }

    const ranges = DriveAnalyzer.decode(warmup.value.meta, warmup.value.data)

    const [m, b] = await Promise.all([
      prepareRanges(ranges.meta, this.drive.db.core),
      blobs ? prepareRanges(ranges.data, blobs.core) : { blocks: [], ranges: [] }
    ])

    this._timer = setInterval(() => this.update(), this._interval)

    if (blobs) {
      blobs.core.on('download', this._ondownloadBound)
      blobs.core.on('upload', this._onuploadBound)
    }
    this.drive.db.core.on('download', this._ondownloadBound)
    this.drive.db.core.on('upload', this._onuploadBound)

    this._downloadedBlocksEstimate = 0

    for (const dl of m.ranges) {
      if (!dl.request.context) continue
      this._downloadedBlocksEstimate += dl.request.context.end - dl.request.context.start
    }

    for (const dl of b.ranges) {
      if (!dl.request.context) continue
      this._downloadedBlocksEstimate += dl.request.context.end - dl.request.context.start
    }

    let mBlocks = null

    if (m.blocks.length) {
      this._downloadedBlocksEstimate += m.blocks.length
      mBlocks = this.drive.db.core.download({ blocks: m.blocks })
    }

    let bBlocks = null

    if (b.blocks.length) {
      this._downloadedBlocksEstimate += b.blocks.length
      bBlocks = blobs.core.download({ blocks: b.blocks })
    }

    if (this.mirror) await this.mirror.done()

    if (mBlocks) await mBlocks.done()
    if (bBlocks) await bBlocks.done()

    for (const r of m.ranges) await r.done()
    for (const r of b.ranges) await r.done()

    if (this.monitor) {
      this.monitor.destroy()
    }

    if (blobs) {
      blobs.core.off('download', this._ondownloadBound)
      blobs.core.off('upload', this._onuploadBound)
    }
    this.drive.db.core.off('download', this._ondownloadBound)
    this.drive.db.core.off('upload', this._onuploadBound)

    clearInterval(this._timer)
    this._timer = null

    this.finished = true
    this.update()
  }
}

async function prepareRanges(warmup, core) {
  const blocks = []
  const ranges = []

  for (const { start, end } of warmup) {
    if (end - start === 1) {
      if (!(await core.has(start))) {
        blocks.push(start)
      }
    } else {
      const range = core.download({ start, end })
      await range.ready()
      ranges.push(range)
    }
  }

  return { blocks, ranges }
}
