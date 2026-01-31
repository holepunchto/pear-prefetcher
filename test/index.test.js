const test = require('brittle')
const path = require('bare-path')
const Hyperdrive = require('hyperdrive')
const Mirrordrive = require('mirror-drive')
const Localdrive = require('localdrive')
const Corestore = require('corestore')
const getTmpDir = require('test-tmp')
const DriveAnalyzer = require('drive-analyzer')

const Prefetcher = require('../index.js')

test('warmup prefetch', async (t) => {
  const { app } = await testenv(t)
  const prefetcher = new Prefetcher(app)
  await prefetcher.start()

  t.is(prefetcher.stats.peers, 1)
  t.ok(prefetcher.stats.download.bytes > 1)
  t.ok(prefetcher.stats.download.blocks > 1)
  t.is(prefetcher.stats.download.progress, 1)
})

async function testenv(t) {
  const storageA = await getTmpDir(t)
  const storeA = new Corestore(storageA)
  await storeA.ready()

  const storageB = await getTmpDir(t)
  const storeB = new Corestore(storageB)
  await storeB.ready()

  const s1 = storeA.replicate(true)
  const s2 = storeB.replicate(false)
  s1.pipe(s2).pipe(s1)

  const fixture = new Localdrive(path.join(__dirname, 'fixtures', 'app'))
  await fixture.ready()

  const remote = new Hyperdrive(storeA)
  await remote.ready()
  const mirror = new Mirrordrive(fixture, remote)
  await mirror.done()

  const local = new Hyperdrive(storeB, remote.key)
  await local.ready()

  const analyzer = new DriveAnalyzer(remote)
  analyzer.ready()

  const { warmup } = await analyzer.analyze(['app.js'])
  await remote.db.put('warmup', warmup)

  // event flush
  await new Promise((resolve) => setTimeout(resolve, 1000))

  return { app: local }
}
