import { Actor } from 'apify';
import type { CheerioCrawler } from 'crawlee';
import { sleep, KeyValueStore, BasicCrawler } from 'crawlee';

/**
 * This only persist crawlee.useState. If you use global useState or your own state, you will need to persist that too
 */
const persistUseState = async (crawler: CheerioCrawler) => {
    // Crawlee uses autosaved KV Store values so it is a bit clunky to persist those
    const ourCustomState = await crawler.useState({ pushed: 0 });
    const defaultKVStore = await KeyValueStore.open();
    // @ts-expect-error Accessing private property CRAWLEE_STATE_KEY
    await defaultKVStore.setValue(BasicCrawler.CRAWLEE_STATE_KEY, ourCustomState);
};

const persistState = async (crawler: CheerioCrawler) => {
    // Sleep 10 seconds to
    // 1. Overwrite Crawlee's internal persist state, rather than race condition with it
    // 2. Allow some pending requests to finish, Crawlee doesn't start new ones after event is emitted
    await sleep(10_000);

    // We will await persisting of the components concurrently to minimize the latency
    await Promise.all([
        persistUseState(crawler),
        crawler.stats.persistState(),
        crawler.requestList?.persistState(),
    ]);
};

export const setupMigrateAbortHandlers = async (crawler: CheerioCrawler) => {
    Actor.on('migrating', async () => {
        await persistState(crawler);
        // Reboot usually takes less than 100 ms between calling it and Node.js exiting
        // That's not perfect but chance of mismatch is very low
        const runId = Actor.config.get('actorRunId');
        await Actor.apifyClient.run(runId!).reboot();
    });
    Actor.on('aborting', async () => {
        await persistState(crawler);
        // Shut down immediately after state is persisted
        process.exit(0);
    });
};
