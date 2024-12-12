import { Actor, log } from 'apify';
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
    // 1. Not race condition with Crawlee's internal persist state, instead cleanly overwrite it
    // 2. Allow some pending requests to finish, Crawlee doesn't start new ones after event is emitted
    await sleep(10_000);
    log.info('Waited 10 seconds before persisting state, now storing state');

    // We will await persisting of the components concurrently to minimize the latency
    await Promise.all([
        persistUseState(crawler),
        crawler.stats.persistState(),
        crawler.requestList?.persistState(),
    ]);
};

export const setupMigrateAbortHandlers = async (crawler: CheerioCrawler) => {
    Actor.on('migrating', async () => {
        log.info('[migrating]: Registered event, starting state persist with initial 10s sleep');
        await persistState(crawler);
        log.info('[migrating]: State persist finished, rebooting actor');
        // Reboot usually takes less than 100 ms between calling it and Node.js exiting
        // That's not perfect but chance of mismatch is very low
        const runId = Actor.config.get('actorRunId');
        await Actor.apifyClient.run(runId!).reboot();
        log.info('[migrating]: Reboot called, waiting to be killed');
    });
    Actor.on('aborting', async () => {
        log.info('[aborting]: Registered event, starting state persist with initial 10s sleep');
        await persistState(crawler);
        log.info('[aborting]: State persist finished, shutting down');
        // Shut down immediately after state is persisted
        process.exit(0);
    });
};
