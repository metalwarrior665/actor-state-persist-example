import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';

import { setupMigrateAbortHandlers } from './handle-migrate-abort.js';

// State mismatch usually happens when persisted components get out of sync. The components usually are:
// 1. Pushing data to dataset
// 2. Marking request as handled (done by Crawlee after requestHandler)
// 3. Updating state object that is persisted to KV Store

// There isn't any completely bulletproof way to prevent state mismatch
// as in extreme cases, Apify API itself might be slow or unresponsive.
// The closest we could get would be to wrap every state update in if statements
// but such code would be hard to read.
// Instead we will implement solution to minimize the latency between state updates

await Actor.init();

const crawler = new CheerioCrawler({
    requestHandler: async ({ request, enqueueLinks }) => {
        log.info(`Processing ${request.loadedUrl}`);
        await enqueueLinks();

        const state = await crawler.useState({ pushed: 0 });

        // In ideal case, state updates should directly follow each other
        // push data -> update object -> mark request as handled
        await Dataset.pushData({ url: request.loadedUrl });
        // If the process exits exactly here...
        state.pushed++;
        // ...or here...
        // ...We will have a mismatched state
    },
});

await setupMigrateAbortHandlers(crawler);

await crawler.run(['https://apify.com']);

await Actor.exit();
