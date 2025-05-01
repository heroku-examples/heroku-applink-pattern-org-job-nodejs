# Testing Strategy and Local Debugging

Follow the README instructions to set up a local testing environment connected to a real Salesforce org and Heroku Redis instance. Then, use the provided `invoke.sh` script to test API endpoints locally and debug the `web` and `worker` processes.

## Completed Tasks
- [x] Updated README with Node.js specific instructions.

## In Progress Tasks (Setup)
- [x] Create Heroku app (`heroku create`) - App: tranquil-gorge-35248
- [x] Provision Heroku Redis add-on (`heroku addons:create heroku-redis:mini --wait`) - Addon: redis-pointy-85355
- [x] Populate local `.env` file with Redis URL (`heroku config --shell > .env`) & `NODE_TLS_REJECT_UNAUTHORIZED=0`
- [x] Install project dependencies (`pnpm install`)

## In Progress Tasks (Local Testing & Debugging)
- [x] Run application locally using `heroku local` (Fixed Redis TLS & EADDRINUSE errors)
- [x] Use `./bin/invoke.sh` to generate sample data (`POST /api/data/create`)
    - [x] Verify job ID is returned.
    - [x] Monitor `heroku local` logs for `web` and `worker` activity (Data creation).
    - [x] Check Salesforce org for newly created Opportunity/OLI records.
    - [x] Debug any errors in `handleDataMessage` (`server/worker.js`).
    - [ ] **Cleanup & Alignment:**
        - [x] Modify `generateSampleOLIs` for a fixed count of 2 OLIs.
        - [x] Fetch `Product2Id` from `PricebookEntry` in `handleDataMessage`.
        - [x] Include `Product2Id` in OLI generation and Bulk API submission.
        - [x] Remove detailed debug logs added previously.
        - [x] Refine remaining logs for conciseness and alignment with Java style.
- [ ] Use `./bin/invoke.sh` to generate quotes (`POST /api/executebatch`)
    - [ ] Verify job ID is returned.
    - [ ] Monitor `heroku local` logs for `web` and `worker` activity (Quote generation).
    - [ ] Check Salesforce org for newly created Quote/QuoteLineItem records.
    - [ ] Debug any errors in `handleQuoteMessage` (`server/worker.js`).
- [ ] Use `./bin/invoke.sh` to delete sample data (`POST /api/data/delete`)
    - [ ] Verify job ID is returned.
    - [ ] Monitor `heroku local` logs for `web` and `worker` activity (Data deletion).
    - [ ] Check Salesforce org to confirm data deletion.
    - [ ] Debug any errors in `handleDataMessage` (`server/worker.js`).

## Future Tasks
- [ ] Implement more robust error handling in API endpoints and worker handlers.
- [ ] Add automated tests (Unit/Integration) if desired.
- [ ] Test deployment to Heroku and invocation from Salesforce (Apex/Flow).

## Implementation Plan

1.  **Setup Heroku Resources:** Execute the necessary `heroku` CLI commands to create the app and Redis instance required for local testing against live services.
2.  **Configure Locally:** Ensure the `.env` file is correctly populated so the local application can connect to the Heroku Redis instance.
3.  **Run Locally:** Start the application using `heroku local` to run both the `web` and `worker` processes simultaneously.
4.  **Invoke & Observe:** Use the `./bin/invoke.sh` script (provided in the original project, assuming it exists) to simulate calls from Salesforce to the locally running `web` process.
5.  **Monitor & Debug:** Watch the console output from `heroku local` for logs from both processes. Use standard Node.js debugging techniques (console logs, potentially a debugger) if issues arise in `server/index.js` or `server/worker.js`.
6.  **Verify in Salesforce:** Log in to the target Salesforce org to confirm that the expected data changes (creation/deletion) have occurred.

## Relevant files

- `README.md` - Contains the setup and invocation instructions being followed.
- `.env` - Stores the connection string for Heroku Redis.
- `Procfile` - Defines the `web` and `worker` processes run by `heroku local`.
- `bin/invoke.sh` - Script used to simulate Salesforce requests to the local server.
- `server/index.js` - The `web` process, receives API calls.
- `server/routes/api.js` - API route handlers.
- `server/worker.js` - The `worker` process, handles background jobs. 