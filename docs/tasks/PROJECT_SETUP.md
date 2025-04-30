# Node.js Org Job Application Implementation (JavaScript, Single Worker)

Replicate the functionality and process structure of the `heroku-integration-pattern-org-job-java` project using Node.js (Fastify, AppLink SDK, Redis Pub/Sub). The Node.js version will have one `web` process and one `worker` process, mirroring the Java `Procfile`. **Project will use JavaScript.**

## Completed Tasks
- [x] Initial project setup with basic Node.js structure.
- [x] Copied `api-docs.yaml` from the Java project.
- [x] Copied and adapted `README.md` from the Java project (initial copy).
- [x] Analyzed Java project structure and dependencies.
- [x] Analyzed API layer (`PricingEngineService.java`).
- [x] Analyzed Quote Worker logic (`PricingEngineWorkerService.java`).
- [x] Analyzed Data Worker logic (`SampleDataWorkerService.java`).
- [x] Clarified Apex class is not used.
- [x] Confirmed AppLink SDK supports Bulk API v2 (`org.bulkApi`).
- [x] Decision: Use JavaScript.
- [x] Correction: Align with Java `Procfile` - use a single `worker` process.
- [x] Created memory file (`.cursor/rules/learned-memories.mdc`).
- [x] Created task list file (`docs/tasks/PROJECT_SETUP.md`).
- [x] Set up Node.js project foundation (Fastify, Redis, basic structure, dependencies).

## In Progress Tasks

## Future Tasks
- [ ] **Set up Fastify server (`server/index.js` - Web Process):**
    - [x] **Install Swagger dependencies (`@fastify/swagger`, `@fastify/swagger-ui`).**
    - [x] **Install YAML parser (`js-yaml`).**
    - [x] **Require necessary modules (Fastify, dotenv, path, fs, swagger, swagger-ui, js-yaml).**
    - [x] **Initialize Fastify instance.**
    - [x] **Register `@fastify/swagger` for dynamic generation with basic OpenAPI info.**
    - [x] **Register `@fastify/swagger-ui` at `/docs` route prefix.**
    - [ ] **Register Salesforce SDK middleware.**
    - [ ] **Register API routes.**
    - [x] **Implement basic server start logic with error handling.**
    - [x] **Add basic health check endpoint (`/health`).**
- [x] Implement basic server structure (routes, config, lib):
    - [x] Create placeholder files (`config/index.js`, `config/redis.js`, `middleware/salesforce.js`, `routes/api.js`).
    - [x] Add basic central configuration (`server/config/index.js`).
    - [x] Update `server/index.js` to use central config.
- [x] Set up Redis connection using `ioredis` (`server/config/redis.js`).
- [ ] Set up a single Worker process (`server/worker.js`) that:
    - [ ] **Listens to `quoteQueue` on Redis.**
    - [ ] **Listens to `dataQueue` on Redis.**
- [ ] Set up Salesforce SDK middleware in Fastify (`server/middleware/salesforce.js`) to parse `x-client-context`.
- [ ] Implement API endpoints (`server/routes/api.js`) as job dispatchers:
    - [ ] **`POST /api/executebatch`: Parse request, generate Job ID, publish job to `quoteQueue` (Payload: { jobId, context, soqlWhereClause }).**
    - [ ] **`POST /api/data/create`: Parse request, generate Job ID, publish job to `dataQueue` (Payload: { jobId, context, operation: 'create', count }).**
    - [ ] **`POST /api/data/delete`: Parse request, generate Job ID, publish job to `dataQueue` (Payload: { jobId, context, operation: 'delete' }).**
- [ ] Implement Worker Logic within `server/worker.js` (or delegate to helper modules):
    - [ ] **Handler for `quoteQueue` messages:**
        - [ ] **Initialize AppLink SDK with `context` from job payload.**
        - [ ] **Query Opportunities/OLIs using `org.dataApi.query` with `soqlWhereClause`.**
        - [ ] **Create `UnitOfWork`.**
        - [ ] **Register `Quote` creations.**
        - [ ] **Register `QuoteLineItem` creations (applying discount logic - make dynamic?).**
        - [ ] **`commitUnitOfWork`.**
        - [ ] **Implement job status tracking/logging (optional).**
    - [ ] **Handler for `dataQueue` messages:**
        - [ ] **Initialize AppLink SDK with `context`.**
        - [ ] **If operation is 'create':**
            - [ ] **Query Standard Pricebook ID (`org.dataApi.query`).**
            - [ ] **Query active Pricebook Entries (`org.dataApi.query`).**
            - [ ] **Prepare data for Opportunities using `org.bulkApi.createDataTableBuilder`.**
            - [ ] **Submit Opportunity creation job using `org.bulkApi.ingest`.**
            - [ ] **Monitor job status using `org.bulkApi.getInfo`.**
            - [ ] **Query created Opportunity IDs.**
            - [ ] **Prepare data for OpportunityLineItems using `org.bulkApi.createDataTableBuilder`.**
            - [ ] **Submit OLI creation job using `org.bulkApi.ingest`.**
            - [ ] **Monitor job status.**
        - [ ] **If operation is 'delete':**
            - [ ] **Query sample Opportunity IDs (`org.dataApi.query`).**
            - [ ] **Prepare ID data using `org.bulkApi.createDataTableBuilder`.**
            - [ ] **Submit Opportunity deletion job using `org.bulkApi.ingest` with `operation: 'hardDelete'`.**
            - [ ] **Monitor job status.**
        - [ ] **Implement job status tracking/logging (optional, possibly using `org.bulkApi.getFailedResults`).**
- [ ] Configure environment variables (`.env`, `.env.example`).
- [ ] Create `Procfile` for Heroku deployment (`web`, `worker`).
- [ ] Set up run scripts (`package.json`).
- [ ] Configure linting and formatting (`eslint`, `prettier`).
- [ ] Review and update `README.md` to reflect Node.js usage.
- [x] Add basic health check endpoint (`/health`).
- [ ] (Optional) Add unit/integration tests (`tests/`).

## Implementation Plan (Single Worker)

1.  **Foundation:** Set up Fastify, Redis (`ioredis`), AppLink SDK middleware, basic structure using JavaScript.
2.  **API Implementation:** Create Fastify routes (`/api/...`) in `server/index.js` to publish jobs to Redis.
3.  **Worker Implementation:** Create a single `server/worker.js` process that subscribes to *both* `quoteQueue` and `dataQueue`. Implement message handlers within this worker to:
    *   Process quote jobs using AppLink SDK `DataApi`/`UnitOfWork`.
    *   Process data jobs using AppLink SDK `org.bulkApi`.
4.  **Configuration & Deployment:** Finalize `.env`, `Procfile` (with `web` and `worker`), run scripts.
5.  **Quality:** Ensure standards compliance.

## Relevant Files (Single Worker)

- api-docs.yaml
- README.md
- .cursor/rules/*
- server/index.js (Web process)
- server/worker.js (Worker process - listens to both queues)
- server/config/redis.js
- server/middleware/salesforce.js
- server/routes/api.js
- server/lib/ (Optional JS helper modules for worker logic)
- Procfile (`web`, `worker`)
- .env.example
- package.json 