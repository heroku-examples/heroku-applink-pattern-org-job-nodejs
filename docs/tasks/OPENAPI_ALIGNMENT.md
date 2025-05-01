# OpenAPI Schema Alignment

Goal: Align the dynamically generated OpenAPI schema (from Fastify/Swagger) with the static `api-docs.yaml` file.

## Completed Tasks
- [x] Run web server only (`pnpm start`).
- [x] Fetch dynamic schema from `/docs/json`.
- [x] Read static `api-docs.yaml` schema.
- [x] Compare schemas and identify differences.
- [x] Define specific code modification tasks based on differences.
- [x] Update global Swagger config (`server/index.js`) to define a default server URL (`http://localhost:5000`).
- [x] Add `description` and `operationId` to all routes in `server/routes/api.js` matching `api-docs.yaml`.
- [x] Define reusable schemas (`BatchExecutionRequest`, `JobResponse`) in global Swagger config (`server/index.js`) under `components.schemas`.
- [x] Update routes in `server/routes/api.js` to reference reusable schemas (`$ref`) instead of inline definitions. (Note: Dynamic schema uses internal `def-N` refs, but structure is equivalent).
- [x] Standardize response codes to `202` (Accepted) for all endpoints in `server/routes/api.js`.
- [x] Change tag for `/api/data/create` and `/api/data/delete` to "Sample Data" in `server/routes/api.js`. (Corrected from "Pricing Engine").
- [x] Add request body definition for `count` parameter to `/api/data/create` route schema in `server/routes/api.js`.
- [x] Update `summary` for `/api/data/delete` route in `server/routes/api.js`.

## In Progress Tasks
- [ ] *None*

## Future Tasks
- [ ] *None*

## Implementation Plan

1.  Start the web server using `pnpm start`.
2.  Access the dynamic schema (likely `http://localhost:3000/docs/json`).
3.  Read the `api-docs.yaml` file.
4.  Perform a diff/comparison to identify discrepancies in paths, operations, schemas, descriptions, etc.
5.  For each significant difference, create a task to update the Fastify route definitions or Swagger configuration in the Node.js code.
6.  Re-run the server, fetch the dynamic schema, and compare again until alignment is satisfactory.

## Relevant files

- `api-docs.yaml` - The target static schema definition.
- `server/index.js` - Contains global Swagger configuration.
- `server/routes/api.js` - Contains route definitions where schemas need to be added/modified.
- `server/config/index.js` - May influence server details if referenced in Swagger config. 