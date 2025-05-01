import crypto from 'node:crypto';
import redisClient from '../config/redis.js'; // Adjusted path

// Define a single channel for all job types
const JOBS_CHANNEL = 'jobsChannel';
// const QUOTE_QUEUE = 'quoteQueue'; // No longer needed
// const DATA_QUEUE = 'dataQueue'; // No longer needed

// Define schemas for request validation and Swagger generation
const executeBatchSchema = {
  // description: 'Submits a batch pricing job for opportunities matching the SOQL WHERE clause.',
  tags: ['Pricing Engine'],
  summary: 'Submit Batch Pricing Job',
  description: "Calculate pricing and generate quotes from Opportunities queried using the SOQL WHERE clause.",
  operationId: 'executeBatch',
  body: {
    type: 'object',
    required: ['soqlWhereClause'],
    properties: {
      soqlWhereClause: { type: 'string', description: 'SOQL WHERE clause to select Opportunities' }
    }
  },
  response: {
    202: { // Use 202 Accepted as the job is queued
      description: 'Job accepted for processing',
      type: 'object',
      properties: {
        jobId: { type: 'string', format: 'uuid' }
      }
    }
  }
};

const dataOperationSchema = {
  // description: 'Submits a sample data operation (create or delete).',
  tags: ['Sample Data'],
  response: {
    202: { // Use 202 Accepted
      description: 'Data operation job accepted',
      type: 'object',
      properties: {
        jobId: { type: 'string', format: 'uuid' }
      }
    }
  }
};

const dataCreateSchema = {
  ...dataOperationSchema,
  summary: 'Submit Sample Data Creation Job',
  description: "Starts a job to create a large amount of Opportunity records.",
  operationId: 'datacreate',
  body: {
    type: 'object',
    required: [],
    properties: {
      count: { type: 'integer', minimum: 1, default: 10, description: 'Number of sample Opportunity records to create (defaults to 10)' }
    }
  }
};

const dataDeleteSchema = {
  ...dataOperationSchema,
  summary: 'Submit Sample Data Deletion Job',
  description: "Starts a job to delete generate Quotes",
  operationId: 'datadelete'
  // No body required for delete operation in this example
};

/**
 * API Routes plugin for handling job submissions.
 * @param {import('fastify').FastifyInstance} fastify
 * @param {object} opts Plugin options
 */
export default async function apiRoutes (fastify, opts) {
  // === Helper Function ===
  // Updated to use PUBLISH instead of LPUSH
  async function publishJob (request, reply, payload) {
    if (!request.salesforce || !request.salesforce.context) {
      return reply.code(401).send({ error: 'Salesforce context not found. Ensure x-client-context header is provided.' });
    }

    // Log the context object before publishing
    request.log.info({ salesforceContext: request.salesforce.context }, 'Salesforce context before publishing job');

    const jobId = crypto.randomUUID();
    const jobPayload = JSON.stringify({
      jobId,
      context: request.salesforce.context,
      ...payload // Include specific payload data (like operation, count, soqlWhereClause)
    });

    try {
      // Use PUBLISH instead of LPUSH
      const receivers = await redisClient.publish(JOBS_CHANNEL, jobPayload);
      request.log.info({ jobId, channel: JOBS_CHANNEL, payload, receivers }, `Job published to Redis channel ${JOBS_CHANNEL}. Receivers: ${receivers}`);
      return reply.code(202).send({ jobId }); // Respond with 202 Accepted and Job ID
    } catch (error) {
      request.log.error({ err: error, jobId, channel: JOBS_CHANNEL }, 'Failed to publish job to Redis channel');
      return reply.code(500).send({ error: 'Failed to publish job.' });
    }
  }

  // === Routes ===
  // Routes now call publishJob without specifying queueName

  fastify.post('/executebatch', { schema: executeBatchSchema }, async (request, reply) => {
    const { soqlWhereClause } = request.body;
    // Payload now needs to implicitly define the job type for the subscriber
    await publishJob(request, reply, { jobType: 'quote', soqlWhereClause });
  });

  fastify.post('/data/create', { schema: dataCreateSchema }, async (request, reply) => {
    const count = request.body?.count ?? dataCreateSchema.body.properties.count.default ?? 10;
    // Add jobType to distinguish
    await publishJob(request, reply, { jobType: 'data', operation: 'create', count });
  });

  fastify.post('/data/delete', { schema: dataDeleteSchema }, async (request, reply) => {
    // Add jobType to distinguish
    await publishJob(request, reply, { jobType: 'data', operation: 'delete' });
  });

  fastify.log.info('API routes registered for Pub/Sub.');
}
