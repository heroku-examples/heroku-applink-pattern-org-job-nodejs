import crypto from 'node:crypto';
import redisClient from '../config/redis.js'; // Adjusted path

const QUOTE_QUEUE = 'quoteQueue';
const DATA_QUEUE = 'dataQueue';

// Define schemas for request validation and Swagger generation
const executeBatchSchema = {
  // description: 'Submits a batch pricing job for opportunities matching the SOQL WHERE clause.',
  tags: ['Pricing Engine'],
  summary: 'Submit Batch Pricing Job',
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
  body: {
    type: 'object',
    required: ['count'],
    properties: {
      count: { type: 'integer', minimum: 1, description: 'Number of sample Opportunity records to create' }
    }
  }
};

const dataDeleteSchema = {
  ...dataOperationSchema,
  summary: 'Submit Sample Data Deletion Job'
  // No body required for delete operation in this example
};

/**
 * API Routes plugin for handling job submissions.
 * @param {import('fastify').FastifyInstance} fastify
 * @param {object} opts Plugin options
 */
export default async function apiRoutes (fastify, opts) {
  // === Helper Function ===
  async function publishJob (request, reply, queueName, payload) {
    if (!request.salesforce || !request.salesforce.context) {
      return reply.code(401).send({ error: 'Salesforce context not found. Ensure x-client-context header is provided.' });
    }

    const jobId = crypto.randomUUID();
    const jobPayload = JSON.stringify({
      jobId,
      context: request.salesforce.context,
      ...payload // Include specific payload data
    });

    try {
      await redisClient.lpush(queueName, jobPayload);
      request.log.info({ jobId, queue: queueName, payload }, 'Job published to Redis queue');
      return reply.code(202).send({ jobId }); // Respond with 202 Accepted and Job ID
    } catch (error) {
      request.log.error({ err: error, jobId, queue: queueName }, 'Failed to publish job to Redis');
      return reply.code(500).send({ error: 'Failed to queue job.' });
    }
  }

  // === Routes ===

  fastify.post('/executebatch', { schema: executeBatchSchema }, async (request, reply) => {
    const { soqlWhereClause } = request.body;
    await publishJob(request, reply, QUOTE_QUEUE, { soqlWhereClause });
  });

  fastify.post('/data/create', { schema: dataCreateSchema }, async (request, reply) => {
    const { count } = request.body;
    await publishJob(request, reply, DATA_QUEUE, { operation: 'create', count });
  });

  fastify.post('/data/delete', { schema: dataDeleteSchema }, async (request, reply) => {
    await publishJob(request, reply, DATA_QUEUE, { operation: 'delete' });
  });

  fastify.log.info('API routes registered.');
}
