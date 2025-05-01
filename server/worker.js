'use strict';

import redisClient from './config/redis.js';
// We don't need AppLinkClient import here anymore, we need ContextImpl
// import AppLinkClient from '@heroku/salesforce-sdk-nodejs';
import { ContextImpl } from '@heroku/salesforce-sdk-nodejs/dist/sdk/context.js';
// import pkg from '@heroku/salesforce-sdk-nodejs';
// const { AppLinkClient } = pkg;
// import Redis from 'ioredis'; // No longer need Redis import here
// import config from './config/index.js'; // No longer need config import here

// Remove dedicated blocking client
// const blockingRedisClient = new Redis(...)

const JOBS_CHANNEL = 'jobsChannel'; // Use the same channel name as the publisher
// const QUOTE_QUEUE = 'quoteQueue'; // Removed
// const DATA_QUEUE = 'dataQueue'; // Removed

// Helper function mirroring the Java example's discount logic
function getDiscountForRegion (region, logger) {
  // Basic discount logic based on region
  switch (region) {
    case 'NAMER':
      logger?.info(`[Worker] Applying NAMER discount for region: ${region}`);
      return 0.1; // 10%
    case 'EMEA':
      logger?.info(`[Worker] Applying EMEA discount for region: ${region}`);
      return 0.15; // 15%
    case 'APAC':
      logger?.info(`[Worker] Applying APAC discount for region: ${region}`);
      return 0.08; // 8%
    default:
      logger?.warn(`[Worker] No specific discount for region: ${region}, applying default.`);
      return 0.05; // 5%
  }
}

// Import the service handlers
import { handleDataMessage } from './services/data.js';
import { handleQuoteMessage } from './services/quote.js';

// --- Pub/Sub Message Handler ---
async function handleJobMessage (channel, message) {
  if (channel !== JOBS_CHANNEL) {
    return;
  }

  console.log(`[Worker] Received message from channel: ${channel}`);
  let jobData;
  try {
    jobData = JSON.parse(message);
  } catch (err) {
    console.error('[Worker] Failed to parse job message:', message, err);
    return; // Cannot proceed
  }

  const { jobId, context, jobType } = jobData;
  const logger = console; // Use console logger for simplicity here

  // Check for context before proceeding
  if (!context || !context.org || !context.org.accessToken || !context.org.domainUrl) {
      logger.error({ jobId, jobType }, '[Worker] Received job missing required context information. Skipping.');
      return;
  }

  // Determine which handler to call based on payload
  try {
    // *** Instantiate ContextImpl here ***
    const sfContext = new ContextImpl(
      context.org.accessToken,
      context.org.apiVersion,
      context.requestId || jobId,
      context.org.namespace,
      context.org.id,
      context.org.domainUrl,
      context.org.user?.id, // Use optional chaining
      context.org.user?.username // Use optional chaining
    );

    // *** Route to imported service handlers ***
    if (jobType === 'quote') {
      logger.info(`[Worker] Routing job ${jobId} to handleQuoteMessage`);
      await handleQuoteMessage(jobData, sfContext, logger);
    } else if (jobType === 'data') {
      logger.info(`[Worker] Routing job ${jobId} to handleDataMessage`);
      await handleDataMessage(jobData, sfContext, logger);
    } else {
      logger.warn(`[Worker] Received job with unknown jobType:`, jobData);
    }
  } catch (handlerError) {
    logger.error({ err: handlerError, jobId, jobType }, `[Worker] Error executing handler for job`);
  }
}

async function startWorker () {
  console.log('[Worker] Starting (Pub/Sub mode)...');

  if (redisClient.status !== 'ready') {
    console.log('[Worker] Redis client not ready, waiting for ready event...');
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Redis connection timeout')), 10000);
      redisClient.once('ready', () => {
        clearTimeout(timeout);
        resolve();
      });
      redisClient.once('error', (err) => {
        clearTimeout(timeout);
        reject(err);
      });
    });
  }
  console.log('[Worker] Redis client connected.');

  redisClient.subscribe(JOBS_CHANNEL, (err, count) => {
    if (err) {
      console.error(`[Worker] Failed to subscribe to ${JOBS_CHANNEL}:`, err);
      process.exit(1);
    }
    console.log(`[Worker] Subscribed successfully to ${JOBS_CHANNEL}. Listener count: ${count}`);
  });

  redisClient.on('message', handleJobMessage);

  console.log(`[Worker] Subscribed to ${JOBS_CHANNEL} and waiting for messages...`);
}

startWorker()
  .catch(err => {
    console.error('[Worker] Critical error during startup:', err);
    process.exit(1);
  });
