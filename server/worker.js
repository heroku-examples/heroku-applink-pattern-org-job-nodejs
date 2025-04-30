'use strict';

import redisClient from './config/redis.js';

const QUOTE_QUEUE = 'quoteQueue';
const DATA_QUEUE = 'dataQueue';

async function handleQuoteMessage (message) {
  console.log(`[Worker] Received quote message: ${message}`);
  // Placeholder for actual quote processing logic
}

async function handleDataMessage (message) {
  console.log(`[Worker] Received data message: ${message}`);
  // Placeholder for actual data processing logic
}

async function listenToQueue (queueName, handler) {
  console.log(`[Worker] Listening to queue: ${queueName}`);
  // Use a blocking pop (BLPOP) to wait for messages indefinitely
  // Adjust timeout (0 means wait forever) and error handling as needed
  while (true) {
    try {
      const result = await redisClient.blpop(queueName, 0);
      if (result && result.length === 2) {
        const message = result[1]; // blpop returns [queueName, message]
        await handler(message);
      }
    } catch (error) {
      console.error(`[Worker] Error listening to ${queueName}:`, error);
      // Implement backoff strategy before retrying
      await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5 seconds before retrying
    }
  }
}

async function startWorker () {
  console.log('[Worker] Starting worker process...');

  // Wait for Redis client to connect and be ready
  await new Promise((resolve, reject) => {
    redisClient.on('ready', () => {
      console.log('[Worker] Redis client connected.');
      resolve();
    });
    redisClient.on('error', (err) => {
      console.error('[Worker] Redis connection error:', err);
      reject(err);
    });
  });

  // Start listeners for both queues concurrently
  Promise.all([
    listenToQueue(QUOTE_QUEUE, handleQuoteMessage),
    listenToQueue(DATA_QUEUE, handleDataMessage)
  ]).catch(error => {
    console.error('[Worker] Critical error in listeners:', error);
    process.exit(1); // Exit if a listener fails critically
  });
}

startWorker();
