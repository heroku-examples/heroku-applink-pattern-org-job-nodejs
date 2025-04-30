'use strict';

import redisClient from './config/redis.js';
import pkg from '@heroku/salesforce-sdk-nodejs';
const { AppLinkClient } = pkg;

const QUOTE_QUEUE = 'quoteQueue';
const DATA_QUEUE = 'dataQueue';

// Helper function mirroring the Java example's discount logic
function getDiscountForRegion (region) {
  // Simple hardcoded discount logic - Mirroring Java example which uses "US"
  switch (region) {
    case "US": return 0.10; // 10% discount
    case "EU": return 0.15;
    case "APAC": return 0.05;
    default: return 0.0;
  }
}

async function handleQuoteMessage (message) {
  console.log(`[Worker] Received quote job message`);
  let jobData;
  try {
    jobData = JSON.parse(message);
  } catch (err) {
    console.error('[Worker] Failed to parse quote job message:', message, err);
    return; // Cannot proceed without valid job data
  }

  const { jobId, context, soqlWhereClause } = jobData;
  console.log(`[Worker] Processing Quote Job ID: ${jobId}`);

  // Create a temporary SDK instance using the provided context for this job
  // This ensures operations are performed with the correct user/org context
  const sf = AppLinkClient.init(context);
  const org = sf.context.org;

  try {
    // 1. Query Opportunities based on the WHERE clause
    const oppQuery = `SELECT Id, Name, AccountId FROM Opportunity WHERE ${soqlWhereClause}`;
    console.log(`[Worker] Querying Opportunities: ${oppQuery}`);
    const oppResult = await org.dataApi.query(oppQuery);
    console.log(`[Worker] Found ${oppResult.records.length} opportunities for Job ID: ${jobId}`);

    if (oppResult.records.length === 0) {
      console.log(`[Worker] No opportunities found matching criteria for Job ID: ${jobId}. Nothing to process.`);
      return;
    }

    // Process each opportunity
    for (const opp of oppResult.records) {
      console.log(`[Worker] Processing Opportunity ID: ${opp.Id}`);
      try {
        // 2. Query related OpportunityLineItems for this Opportunity
        const oliQuery = `SELECT Id, Product2Id, Quantity, UnitPrice, PricebookEntryId FROM OpportunityLineItem WHERE OpportunityId = '${opp.Id}'`;
        const oliResult = await org.dataApi.query(oliQuery);
        console.log(`[Worker] Found ${oliResult.records.length} line items for Opportunity ID: ${opp.Id}`);

        if (oliResult.records.length === 0) {
          console.log(`[Worker] No line items found for Opportunity ID: ${opp.Id}. Skipping quote generation.`);
          continue; // Move to the next opportunity
        }

        // 3. Create UnitOfWork to generate Quote and QuoteLineItems atomically
        const uow = org.dataApi.newUnitOfWork();

        // 4. Register Quote creation
        const quoteRef = uow.registerCreate({
          type: 'Quote',
          fields: {
            Name: `${opp.Name} - Quote - ${jobId}`,
            OpportunityId: opp.Id
            // Pricebook2Id is not explicitly set, mirroring Java logic
          }
        });

        // Get discount rate (hardcoded to US like Java example)
        const discountRate = getDiscountForRegion("US");

        // 5. Register QuoteLineItem creation for each OLI
        for (const oli of oliResult.records) {
          const quantity = parseFloat(oli.Quantity);
          const unitPrice = parseFloat(oli.UnitPrice);
          // Apply discount to total price, then calculate discounted unit price
          const discountedTotalPrice = (quantity * unitPrice) * (1 - discountRate);
          const discountedUnitPrice = quantity > 0 ? discountedTotalPrice / quantity : 0;

          uow.registerCreate({
            type: 'QuoteLineItem',
            fields: {
              QuoteId: quoteRef, // Reference the quote created in this UoW
              Product2Id: oli.Product2Id,
              Quantity: quantity,
              UnitPrice: discountedUnitPrice, // Use calculated discounted unit price
              PricebookEntryId: oli.PricebookEntryId
            }
          });
        }

        // 6. Commit the UnitOfWork
        console.log(`[Worker] Committing UnitOfWork for Opportunity ID: ${opp.Id}`);
        const commitResult = await org.dataApi.commitUnitOfWork(uow);
        console.log(`[Worker] UnitOfWork commit result for Opportunity ID: ${opp.Id}`, commitResult);

        // Basic logging of success/failure
        const quoteCommitResult = commitResult.get(quoteRef);
        if (quoteCommitResult?.success) {
          console.log(`[Worker] Successfully created Quote ${quoteCommitResult.id} for Opportunity ID: ${opp.Id}`);
        } else {
          console.error(`[Worker] Failed to create Quote for Opportunity ID: ${opp.Id}`, quoteCommitResult?.errors);
          // Log errors for individual line items if needed
        }

      } catch (oppError) {
        console.error(`[Worker] Error processing Opportunity ID ${opp.Id} for Job ID ${jobId}:`, oppError);
        // Continue to the next opportunity if one fails
      }
    }

    console.log(`[Worker] Finished processing Quote Job ID: ${jobId}`);

  } catch (error) {
    console.error(`[Worker] Critical error processing Quote Job ID ${jobId}:`, error);
    // Decide if the job should be retried, DLQ'd, etc.
  }
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
