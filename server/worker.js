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

// --- Bulk API Helper Functions ---

// Polls Bulk API job status until completion or failure
async function pollBulkJobStatus (org, jobId, jobType, requestLog) {
  const POLLING_INTERVAL_MS = 5000; // Poll every 5 seconds
  const MAX_POLLS = 24; // Max wait time: 2 minutes (adjust as needed)
  let polls = 0;

  requestLog.info(`[Worker] Polling status for ${jobType} Job ID: ${jobId}`);

  while (polls < MAX_POLLS) {
    try {
      // Assume getInfo method exists and returns job status
      const jobInfo = await org.bulkApi.getInfo(jobId);
      requestLog.debug(`[Worker] Job ${jobId} status: ${jobInfo?.state}`);

      if (!jobInfo) {
        throw new Error('Job info not found.');
      }

      switch (jobInfo.state) {
        case 'JobComplete':
        case 'Completed': // Allow for variations in state names
          requestLog.info(`[Worker] ${jobType} Job ID: ${jobId} completed successfully.`);
          // Optional: Fetch results if needed (e.g., getSuccessfulResults, getFailedResults)
          // const failedResults = await org.bulkApi.getFailedResults(jobId);
          // if (failedResults && failedResults.length > 0) { ... }
          return { success: true, jobInfo };
        case 'Failed':
          requestLog.error(`[Worker] ${jobType} Job ID: ${jobId} failed. State: ${jobInfo.state}, Message: ${jobInfo.errorMessage}`);
          // Optional: Fetch failed results for detailed logging
          return { success: false, jobInfo };
        case 'Aborted':
          requestLog.warn(`[Worker] ${jobType} Job ID: ${jobId} was aborted.`);
          return { success: false, jobInfo };
        case 'UploadComplete': // Still processing
        case 'InProgress': // Still processing
          // Continue polling
          break;
        default:
          requestLog.warn(`[Worker] Unknown job state for ${jobType} Job ID ${jobId}: ${jobInfo.state}`);
          // Continue polling but log warning
          break;
      }
    } catch (pollError) {
      requestLog.error({ err: pollError }, `[Worker] Error polling status for ${jobType} Job ID ${jobId}`);
      // Decide if polling should stop or continue after an error
      // For now, we stop polling on error
      return { success: false, error: pollError };
    }

    polls++;
    await new Promise(resolve => setTimeout(resolve, POLLING_INTERVAL_MS));
  }

  requestLog.warn(`[Worker] Polling timeout for ${jobType} Job ID: ${jobId}. Job may still be running.`);
  return { success: false, error: new Error('Polling timeout') };
}

// Placeholder for actually generating Opportunity data
function generateSampleOpportunities (count, accountId, pricebookId) {
  const opportunities = [];
  for (let i = 0; i < count; i++) {
    opportunities.push({
      // Ensure required fields are included
      Name: `Sample Opp ${Date.now()}-${i}`,
      AccountId: accountId, // Need a valid Account ID
      StageName: 'Prospecting',
      CloseDate: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0], // ~30 days from now
      Pricebook2Id: pricebookId
    });
  }
  return opportunities;
}

// Placeholder for generating OLI data
function generateSampleOLIs (createdOppIds, pricebookEntryIds) {
  const olis = [];
  if (!createdOppIds || createdOppIds.length === 0 || !pricebookEntryIds || pricebookEntryIds.length === 0) {
    return olis;
  }
  createdOppIds.forEach(oppId => {
    // Add 1-3 random line items for each opp
    const lineItemCount = Math.floor(Math.random() * 3) + 1;
    for (let i = 0; i < lineItemCount; i++) {
      olis.push({
        OpportunityId: oppId,
        PricebookEntryId: pricebookEntryIds[Math.floor(Math.random() * pricebookEntryIds.length)], // Pick a random PBE
        Quantity: Math.floor(Math.random() * 10) + 1,
        UnitPrice: Math.floor(Math.random() * 100) + 10 // Random price between 10-110
      });
    }
  });
  return olis;
}

// --- Main Data Handler ---

async function handleDataMessage (message) {
  console.log(`[Worker] Received data job message`);
  let jobData;
  try {
    jobData = JSON.parse(message);
  } catch (err) {
    console.error('[Worker] Failed to parse data job message:', message, err);
    return; // Cannot proceed
  }

  const { jobId, context, operation, count } = jobData;
  console.log(`[Worker] Processing Data Job ID: ${jobId}, Operation: ${operation}`);

  const sf = AppLinkClient.init(context);
  const org = sf.context.org;
  const logger = sf.logger || console; // Use SDK logger if available, else console

  try {
    if (operation === 'create') {
      logger.info(`[Worker] Starting data creation for Job ID: ${jobId}, Count: ${count}`);

      // --- Create Operation --- //

      // 1. Prerequisites (Account, Pricebook, PBEs) - Simplified for example
      //    In a real scenario, you might query or ensure these exist.
      //    Using placeholders - THESE MUST BE VALID IDs IN THE TARGET ORG
      const placeholderAccountId = '001xxxxxxxxxxxxxxx'; // !! REPLACE with a valid Account ID from your org !!
      logger.warn('[Worker] Using placeholder Account ID. Replace with a valid ID from your org.');
      const standardPricebook = await org.dataApi.query("SELECT Id FROM Pricebook2 WHERE IsStandard = true LIMIT 1");
      if (!standardPricebook?.records?.[0]?.Id) {
        throw new Error('Standard Pricebook not found.');
      }
      const standardPricebookId = standardPricebook.records[0].Id;

      const pbes = await org.dataApi.query(`SELECT Id FROM PricebookEntry WHERE Pricebook2Id = '${standardPricebookId}' AND IsActive = true LIMIT 5`);
      if (!pbes?.records || pbes.records.length === 0) {
        throw new Error('No active Pricebook Entries found in Standard Pricebook.');
      }
      const pricebookEntryIds = pbes.records.map(pbe => pbe.Id);

      // 2. Generate and Ingest Opportunities
      const oppsToCreate = generateSampleOpportunities(count, placeholderAccountId, standardPricebookId);
      logger.info(`[Worker] Generated ${oppsToCreate.length} sample opportunities.`);
      // Assume createDataTableBuilder exists and takes object name and data array
      const oppTable = await org.bulkApi.createDataTableBuilder('Opportunity', oppsToCreate);
      // Assume ingest exists and returns a jobInfo object with an id
      const oppIngestJob = await org.bulkApi.ingest(oppTable);
      logger.info(`[Worker] Submitted Opportunity creation job: ${oppIngestJob.id}`);

      // 3. Monitor Opportunity Job
      const oppJobResult = await pollBulkJobStatus(org, oppIngestJob.id, 'Opportunity Create', logger);
      if (!oppJobResult.success) {
        throw new Error(`Opportunity creation job ${oppIngestJob.id} failed or timed out.`);
      }

      // 4. Get Created Opportunity IDs (Requires querying results - simplified)
      //    THIS IS A MAJOR SIMPLIFICATION. Bulk API requires fetching successful results.
      //    Let's query them back based on name for this example, assuming names are unique enough.
      logger.warn('[Worker] Simplified fetching of created Opp IDs. Real implementation needs Bulk API result retrieval.');
      const oppNames = oppsToCreate.map(o => o.Name).map(name => `'${name.replace(/'/g, "\\'")}'`); // Escape names for SOQL
      const createdOpps = await org.dataApi.query(`SELECT Id FROM Opportunity WHERE Name IN (${oppNames.join(',')})`);
      const createdOppIds = createdOpps.records.map(o => o.Id);
      logger.info(`[Worker] Retrieved ${createdOppIds.length} created Opportunity IDs.`);

      if (createdOppIds.length === 0) {
        logger.warn('[Worker] No Opportunity IDs retrieved after creation job. Cannot create OLIs.');
        return;
      }

      // 5. Generate and Ingest OpportunityLineItems
      const olisToCreate = generateSampleOLIs(createdOppIds, pricebookEntryIds);
      if (olisToCreate.length === 0) {
        logger.info('[Worker] No OLIs generated to create.');
        return; // Nothing more to do
      }
      logger.info(`[Worker] Generated ${olisToCreate.length} sample OLIs.`);
      const oliTable = await org.bulkApi.createDataTableBuilder('OpportunityLineItem', olisToCreate);
      const oliIngestJob = await org.bulkApi.ingest(oliTable);
      logger.info(`[Worker] Submitted OLI creation job: ${oliIngestJob.id}`);

      // 6. Monitor OLI Job
      const oliJobResult = await pollBulkJobStatus(org, oliIngestJob.id, 'OLI Create', logger);
      if (!oliJobResult.success) {
        throw new Error(`OLI creation job ${oliIngestJob.id} failed or timed out.`);
      }

      logger.info(`[Worker] Successfully completed data creation for Job ID: ${jobId}`);

    } else if (operation === 'delete') {
      logger.info(`[Worker] Starting data deletion for Job ID: ${jobId}`);

      // --- Delete Operation --- //

      // 1. Query Opportunities to Delete (Example: query by name prefix)
      //    Adjust the query logic as needed to target the correct records.
      const oppsToDeleteQuery = "SELECT Id FROM Opportunity WHERE Name LIKE 'Sample Opp %' LIMIT 1000"; // Limit deletion scope
      logger.info(`[Worker] Querying Opportunities for deletion: ${oppsToDeleteQuery}`);
      const oppsToDeleteResult = await org.dataApi.query(oppsToDeleteQuery);

      if (!oppsToDeleteResult?.records || oppsToDeleteResult.records.length === 0) {
        logger.info(`[Worker] No sample Opportunities found to delete for Job ID: ${jobId}`);
        return;
      }
      logger.info(`[Worker] Found ${oppsToDeleteResult.records.length} Opportunities to delete.`);

      // 2. Prepare IDs for Deletion
      const oppIdsToDelete = oppsToDeleteResult.records.map(opp => ({ Id: opp.Id }));
      const deleteTable = await org.bulkApi.createDataTableBuilder('Opportunity', oppIdsToDelete);

      // 3. Submit Deletion Job
      // Assume ingest can take an operation type like 'hardDelete'
      const deleteIngestJob = await org.bulkApi.ingest(deleteTable, { operation: 'hardDelete' });
      logger.info(`[Worker] Submitted Opportunity deletion job: ${deleteIngestJob.id}`);

      // 4. Monitor Deletion Job
      const deleteJobResult = await pollBulkJobStatus(org, deleteIngestJob.id, 'Opportunity Delete', logger);
      if (!deleteJobResult.success) {
        throw new Error(`Opportunity deletion job ${deleteIngestJob.id} failed or timed out.`);
      }

      logger.info(`[Worker] Successfully completed data deletion for Job ID: ${jobId}`);

    } else {
      logger.warn(`[Worker] Unknown data operation requested: ${operation} for Job ID: ${jobId}`);
    }

  } catch (error) {
    logger.error({ err: error }, `[Worker] Critical error processing Data Job ID ${jobId}`);
    // Consider error handling strategy (retry, DLQ, etc.)
  }
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
