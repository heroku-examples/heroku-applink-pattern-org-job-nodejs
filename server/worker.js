'use strict';

import redisClient from './config/redis.js';
import pkg from '@heroku/salesforce-sdk-nodejs';
const { AppLinkClient } = pkg;

const QUOTE_QUEUE = 'quoteQueue';
const DATA_QUEUE = 'dataQueue';

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

async function handleQuoteMessage (message) {
  console.log('[Worker] Received quote job message');
  let jobData;
  try {
    jobData = JSON.parse(message);
  } catch (err) {
    console.error('[Worker] Failed to parse quote job message:', message, err);
    return; // Cannot proceed
  }

  const { jobId, context, opportunityIds } = jobData;
  console.log(`[Worker] Processing Quote Job ID: ${jobId}`);

  const sf = AppLinkClient.init(context);
  const org = sf.context.org;
  const logger = sf.logger || console; // Use SDK logger if available, else console

  if (!opportunityIds || opportunityIds.length === 0) {
    logger.warn(`[Worker] No opportunity IDs provided for Job ID: ${jobId}`);
    return;
  }

  try {
    const oppIdList = opportunityIds.map(id => `'${id}'`).join(',');
    logger.info(`[Worker] Querying ${opportunityIds.length} opportunities and their OLIs for Job ID: ${jobId}`);

    // Query Opportunities and related OLIs
    const oppQuery = `
      SELECT Id, Name, AccountId, CloseDate, StageName, Amount, Billing_Region__c,
             (SELECT Id, Product2Id, Quantity, UnitPrice, PricebookEntryId FROM OpportunityLineItems)
      FROM Opportunity
      WHERE Id IN (${oppIdList})
    `;
    const oppResult = await org.dataApi.query(oppQuery);
    const opportunities = oppResult.records;

    if (!opportunities || opportunities.length === 0) {
      logger.warn(`[Worker] No opportunities found for IDs: ${opportunityIds.join(', ')} in Job ID: ${jobId}`);
      return;
    }
    logger.info(`[Worker] Found ${opportunities.length} opportunities for Job ID: ${jobId}`);

    const unitOfWork = org.dataApi.newUnitOfWork();
    const quoteRefs = new Map(); // Map OpportunityId to its Quote reference

    opportunities.forEach(opp => {
      if (!opp.OpportunityLineItems?.records || opp.OpportunityLineItems.records.length === 0) {
        logger.warn(`[Worker] Opportunity ${opp.Id} has no line items. Skipping quote creation for Job ID: ${jobId}`);
        return;
      }

      try {
        // 1. Create Quote
        const quoteName = `Quote for ${opp.Name} - ${new Date().toISOString().split('T')[0]}`;
        const expirationDate = new Date(opp.CloseDate);
        expirationDate.setDate(expirationDate.getDate() + 30); // Quote expires 30 days after CloseDate

        // Calculate discount based on custom field Billing_Region__c
        const discount = getDiscountForRegion(opp.Billing_Region__c, logger);

        const quoteRef = unitOfWork.registerCreate({
          type: 'Quote',
          fields: {
            Name: quoteName.substring(0, 80), // Ensure name is within limit
            OpportunityId: opp.Id,
            Pricebook2Id: 'STANDARD_PRICEBOOK_ID', // Needs a valid standard pricebook ID or lookup logic
            ExpirationDate: expirationDate.toISOString().split('T')[0],
            Status: 'Draft',
            Discount: discount * 100 // Store discount as percentage
            // Add other required fields based on org config
          }
        });
        quoteRefs.set(opp.Id, quoteRef);

        // 2. Create QuoteLineItems from OpportunityLineItems
        opp.OpportunityLineItems.records.forEach(oli => {
          unitOfWork.registerCreate({
            type: 'QuoteLineItem',
            fields: {
              QuoteId: quoteRef.toApiString(), // Reference the quote created above
              PricebookEntryId: oli.PricebookEntryId, // Must be valid PBE in the Quote's Pricebook
              Quantity: oli.Quantity,
              UnitPrice: oli.UnitPrice
              // Salesforce calculates LineItemNumber, TotalPrice automatically
              // Add Discount if applicable/needed based on org setup
            }
          });
        });
        logger.info(`[Worker] Registered Quote and ${opp.OpportunityLineItems.records.length} Line Items for Opp ${opp.Id} in Job ID: ${jobId}`);
      } catch (err) {
        logger.error({ err: err, opportunityId: opp.Id }, `[Worker] Error processing Opportunity ${opp.Id} for Job ID: ${jobId}`);
        // Continue processing other opportunities
      }
    });

    if (quoteRefs.size === 0) {
      logger.warn(`[Worker] No quotes were registered for creation for Job ID: ${jobId}.`);
      return;
    }

    logger.info(`[Worker] Committing Unit of Work with ${quoteRefs.size} Quotes and related Line Items for Job ID: ${jobId}`);
    const commitResult = await org.dataApi.commitUnitOfWork(unitOfWork);
    logger.info(`[Worker] Unit of Work commit attempted for Job ID: ${jobId}`);

    // Process results
    let successCount = 0;
    let failureCount = 0;
    commitResult.forEach((result, ref) => {
      // Only log results for the main Quote records for brevity
      if (ref.type === 'Quote') {
        const oppId = [...quoteRefs.entries()].find(([key, value]) => value === ref)?.[0];
        if (result.success) {
          successCount++;
          logger.info(`[Worker] Successfully created Quote ${result.id} for Opportunity ${oppId} in Job ID: ${jobId}`);
        } else {
          failureCount++;
          logger.error({ errors: result.errors, opportunityId: oppId }, `[Worker] Failed to create Quote for Opportunity ${oppId} in Job ID: ${jobId}`);
        }
      }
    });
    logger.info(`[Worker] Quote Creation Results for Job ID ${jobId}: ${successCount} succeeded, ${failureCount} failed.`);

  } catch (error) {
    logger.error({ err: error }, `[Worker] Critical error processing Quote Job ID ${jobId}`);
    // Consider error handling strategy (retry, DLQ, etc.)
  }
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

// --- Bulk API Helper ---
const BULK_API_POLL_INTERVAL = 5000; // 5 seconds
const BULK_API_TIMEOUT = 300000; // 5 minutes

// Polls the status of a Bulk API job until completion or timeout
async function pollBulkJobStatus (jobId, org, logger) {
  const startTime = Date.now();
  let jobInfo;

  logger.info(`[Worker][BulkAPI] Starting to poll status for Job ID: ${jobId}`);

  while (Date.now() - startTime < BULK_API_TIMEOUT) {
    try {
      jobInfo = await org.bulkApi.getInfo(jobId);
      logger.debug(`[Worker][BulkAPI] Job ${jobId} status: ${jobInfo.state}`);

      if (jobInfo.state === 'JobComplete') {
        logger.info(`[Worker][BulkAPI] Job ${jobId} completed successfully.`);
        return jobInfo; // Success
      } else if (jobInfo.state === 'Failed' || jobInfo.state === 'Aborted') {
        logger.error(`[Worker][BulkAPI] Job ${jobId} failed or was aborted. State: ${jobInfo.state}, Message: ${jobInfo.errorMessage}`);
        throw new Error(`Bulk API Job ${jobId} failed or aborted: ${jobInfo.state}`);
      }
      // Continue polling if state is UploadComplete or InProgress
    } catch (err) {
      logger.error({ err: err }, `[Worker][BulkAPI] Error polling job ${jobId}`);
      throw err; // Rethrow error after logging
    }

    await new Promise(resolve => setTimeout(resolve, BULK_API_POLL_INTERVAL));
  }

  // Timeout reached
  logger.error(`[Worker][BulkAPI] Timeout polling job ${jobId}. Last state: ${jobInfo?.state}`);
  throw new Error(`Timeout polling Bulk API Job ${jobId}`);
}

// --- Main Data Handler (Rewritten for Bulk API) ---
async function handleDataMessage (message) {
  console.log(`[Worker] Received data job message`);
  let jobData;
  try {
    jobData = JSON.parse(message);
  } catch (err) {
    console.error('[Worker] Failed to parse data job message:', message, err);
    return; // Cannot proceed
  }

  const { jobId: processJobId, context, operation, count = 10 } = jobData; // Rename jobId to avoid clash
  console.log(`[Worker] Processing Data Job ID: ${processJobId}, Operation: ${operation}, Count: ${count}`);

  const sf = AppLinkClient.init(context);
  const org = sf.context.org;
  const logger = sf.logger || console;

  try {
    if (operation === 'create') {
      logger.info(`[Worker] Starting data creation via Bulk API for Job ID: ${processJobId}, Count: ${count}`);

      // 1. Prerequisites (same as before)
      logger.info(`[Worker] Fetching prerequisites for Job ID: ${processJobId}`);
      const accounts = await org.dataApi.query("SELECT Id FROM Account LIMIT 1");
      if (!accounts?.records?.[0]?.Id) throw new Error('No Account found.');
      const accountId = accounts.records[0].Id;
      logger.info(`[Worker] Using Account ID: ${accountId} for Job ID: ${processJobId}`);

      const standardPricebook = await org.dataApi.query("SELECT Id FROM Pricebook2 WHERE IsStandard = true LIMIT 1");
      if (!standardPricebook?.records?.[0]?.Id) throw new Error('Standard Pricebook not found.');
      const standardPricebookId = standardPricebook.records[0].Id;

      const pbes = await org.dataApi.query(`SELECT Id FROM PricebookEntry WHERE Pricebook2Id = '${standardPricebookId}' AND IsActive = true LIMIT 10`);
      if (!pbes?.records || pbes.records.length === 0) throw new Error('No active Pricebook Entries found.');
      const pricebookEntryIds = pbes.records.map(pbe => pbe.Id);
      logger.info(`[Worker] Found ${pricebookEntryIds.length} PBEs from Standard Pricebook for Job ID: ${processJobId}`);


      // --- Create Opportunities via Bulk API ---
      logger.info(`[Worker][BulkAPI] Preparing Opportunity creation job for Job ID: ${processJobId}`);
      const oppsToCreate = generateSampleOpportunities(count, accountId, standardPricebookId);
      const oppDataTable = org.bulkApi.createDataTableBuilder('Opportunity', oppsToCreate);
      const oppIngestJobId = await org.bulkApi.ingest(oppDataTable);
      logger.info(`[Worker][BulkAPI] Submitted Opportunity creation job ${oppIngestJobId} for Job ID: ${processJobId}`);

      // Poll Opportunity job
      const oppJobInfo = await pollBulkJobStatus(oppIngestJobId, org, logger);
      logger.info(`[Worker][BulkAPI] Opportunity creation job ${oppIngestJobId} completed. State: ${oppJobInfo.state}, Records Processed: ${oppJobInfo.numberRecordsProcessed}, Failed: ${oppJobInfo.numberRecordsFailed}`);

      if (oppJobInfo.numberRecordsFailed > 0) {
          // Optionally fetch failed records details
          try {
              const failedRecords = await org.bulkApi.getFailedResults(oppIngestJobId);
              logger.warn(`[Worker][BulkAPI] Opportunity creation job ${oppIngestJobId} had ${oppJobInfo.numberRecordsFailed} failures. Details:`, failedRecords);
          } catch(failErr) {
              logger.error({err: failErr}, `[Worker][BulkAPI] Error fetching failed results for job ${oppIngestJobId}`);
          }
      }

      if (oppJobInfo.numberRecordsProcessed === 0 || oppJobInfo.numberRecordsProcessed === oppJobInfo.numberRecordsFailed) {
          logger.error(`[Worker][BulkAPI] No Opportunities successfully created by job ${oppIngestJobId}. Aborting OLI creation for Job ID: ${processJobId}.`);
          return;
      }

      // --- Create OLIs via Bulk API ---
      // Need to get IDs of successfully created Opportunities first
      logger.info(`[Worker][BulkAPI] Fetching successful results for Opportunity job ${oppIngestJobId}`);
      let successfulOppIds = [];
       try {
            const successfulRecords = await org.bulkApi.getSuccessfulResults(oppIngestJobId);
            // Assuming successfulRecords is an array of objects like { sf__Id: '...', ... }
            successfulOppIds = successfulRecords.map(rec => rec.sf__Id).filter(id => id); // Extract IDs
            logger.info(`[Worker][BulkAPI] Extracted ${successfulOppIds.length} successful Opportunity IDs for Job ID: ${processJobId}`);
       } catch(successErr) {
           logger.error({err: successErr}, `[Worker][BulkAPI] Error fetching successful results for Opportunity job ${oppIngestJobId}. Cannot create OLIs.`);
           return; // Cannot proceed without Opp IDs
       }

       if (successfulOppIds.length === 0) {
            logger.warn(`[Worker][BulkAPI] No successful Opportunity IDs retrieved from job ${oppIngestJobId}. Cannot create OLIs for Job ID: ${processJobId}.`);
            return;
       }

      logger.info(`[Worker][BulkAPI] Preparing OLI creation job for ${successfulOppIds.length} Opportunities for Job ID: ${processJobId}`);
      const olisToCreate = generateSampleOLIs(successfulOppIds, pricebookEntryIds);

      if (olisToCreate.length === 0) {
          logger.info(`[Worker][BulkAPI] No OLIs generated. Skipping OLI creation job for Job ID: ${processJobId}`);
      } else {
          const oliDataTable = org.bulkApi.createDataTableBuilder('OpportunityLineItem', olisToCreate);
          const oliIngestJobId = await org.bulkApi.ingest(oliDataTable);
          logger.info(`[Worker][BulkAPI] Submitted OLI creation job ${oliIngestJobId} for Job ID: ${processJobId}`);

          // Poll OLI job
          const oliJobInfo = await pollBulkJobStatus(oliIngestJobId, org, logger);
           logger.info(`[Worker][BulkAPI] OLI creation job ${oliIngestJobId} completed. State: ${oliJobInfo.state}, Records Processed: ${oliJobInfo.numberRecordsProcessed}, Failed: ${oliJobInfo.numberRecordsFailed}`);
          if (oliJobInfo.numberRecordsFailed > 0) {
              try {
                 const failedRecords = await org.bulkApi.getFailedResults(oliIngestJobId);
                 logger.warn(`[Worker][BulkAPI] OLI creation job ${oliIngestJobId} had ${oliJobInfo.numberRecordsFailed} failures. Details:`, failedRecords);
              } catch(failErr) {
                 logger.error({err: failErr}, `[Worker][BulkAPI] Error fetching failed results for OLI job ${oliIngestJobId}`);
              }
          }
      }

      logger.info(`[Worker][BulkAPI] Completed data creation process for Job ID: ${processJobId}`);


    } else if (operation === 'delete') {
      logger.info(`[Worker] Starting data deletion via Bulk API for Job ID: ${processJobId}`);

      // --- Delete Operation via Bulk API ---
      const MAX_DELETE_QUERY = 5000; // Limit query size
      const oppsToDeleteQuery = `SELECT Id FROM Opportunity WHERE Name LIKE 'Sample Opp %' LIMIT ${MAX_DELETE_QUERY}`;
      logger.info(`[Worker][BulkAPI] Querying up to ${MAX_DELETE_QUERY} Opportunities for deletion: ${oppsToDeleteQuery} for Job ID: ${processJobId}`);
      const oppsToDeleteResult = await org.dataApi.query(oppsToDeleteQuery);

      if (!oppsToDeleteResult?.records || oppsToDeleteResult.records.length === 0) {
        logger.info(`[Worker][BulkAPI] No sample Opportunities found to delete for Job ID: ${processJobId}`);
        return;
      }
      const oppIdsToDelete = oppsToDeleteResult.records.map(opp => ({ Id: opp.Id })); // Format for Bulk API {Id: '...'}
      logger.info(`[Worker][BulkAPI] Found ${oppIdsToDelete.length} Opportunities to delete for Job ID: ${processJobId}`);

      logger.info(`[Worker][BulkAPI] Preparing Opportunity deletion job for Job ID: ${processJobId}`);
      const deleteDataTable = org.bulkApi.createDataTableBuilder('Opportunity', oppIdsToDelete);
      // Assuming ingest takes an operation type, if not, the builder might handle it
      const deleteIngestJobId = await org.bulkApi.ingest(deleteDataTable, { operation: 'hardDelete' }); // Specify hardDelete
      logger.info(`[Worker][BulkAPI] Submitted Opportunity deletion job ${deleteIngestJobId} for Job ID: ${processJobId}`);

      // Poll Deletion job
      const deleteJobInfo = await pollBulkJobStatus(deleteIngestJobId, org, logger);
      logger.info(`[Worker][BulkAPI] Deletion job ${deleteIngestJobId} completed. State: ${deleteJobInfo.state}, Records Processed: ${deleteJobInfo.numberRecordsProcessed}, Failed: ${deleteJobInfo.numberRecordsFailed}`);
       if (deleteJobInfo.numberRecordsFailed > 0) {
           try {
              const failedRecords = await org.bulkApi.getFailedResults(deleteIngestJobId);
              logger.warn(`[Worker][BulkAPI] Deletion job ${deleteIngestJobId} had ${deleteJobInfo.numberRecordsFailed} failures. Details:`, failedRecords);
           } catch(failErr) {
              logger.error({err: failErr}, `[Worker][BulkAPI] Error fetching failed results for deletion job ${deleteIngestJobId}`);
           }
       }

      logger.info(`[Worker][BulkAPI] Completed data deletion process for Job ID: ${processJobId}`);

    } else {
      logger.warn(`[Worker] Unknown data operation requested: ${operation} for Job ID: ${processJobId}`);
    }

  } catch (error) {
    logger.error({ err: error }, `[Worker] Critical error processing Data Job ID ${processJobId}`);
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
