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

async function handleQuoteMessage (jobData) {
  console.log('[Worker] Handling quote job object');

  const { jobId, context, opportunityIds } = jobData;
  const logger = console; // Use console logger

  try {
    // *** Instantiate ContextImpl directly using received context details ***
    const sfContext = new ContextImpl(
      context.org.accessToken,
      context.org.apiVersion,
      context.requestId || jobId, // Use request ID from context if available, fallback to jobId
      context.org.namespace,
      context.org.id,
      context.org.domainUrl,
      context.org.user.id,      // Correct path
      context.org.user.username // Correct path
    );

    // *** Access APIs via sfContext.org ***
    if (!sfContext.org || !sfContext.org.dataApi) {
        logger.error(`[Worker] sfContext.org.dataApi not found after ContextImpl instantiation for Quote Job ID: ${jobId}`);
        return;
    }
    const dataApi = sfContext.org.dataApi;

    console.log(`[Worker] Processing Quote Job ID: ${jobId}`);

    if (!opportunityIds || opportunityIds.length === 0) {
      logger.warn(`[Worker] No opportunity IDs provided for Job ID: ${jobId}`);
      return;
    }

    const oppIdList = opportunityIds.map(id => `'${id}'`).join(',');
    logger.info(`[Worker] Querying ${opportunityIds.length} opportunities and their OLIs for Job ID: ${jobId}`);

    const oppQuery = `
      SELECT Id, Name, AccountId, CloseDate, StageName, Amount, Billing_Region__c,
             (SELECT Id, Product2Id, Quantity, UnitPrice, PricebookEntryId FROM OpportunityLineItems)
      FROM Opportunity
      WHERE Id IN (${oppIdList})
    `;
    const oppResult = await dataApi.query(oppQuery);
    const opportunities = oppResult.records;

    if (!opportunities || opportunities.length === 0) {
      logger.warn(`[Worker] No opportunities found for IDs: ${opportunityIds.join(', ')} in Job ID: ${jobId}`);
      return;
    }
    const firstOppId = opportunities[0]?.fields?.Id || opportunities[0]?.fields?.id;
    if (!firstOppId) {
        logger.error(`[Worker] First Opportunity record missing fields.Id/fields.id field. Query Result: ${JSON.stringify(oppResult)}`);
        throw new Error('First Opportunity record missing fields.Id/fields.id field.');
    }
    logger.info(`[Worker] Found ${opportunities.length} opportunities for Job ID: ${jobId}`);

    const unitOfWork = dataApi.newUnitOfWork();
    const quoteRefs = new Map();

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
    const commitResult = await dataApi.commitUnitOfWork(unitOfWork);
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
// *** Update to generate a fixed number (2) and include Product2Id ***
function generateSampleOLIs (createdOppIds, pricebookEntries) {
  const olis = [];
  const FIXED_OLI_COUNT = 2;
  if (!createdOppIds || createdOppIds.length === 0 || !pricebookEntries || pricebookEntries.length === 0) {
    return olis;
  }

  createdOppIds.forEach(oppId => {
    // Add FIXED_OLI_COUNT line items for each opp
    for (let i = 0; i < FIXED_OLI_COUNT; i++) {
      // Pick a random PBE ensuring we don't exceed available entries
      const entryIndex = Math.floor(Math.random() * pricebookEntries.length);
      const pbe = pricebookEntries[entryIndex];

      // Ensure we got a valid PBE object with Id and Product2Id
      if (pbe && pbe.Id && pbe.Product2Id) {
          olis.push({
            OpportunityId: oppId,
            PricebookEntryId: pbe.Id,
            Product2Id: pbe.Product2Id, // Include Product2Id
            Quantity: Math.floor(Math.random() * 10) + 1,
            UnitPrice: Math.floor(Math.random() * 100) + 10 // Random price between 10-110
          });
      } else {
          // Log a warning if a valid PBE couldn't be found for this iteration
          console.warn(`[Worker][DataGen] Could not find valid PricebookEntry with Product2Id for iteration ${i} on Opp ${oppId}. Skipping OLI.`);
      }
    }
  });
  return olis;
}

// --- Bulk API Helper ---
const BULK_API_POLL_INTERVAL = 5000; // 5 seconds
const BULK_API_TIMEOUT = 300000; // 5 minutes

// Ensure the function accepts the job reference *object*
async function pollBulkJobStatus (jobReference, bulkApi, logger) {
  let jobInfo;
  const startTime = Date.now();

  // Log the ID from the reference object
  const jobId = jobReference.id;
  logger.info(`[Worker][BulkAPI] Starting to poll status for Job ID: ${jobId} (Type: ${jobReference.type})`);

  while (Date.now() - startTime < BULK_API_TIMEOUT) {
    try {
      logger.info(`[Worker][BulkAPI] Polling job with ID: ${jobId}`);
      // *** Pass the full jobReference object to getInfo ***
      jobInfo = await bulkApi.getInfo(jobReference);

      if (!jobInfo) {
          logger.error(`[Worker][BulkAPI] bulkApi.getInfo(${jobId}) returned undefined.`);
          // Consider retrying after a delay or throwing immediately
          // For now, let's throw to make the failure clear
          throw new Error(`bulkApi.getInfo(${jobId}) returned undefined.`);
      }

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
async function handleDataMessage (jobData) {
  console.log(`[Worker] Handling data job object`);

  const { jobId: processJobId, context, operation, count = 10 } = jobData;

  // *** Add logging for received context details ***
  console.log(`[Worker] Received context for Job ID ${processJobId}:`);
  if (context && context.org) {
    console.log(`  -> Access Token: ${context.org.accessToken ? 'Present' : 'MISSING'}`);
    console.log(`  -> Domain URL: ${context.org.domainUrl || 'MISSING'}`);
    console.log(`  -> API Version: ${context.org.apiVersion || 'MISSING'}`);
    // Avoid logging the full token for security, just confirm presence
  } else {
    console.log('  -> Context or context.org is MISSING!');
    return; // Cannot proceed without context
  }

  const logger = console; // Use console logger

  try {
    // *** Instantiate ContextImpl directly using received context details ***
    const sfContext = new ContextImpl(
      context.org.accessToken,
      context.org.apiVersion,
      context.requestId || processJobId, // Use request ID from context if available, fallback to jobId
      context.org.namespace,
      context.org.id,
      context.org.domainUrl,
      context.org.user.id,      // Correct path
      context.org.user.username // Correct path
    );

    // *** Access APIs via sfContext.org ***
    if (!sfContext.org || !sfContext.org.dataApi || !sfContext.org.bulkApi) {
        logger.error(`[Worker] sfContext.org.dataApi or sfContext.org.bulkApi not found after ContextImpl instantiation for Data Job ID: ${processJobId}`);
        return;
    }
    const dataApi = sfContext.org.dataApi;
    const bulkApi = sfContext.org.bulkApi;

    console.log(`[Worker] Processing Data Job ID: ${processJobId}, Operation: ${operation}, Count: ${count}`);

    if (operation === 'create') {
      logger.info(`[Worker] Starting data creation via Bulk API for Job ID: ${processJobId}, Count: ${count}`);

      // 1. Prerequisites (use dataApi)
      logger.info(`[Worker] Fetching prerequisites for Job ID: ${processJobId}`);
      let accounts;
      try {
        logger.info('[Worker] Attempting: dataApi.query("SELECT Id FROM Account LIMIT 1")');
        accounts = await dataApi.query("SELECT Id FROM Account LIMIT 1");
        logger.info(`[Worker] Raw result from Account query: ${JSON.stringify(accounts, null, 2)}`);
      } catch (queryError) {
        logger.error({ err: queryError }, '[Worker] Error during dataApi.query for Account');
        // Rethrow or handle as appropriate - for now, let the main catch handle it
        throw queryError;
      }

      if (!(accounts?.records?.[0]?.fields?.Id || accounts?.records?.[0]?.fields?.id)) {
          const reason = !accounts ? 'Query result is null/undefined'
                       : !accounts.records ? 'Query result missing \'records\' property'
                       : accounts.records.length === 0 ? 'Query returned 0 records'
                       : 'First record missing fields.Id/fields.id property';
          logger.error(`[Worker] No Account found. Reason: ${reason}. Check preceding raw query result log.`);
          throw new Error(`No Account found. Reason: ${reason}`);
      }
      const accountId = accounts.records[0].fields.Id || accounts.records[0].fields.id;
      logger.info(`[Worker] Using Account ID: ${accountId} for Job ID: ${processJobId}`);

      const standardPricebook = await dataApi.query("SELECT Id FROM Pricebook2 WHERE IsStandard = true LIMIT 1");
      if (!(standardPricebook?.records?.[0]?.fields?.Id || standardPricebook?.records?.[0]?.fields?.id)) { throw new Error('Standard Pricebook not found or missing Id.'); }
      const standardPricebookId = standardPricebook.records[0].fields.Id || standardPricebook.records[0].fields.id;

      // *** Update PBE query to fetch Product2Id ***
      const pbes = await dataApi.query(`SELECT Id, Product2Id FROM PricebookEntry WHERE Pricebook2Id = '${standardPricebookId}' AND IsActive = true LIMIT 10`);
      if (!pbes?.records || pbes.records.length === 0) { throw new Error('No active Pricebook Entries found.'); }
      // *** Store the full PBE objects (including Product2Id) ***
      const pricebookEntries = pbes.records.map(pbe => pbe?.fields).filter(pbe => pbe && pbe.Id && pbe.Product2Id);
      if (pricebookEntries.length === 0) { throw new Error('No valid Pricebook Entries with Product2Id found.'); }
      logger.info(`[Worker] Found ${pricebookEntries.length} valid PBEs from Standard Pricebook for Job ID: ${processJobId}`);

      // --- Create Opportunities via Bulk API (use bulkApi) ---
      logger.info(`[Worker][BulkAPI] Preparing Opportunity creation job for Job ID: ${processJobId}`);
      const oppsToCreate = generateSampleOpportunities(count, accountId, standardPricebookId);

      // Manually construct the dataTable object for Opportunities
      const oppColumns = Object.keys(oppsToCreate[0] || {}); // Get columns from first object
      const oppDataTable = oppsToCreate.map(opp => {
          const rowMap = new Map();
          oppColumns.forEach(col => rowMap.set(col, opp[col]));
          return rowMap;
      });
      oppDataTable.columns = oppColumns; // Add the columns property

      // Call ingest, add operation, and capture the full result array
      const oppIngestResult = await bulkApi.ingest({ object: 'Opportunity', operation: 'insert', dataTable: oppDataTable });

      // Check for errors and extract the actual Job Reference *object*
      let oppJobReference;
      if (Array.isArray(oppIngestResult) && oppIngestResult[0]?.error) {
          logger.error({ errorDetails: oppIngestResult[0].error }, `[Worker][BulkAPI] bulkApi.ingest for Opportunities failed.`);
          throw new Error(`bulkApi.ingest for Opportunities failed.`);
      } else if (Array.isArray(oppIngestResult) && oppIngestResult[0]?.id && oppIngestResult[0]?.type) {
          oppJobReference = oppIngestResult[0]; // Assign the object
      } else {
          logger.error({ oppIngestResult }, `[Worker][BulkAPI] bulkApi.ingest for Opportunities returned unexpected structure.`);
          throw new Error('bulkApi.ingest for Opportunities returned unexpected structure.');
      }

      // Log the Job ID from the extracted object
      logger.info(`[Worker][BulkAPI] Submitted Opportunity creation job with ID: ${oppJobReference.id} for main Job ID: ${processJobId}`);

      // Pass the full job reference object and bulkApi to pollBulkJobStatus
      const oppJobInfo = await pollBulkJobStatus(oppJobReference, bulkApi, logger);
      logger.info(`[Worker][BulkAPI] Opp job ${oppJobReference.id} completed. State: ${oppJobInfo.state}, Processed: ${oppJobInfo.numberRecordsProcessed}, Failed: ${oppJobInfo.numberRecordsFailed}`);

      if (oppJobInfo.numberRecordsFailed > 0) {
          try {
              // Pass the job reference object
              const failedRecords = await bulkApi.getFailedResults(oppJobReference);
              logger.warn(`[Worker][BulkAPI] Opportunity creation job ${oppJobReference.id} had ${oppJobInfo.numberRecordsFailed} failures. Details:`, failedRecords);
          } catch(failErr) {
              logger.error({err: failErr}, `[Worker][BulkAPI] Error fetching failed results for job ${oppJobReference.id}`);
          }
      }

      if (oppJobInfo.numberRecordsProcessed === 0 || oppJobInfo.numberRecordsProcessed === oppJobInfo.numberRecordsFailed) {
          logger.error(`[Worker][BulkAPI] No Opportunities successfully created by job ${oppJobReference.id}. Aborting OLI creation for Job ID: ${processJobId}.`);
          return;
      }

      // --- Create OLIs via Bulk API (use bulkApi) ---
      // Need to get IDs of successfully created Opportunities first
      logger.info(`[Worker][BulkAPI] Fetching successful results for Opportunity job ${oppJobReference.id}`);
      let successfulOppIds = [];
       try {
            // Pass the job reference object
            const successfulRecords = await bulkApi.getSuccessfulResults(oppJobReference);
            // *** Use Map.get() to access the ID ***
            successfulOppIds = successfulRecords.map(rec => rec.get('sf__Id')).filter(id => id);
            logger.info(`[Worker][BulkAPI] Extracted ${successfulOppIds.length} successful Opportunity IDs for Job ID: ${processJobId}`);
       } catch(successErr) {
           logger.error({err: successErr}, `[Worker][BulkAPI] Error fetching successful results for Opportunity job ${oppJobReference.id}. Cannot create OLIs.`);
           return; // Cannot proceed without Opp IDs
       }

       if (successfulOppIds.length === 0) {
            logger.warn(`[Worker][BulkAPI] No successful Opportunity IDs retrieved from job ${oppJobReference.id}. Cannot create OLIs for Job ID: ${processJobId}.`);
            return;
       }

      logger.info(`[Worker][BulkAPI] Preparing OLI creation job for ${successfulOppIds.length} Opportunities for Job ID: ${processJobId}`);
      // *** Pass the full pricebookEntries array ***
      const olisToCreate = generateSampleOLIs(successfulOppIds, pricebookEntries);

      if (olisToCreate.length === 0) {
          logger.info(`[Worker][BulkAPI] No OLIs generated. Skipping OLI creation job for Job ID: ${processJobId}`);
      } else {
          // Manually construct the dataTable object for OLIs
          const oliColumns = Object.keys(olisToCreate[0] || {});
          const oliDataTable = olisToCreate.map(oli => {
              const rowMap = new Map();
              oliColumns.forEach(col => rowMap.set(col, oli[col]));
              return rowMap;
          });
          oliDataTable.columns = oliColumns; // Add the columns property

          // Call ingest, add operation, and capture the full result array
          const oliIngestResult = await bulkApi.ingest({ object: 'OpportunityLineItem', operation: 'insert', dataTable: oliDataTable });

          // Check for errors and extract the actual Job Reference *object*
          let oliJobReference;
          if (Array.isArray(oliIngestResult) && oliIngestResult[0]?.error) {
              logger.error({ errorDetails: oliIngestResult[0].error }, `[Worker][BulkAPI] bulkApi.ingest for OLIs failed.`);
              // Decide if this should throw or just warn
              logger.warn(`[Worker][BulkAPI] OLI creation job submission failed.`);
          } else if (Array.isArray(oliIngestResult) && oliIngestResult[0]?.id && oliIngestResult[0]?.type) {
              oliJobReference = oliIngestResult[0]; // Assign the object
          } else {
              logger.error({ oliIngestResult }, `[Worker][BulkAPI] bulkApi.ingest for OLIs returned unexpected structure.`);
              // Decide if this should throw or just warn
              logger.warn(`[Worker][BulkAPI] OLI creation job submission returned unexpected structure.`);
          }

          // Only proceed if we got a valid job reference
          if (oliJobReference) {
              // Log the Job ID from the extracted object
              logger.info(`[Worker][BulkAPI] Submitted OLI creation job with ID: ${oliJobReference.id} for main Job ID: ${processJobId}`);

              // Pass the full job reference object and bulkApi to pollBulkJobStatus
              const oliJobInfo = await pollBulkJobStatus(oliJobReference, bulkApi, logger);
              logger.info(`[Worker][BulkAPI] OLI job ${oliJobReference.id} completed. State: ${oliJobInfo.state}, Processed: ${oliJobInfo.numberRecordsProcessed}, Failed: ${oliJobInfo.numberRecordsFailed}`);
              if (oliJobInfo.numberRecordsFailed > 0) {
                  try {
                     // Pass the job reference object
                     const failedRecords = await bulkApi.getFailedResults(oliJobReference);
                     logger.warn(`[Worker][BulkAPI] OLI creation job ${oliJobReference.id} had ${oliJobInfo.numberRecordsFailed} failures. Details:`, failedRecords);
                  } catch(failErr) {
                     logger.error({err: failErr}, `[Worker][BulkAPI] Error fetching failed results for OLI job ${oliJobReference.id}`);
                  }
              }
          } else {
             logger.warn(`[Worker][BulkAPI] Skipping OLI job polling because submission failed or returned invalid reference.`);
          }
      }

      logger.info(`[Worker][BulkAPI] Completed data creation process for Job ID: ${processJobId}`);

    } else if (operation === 'delete') {
      logger.info(`[Worker] Starting data deletion via Bulk API for Job ID: ${processJobId}`);

      // --- Delete Operation via Bulk API ---
      const MAX_DELETE_QUERY = 5000; // Limit query size
      const oppsToDeleteQuery = `SELECT Id FROM Opportunity WHERE Name LIKE 'Sample Opp %' LIMIT ${MAX_DELETE_QUERY}`;
      logger.info(`[Worker][BulkAPI] Querying up to ${MAX_DELETE_QUERY} Opportunities for deletion: ${oppsToDeleteQuery} for Job ID: ${processJobId}`);
      const oppsToDeleteResult = await dataApi.query(oppsToDeleteQuery);

      if (!oppsToDeleteResult?.records || oppsToDeleteResult.records.length === 0) {
        logger.info(`[Worker][BulkAPI] No sample Opportunities found to delete for Job ID: ${processJobId}`);
        return;
      }
      const oppIdsToDelete = oppsToDeleteResult.records.map(opp => ({ Id: opp?.fields?.Id || opp?.fields?.id })).filter(item => item.Id);
      logger.info(`[Worker][BulkAPI] Found ${oppIdsToDelete.length} Opportunities to delete for Job ID: ${processJobId}`);

      logger.info(`[Worker][BulkAPI] Preparing Opportunity deletion job for Job ID: ${processJobId}`);
      // const deleteDataTable = org.bulkApi.createDataTableBuilder('Opportunity', oppIdsToDelete); // Incorrect

       // Manually construct the dataTable object for Deletion
       const deleteColumns = ['Id'];
       const deleteDataTable = oppIdsToDelete.map(opp => {
           const rowMap = new Map();
           deleteColumns.forEach(col => rowMap.set(col, opp[col])); // oppIdsToDelete is array of {Id: '...'} 
           return rowMap;
       });
       deleteDataTable.columns = deleteColumns; // Add the columns property

      // Call ingest, ensure operation is correct, and capture the full result array
      const deleteIngestResult = await bulkApi.ingest({ object: 'Opportunity', operation: 'hardDelete', dataTable: deleteDataTable });

      // Check for errors and extract the actual Job Reference *object*
      let deleteJobReference;
      if (Array.isArray(deleteIngestResult) && deleteIngestResult[0]?.error) {
          logger.error({ errorDetails: deleteIngestResult[0].error }, `[Worker][BulkAPI] bulkApi.ingest for Deletion failed.`);
          throw new Error(`bulkApi.ingest for Deletion failed.`);
      } else if (Array.isArray(deleteIngestResult) && deleteIngestResult[0]?.id && deleteIngestResult[0]?.type) {
          deleteJobReference = deleteIngestResult[0]; // Assign the object
      } else {
          logger.error({ deleteIngestResult }, `[Worker][BulkAPI] bulkApi.ingest for Deletion returned unexpected structure.`);
          throw new Error('bulkApi.ingest for Deletion returned unexpected structure.');
      }

      // Log the Job ID from the extracted object
      logger.info(`[Worker][BulkAPI] Submitted Deletion job with ID: ${deleteJobReference.id} for main Job ID: ${processJobId}`);

      // Pass the full job reference object and bulkApi to pollBulkJobStatus
      const deleteJobInfo = await pollBulkJobStatus(deleteJobReference, bulkApi, logger);
      logger.info(`[Worker][BulkAPI] Deletion job ${deleteJobReference.id} completed. State: ${deleteJobInfo.state}, Processed: ${deleteJobInfo.numberRecordsProcessed}, Failed: ${deleteJobInfo.numberRecordsFailed}`);
       if (deleteJobInfo.numberRecordsFailed > 0) {
           try {
              // Pass the job reference object
              const failedRecords = await bulkApi.getFailedResults(deleteJobReference);
              logger.warn(`[Worker][BulkAPI] Deletion job ${deleteJobReference.id} had ${deleteJobInfo.numberRecordsFailed} failures. Details:`, failedRecords);
           } catch(failErr) {
              logger.error({err: failErr}, `[Worker][BulkAPI] Error fetching failed results for deletion job ${deleteJobReference.id}`);
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

// --- Pub/Sub Message Handler ---
async function handleJobMessage (channel, message) {
  if (channel !== JOBS_CHANNEL) {
    // Ignore messages from other channels if any
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

  // Determine which handler to call based on payload
  // Use the 'jobType' field we added in the publisher
  try {
    if (jobData.jobType === 'quote') {
      console.log(`[Worker] Routing job ${jobData.jobId} to handleQuoteMessage`);
      await handleQuoteMessage(jobData); // Pass the parsed jobData object
    } else if (jobData.jobType === 'data') {
      console.log(`[Worker] Routing job ${jobData.jobId} to handleDataMessage`);
      await handleDataMessage(jobData); // Pass the parsed jobData object
    } else {
      console.warn(`[Worker] Received job with unknown jobType:`, jobData);
    }
  } catch (handlerError) {
    console.error({ err: handlerError, jobId: jobData?.jobId, jobType: jobData?.jobType }, `[Worker] Error executing handler for job`);
  }
}

async function startWorker () {
  console.log('[Worker] Starting (Pub/Sub mode)...');

  // Ensure the MAIN Redis client is connected before subscribing
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

  // Remove check for blocking client
  // console.log('[Worker] Blocking Redis client connected.');

  // Subscribe to the jobs channel
  redisClient.subscribe(JOBS_CHANNEL, (err, count) => {
    if (err) {
      console.error(`[Worker] Failed to subscribe to ${JOBS_CHANNEL}:`, err);
      // Handle subscription error (e.g., exit or retry)
      process.exit(1);
    }
    console.log(`[Worker] Subscribed successfully to ${JOBS_CHANNEL}. Listener count: ${count}`);
  });

  // Set up the message listener
  redisClient.on('message', handleJobMessage);

  console.log(`[Worker] Subscribed to ${JOBS_CHANNEL} and waiting for messages...`);
}

startWorker()
  .catch(err => {
    console.error('[Worker] Critical error during startup:', err);
    process.exit(1);
  });
