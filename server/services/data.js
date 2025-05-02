'use strict';

// --- Bulk API Helper Constants ---
const BULK_API_POLL_INTERVAL = 5000; // 5 seconds

// --- Data Generation Helpers ---

function generateSampleOpportunities (count, accountId, pricebookId) {
  const opportunities = [];
  for (let i = 0; i < count; i++) {
    opportunities.push({
      // Ensure required fields are included
      Name: `Sample Opportunity ${i}`,
      AccountId: accountId, // Need a valid Account ID
      StageName: 'Prospecting',
      CloseDate: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0], // ~30 days from now
      Pricebook2Id: pricebookId
    });
  }
  return opportunities;
}

function generateSampleOLIs (createdOppIds, pricebookEntries) {
  const olis = [];
  const FIXED_OLI_COUNT = 2;
  if (!createdOppIds || createdOppIds.length === 0 || !pricebookEntries || pricebookEntries.length === 0) {
    return olis;
  }

  createdOppIds.forEach(oppId => {
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
          // Use console.warn directly as logger might not be available here
          console.warn(`[DataGen] Could not find valid PricebookEntry with Product2Id for iteration ${i} on Opp ${oppId}. Skipping OLI.`);
      }
    }
  });
  return olis;
}

// --- Bulk API Polling Helper ---

async function pollBulkJobStatus (jobReference, bulkApi, logger) {
  let jobInfo;

  const jobId = jobReference.id;
  logger.info(`Polling Bulk API v2 job status for Job ID: ${jobId}`);

  while (true) {
    try {
      jobInfo = await bulkApi.getInfo(jobReference);
      logger.debug(`Bulk API v2 Job ${jobId} status: ${jobInfo.state}`);
      if (jobInfo.state === 'JobComplete') {
        logger.info(`Bulk API v2 Job ${jobId} processing complete.`);
        return jobInfo; // Success
      } else if (jobInfo.state === 'Failed' || jobInfo.state === 'Aborted') {
        logger.error(`Bulk API v2 Job ${jobId} failed or was aborted. State: ${jobInfo.state}, Message: ${jobInfo.errorMessage}`);
        throw new Error(`Bulk API Job ${jobId} failed or aborted: ${jobInfo.state}`);
      }
    } catch (err) {
      logger.error({ err: err }, `Error polling Bulk API v2 job ${jobId}`);
      throw err; // Rethrow error after logging
    }

    await new Promise(resolve => setTimeout(resolve, BULK_API_POLL_INTERVAL));
  }

  // Timeout reached
  logger.error(`Timeout polling Bulk API v2 job ${jobId}. Last state: ${jobInfo?.state}`);
  throw new Error(`Timeout polling Bulk API Job ${jobId}`);
}

/**
 * Helper function to fetch all records for a SOQL query, handling pagination.
 * @param {string} soql - The SOQL query string.
 * @param {object} sfContext - The initialized Salesforce context (ContextImpl instance).
 * @param {object} logger - A logger instance.
 * @returns {Promise<Array>} - A promise that resolves with an array of all records.
 */
async function queryAll (soql, sfContext, logger) {
  let allRecords = [];
  try {
    let result = await sfContext.org.dataApi.query(soql);
    allRecords = allRecords.concat(result.records);
    while (!result.done && result.nextRecordsUrl) {
      result = await sfContext.org.dataApi.queryMore(result); // Use result object directly
      allRecords = allRecords.concat(result.records);
    }
  } catch (error) {
    logger.error({ err: error, soql }, 'Error during queryAll execution');
    throw error; // Re-throw the error to be caught by the caller
  }
  return allRecords;
}

// --- Main Data Handler ---

/**
 * Handles data creation or deletion jobs.
 * @param {object} jobData - The job data object from Redis.
 * @param {object} sfContext - The initialized Salesforce context (ContextImpl instance).
 * @param {object} logger - A logger instance.
 */
async function handleDataMessage (jobData, sfContext, logger) {
  const { jobId: processJobId, operation, count = 10 } = jobData;
  // Note: context is no longer destructured here, sfContext is passed in
  logger.info(`Worker received job with ID: ${processJobId} for data operation: ${operation}`);

  try {
    // *** Access APIs via sfContext.org ***
    if (!sfContext || !sfContext.org || !sfContext.org.dataApi || !sfContext.org.bulkApi) {
        logger.error(`Invalid sfContext or missing APIs for Data Job ID: ${processJobId}`);
        return;
    }
    const dataApi = sfContext.org.dataApi;
    const bulkApi = sfContext.org.bulkApi;

    if (operation === 'create') {
      logger.info(`Starting data creation via Bulk API v2 for Job ID: ${processJobId}, Count: ${count}`);

      // 1. Prerequisites (use dataApi)
      let accounts;
      try {
        accounts = await queryAll("SELECT Id FROM Account LIMIT 1", sfContext, logger);
      } catch (queryError) {
        logger.error({ err: queryError }, 'Error during queryAll for Account');
        throw queryError;
      }

      if (!accounts || accounts.length === 0) {
          const reason = !accounts ? 'Query result is null/undefined or empty'
                       : 'Query returned 0 records';
          logger.error(`No Account found. Reason: ${reason}.`);
          throw new Error(`No Account found. Reason: ${reason}`);
      }
      // Access fields correctly from the result array
      const accountId = accounts[0].fields.Id || accounts[0].fields.id;

      // Use query for Pricebook2
      const standardPricebookRecords = await queryAll("SELECT Id FROM Pricebook2 WHERE IsStandard = true LIMIT 1", sfContext, logger);
      if (!standardPricebookRecords || standardPricebookRecords.length === 0) { throw new Error('Standard Pricebook not found.'); }
      const standardPricebookId = standardPricebookRecords[0].fields.Id || standardPricebookRecords[0].fields.id;

      // Use quer for PricebookEntry
      const pbeQuery = `SELECT Id, Product2Id FROM PricebookEntry WHERE Pricebook2Id = '${standardPricebookId}' AND IsActive = true`;
      const pbes = await queryAll(pbeQuery, sfContext, logger);
      if (!pbes || pbes.length === 0) { throw new Error('No active Pricebook Entries found.'); }
      const pricebookEntries = pbes.map(pbe => pbe?.fields).filter(pbe => pbe && pbe.Id && pbe.Product2Id);
      if (pricebookEntries.length === 0) { throw new Error('No valid Pricebook Entries with Product2Id found.'); }

      // --- Create Opportunities via Bulk API (use bulkApi) ---
      logger.info(`Preparing Bulk API v2 Opportunity creation job for Job ID: ${processJobId}`);
      const oppsToCreate = generateSampleOpportunities(count, accountId, standardPricebookId);
      const oppColumns = Object.keys(oppsToCreate[0] || {});
      const oppDataTable = oppsToCreate.map(opp => {
          const rowMap = new Map();
          oppColumns.forEach(col => rowMap.set(col, opp[col]));
          return rowMap;
      });
      oppDataTable.columns = oppColumns;
      const oppIngestResult = await bulkApi.ingest({ object: 'Opportunity', operation: 'insert', dataTable: oppDataTable });
      let oppJobReference;
      if (Array.isArray(oppIngestResult) && oppIngestResult[0]?.error) {
          logger.error({ errorDetails: oppIngestResult[0].error }, `Bulk API v2 ingest for Opportunities failed.`);
          throw new Error(`bulkApi.ingest for Opportunities failed.`);
      } else if (Array.isArray(oppIngestResult) && oppIngestResult[0]?.id && oppIngestResult[0]?.type) {
          oppJobReference = oppIngestResult[0];
      } else {
          logger.error({ oppIngestResult }, `Bulk API v2 ingest for Opportunities returned unexpected structure.`);
          throw new Error('bulkApi.ingest for Opportunities returned unexpected structure.');
      }
      logger.info(`Submitted Bulk API v2 Opportunity creation job with ID: ${oppJobReference.id} for main Job ID: ${processJobId}`);
      const oppJobInfo = await pollBulkJobStatus(oppJobReference, bulkApi, logger);
      logger.info(`Opportunity creation job ${oppJobReference.id} completed. State: ${oppJobInfo.state}, Processed: ${oppJobInfo.numberRecordsProcessed}, Failed: ${oppJobInfo.numberRecordsFailed}`);
      if (oppJobInfo.numberRecordsFailed > 0) {
          try {
              const failedRecords = await bulkApi.getFailedResults(oppJobReference);
              logger.warn(`Opportunity creation job ${oppJobReference.id} had ${oppJobInfo.numberRecordsFailed} failures. Details:`, failedRecords);
          } catch(failErr) {
              logger.error({err: failErr}, `Error fetching failed results for job ${oppJobReference.id}`);
          }
      }
      if (oppJobInfo.numberRecordsProcessed === 0 || oppJobInfo.numberRecordsProcessed === oppJobInfo.numberRecordsFailed) {
          logger.error(`No Opportunities successfully created by job ${oppJobReference.id}. Aborting OLI creation for Job ID: ${processJobId}.`);
          return;
      }

      // --- Create OLIs via Bulk API (use bulkApi) ---
      let successfulOppIds = [];
       try {
            const successfulRecords = await bulkApi.getSuccessfulResults(oppJobReference);
            successfulOppIds = successfulRecords.map(rec => rec.get('sf__Id')).filter(id => id);
            logger.info(`Extracted ${successfulOppIds.length} successful Opportunity IDs for Job ID: ${processJobId}`);
       } catch(successErr) {
           logger.error({err: successErr}, `Error fetching successful results for Opportunity job ${oppJobReference.id}. Cannot create OLIs.`);
           return;
       }
       if (successfulOppIds.length === 0) {
            logger.warn(`No successful Opportunity IDs retrieved from job ${oppJobReference.id}. Cannot create OLIs for Job ID: ${processJobId}.`);
            return;
       }

      logger.info(`Preparing Bulk API v2 OLI creation job for ${successfulOppIds.length} Opportunities for Job ID: ${processJobId}`);
      const olisToCreate = generateSampleOLIs(successfulOppIds, pricebookEntries);
      if (olisToCreate.length === 0) {
          logger.info(`No OLIs generated. Skipping OLI creation job for Job ID: ${processJobId}`);
      } else {
          const oliColumns = Object.keys(olisToCreate[0] || {});
          const oliDataTable = olisToCreate.map(oli => {
              const rowMap = new Map();
              oliColumns.forEach(col => rowMap.set(col, oli[col]));
              return rowMap;
          });
          oliDataTable.columns = oliColumns;
          const oliIngestResult = await bulkApi.ingest({ object: 'OpportunityLineItem', operation: 'insert', dataTable: oliDataTable });
          let oliJobReference;
          if (Array.isArray(oliIngestResult) && oliIngestResult[0]?.error) {
              logger.error({ errorDetails: oliIngestResult[0].error }, `Bulk API v2 ingest for OLIs failed.`);
              logger.warn(`OLI creation job submission failed.`);
          } else if (Array.isArray(oliIngestResult) && oliIngestResult[0]?.id && oliIngestResult[0]?.type) {
              oliJobReference = oliIngestResult[0];
          } else {
              logger.error({ oliIngestResult }, `Bulk API v2 ingest for OLIs returned unexpected structure.`);
              logger.warn(`OLI creation job submission returned unexpected structure.`);
          }
          if (oliJobReference) {
              logger.info(`Submitted Bulk API v2 OLI creation job with ID: ${oliJobReference.id} for main Job ID: ${processJobId}`);
              const oliJobInfo = await pollBulkJobStatus(oliJobReference, bulkApi, logger);
              logger.info(`OLI creation job ${oliJobReference.id} completed. State: ${oliJobInfo.state}, Processed: ${oliJobInfo.numberRecordsProcessed}, Failed: ${oliJobInfo.numberRecordsFailed}`);
              if (oliJobInfo.numberRecordsFailed > 0) {
                  try {
                     const failedRecords = await bulkApi.getFailedResults(oliJobReference);
                     logger.warn(`OLI creation job ${oliJobReference.id} had ${oliJobInfo.numberRecordsFailed} failures. Details:`, failedRecords);
                  } catch(failErr) {
                     logger.error({err: failErr}, `Error fetching failed results for OLI job ${oliJobReference.id}`);
                  }
              }
          } else {
             logger.warn(`Skipping OLI job polling because submission failed or returned invalid reference.`);
          }
      }

    } else if (operation === 'delete') {
      logger.info(`Starting data deletion via Bulk API v2 for Job ID: ${processJobId}`);

      // Use queryAll to find all Opportunities matching the sample pattern
      const oppQuery = `SELECT Id FROM Opportunity WHERE Name LIKE 'Sample Opp%'`;
      const opportunitiesToDelete = await queryAll(oppQuery, sfContext, logger);
      if (!opportunitiesToDelete || opportunitiesToDelete.length === 0) {
        logger.info(`No sample Opportunities found to delete for Job ID: ${processJobId}.`);
        return;
      }
      const oppIdsToDelete = opportunitiesToDelete.map(opp => opp.fields.Id || opp.fields.id).filter(id => id);
      logger.info(`Found ${oppIdsToDelete.length} Opportunities to delete for Job ID: ${processJobId}`);

      // --- Delete Opportunities via Bulk API (use bulkApi) ---
      logger.info(`Preparing Bulk API v2 Opportunity deletion job for Job ID: ${processJobId}`);
      const oppDeleteColumns = ['Id'];
      const oppDeleteDataTable = oppIdsToDelete.map(id => {
          const rowMap = new Map();
          rowMap.set('Id', id);
          return rowMap;
      });
      oppDeleteDataTable.columns = oppDeleteColumns;
      const oppDeleteIngestResult = await bulkApi.ingest({ object: 'Opportunity', operation: 'hardDelete', dataTable: oppDeleteDataTable });
      let oppDeleteJobReference;
       if (Array.isArray(oppDeleteIngestResult) && oppDeleteIngestResult[0]?.error) {
           logger.error({ errorDetails: oppDeleteIngestResult[0].error }, `Bulk API v2 delete for Opportunities failed.`);
           throw new Error(`bulkApi.ingest (delete) for Opportunities failed.`);
       } else if (Array.isArray(oppDeleteIngestResult) && oppDeleteIngestResult[0]?.id && oppDeleteIngestResult[0]?.type) {
           oppDeleteJobReference = oppDeleteIngestResult[0];
       } else {
           logger.error({ oppDeleteIngestResult }, `Bulk API v2 delete for Opportunities returned unexpected structure.`);
           throw new Error('bulkApi.ingest (delete) for Opportunities returned unexpected structure.');
       }

      logger.info(`Submitted Bulk API v2 Deletion job with ID: ${oppDeleteJobReference.id} for main Job ID: ${processJobId}`);
      const oppDeleteJobInfo = await pollBulkJobStatus(oppDeleteJobReference, bulkApi, logger);
      logger.info(`Opportunity deletion job ${oppDeleteJobReference.id} completed. State: ${oppDeleteJobInfo.state}, Processed: ${oppDeleteJobInfo.numberRecordsProcessed}, Failed: ${oppDeleteJobInfo.numberRecordsFailed}`);
      if (oppDeleteJobInfo.numberRecordsFailed > 0) {
          try {
             const failedRecords = await bulkApi.getFailedResults(oppDeleteJobReference);
             logger.warn(`Opportunity deletion job ${oppDeleteJobReference.id} had ${oppDeleteJobInfo.numberRecordsFailed} failures. Details:`, failedRecords);
          } catch(failErr) {
             logger.error({err: failErr}, `Error fetching failed results for delete job ${oppDeleteJobReference.id}`);
          }
      }

    } else {
      logger.warn(`Unknown data operation: ${operation} for Job ID: ${processJobId}`);
    }

    logger.info(`Job processing completed for Job ID: ${processJobId}`);

  } catch (error) {
    logger.error({ err: error }, `Error processing data message for Job ID: ${processJobId}`);
  }
}

export {
  handleDataMessage
}; 