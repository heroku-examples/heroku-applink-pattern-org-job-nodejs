'use strict';

// Helper function mirroring the Java example's discount logic
function getDiscountForRegion (region, logger) {
  // Basic discount logic based on region
  switch (region) {
    case 'NAMER':
      logger?.info(`[QuoteService] Applying NAMER discount for region: ${region}`);
      return 0.1; // 10%
    case 'EMEA':
      logger?.info(`[QuoteService] Applying EMEA discount for region: ${region}`);
      return 0.15; // 15%
    case 'APAC':
      logger?.info(`[QuoteService] Applying APAC discount for region: ${region}`);
      return 0.08; // 8%
    default:
      logger?.warn(`[QuoteService] No specific discount for region: ${region}, applying default.`);
      return 0.05; // 5%
  }
}

/**
 * Handles quote generation jobs.
 * @param {object} jobData - The job data object from Redis.
 * @param {object} sfContext - The initialized Salesforce context (ContextImpl instance).
 * @param {object} logger - A logger instance.
 */
async function handleQuoteMessage (jobData, sfContext, logger) {
  logger.info('[QuoteService] Handling quote job object');

  // *** 1. Use soqlWhereClause instead of opportunityIds ***
  const { jobId, soqlWhereClause } = jobData;
  // Note: context is no longer destructured here, sfContext is passed in

  try {
    // *** Access APIs via sfContext.org ***
    if (!sfContext || !sfContext.org || !sfContext.org.dataApi) {
        logger.error(`[QuoteService] Invalid sfContext or sfContext.org.dataApi for Quote Job ID: ${jobId}`);
        return;
    }
    const dataApi = sfContext.org.dataApi;

    logger.info(`[QuoteService] Processing Quote Job ID: ${jobId}`);

    // *** 2. Fetch Standard Pricebook ID ***
    logger.info(`[QuoteService] Fetching Standard Pricebook ID for Job ID: ${jobId}`);
    const standardPricebookResult = await dataApi.query("SELECT Id FROM Pricebook2 WHERE IsStandard = true LIMIT 1");
    if (!(standardPricebookResult?.records?.[0]?.fields?.Id || standardPricebookResult?.records?.[0]?.fields?.id)) {
        logger.error(`[QuoteService] Standard Pricebook not found for Job ID: ${jobId}.`);
        throw new Error('Standard Pricebook not found.');
    }
    const standardPricebookId = standardPricebookResult.records[0].fields.Id || standardPricebookResult.records[0].fields.id;
    logger.info(`[QuoteService] Found Standard Pricebook ID: ${standardPricebookId} for Job ID: ${jobId}`);

    // *** 1. Use soqlWhereClause in the query ***
    if (!soqlWhereClause) {
      logger.warn(`[QuoteService] No soqlWhereClause provided for Job ID: ${jobId}`);
      return;
    }
    logger.info(`[QuoteService] Querying opportunities and OLIs using WHERE clause: ${soqlWhereClause} for Job ID: ${jobId}`);
    const oppQuery = `
      SELECT Id, Name, AccountId, CloseDate, StageName, Amount, Billing_Region__c,
             (SELECT Id, Product2Id, Quantity, UnitPrice, PricebookEntryId FROM OpportunityLineItems)
      FROM Opportunity
      WHERE ${soqlWhereClause}
    `; // Use the provided WHERE clause
    const oppResult = await dataApi.query(oppQuery);
    const opportunities = oppResult.records;

    if (!opportunities || opportunities.length === 0) {
      logger.warn(`[QuoteService] No opportunities found for WHERE clause: ${soqlWhereClause} in Job ID: ${jobId}`);
      return;
    }
    // Access fields using .fields property
    const firstOppRecord = opportunities[0]?.fields;
    if (!firstOppRecord?.Id && !firstOppRecord?.id) {
        logger.error(`[QuoteService] First Opportunity record missing fields.Id/fields.id field. Query Result: ${JSON.stringify(oppResult)}`);
        throw new Error('First Opportunity record missing fields.Id/fields.id field.');
    }
    logger.info(`[QuoteService] Found ${opportunities.length} opportunities for Job ID: ${jobId}`);

    const unitOfWork = dataApi.newUnitOfWork();
    const quoteRefs = new Map();

    opportunities.forEach(oppSObject => {
      // Access fields using .fields property
      const opp = oppSObject.fields;
      const oppId = opp.Id || opp.id; // Get the actual ID

      // Access subquery results correctly
      const lineItemsResult = oppSObject.subQueryResults?.OpportunityLineItems;

      if (!lineItemsResult?.records || lineItemsResult.records.length === 0) {
        logger.warn(`[QuoteService] Opportunity ${oppId} has no line items. Skipping quote creation for Job ID: ${jobId}`);
        return;
      }

      try {
        // 1. Create Quote
        const quoteName = `Quote for ${opp.Name} - ${new Date().toISOString().split('T')[0]}`;
        const expirationDate = new Date(opp.CloseDate);
        expirationDate.setDate(expirationDate.getDate() + 30); // Quote expires 30 days after CloseDate

        // Calculate discount based on custom field Billing_Region__c
        // Note: Java version hardcoded region, we use the Opp field
        const discount = getDiscountForRegion(opp.Billing_Region__c, logger);

        const quoteRef = unitOfWork.registerCreate({
          type: 'Quote',
          fields: {
            Name: quoteName.substring(0, 80), // Ensure name is within limit
            OpportunityId: oppId,
            Pricebook2Id: standardPricebookId, // *** 3. Use fetched Standard Pricebook ID ***
            ExpirationDate: expirationDate.toISOString().split('T')[0],
            Status: 'Draft'
            // *** 4. Remove Discount field from Quote ***
          }
        });
        quoteRefs.set(oppId, quoteRef);

        // 2. Create QuoteLineItems from OpportunityLineItems
        lineItemsResult.records.forEach(oliSObject => {
          // Access fields using .fields property
          const oli = oliSObject.fields;

          // *** 4. Apply discount to QuoteLineItem UnitPrice ***
          const originalUnitPrice = oli.UnitPrice;
          const quantity = oli.Quantity;
          // Ensure discount is a number between 0 and 1
          const validDiscount = (typeof discount === 'number' && discount >= 0 && discount <= 1) ? discount : 0;
          // Check for null/undefined before calculation
          const calculatedDiscountedPrice = (originalUnitPrice != null && validDiscount != null)
                                            ? originalUnitPrice * (1 - validDiscount)
                                            : originalUnitPrice; // Default to original if calculation fails

          unitOfWork.registerCreate({
            type: 'QuoteLineItem',
            fields: {
              QuoteId: quoteRef.toApiString(), // Reference the quote created above
              PricebookEntryId: oli.PricebookEntryId, // Must be valid PBE in the Quote's Pricebook
              Quantity: quantity,
              UnitPrice: calculatedDiscountedPrice // Use the calculated discounted price
            }
          });
        });
        logger.info(`[QuoteService] Registered Quote and ${lineItemsResult.records.length} Line Items for Opp ${oppId} in Job ID: ${jobId}`);
      } catch (err) {
        logger.error({ err: err, opportunityId: oppId }, `[QuoteService] Error processing Opportunity ${oppId} for Job ID: ${jobId}`);
      }
    });

    if (quoteRefs.size === 0) {
      logger.warn(`[QuoteService] No quotes were registered for creation for Job ID: ${jobId}.`);
      return;
    }

    logger.info(`[QuoteService] Committing Unit of Work with ${quoteRefs.size} Quotes and related Line Items for Job ID: ${jobId}`);
    const commitResult = await dataApi.commitUnitOfWork(unitOfWork);
    logger.info(`[QuoteService] Unit of Work commit attempted for Job ID: ${jobId}`);

    // Process results
    let successCount = 0;
    let failureCount = 0;
    commitResult.forEach((result, ref) => {
      // Only log results for the main Quote records for brevity
      if (ref.type === 'Quote') {
        const oppId = [...quoteRefs.entries()].find(([key, value]) => value === ref)?.[0];
        if (result.success) {
          successCount++;
          logger.info(`[QuoteService] Successfully created Quote ${result.id} for Opportunity ${oppId} in Job ID: ${jobId}`);
        } else {
          failureCount++;
          logger.error({ errors: result.errors, opportunityId: oppId }, `[QuoteService] Failed to create Quote for Opportunity ${oppId} in Job ID: ${jobId}`);
        }
      }
    });
    logger.info(`[QuoteService] Quote Creation Results for Job ID ${jobId}: ${successCount} succeeded, ${failureCount} failed.`);

  } catch (error) {
    logger.error({ err: error }, `[QuoteService] Critical error processing Quote Job ID ${jobId}`);
  }
}

export {
  handleQuoteMessage
}; 