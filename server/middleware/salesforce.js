import fp from 'fastify-plugin';
// Correctly import from CJS module in ESM
import pkg from '@heroku/salesforce-sdk-nodejs';
const { AppLinkClient } = pkg;

/**
 * Fastify plugin to initialize the Salesforce AppLink SDK
 * and parse the client context from incoming requests.
 *
 * Decorates the request object with `request.salesforce` containing
 * the initialized SDK instance and parsed context.
 *
 * @param {import('fastify').FastifyInstance} fastify
 * @param {object} opts Plugin options
 */
async function salesforceMiddlewarePlugin (fastify, opts) {
  fastify.log.info('Registering Salesforce middleware plugin...');

  // Decorate request with salesforce object, initially null
  fastify.decorateRequest('salesforce', null);

  // Add preHandler hook to parse Salesforce context for every request
  // Note: Consider making this route-specific if not all routes need Salesforce context
  fastify.addHook('preHandler', async (request, reply) => {
    // Check if the x-client-context header exists before attempting to parse
    if (!request.headers || !request.headers['x-client-context']) {
      // If the header is missing, just proceed without initializing Salesforce context
      // Specific routes needing the context should check request.salesforce
      request.log.debug('No x-client-context header found, skipping Salesforce SDK initialization.');
      return;
    }

    request.log.info('Salesforce middleware: Initializing SDK and parsing request...');
    const sdk = AppLinkClient.init();
    try {
      const parsedRequest = sdk.salesforce.parseRequest(
        request.headers,
        request.body,
        request.log // Use Fastify's built-in logger
      );

      // Assign the initialized SDK instance merged with parsed data to request.salesforce
      request.salesforce = Object.assign(sdk, parsedRequest);
      request.log.info('Salesforce middleware: SDK initialized and context parsed successfully.');
    } catch (error) {
      request.log.error({ err: error }, 'Salesforce middleware: Failed to parse request');
      // Use Fastify's standard error handling
      // Throwing an error here will trigger Fastify's error handling mechanism
      const wrappedError = new Error('Failed to initialize Salesforce client due to invalid request context.');
      wrappedError.statusCode = 401; // Unauthorized or Bad Request might be appropriate
      throw wrappedError;
    }
  });

  fastify.log.info('Salesforce middleware plugin registration complete.');
}

// Export the plugin using fastify-plugin
// The name metadata helps prevent double registration
// The fastify dependency version constraint ensures compatibility
export default fp(salesforceMiddlewarePlugin, {
  name: 'salesforce-middleware',
  fastify: '4.x' // Specify Fastify version compatibility
});
