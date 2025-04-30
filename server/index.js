require('dotenv').config();
const Fastify = require('fastify');
// const path = require('node:path'); // No longer needed here
// const fs = require('node:fs'); // No longer needed here
const swagger = require('@fastify/swagger');
const swaggerUi = require('@fastify/swagger-ui');
const { config } = require('./config'); // Use centralized config
// TODO: Create these files later
// const { salesforcePlugin } = require('./middleware/salesforce.js');
// const apiRoutes = require('./routes/api.js');

// Basic logging configuration
const fastify = Fastify({
  logger: {
    level: config.logLevel // Use config value
  }
});

// Remove YAML loading logic
// let openapiSpec = {};
// try {
//   const yaml = require('js-yaml');
//   openapiSpec = yaml.load(fs.readFileSync(path.join(__dirname, '../api-docs.yaml'), 'utf8'));
//   fastify.log.info('Successfully loaded api-docs.yaml');
// } catch (e) {
//   fastify.log.error('Could not load or parse api-docs.yaml', e);
//   // Proceed with a minimal spec if file loading fails
//   openapiSpec = {
//     openapi: '3.0.0',
//     info: {
//       title: 'API Docs (Error Loading File)',
//       version: '1.0.0'
//     },
//     paths: {}
//   };
// }

// Register Swagger for dynamic generation
fastify.register(swagger, {
  // Remove mode: 'static' and specification block
  openapi: {
    // Basic OpenAPI info - details will come from route schemas
    openapi: '3.0.0', // Specify OpenAPI version
    info: {
      title: 'Org Job Pricing Engine API',
      description: 'API for calculating pricing and managing sample data, interacting with Salesforce via AppLink.',
      version: '1.0.0' // Or pull from package.json
    },
    servers: [
      // Add server info if needed, e.g., for local testing
      // { url: 'http://localhost:3000', description: 'Local server' }
    ],
    tags: [
      // Define tags used in route schemas later
      { name: 'Pricing Engine', description: 'Quote generation endpoints' },
      { name: 'Sample Data', description: 'Sample data management endpoints' }
    ]
    // Components (like securitySchemes) can be added here if needed globally
  }
});

// Register Swagger UI
fastify.register(swaggerUi, {
  routePrefix: '/docs',
  uiConfig: {
    docExpansion: 'list', // Expand operations list by default
    deepLinking: false
  },
  staticCSP: true,
  transformStaticCSP: (header) => header
});

// Placeholder for health check
fastify.get('/health', async (request, reply) => {
  return { status: 'ok' };
});

// TODO: Register Salesforce middleware
// fastify.register(salesforcePlugin);

// TODO: Register API routes
// fastify.register(apiRoutes, { prefix: '/api' });

// Start server
const start = async () => {
  try {
    await fastify.listen({ port: config.port, host: '0.0.0.0' }); // Listen on all interfaces for Heroku
    fastify.log.info(`Server listening on port ${config.port}`);
    fastify.log.info(`Swagger UI available at /docs`);
  } catch (err) {
    fastify.log.error('Error starting server:', err);
    process.exit(1);
  }
};

start();
