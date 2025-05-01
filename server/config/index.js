import 'dotenv/config';

// Centralized configuration
const config = {
  env: process.env.NODE_ENV || 'development',
  port: process.env.APP_PORT || 5000,
  logLevel: process.env.LOG_LEVEL || 'info',
  redisUrl: process.env.REDIS_URL || 'redis://localhost:6379' // Default for local dev
  // Add other configurations as needed
};

export default config;
