/**
 * Minimalist logger with ISO timestamps
 * Wraps native console API with timestamp prefixes
 * Follows AGENTS.md: strict typing, zero dependencies, minimal code
 */

function getTimestamp(): string {
  return new Date().toISOString();
}

export const logger = {
  debug: (...args: unknown[]): void => {
    console.debug(`[${getTimestamp()}]`, ...args);
  },
  info: (...args: unknown[]): void => {
    console.info(`[${getTimestamp()}]`, ...args);
  },
  warn: (...args: unknown[]): void => {
    console.warn(`[${getTimestamp()}]`, ...args);
  },
  error: (...args: unknown[]): void => {
    console.error(`[${getTimestamp()}]`, ...args);
  },
  log: (...args: unknown[]): void => {
    console.log(`[${getTimestamp()}]`, ...args);
  }
};
