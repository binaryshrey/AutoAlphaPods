export const ALPACA_URL = "https://paper-api.alpaca.markets/v2/orders";

const ALPACA_KEY_ENV_NAMES = [
  "APCA_API_KEY_ID",
  "ALPACA_API_KEY_ID",
  "ALPACA_API_KEY",
  "ALPACA_KEY",
];

const ALPACA_SECRET_ENV_NAMES = [
  "APCA_API_SECRET_KEY",
  "ALPACA_API_SECRET_KEY",
  "ALPACA_SECRET_KEY",
  "ALPACA_SECRET",
];

function firstEnvValue(keys: string[]): string | undefined {
  for (const key of keys) {
    const value = process.env[key]?.trim();
    if (value) return value;
  }
  return undefined;
}

function resolveAlpacaOrderUrl() {
  const explicitUrl = process.env.ALPACA_URL?.trim();
  if (explicitUrl) return explicitUrl;

  const baseUrl = process.env.APCA_API_BASE_URL?.trim();
  if (baseUrl) return `${baseUrl.replace(/\/$/, "")}/v2/orders`;

  return ALPACA_URL;
}

export function getAlpacaServerConfig() {
  const keyId = firstEnvValue(ALPACA_KEY_ENV_NAMES);
  const secretKey = firstEnvValue(ALPACA_SECRET_ENV_NAMES);
  const orderUrl = resolveAlpacaOrderUrl();

  return { keyId, secretKey, orderUrl };
}

export function hasAlpacaCredentials() {
  const { keyId, secretKey } = getAlpacaServerConfig();
  return Boolean(keyId && secretKey);
}
