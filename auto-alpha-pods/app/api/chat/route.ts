import { NextRequest } from "next/server";

const OPENROUTER_KEY = process.env.OPENROUTER_KEY;
const OPENROUTER_BASE = (
  process.env.OPENROUTER_BASE || "https://openrouter.ai/api/v1"
).replace(/\/$/, "");
const MODEL = process.env.MODEL || "openai/gpt-4o-mini";

function extractDeltaText(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";

  return content
    .map((part) => {
      if (typeof part === "string") return part;
      if (
        part &&
        typeof part === "object" &&
        "type" in part &&
        "text" in part &&
        (part as { type?: string }).type === "text"
      ) {
        const text = (part as { text?: unknown }).text;
        return typeof text === "string" ? text : "";
      }
      return "";
    })
    .join("");
}

export async function POST(req: NextRequest) {
  const {
    message,
    portfolioContext,
    newsContext,
    backtestEnabled,
  } = (await req.json()) as {
    message: string;
    portfolioContext?: unknown;
    newsContext?: unknown;
    backtestEnabled?: boolean;
  };

  if (!OPENROUTER_KEY) {
    return new Response("Missing OPENROUTER_KEY", { status: 500 });
  }

  const hasPortfolioContext =
    portfolioContext !== null && portfolioContext !== undefined;
  const hasNewsContext = newsContext !== null && newsContext !== undefined;
  const allowBacktestCard = Boolean(backtestEnabled);
  const portfolioContextText = hasPortfolioContext
    ? JSON.stringify(portfolioContext)
    : "";
  const newsContextText = hasNewsContext ? JSON.stringify(newsContext) : "";

  const contextBlocks: string[] = [];
  if (hasPortfolioContext) {
    contextBlocks.push(`<portfolio_context_json>
${portfolioContextText}
</portfolio_context_json>`);
  }
  if (hasNewsContext) {
    contextBlocks.push(`<news_context_json>
${newsContextText}
</news_context_json>`);
  }
  contextBlocks.push(
    `<backtest_enabled>${allowBacktestCard ? "true" : "false"}</backtest_enabled>`,
  );

  const userContent = contextBlocks.length
    ? `${contextBlocks.join("\n\n")}

User question:
${message}`
    : message;

  const encoder = new TextEncoder();
  const readable = new ReadableStream({
    async start(controller) {
      try {
        const upstream = await fetch(`${OPENROUTER_BASE}/chat/completions`, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${OPENROUTER_KEY}`,
            "Content-Type": "application/json",
            "HTTP-Referer":
              process.env.NEXT_PUBLIC_APP_URL || "http://localhost:3000",
            "X-Title": "AutoAlphaPods Chat",
          },
          body: JSON.stringify({
            model: MODEL,
            max_tokens: 1100,
            temperature: 0.2,
            stream: true,
            messages: [
              {
                role: "system",
                content: `You are the AutoAlphaPods market assistant and backtest coordinator. Detect intent and follow this output contract exactly:

0. Backtest intent:
Only when <backtest_enabled>true</backtest_enabled> and the user asks to create/run/simulate/evaluate a strategy using historical data, respond with:
__BACKTEST_CARD__{"prompt":"<clean backtest strategy prompt>","start":"2015-01-01","end":"2024-12-31","initial_cash":100000}
Then on the next line add 1-2 concise sentences explaining what the backtest will evaluate.

1. Portfolio/account intent:
If they ask about THEIR PORTFOLIO, OPEN POSITIONS, P&L, ACCOUNT PERFORMANCE, or ORDER STATUS, respond with:
__PORTFOLIO_CARD__{"scope":"account"}
Then on the next line add 1-2 concise sentences summarizing what you'll show.

2. Asset quote/chart intent:
If they ask for current price, market stats, chart, or performance of a specific asset, respond with one asset card marker in this exact format:

For crypto:
__ASSET_CARD__{"assetType":"crypto","coinId":"<coingecko-id>","symbol":"<TICKER>","name":"<Full Name>"}

For equities/ETFs:
__ASSET_CARD__{"assetType":"equity","symbol":"<TICKER>","name":"<Company or ETF Name>"}

If the user asks to buy/trade the asset, include "buyIntent":true.
Then on the next line add 2-3 concise sentences with current context and one key insight.

3. All other intents:
Respond normally in concise markdown, under 350 words, using bullets when useful.

Context usage:
- When <portfolio_context_json> is present, treat it as the latest portfolio snapshot.
- When <news_context_json> is present, treat it as latest headline context.
- Do not claim live refresh unless explicitly requested.

Source footer:
After every normal text response (not __ASSET_CARD__, __PORTFOLIO_CARD__, or __BACKTEST_CARD__), append exactly:
**Sources:** [pick all that apply, separated by " · ": CoinGecko (for crypto price/market data), Yahoo Finance (for equity/ETF market data), Alpaca (for account/portfolio/order data), Polymarket (for prediction markets), Alternative.me (for fear & greed sentiment), OpenRouter (for LLM analysis)]`,
              },
              { role: "user", content: userContent },
            ],
          }),
        });

        if (!upstream.ok) {
          const errText = await upstream.text();
          throw new Error(`OpenRouter ${upstream.status}: ${errText}`);
        }
        if (!upstream.body) {
          throw new Error("OpenRouter stream missing body");
        }

        const reader = upstream.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split("\n");
          buffer = lines.pop() || "";

          for (const rawLine of lines) {
            const line = rawLine.trim();
            if (!line || line.startsWith(":")) continue;
            if (!line.startsWith("data:")) continue;

            const payload = line.slice(5).trim();
            if (!payload || payload === "[DONE]") continue;

            try {
              const parsed = JSON.parse(payload) as {
                choices?: Array<{
                  delta?: { content?: unknown };
                }>;
              };
              const deltaText = extractDeltaText(
                parsed.choices?.[0]?.delta?.content,
              );
              if (deltaText) {
                controller.enqueue(encoder.encode(deltaText));
              }
            } catch {
              // Ignore malformed SSE payload chunks.
            }
          }
        }

        const tail = buffer.trim();
        if (tail.startsWith("data:")) {
          const payload = tail.slice(5).trim();
          if (payload && payload !== "[DONE]") {
            try {
              const parsed = JSON.parse(payload) as {
                choices?: Array<{
                  delta?: { content?: unknown };
                }>;
              };
              const deltaText = extractDeltaText(
                parsed.choices?.[0]?.delta?.content,
              );
              if (deltaText) {
                controller.enqueue(encoder.encode(deltaText));
              }
            } catch {
              // Ignore malformed tail chunk.
            }
          }
        }

        controller.close();
      } catch (err) {
        controller.error(err);
      }
    },
  });

  return new Response(readable, {
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
      "Cache-Control": "no-cache",
    },
  });
}
