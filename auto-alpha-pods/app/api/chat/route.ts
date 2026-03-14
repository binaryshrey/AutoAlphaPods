import Anthropic from "@anthropic-ai/sdk";
import { NextRequest } from "next/server";

const client = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY,
});

export async function POST(req: NextRequest) {
  const { message, portfolioContext, newsContext } = (await req.json()) as {
    message: string;
    portfolioContext?: unknown;
    newsContext?: unknown;
  };

  const hasPortfolioContext =
    portfolioContext !== null && portfolioContext !== undefined;
  const hasNewsContext = newsContext !== null && newsContext !== undefined;
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

  const userContent = contextBlocks.length
    ? `${contextBlocks.join("\n\n")}

User question:
${message}`
    : message;

  const encoder = new TextEncoder();
  const readable = new ReadableStream({
    async start(controller) {
      try {
        const response = await client.messages.create({
          model: "claude-sonnet-4-6",
          max_tokens: 1024,
          stream: true,
          system: `You are a market assistant for crypto, equities, and ETFs. Detect the user's intent:

1. If they ask about THEIR PORTFOLIO, OPEN POSITIONS, P&L, ACCOUNT PERFORMANCE, PENDING ORDERS, PARTIALLY FILLED ORDERS, or FILLED ORDERS, respond with:
__PORTFOLIO_CARD__{"scope":"account"}

Then on the next line add 1-2 concise sentences summarizing what you'll show.

2. If they ask about the CURRENT PRICE, trading data, market stats, chart, or performance of a specific asset, respond with one asset card marker in this exact format:

For CRYPTO:
__ASSET_CARD__{"assetType":"crypto","coinId":"<coingecko-id>","symbol":"<TICKER>","name":"<Full Name>"}

For EQUITIES/ETFs:
__ASSET_CARD__{"assetType":"equity","symbol":"<TICKER>","name":"<Company or ETF Name>"}

If the user specifically wants to BUY, PURCHASE, or TRADE the asset, include "buyIntent":true.

Then on the very next line add 2–3 sentences of concise market context: current price level, recent price action/trend, and one key insight. Use **bold** for key figures. If buyIntent is true, end with a brief risk note.

Use official CoinGecko IDs for crypto and standard uppercase tickers for equities/ETFs (e.g., NVDA, AAPL, SPY, QQQ, TSLA, MSFT).

3. For ALL other questions (market analysis, trends, predictions, explanations, comparisons, general knowledge) respond normally. Use **bold** for key terms and bullet points for lists. Keep responses under 350 words.

When a <portfolio_context_json> block is present in the user message, treat it as the latest portfolio snapshot and use it for follow-up reasoning about positions, P&L, risk, order status, and trade ideas. Do not claim live refresh unless explicitly requested.
When a <news_context_json> block is present, use those headlines as the latest related context when answering follow-up questions. Reference them when useful and keep links in the UI separate (they are rendered by the client).

After every normal text response (not asset card), append exactly:
**Sources:** [pick all that apply, separated by " · ": CoinGecko (for crypto price/market data), Yahoo Finance (for equity/ETF market data), Alpaca (for account/portfolio/order data), Polymarket (for prediction markets), Alternative.me (for fear & greed sentiment), Claude AI (for analysis, trends, general knowledge)]`,
          messages: [{ role: "user", content: userContent }],
        });

        for await (const event of response) {
          if (
            event.type === "content_block_delta" &&
            event.delta.type === "text_delta"
          ) {
            controller.enqueue(encoder.encode(event.delta.text));
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
