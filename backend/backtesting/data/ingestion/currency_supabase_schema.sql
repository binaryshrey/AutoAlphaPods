create table if not exists public.currency_price_bars (
  source text not null default 'yfinance',
  category text not null,
  instrument_name text not null,
  currency_code text not null,
  quote_currency text not null default 'USD',
  yahoo_symbol text not null,
  quote_direction text not null,
  timestamp date not null,
  raw_open double precision,
  raw_high double precision,
  raw_low double precision,
  raw_close double precision,
  raw_adj_close double precision,
  volume bigint,
  usd_per_currency_open double precision,
  usd_per_currency_high double precision,
  usd_per_currency_low double precision,
  usd_per_currency_close double precision,
  usd_per_currency_adj_close double precision,
  inserted_at timestamptz not null default timezone('utc', now()),
  primary key (currency_code, timestamp)
);

create index if not exists currency_price_bars_timestamp_idx
  on public.currency_price_bars (timestamp);

create index if not exists currency_price_bars_category_idx
  on public.currency_price_bars (category);

create index if not exists currency_price_bars_yahoo_symbol_idx
  on public.currency_price_bars (yahoo_symbol);

alter table public.currency_price_bars enable row level security;

drop policy if exists "anon can read currency price bars"
  on public.currency_price_bars;

create policy "anon can read currency price bars"
  on public.currency_price_bars
  for select
  to anon
  using (true);

drop policy if exists "anon can insert currency price bars"
  on public.currency_price_bars;

create policy "anon can insert currency price bars"
  on public.currency_price_bars
  for insert
  to anon
  with check (true);

drop policy if exists "anon can upsert currency price bars"
  on public.currency_price_bars;

create policy "anon can upsert currency price bars"
  on public.currency_price_bars
  for update
  to anon
  using (true)
  with check (true);
