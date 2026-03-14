create table if not exists public.commodity_price_bars (
  source text not null default 'yfinance',
  asset_class text not null,
  universe text not null,
  category text not null,
  instrument_name text not null,
  symbol text not null,
  timestamp date not null,
  open double precision,
  high double precision,
  low double precision,
  close double precision,
  adj_close double precision,
  volume bigint,
  dividends double precision,
  capital_gains double precision,
  stock_splits double precision,
  inserted_at timestamptz not null default timezone('utc', now()),
  primary key (symbol, timestamp)
);

create index if not exists commodity_price_bars_timestamp_idx
  on public.commodity_price_bars (timestamp);

create index if not exists commodity_price_bars_category_idx
  on public.commodity_price_bars (category);

create index if not exists commodity_price_bars_universe_idx
  on public.commodity_price_bars (universe);

alter table public.commodity_price_bars enable row level security;

drop policy if exists "anon can read commodity price bars"
  on public.commodity_price_bars;

create policy "anon can read commodity price bars"
  on public.commodity_price_bars
  for select
  to anon
  using (true);

drop policy if exists "anon can insert commodity price bars"
  on public.commodity_price_bars;

create policy "anon can insert commodity price bars"
  on public.commodity_price_bars
  for insert
  to anon
  with check (true);

drop policy if exists "anon can upsert commodity price bars"
  on public.commodity_price_bars;

create policy "anon can upsert commodity price bars"
  on public.commodity_price_bars
  for update
  to anon
  using (true)
  with check (true);
