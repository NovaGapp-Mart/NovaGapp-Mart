create extension if not exists pgcrypto;

alter table if exists public.orders
  add column if not exists paid boolean not null default false;

alter table if exists public.orders
  add column if not exists paid_at timestamptz;

alter table if exists public.orders
  add column if not exists item_purchased text;

update public.orders
set
  paid = true,
  paid_at = coalesce(paid_at, updated_at, created_at, now())
where lower(trim(coalesce(payment_status, ''))) in ('paid', 'captured');

alter table if exists public.subscription_state
  add column if not exists item_purchased text;

alter table if exists public.subscription_state
  add column if not exists paid_at timestamptz;

create table if not exists public.user_push_tokens (
  id uuid primary key default gen_random_uuid(),
  user_id text not null,
  token text not null,
  platform text not null default 'web',
  user_agent text,
  is_active boolean not null default true,
  last_seen_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (user_id, token)
);

create index if not exists idx_user_push_tokens_user_active
  on public.user_push_tokens (user_id, is_active, updated_at desc);

create index if not exists idx_user_push_tokens_token
  on public.user_push_tokens (token);