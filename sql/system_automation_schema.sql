-- NOVAGAPP System Automation Schema
-- Run in Supabase SQL editor.

alter table if exists public.users
  add column if not exists display_name text;

alter table if exists public.users
  add column if not exists email_local text;

alter table if exists public.users
  add column if not exists search_tokens text[];

create index if not exists idx_users_display_name on public.users(display_name);
create index if not exists idx_users_email_local on public.users(email_local);

create table if not exists public.subscription_state (
  user_id text primary key,
  plan text not null default 'free',
  status text not null default 'free',
  started_at timestamptz,
  expires_at timestamptz,
  source text,
  payment_id text,
  order_id text,
  amount_paise bigint default 0,
  currency text default 'INR',
  metadata jsonb default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_subscription_state_plan_status
  on public.subscription_state(plan, status);

create index if not exists idx_subscription_state_expires
  on public.subscription_state(expires_at);

create table if not exists public.subscription_webhook_events (
  event_id text primary key,
  event_name text,
  payment_id text,
  order_id text,
  payload jsonb default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists public.automation_media_events (
  id text primary key,
  user_id text not null,
  media_type text,
  bucket text,
  path text,
  url text,
  file_name text,
  mime text,
  size_bytes bigint default 0,
  source text,
  product_id text,
  created_at timestamptz not null default now()
);

create index if not exists idx_automation_media_events_user_created
  on public.automation_media_events(user_id, created_at desc);

create table if not exists public.automation_funnel_events (
  id text primary key,
  user_id text not null,
  step text not null,
  month_key text not null,
  meta jsonb default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_automation_funnel_events_user_month
  on public.automation_funnel_events(user_id, month_key);
