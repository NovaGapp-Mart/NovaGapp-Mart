-- Run this in Supabase SQL Editor before enabling contest sync from server.js

create table if not exists public.contest_users (
  user_id text primary key,
  display_name text,
  referral_code text,
  share_actions bigint not null default 0,
  share_actions_by_type jsonb not null default '{}'::jsonb,
  unique_share_visits bigint not null default 0,
  verified_installs bigint not null default 0,
  paid_votes bigint not null default 0,
  contest_votes jsonb not null default '{}'::jsonb,
  share_weight numeric(12,2) not null default 0,
  weighted_entries numeric(12,2) not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.contest_orders (
  razorpay_order_id text primary key,
  receipt text,
  status text not null default 'created',
  user_id text,
  contest_id text,
  side_id text,
  pack_id text,
  votes integer not null default 0,
  amount_usd numeric(12,2) not null default 0,
  amount_inr_paise bigint not null default 0,
  usd_inr_rate numeric(12,4) not null default 0,
  currency text not null default 'INR',
  payment_id text,
  signature text,
  created_at timestamptz not null default now(),
  paid_at timestamptz,
  updated_at timestamptz not null default now()
);

create table if not exists public.contest_votes (
  contest_id text not null,
  side_id text not null,
  votes bigint not null default 0,
  updated_at timestamptz not null default now(),
  primary key (contest_id, side_id)
);

create index if not exists idx_contest_orders_user_id on public.contest_orders(user_id);
create index if not exists idx_contest_orders_status on public.contest_orders(status);
create index if not exists idx_contest_users_referral_code on public.contest_users(referral_code);

alter table public.contest_users enable row level security;
alter table public.contest_orders enable row level security;
alter table public.contest_votes enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'contest_votes'
      and policyname = 'contest_votes_read_public'
  ) then
    create policy "contest_votes_read_public"
      on public.contest_votes
      for select
      using (true);
  end if;
end $$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'contest_users'
      and policyname = 'contest_users_read_self'
  ) then
    create policy "contest_users_read_self"
      on public.contest_users
      for select
      using (auth.uid()::text = user_id);
  end if;
end $$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'contest_orders'
      and policyname = 'contest_orders_read_own'
  ) then
    create policy "contest_orders_read_own"
      on public.contest_orders
      for select
      using (auth.uid()::text = user_id);
  end if;
end $$;
