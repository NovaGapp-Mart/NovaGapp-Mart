-- Production upgrade for Ride + Food/Grocery + Agent modules
-- Safe to run multiple times.

create extension if not exists pgcrypto;

-- =========================================
-- Existing local_* tables: add missing cols
-- =========================================

alter table if exists public.local_listings add column if not exists delivery_charge_inr numeric(12,2) not null default 0;
alter table if exists public.local_listings add column if not exists minimum_order_inr numeric(12,2) not null default 0;
alter table if exists public.local_listings add column if not exists open_time text;
alter table if exists public.local_listings add column if not exists close_time text;
alter table if exists public.local_listings add column if not exists self_delivery boolean not null default true;
alter table if exists public.local_listings add column if not exists ride_vehicle_type text;
alter table if exists public.local_listings add column if not exists vehicle_number text;
alter table if exists public.local_listings add column if not exists base_fare_inr numeric(12,2);
alter table if exists public.local_listings add column if not exists per_km_rate_inr numeric(12,2);
alter table if exists public.local_listings add column if not exists per_min_rate_inr numeric(12,2);
alter table if exists public.local_listings add column if not exists service_radius_km integer;
alter table if exists public.local_listings add column if not exists documents_url text;

alter table if exists public.local_orders add column if not exists item_snapshot jsonb;
alter table if exists public.local_orders add column if not exists payment_method text;
alter table if exists public.local_orders add column if not exists payment_status text;
alter table if exists public.local_orders add column if not exists payment_order_id text;
alter table if exists public.local_orders add column if not exists payment_id text;
alter table if exists public.local_orders add column if not exists payment_ref text;
alter table if exists public.local_orders add column if not exists commission_inr numeric(12,2) not null default 0;
alter table if exists public.local_orders add column if not exists seller_earning_inr numeric(12,2) not null default 0;
alter table if exists public.local_orders add column if not exists delivered_at timestamptz;

alter table if exists public.local_orders drop constraint if exists local_orders_status_check;
alter table if exists public.local_orders
  add constraint local_orders_status_check
  check (status in ('placed','accepted','preparing','rejected','out_for_delivery','delivered','completed','cancelled'));

alter table if exists public.local_ride_requests add column if not exists vehicle_type text not null default 'auto';
alter table if exists public.local_ride_requests add column if not exists payment_method text;
alter table if exists public.local_ride_requests add column if not exists payment_status text;
alter table if exists public.local_ride_requests add column if not exists payment_order_id text;
alter table if exists public.local_ride_requests add column if not exists payment_id text;
alter table if exists public.local_ride_requests add column if not exists payment_ref text;
alter table if exists public.local_ride_requests add column if not exists fare_inr numeric(12,2) not null default 0;
alter table if exists public.local_ride_requests add column if not exists distance_km numeric(10,3) not null default 0;
alter table if exists public.local_ride_requests add column if not exists duration_min numeric(10,2) not null default 0;
alter table if exists public.local_ride_requests add column if not exists commission_inr numeric(12,2) not null default 0;
alter table if exists public.local_ride_requests add column if not exists driver_earning_inr numeric(12,2) not null default 0;
alter table if exists public.local_ride_requests add column if not exists completed_at timestamptz;

alter table if exists public.local_ride_requests drop constraint if exists local_ride_requests_vehicle_type_check;
alter table if exists public.local_ride_requests
  add constraint local_ride_requests_vehicle_type_check
  check (vehicle_type in ('bike','auto','car'));

alter table if exists public.local_agents add column if not exists per_hour_rate_inr numeric(12,2) not null default 0;
alter table if exists public.local_agents add column if not exists experience_years integer not null default 0;
alter table if exists public.local_agents add column if not exists service_radius_km integer not null default 5;
alter table if exists public.local_agents add column if not exists rating numeric(3,1) not null default 0;
alter table if exists public.local_agents add column if not exists rating_count integer not null default 0;

alter table if exists public.local_agent_bookings add column if not exists scheduled_at timestamptz;
alter table if exists public.local_agent_bookings add column if not exists hours_booked integer not null default 1;
alter table if exists public.local_agent_bookings add column if not exists estimated_price_inr numeric(12,2) not null default 0;
alter table if exists public.local_agent_bookings add column if not exists payment_method text;
alter table if exists public.local_agent_bookings add column if not exists payment_status text;
alter table if exists public.local_agent_bookings add column if not exists payment_order_id text;
alter table if exists public.local_agent_bookings add column if not exists payment_id text;
alter table if exists public.local_agent_bookings add column if not exists payment_ref text;
alter table if exists public.local_agent_bookings add column if not exists commission_inr numeric(12,2) not null default 0;
alter table if exists public.local_agent_bookings add column if not exists agent_earning_inr numeric(12,2) not null default 0;
alter table if exists public.local_agent_bookings add column if not exists completed_at timestamptz;

alter table if exists public.local_agent_bookings drop constraint if exists local_agent_bookings_status_check;
alter table if exists public.local_agent_bookings
  add constraint local_agent_bookings_status_check
  check (status in ('requested','accepted','on_the_way','started','completed','cancelled'));

alter table if exists public.local_rider_locations add column if not exists accuracy_m numeric(10,2);
alter table if exists public.local_rider_locations add column if not exists heading_deg numeric(7,2);
alter table if exists public.local_rider_locations add column if not exists speed_kmph numeric(10,2);

create index if not exists idx_local_listings_user_type_status on public.local_listings (user_id, listing_type, status);
create index if not exists idx_local_orders_status_updated on public.local_orders (status, updated_at desc);
create index if not exists idx_local_orders_payment on public.local_orders (payment_order_id, payment_id);
create index if not exists idx_local_ride_requests_driver_status on public.local_ride_requests (driver_user_id, status, updated_at desc);
create index if not exists idx_local_ride_requests_payment on public.local_ride_requests (payment_order_id, payment_id);
create index if not exists idx_local_agents_user_status on public.local_agents (user_id, status, available_now);
create index if not exists idx_local_agent_bookings_status_updated on public.local_agent_bookings (status, updated_at desc);

-- =========================================
-- Wallet + Transactions + Weekly payout
-- =========================================

create table if not exists public.wallets (
  id uuid primary key default gen_random_uuid(),
  owner_user_id text not null,
  owner_type text not null check (owner_type in ('platform','driver','seller','agent','customer')),
  balance_inr numeric(14,2) not null default 0,
  currency text not null default 'INR',
  status text not null default 'active' check (status in ('active','blocked')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(owner_user_id, owner_type)
);

create index if not exists idx_wallets_owner_type on public.wallets (owner_type, updated_at desc);

create table if not exists public.transactions (
  id uuid primary key default gen_random_uuid(),
  owner_user_id text not null,
  owner_type text not null check (owner_type in ('platform','driver','seller','agent','customer')),
  module text not null check (module in ('ride','food','grocery','agent','payment','platform')),
  transaction_type text not null check (transaction_type in ('payment_order','payment_capture','settlement','adjustment','refund')),
  reference_type text not null,
  reference_id text not null,
  payer_user_id text,
  payee_user_id text,
  amount_inr numeric(14,2) not null default 0,
  gross_inr numeric(14,2) not null default 0,
  commission_inr numeric(14,2) not null default 0,
  net_inr numeric(14,2) not null default 0,
  platform_share_inr numeric(14,2) not null default 0,
  payment_method text,
  payment_status text,
  payment_gateway text,
  payment_order_id text,
  payment_id text,
  payment_ref text,
  metadata jsonb not null default '{}'::jsonb,
  status text not null default 'pending' check (status in ('created','captured','completed','failed','cancelled','refunded','pending')),
  settled_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists uq_transactions_settlement_ref
  on public.transactions (module, transaction_type, reference_type, reference_id);
create unique index if not exists uq_transactions_payment_id
  on public.transactions (module, transaction_type, payment_id)
  where payment_id is not null;
create unique index if not exists uq_transactions_payment_order
  on public.transactions (module, transaction_type, payment_order_id)
  where payment_order_id is not null;
create index if not exists idx_transactions_owner_recent on public.transactions (owner_user_id, owner_type, created_at desc);
create index if not exists idx_transactions_module_recent on public.transactions (module, created_at desc);

create table if not exists public.weekly_payouts (
  id uuid primary key default gen_random_uuid(),
  owner_user_id text not null,
  owner_type text not null check (owner_type in ('platform','driver','seller','agent','customer')),
  module text not null check (module in ('ride','food','grocery','agent','payment','platform')),
  week_start_date date not null,
  week_end_date date not null,
  gross_inr numeric(14,2) not null default 0,
  commission_inr numeric(14,2) not null default 0,
  payout_inr numeric(14,2) not null default 0,
  status text not null default 'pending' check (status in ('pending','processing','paid','failed')),
  paid_at timestamptz,
  paid_ref text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(owner_user_id, owner_type, module, week_start_date)
);

create index if not exists idx_weekly_payouts_owner_week on public.weekly_payouts (owner_user_id, owner_type, week_start_date desc);
create index if not exists idx_weekly_payouts_status on public.weekly_payouts (status, week_start_date desc);

-- =========================================
-- Requested compatibility tables (only create if missing)
-- =========================================

do $$
begin
  if not exists (select 1 from information_schema.tables where table_schema='public' and table_name='users') then
    create table public.users (
      id text primary key,
      name text,
      phone text,
      role text,
      wallet_balance numeric(14,2) not null default 0,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    );
  end if;
end $$;

do $$
begin
  if not exists (select 1 from information_schema.tables where table_schema='public' and table_name='drivers') then
    create table public.drivers (
      id uuid primary key default gen_random_uuid(),
      user_id text references public.users(id) on delete set null,
      vehicle_type text,
      vehicle_number text,
      status text,
      lat double precision,
      lng double precision,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    );
    create index idx_drivers_status_loc on public.drivers (status, updated_at desc);
  end if;
end $$;

do $$
begin
  if not exists (select 1 from information_schema.tables where table_schema='public' and table_name='restaurants') then
    create table public.restaurants (
      id uuid primary key default gen_random_uuid(),
      user_id text references public.users(id) on delete set null,
      name text,
      address text,
      lat double precision,
      lng double precision,
      status text,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    );
    create index idx_restaurants_status_loc on public.restaurants (status, updated_at desc);
  end if;
end $$;

do $$
begin
  if not exists (select 1 from information_schema.tables where table_schema='public' and table_name='agents') then
    create table public.agents (
      id uuid primary key default gen_random_uuid(),
      user_id text references public.users(id) on delete set null,
      category text,
      experience_years integer,
      base_visit_charge_inr numeric(12,2),
      per_hour_rate_inr numeric(12,2),
      status text,
      lat double precision,
      lng double precision,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    );
    create index idx_agents_status_loc on public.agents (status, updated_at desc);
  end if;
end $$;

do $$
begin
  if not exists (select 1 from information_schema.tables where table_schema='public' and table_name='rides') then
    create table public.rides (
      id uuid primary key default gen_random_uuid(),
      rider_id text references public.users(id) on delete set null,
      driver_id text references public.users(id) on delete set null,
      pickup_lat double precision,
      pickup_lng double precision,
      drop_lat double precision,
      drop_lng double precision,
      fare numeric(12,2),
      commission numeric(12,2),
      status text,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    );
    create index idx_rides_status_created on public.rides (status, created_at desc);
  end if;
end $$;

do $$
begin
  if not exists (select 1 from information_schema.tables where table_schema='public' and table_name='orders') then
    create table public.orders (
      id uuid primary key default gen_random_uuid(),
      customer_id text references public.users(id) on delete set null,
      restaurant_id uuid references public.restaurants(id) on delete set null,
      amount_inr numeric(12,2),
      commission numeric(12,2),
      status text,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    );
    create index idx_orders_status_created on public.orders (status, created_at desc);
  end if;
end $$;

do $$
begin
  if not exists (select 1 from information_schema.tables where table_schema='public' and table_name='service_bookings') then
    create table public.service_bookings (
      id uuid primary key default gen_random_uuid(),
      customer_id text references public.users(id) on delete set null,
      agent_id uuid references public.agents(id) on delete set null,
      amount_inr numeric(12,2),
      commission numeric(12,2),
      status text,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    );
    create index idx_service_bookings_status_created on public.service_bookings (status, created_at desc);
  end if;
end $$;
