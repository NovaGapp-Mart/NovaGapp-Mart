-- Local hyperlocal listings (food / grocery / ride-auto)
create extension if not exists pgcrypto;

create table if not exists public.local_listings (
  id uuid primary key default gen_random_uuid(),
  user_id text not null,
  store_name text not null,
  listing_type text not null check (listing_type in ('food','grocery','ride')),
  phone text not null,
  image_url text,
  lat double precision not null,
  lng double precision not null,
  listing_fee_inr numeric(10,2) not null default 500,
  platform_monthly_share_percent numeric(5,2) not null default 5,
  status text not null default 'pending_approval' check (status in ('pending_approval','approved','rejected')),
  rejection_reason text,
  approved_at timestamptz,
  open_now boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_local_listings_type_status on public.local_listings (listing_type, status);
create index if not exists idx_local_listings_user_id on public.local_listings (user_id);

create table if not exists public.local_orders (
  id uuid primary key default gen_random_uuid(),
  buyer_user_id text not null,
  seller_user_id text not null,
  listing_id uuid not null references public.local_listings(id) on delete restrict,
  service_type text not null check (service_type in ('food','grocery','ride')),
  amount_inr numeric(12,2) not null default 0,
  delivery_address text not null,
  note text,
  status text not null default 'placed' check (status in ('placed','accepted','rejected','out_for_delivery','completed','cancelled')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_local_orders_seller_created_at on public.local_orders (seller_user_id, created_at desc);
create index if not exists idx_local_orders_buyer_created_at on public.local_orders (buyer_user_id, created_at desc);

create table if not exists public.local_ride_requests (
  id uuid primary key default gen_random_uuid(),
  rider_user_id text not null,
  driver_user_id text,
  pickup_lat double precision not null,
  pickup_lng double precision not null,
  drop_lat double precision not null,
  drop_lng double precision not null,
  pickup_text text,
  drop_text text,
  offered_driver_ids jsonb not null default '[]'::jsonb,
  status text not null default 'searching' check (status in ('searching','accepted','arriving','on_trip','completed','cancelled')),
  accepted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_local_ride_requests_status_created_at on public.local_ride_requests (status, created_at desc);
create index if not exists idx_local_ride_requests_rider_created_at on public.local_ride_requests (rider_user_id, created_at desc);

create table if not exists public.local_roles (
  id uuid primary key default gen_random_uuid(),
  user_id text not null,
  role text not null check (role in ('consumer','seller','rider','agent')),
  status text not null default 'active' check (status in ('active','blocked','pending')),
  fee_required_inr numeric(10,2) not null default 0,
  fee_paid boolean not null default false,
  payment_ref text,
  display_name text,
  phone text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(user_id, role)
);

create index if not exists idx_local_roles_user_id on public.local_roles (user_id);

create table if not exists public.local_listing_items (
  id uuid primary key default gen_random_uuid(),
  listing_id uuid not null references public.local_listings(id) on delete cascade,
  seller_user_id text not null,
  name text not null,
  category text,
  price_inr numeric(12,2) not null,
  stock_qty integer not null default 0,
  image_url text,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(listing_id, name)
);

create index if not exists idx_local_listing_items_listing_id on public.local_listing_items (listing_id);

create table if not exists public.local_agents (
  id uuid primary key default gen_random_uuid(),
  user_id text not null,
  service_category text not null,
  title text not null,
  phone text not null,
  price_per_visit_inr numeric(12,2) not null default 0,
  lat double precision not null,
  lng double precision not null,
  image_url text,
  status text not null default 'active' check (status in ('active','blocked','pending')),
  available_now boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(user_id, service_category)
);

create table if not exists public.local_agent_bookings (
  id uuid primary key default gen_random_uuid(),
  customer_user_id text not null,
  agent_user_id text not null,
  agent_id uuid not null references public.local_agents(id) on delete restrict,
  service_address text not null,
  note text,
  status text not null default 'requested' check (status in ('requested','accepted','on_the_way','completed','cancelled')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.local_rider_locations (
  user_id text primary key,
  lat double precision not null,
  lng double precision not null,
  is_online boolean not null default true,
  updated_at timestamptz not null default now()
);

create table if not exists public.local_role_payments (
  id uuid primary key default gen_random_uuid(),
  user_id text not null,
  role text not null check (role in ('seller','rider')),
  razorpay_order_id text not null unique,
  razorpay_payment_id text not null,
  amount_inr numeric(10,2) not null default 500,
  status text not null default 'captured' check (status in ('created','captured','failed')),
  verified_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_local_role_payments_user_role on public.local_role_payments (user_id, role, status);

create table if not exists public.local_settlements (
  id uuid primary key default gen_random_uuid(),
  seller_user_id text not null,
  month text not null,
  paid_amount_inr numeric(12,2) not null default 0,
  paid_ref text,
  status text not null default 'pending' check (status in ('pending','paid')),
  paid_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(seller_user_id, month)
);

create index if not exists idx_local_settlements_month on public.local_settlements (month, status);
