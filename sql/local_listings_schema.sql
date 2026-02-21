-- Local hyperlocal listings (food / grocery / ride-auto)
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
