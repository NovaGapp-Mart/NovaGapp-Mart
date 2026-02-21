-- Apply Row Level Security policies for local service tables.
-- Run this after local_listings_schema.sql

alter table if exists public.local_listings enable row level security;
alter table if exists public.local_orders enable row level security;
alter table if exists public.local_ride_requests enable row level security;
alter table if exists public.local_roles enable row level security;
alter table if exists public.local_listing_items enable row level security;
alter table if exists public.local_agents enable row level security;
alter table if exists public.local_agent_bookings enable row level security;
alter table if exists public.local_rider_locations enable row level security;
alter table if exists public.local_role_payments enable row level security;
alter table if exists public.local_settlements enable row level security;

-- Local Roles
drop policy if exists local_roles_self_rw on public.local_roles;
create policy local_roles_self_rw on public.local_roles
for all
using (user_id = auth.uid()::text)
with check (user_id = auth.uid()::text);

-- Listings
drop policy if exists local_listings_public_read on public.local_listings;
create policy local_listings_public_read on public.local_listings
for select using (status = 'approved');

drop policy if exists local_listings_owner_rw on public.local_listings;
create policy local_listings_owner_rw on public.local_listings
for all
using (user_id = auth.uid()::text)
with check (user_id = auth.uid()::text);

-- Listing items
drop policy if exists local_listing_items_public_read on public.local_listing_items;
create policy local_listing_items_public_read on public.local_listing_items
for select using (is_active = true);

drop policy if exists local_listing_items_owner_rw on public.local_listing_items;
create policy local_listing_items_owner_rw on public.local_listing_items
for all
using (seller_user_id = auth.uid()::text)
with check (seller_user_id = auth.uid()::text);

-- Orders
drop policy if exists local_orders_party_read on public.local_orders;
create policy local_orders_party_read on public.local_orders
for select using (
  buyer_user_id = auth.uid()::text or seller_user_id = auth.uid()::text
);

drop policy if exists local_orders_buyer_insert on public.local_orders;
create policy local_orders_buyer_insert on public.local_orders
for insert with check (buyer_user_id = auth.uid()::text);

drop policy if exists local_orders_party_update on public.local_orders;
create policy local_orders_party_update on public.local_orders
for update using (
  buyer_user_id = auth.uid()::text or seller_user_id = auth.uid()::text
)
with check (
  buyer_user_id = auth.uid()::text or seller_user_id = auth.uid()::text
);

-- Ride requests
drop policy if exists local_ride_requests_rider_insert on public.local_ride_requests;
create policy local_ride_requests_rider_insert on public.local_ride_requests
for insert with check (rider_user_id = auth.uid()::text);

drop policy if exists local_ride_requests_party_rw on public.local_ride_requests;
create policy local_ride_requests_party_rw on public.local_ride_requests
for all using (
  rider_user_id = auth.uid()::text or driver_user_id = auth.uid()::text
)
with check (
  rider_user_id = auth.uid()::text or driver_user_id = auth.uid()::text
);

-- Agents
drop policy if exists local_agents_public_read on public.local_agents;
create policy local_agents_public_read on public.local_agents
for select using (status = 'active');

drop policy if exists local_agents_owner_rw on public.local_agents;
create policy local_agents_owner_rw on public.local_agents
for all
using (user_id = auth.uid()::text)
with check (user_id = auth.uid()::text);

-- Agent bookings
drop policy if exists local_agent_bookings_party_rw on public.local_agent_bookings;
create policy local_agent_bookings_party_rw on public.local_agent_bookings
for all using (
  customer_user_id = auth.uid()::text or agent_user_id = auth.uid()::text
)
with check (
  customer_user_id = auth.uid()::text or agent_user_id = auth.uid()::text
);

-- Rider locations
drop policy if exists local_rider_locations_owner_rw on public.local_rider_locations;
create policy local_rider_locations_owner_rw on public.local_rider_locations
for all
using (user_id = auth.uid()::text)
with check (user_id = auth.uid()::text);

-- Role payments and settlements (owner read)
drop policy if exists local_role_payments_owner_read on public.local_role_payments;
create policy local_role_payments_owner_read on public.local_role_payments
for select using (user_id = auth.uid()::text);

drop policy if exists local_settlements_owner_read on public.local_settlements;
create policy local_settlements_owner_read on public.local_settlements
for select using (seller_user_id = auth.uid()::text);
