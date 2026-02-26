-- Production order security upgrade
-- Run in Supabase SQL editor (production) after taking backup.

create extension if not exists pgcrypto;

-- ---------- ORDERS TABLE HARDENING ----------
alter table if exists public.orders add column if not exists seller_id uuid;
alter table if exists public.orders add column if not exists buyer_name text;
alter table if exists public.orders add column if not exists buyer_address jsonb;
alter table if exists public.orders add column if not exists amount numeric(12,2);
alter table if exists public.orders add column if not exists payment_id text;
alter table if exists public.orders add column if not exists courier_name text;
alter table if exists public.orders add column if not exists tracking_number text;
alter table if exists public.orders add column if not exists reject_reason text;
alter table if exists public.orders add column if not exists approved_at timestamptz;
alter table if exists public.orders add column if not exists shipped_at timestamptz;
alter table if exists public.orders add column if not exists delivered_at timestamptz;
alter table if exists public.orders add column if not exists completed_at timestamptz;
alter table if exists public.orders add column if not exists client_request_id text;

update public.orders
set status = 'pending'
where lower(trim(coalesce(status,''))) in ('placed','pending_approval','new_order','pending_payment');

update public.orders
set status = 'shipped'
where lower(trim(coalesce(status,''))) in ('hub_reached','out_for_delivery');

alter table if exists public.orders drop constraint if exists orders_status_check;
alter table if exists public.orders
  add constraint orders_status_check
  check (status in ('pending','approved','shipped','delivered','completed','rejected','cancelled'));

create unique index if not exists idx_orders_user_client_request
on public.orders (user_id, client_request_id)
where client_request_id is not null;

create index if not exists idx_orders_seller_created on public.orders (seller_id, created_at desc);
create index if not exists idx_orders_user_created on public.orders (user_id, created_at desc);
create index if not exists idx_orders_status_created on public.orders (status, created_at desc);
create index if not exists idx_orders_tracking on public.orders (tracking_number);

-- ---------- NOTIFICATIONS HARDENING ----------
alter table if exists public.notifications add column if not exists receiver_user_id uuid;
alter table if exists public.notifications add column if not exists type text;
alter table if exists public.notifications add column if not exists title text;
alter table if exists public.notifications add column if not exists message text;
alter table if exists public.notifications add column if not exists order_id uuid;
alter table if exists public.notifications add column if not exists is_read boolean not null default false;
alter table if exists public.notifications add column if not exists is_deleted boolean not null default false;
alter table if exists public.notifications add column if not exists created_at timestamptz not null default now();

create index if not exists idx_notifications_receiver_created
on public.notifications (receiver_user_id, created_at desc);

-- ---------- ORDER STATUS HISTORY ----------
create table if not exists public.order_status_history (
  id uuid primary key default gen_random_uuid(),
  order_id uuid not null references public.orders(id) on delete cascade,
  from_status text,
  to_status text not null,
  actor_user_id uuid,
  actor_role text,
  note text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_order_status_history_order_created
on public.order_status_history (order_id, created_at asc);

drop trigger if exists trg_orders_status_history on public.orders;
drop function if exists public.log_order_status_history_trigger();

-- ---------- VALIDATION HELPERS ----------
create or replace function public.normalize_secure_order_status(p_status text)
returns text
language sql
immutable
as $$
  select lower(trim(coalesce(p_status,'')));
$$;

create or replace function public.is_valid_secure_transition(p_current text, p_next text)
returns boolean
language plpgsql
immutable
as $$
declare
  v_current text := public.normalize_secure_order_status(p_current);
  v_next text := public.normalize_secure_order_status(p_next);
begin
  if v_current = v_next then return false; end if;
  if v_current = 'pending' then
    return v_next in ('approved','rejected','cancelled');
  elsif v_current = 'approved' then
    return v_next in ('shipped','cancelled');
  elsif v_current = 'shipped' then
    return v_next in ('delivered');
  elsif v_current = 'delivered' then
    return v_next in ('completed');
  end if;
  return false;
end;
$$;

-- ---------- SECURE ORDER CREATION (SERVER CONTROLLED) ----------
create or replace function public.create_pending_order_secure(
  p_user_id uuid,
  p_email text default null,
  p_buyer_name text default null,
  p_buyer_phone text default null,
  p_buyer_address jsonb default '{}'::jsonb,
  p_items jsonb default '[]'::jsonb,
  p_payment_method text default 'Pending Payment',
  p_payment_status text default 'pending',
  p_idempotency_key text default null
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actor uuid := auth.uid();
  v_item jsonb;
  v_product record;
  v_product_id uuid;
  v_qty integer;
  v_price numeric;
  v_currency text := 'USD';
  v_total numeric := 0;
  v_items_snapshot jsonb := '[]'::jsonb;
  v_seller uuid := null;
  v_order public.orders%rowtype;
  v_existing public.orders%rowtype;
  v_phone text := left(coalesce(p_buyer_phone,''),30);
begin
  if p_user_id is null then
    raise exception 'user_id_required';
  end if;
  if v_actor is not null and v_actor <> p_user_id then
    raise exception 'actor_user_mismatch';
  end if;
  if jsonb_typeof(coalesce(p_items,'[]'::jsonb)) <> 'array' then
    raise exception 'items_must_be_array';
  end if;

  if coalesce(nullif(trim(p_idempotency_key),''),'') <> '' then
    select * into v_existing
    from public.orders
    where user_id = p_user_id
      and client_request_id = trim(p_idempotency_key)
    order by created_at desc
    limit 1;

    if found then
      return jsonb_build_object('order', to_jsonb(v_existing), 'idempotent', true);
    end if;
  end if;

  for v_item in
    select value from jsonb_array_elements(coalesce(p_items,'[]'::jsonb))
  loop
    v_product_id := nullif(coalesce(v_item->>'product_id', v_item->>'id'), '')::uuid;
    v_qty := greatest(coalesce((v_item->>'qty')::integer, 0), 0);
    if v_product_id is null or v_qty <= 0 then
      raise exception 'invalid_item_payload';
    end if;

    select id, owner_id, seller_id, name, price, currency, quantity, images
    into v_product
    from public.products
    where id = v_product_id
    for update;

    if not found then
      raise exception 'product_not_found:%', v_product_id;
    end if;

    if coalesce(v_product.quantity, 0) < v_qty then
      raise exception 'insufficient_stock:%', v_product_id;
    end if;

    if v_seller is null then
      v_seller := coalesce(v_product.owner_id, v_product.seller_id);
    elsif v_seller <> coalesce(v_product.owner_id, v_product.seller_id) then
      raise exception 'multi_seller_checkout_not_allowed';
    end if;

    v_price := greatest(coalesce(v_product.price, 0), 0);
    v_total := v_total + (v_price * v_qty);
    v_currency := upper(coalesce(nullif(v_product.currency,''), v_currency, 'USD'));

    v_items_snapshot := v_items_snapshot || jsonb_build_array(
      jsonb_build_object(
        'id', v_product.id,
        'product_id', v_product.id,
        'owner_id', coalesce(v_product.owner_id, v_product.seller_id),
        'seller_id', coalesce(v_product.owner_id, v_product.seller_id),
        'name', coalesce(v_product.name,''),
        'qty', v_qty,
        'price', v_price,
        'currency', v_currency,
        'images', coalesce(v_product.images, '[]'::jsonb),
        'item_status', 'pending'
      )
    );
  end loop;

  if jsonb_array_length(v_items_snapshot) = 0 then
    raise exception 'order_items_required';
  end if;

  insert into public.orders (
    user_id,
    seller_id,
    email,
    buyer_name,
    buyer_address,
    address,
    items,
    total,
    amount,
    currency,
    status,
    payment_method,
    payment_status,
    client_request_id,
    created_at,
    updated_at
  )
  values (
    p_user_id,
    v_seller,
    coalesce(p_email,''),
    coalesce(nullif(trim(p_buyer_name),''),'Buyer'),
    p_buyer_address || jsonb_build_object('phone', v_phone),
    p_buyer_address || jsonb_build_object('phone', v_phone),
    v_items_snapshot,
    round(v_total::numeric, 2),
    round(v_total::numeric, 2),
    v_currency,
    'pending',
    'Pending Payment',
    'pending',
    nullif(trim(p_idempotency_key),''),
    now(),
    now()
  )
  returning * into v_order;

  insert into public.order_status_history (
    order_id,
    from_status,
    to_status,
    actor_user_id,
    actor_role,
    note,
    metadata
  )
  values (
    v_order.id,
    null,
    'pending',
    p_user_id,
    'buyer',
    'order_created',
    jsonb_build_object('source','create_pending_order_secure')
  );

  return jsonb_build_object('order', to_jsonb(v_order), 'idempotent', false);
end;
$$;

grant execute on function public.create_pending_order_secure(uuid,text,text,text,jsonb,jsonb,text,text,text) to authenticated;

-- ---------- SECURE STATUS TRANSITION + ATOMIC APPROVE DEDUCTION ----------
create or replace function public.transition_order_status_secure(
  p_order_id uuid,
  p_actor_user_id uuid,
  p_actor_role text,
  p_next_status text,
  p_courier_name text default null,
  p_tracking_number text default null,
  p_reject_reason text default null
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actor uuid := auth.uid();
  v_order public.orders%rowtype;
  v_next text := public.normalize_secure_order_status(p_next_status);
  v_current text;
  v_role text := lower(trim(coalesce(p_actor_role,'')));
  v_courier text := nullif(trim(coalesce(p_courier_name,'')), '');
  v_tracking text := nullif(trim(coalesce(p_tracking_number,'')), '');
  v_reject text := nullif(trim(coalesce(p_reject_reason,'')), '');
  v_item jsonb;
  v_product_id uuid;
  v_qty integer;
  v_available numeric;
  v_updated public.orders%rowtype;
begin
  if p_order_id is null or p_actor_user_id is null then
    raise exception 'order_id_and_actor_required';
  end if;
  if v_actor is not null and v_actor <> p_actor_user_id then
    raise exception 'actor_mismatch';
  end if;

  select * into v_order
  from public.orders
  where id = p_order_id
  for update;

  if not found then
    raise exception 'order_not_found';
  end if;

  if p_actor_user_id <> coalesce(v_order.user_id, p_actor_user_id)
     and p_actor_user_id <> coalesce(v_order.seller_id, p_actor_user_id) then
    raise exception 'order_access_forbidden';
  end if;

  if v_next = '' then
    raise exception 'next_status_required';
  end if;

  if v_role not in ('buyer','seller','admin','system') then
    v_role := null;
  end if;
  if p_actor_user_id = v_order.seller_id then
    v_role := coalesce(v_role, 'seller');
  end if;
  if p_actor_user_id = v_order.user_id then
    v_role := coalesce(v_role, 'buyer');
  end if;

  v_current := public.normalize_secure_order_status(v_order.status);
  if v_current = v_next then
    return jsonb_build_object('order', to_jsonb(v_order), 'status', v_current, 'idempotent', true);
  end if;
  if not public.is_valid_secure_transition(v_current, v_next) then
    raise exception 'invalid_order_transition:%->%', v_current, v_next;
  end if;

  if v_next in ('approved','rejected','shipped','delivered') then
    if v_order.seller_id is null or p_actor_user_id <> v_order.seller_id then
      raise exception 'seller_role_required';
    end if;
  end if;

  if v_next in ('completed','cancelled') then
    if v_order.user_id is null or p_actor_user_id <> v_order.user_id then
      raise exception 'buyer_role_required';
    end if;
  end if;

  if v_next = 'shipped' and (v_courier is null or v_tracking is null) then
    raise exception 'courier_and_tracking_required';
  end if;

  if v_next = 'approved' then
    for v_item in
      select value from jsonb_array_elements(coalesce(v_order.items, '[]'::jsonb))
    loop
      v_product_id := nullif(coalesce(v_item->>'id', v_item->>'product_id'), '')::uuid;
      v_qty := greatest(coalesce((v_item->>'qty')::integer, 0), 0);
      if v_product_id is null or v_qty <= 0 then
        continue;
      end if;

      select quantity into v_available
      from public.products
      where id = v_product_id
      for update;

      if v_available is null then
        raise exception 'product_not_found:%', v_product_id;
      end if;
      if v_available < v_qty then
        raise exception 'insufficient_stock:%', v_product_id;
      end if;

      update public.products
      set quantity = quantity - v_qty
      where id = v_product_id;
    end loop;
  end if;

  update public.orders
  set
    status = v_next,
    courier_name = case when v_next = 'shipped' then v_courier else courier_name end,
    tracking_number = case when v_next = 'shipped' then v_tracking else tracking_number end,
    reject_reason = case when v_next = 'rejected' then v_reject else reject_reason end,
    approved_at = case when v_next = 'approved' then now() else approved_at end,
    shipped_at = case when v_next = 'shipped' then now() else shipped_at end,
    delivered_at = case when v_next = 'delivered' then now() else delivered_at end,
    completed_at = case when v_next = 'completed' then now() else completed_at end,
    updated_at = now()
  where id = p_order_id
  returning * into v_updated;

  insert into public.order_status_history (
    order_id,
    from_status,
    to_status,
    actor_user_id,
    actor_role,
    note,
    metadata
  )
  values (
    v_updated.id,
    v_current,
    v_next,
    p_actor_user_id,
    coalesce(v_role,'system'),
    case
      when v_next = 'approved' then 'seller_approved'
      when v_next = 'rejected' then 'seller_rejected'
      when v_next = 'shipped' then 'shipment_dispatched'
      else 'status_transition'
    end,
    jsonb_build_object(
      'courier_name', v_courier,
      'tracking_number', v_tracking,
      'reject_reason', v_reject
    )
  );

  return jsonb_build_object('order', to_jsonb(v_updated), 'status', v_next);
end;
$$;

grant execute on function public.transition_order_status_secure(uuid,uuid,text,text,text,text,text) to authenticated;

-- ---------- SETTLEMENT BUFFER QUEUE ----------
create table if not exists public.order_settlement_queue (
  id uuid primary key default gen_random_uuid(),
  order_id uuid not null references public.orders(id) on delete cascade,
  payment_id text,
  payment_method text,
  usd_inr_rate numeric(12,6) not null default 0,
  status text not null default 'pending' check (status in ('pending','processed','failed')),
  available_at timestamptz not null,
  processed_at timestamptz,
  last_error text,
  attempt_count integer not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (order_id)
);

create index if not exists idx_order_settlement_queue_status_available
on public.order_settlement_queue (status, available_at asc);

-- ---------- PAYMENT RPC COMPATIBILITY ----------
drop function if exists public.settle_paid_order(uuid, text, text, numeric);
create or replace function public.settle_paid_order(
  p_order_id uuid,
  p_payment_id text default null,
  p_payment_method text default 'Online Payment',
  p_usd_inr_rate numeric default null
)
returns jsonb
language sql
security definer
set search_path = public
as $$
  select public.settle_paid_order_rpc(
    p_order_id,
    p_payment_id,
    p_payment_method,
    p_usd_inr_rate
  );
$$;

grant execute on function public.settle_paid_order(uuid, text, text, numeric) to authenticated;

-- ---------- OPTIONAL RLS HARDENING ----------
alter table if exists public.orders enable row level security;
alter table if exists public.order_status_history enable row level security;

drop policy if exists orders_buyer_read on public.orders;
create policy orders_buyer_read on public.orders
for select
using (user_id::text = auth.uid()::text);

drop policy if exists orders_seller_read on public.orders;
create policy orders_seller_read on public.orders
for select
using (seller_id::text = auth.uid()::text);

drop policy if exists order_status_history_party_read on public.order_status_history;
create policy order_status_history_party_read on public.order_status_history
for select
using (
  exists (
    select 1 from public.orders o
    where o.id = order_status_history.order_id
      and (o.user_id::text = auth.uid()::text or o.seller_id::text = auth.uid()::text)
  )
);
