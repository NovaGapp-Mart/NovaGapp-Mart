-- Run this in Supabase SQL Editor.
-- Purpose: production-grade item-level order flow for multi-vendor commerce.

create extension if not exists pgcrypto;

create table if not exists public.order_items (
  id uuid primary key default gen_random_uuid(),
  order_id uuid not null references public.orders(id) on delete cascade,
  item_ref text not null,
  product_id uuid not null,
  seller_id uuid not null,
  buyer_id uuid not null,
  quantity integer not null check (quantity > 0),
  unit_price numeric not null default 0,
  currency text not null default 'USD',
  item_status text not null default 'pending_approval',
  gross_amount numeric not null default 0,
  commission_rate numeric not null default 0.05,
  commission_amount numeric not null default 0,
  seller_net_amount numeric not null default 0,
  cancelled_at timestamptz null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (order_id, item_ref)
);

create index if not exists order_items_order_idx on public.order_items(order_id);
create index if not exists order_items_seller_idx on public.order_items(seller_id);
create index if not exists order_items_status_idx on public.order_items(item_status);

create or replace function public.normalize_order_item_status(p_status text)
returns text
language sql
immutable
as $$
  select replace(lower(trim(coalesce(p_status, 'pending_approval'))), ' ', '_');
$$;

create or replace function public.try_uuid(p_text text)
returns uuid
language plpgsql
immutable
as $$
begin
  if nullif(trim(coalesce(p_text, '')), '') is null then
    return null;
  end if;
  return trim(p_text)::uuid;
exception
  when invalid_text_representation then
    return null;
end;
$$;

create or replace function public.try_int(p_text text)
returns integer
language plpgsql
immutable
as $$
begin
  if nullif(trim(coalesce(p_text, '')), '') is null then
    return null;
  end if;
  return trim(p_text)::integer;
exception
  when invalid_text_representation then
    return null;
end;
$$;

create or replace function public.try_numeric(p_text text)
returns numeric
language plpgsql
immutable
as $$
begin
  if nullif(trim(coalesce(p_text, '')), '') is null then
    return null;
  end if;
  return trim(p_text)::numeric;
exception
  when invalid_text_representation then
    return null;
end;
$$;

create or replace function public.derive_order_status_from_items(
  p_items jsonb,
  p_fallback text default 'pending_approval'
)
returns text
language plpgsql
immutable
as $$
declare
  v_statuses text[];
begin
  if jsonb_typeof(coalesce(p_items, '[]'::jsonb)) <> 'array'
     or jsonb_array_length(coalesce(p_items, '[]'::jsonb)) = 0 then
    return public.normalize_order_item_status(p_fallback);
  end if;

  select array_agg(
    public.normalize_order_item_status(
      coalesce(value->>'item_status', value->>'status', p_fallback)
    )
  )
  into v_statuses
  from jsonb_array_elements(coalesce(p_items, '[]'::jsonb));

  if exists (select 1 from unnest(v_statuses) s where s = 'delivered') then
    return 'delivered';
  end if;
  if exists (select 1 from unnest(v_statuses) s where s = 'out_for_delivery') then
    return 'out_for_delivery';
  end if;
  if exists (select 1 from unnest(v_statuses) s where s = 'hub_reached') then
    return 'hub_reached';
  end if;
  if exists (select 1 from unnest(v_statuses) s where s = 'shipped') then
    return 'shipped';
  end if;
  if exists (select 1 from unnest(v_statuses) s where s in ('approved', 'seller_approved')) then
    return 'approved';
  end if;
  if exists (
    select 1
    from unnest(v_statuses) s
    where s in ('pending_approval', 'pending', 'placed', 'new_order', '')
  ) then
    return 'pending_approval';
  end if;
  if not exists (
    select 1 from unnest(v_statuses) s where s <> 'cancelled'
  ) then
    return 'cancelled';
  end if;
  if not exists (
    select 1
    from unnest(v_statuses) s
    where s not in ('rejected', 'declined', 'cancelled')
  ) then
    return 'rejected';
  end if;

  return public.normalize_order_item_status(p_fallback);
end;
$$;

create or replace function public.can_transition_order_item(
  p_current text,
  p_next text
)
returns boolean
language plpgsql
immutable
as $$
declare
  v_current text := public.normalize_order_item_status(p_current);
  v_next text := public.normalize_order_item_status(p_next);
begin
  if v_next in ('approved', 'rejected') then
    return v_current in ('pending_approval', 'pending', 'placed', 'new_order', '');
  end if;
  if v_next = 'shipped' then
    return v_current in ('approved', 'seller_approved');
  end if;
  if v_next = 'hub_reached' then
    return v_current = 'shipped';
  end if;
  if v_next = 'out_for_delivery' then
    return v_current = 'hub_reached';
  end if;
  if v_next = 'delivered' then
    return v_current = 'out_for_delivery';
  end if;
  if v_next in ('return_approved', 'return_rejected') then
    return v_current = 'return_requested';
  end if;
  return false;
end;
$$;

create or replace function public.sync_order_items_from_order(
  p_order_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actor uuid := auth.uid();
  v_order record;
  v_item jsonb;
  v_idx integer := 0;
  v_item_ref text;
  v_product_id uuid;
  v_seller_id uuid;
  v_qty integer;
  v_price numeric;
  v_status text;
  v_refs text[] := array[]::text[];
  v_gross numeric;
  v_commission numeric;
  v_net numeric;
begin
  if v_actor is null then
    raise exception 'Authentication required';
  end if;

  select *
  into v_order
  from public.orders
  where id = p_order_id;

  if not found then
    raise exception 'Order not found: %', p_order_id;
  end if;

  if v_actor <> v_order.user_id and not exists (
    select 1
    from jsonb_array_elements(coalesce(v_order.items, '[]'::jsonb)) x
    where coalesce(public.try_uuid(x->>'owner_id'), public.try_uuid(x->>'seller_id')) = v_actor
  ) then
    raise exception 'Permission denied';
  end if;

  if jsonb_typeof(coalesce(v_order.items, '[]'::jsonb)) <> 'array' then
    delete from public.order_items where order_id = p_order_id;
    return jsonb_build_object('ok', true, 'items_synced', 0);
  end if;

  for v_item in
    select value from jsonb_array_elements(coalesce(v_order.items, '[]'::jsonb))
  loop
    v_idx := v_idx + 1;
    v_product_id := public.try_uuid(coalesce(v_item->>'id', v_item->>'product_id'));
    v_qty := greatest(coalesce(public.try_int(v_item->>'qty'), 0), 0);
    v_price := greatest(coalesce(public.try_numeric(v_item->>'price'), 0), 0);
    v_seller_id := coalesce(
      public.try_uuid(v_item->>'owner_id'),
      public.try_uuid(v_item->>'seller_id')
    );
    v_item_ref := coalesce(
      nullif(v_item->>'item_ref', ''),
      coalesce(v_item->>'id', v_item->>'product_id', 'item') || '_' || v_idx::text
    );

    if v_product_id is null or v_qty <= 0 or v_seller_id is null then
      continue;
    end if;

    v_status := public.normalize_order_item_status(
      coalesce(v_item->>'item_status', v_item->>'status', v_order.status)
    );
    v_gross := round((v_qty * v_price)::numeric, 2);
    v_commission := round((v_gross * 0.05)::numeric, 2);
    v_net := round((v_gross - v_commission)::numeric, 2);
    v_refs := array_append(v_refs, v_item_ref);

    insert into public.order_items (
      order_id,
      item_ref,
      product_id,
      seller_id,
      buyer_id,
      quantity,
      unit_price,
      currency,
      item_status,
      gross_amount,
      commission_rate,
      commission_amount,
      seller_net_amount,
      cancelled_at,
      updated_at
    )
    values (
      p_order_id,
      v_item_ref,
      v_product_id,
      v_seller_id,
      v_order.user_id,
      v_qty,
      v_price,
      coalesce(nullif(v_order.currency, ''), 'USD'),
      v_status,
      v_gross,
      0.05,
      v_commission,
      v_net,
      case when v_status = 'cancelled' then now() else null end,
      now()
    )
    on conflict (order_id, item_ref) do update
    set
      product_id = excluded.product_id,
      seller_id = excluded.seller_id,
      buyer_id = excluded.buyer_id,
      quantity = excluded.quantity,
      unit_price = excluded.unit_price,
      currency = excluded.currency,
      item_status = excluded.item_status,
      gross_amount = excluded.gross_amount,
      commission_rate = excluded.commission_rate,
      commission_amount = excluded.commission_amount,
      seller_net_amount = excluded.seller_net_amount,
      cancelled_at = excluded.cancelled_at,
      updated_at = now();
  end loop;

  if array_length(v_refs, 1) is null then
    delete from public.order_items where order_id = p_order_id;
  else
    delete from public.order_items
    where order_id = p_order_id
      and not (item_ref = any(v_refs));
  end if;

  return jsonb_build_object(
    'ok', true,
    'items_synced', coalesce(array_length(v_refs, 1), 0)
  );
end;
$$;

grant execute on function public.sync_order_items_from_order(uuid) to authenticated;

create or replace function public.trg_sync_order_items_after_order_write()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  perform public.sync_order_items_from_order(new.id);
  return new;
end;
$$;

drop trigger if exists trg_sync_order_items_after_order_write on public.orders;
create trigger trg_sync_order_items_after_order_write
after insert or update of items, status, currency, user_id on public.orders
for each row
execute function public.trg_sync_order_items_after_order_write();

create or replace function public.create_order_with_items(
  p_user_id uuid,
  p_email text,
  p_address jsonb,
  p_items jsonb,
  p_total numeric default null,
  p_currency text default 'USD',
  p_status text default 'placed',
  p_payment_method text default 'Pending Payment',
  p_payment_status text default 'pending'
)
returns public.orders
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actor uuid := auth.uid();
  v_item jsonb;
  v_product record;
  v_product_id uuid;
  v_seller_id uuid;
  v_qty integer;
  v_price numeric;
  v_item_ref text;
  v_items jsonb := '[]'::jsonb;
  v_total numeric := 0;
  v_order public.orders%rowtype;
begin
  if v_actor is null then
    raise exception 'Authentication required';
  end if;
  if p_user_id is null then
    raise exception 'User id is required';
  end if;
  if v_actor <> p_user_id then
    raise exception 'Permission denied';
  end if;
  if jsonb_typeof(coalesce(p_items, '[]'::jsonb)) <> 'array' then
    raise exception 'p_items must be a json array';
  end if;

  for v_item in
    select value from jsonb_array_elements(coalesce(p_items, '[]'::jsonb))
  loop
    v_product_id := public.try_uuid(coalesce(v_item->>'id', v_item->>'product_id'));
    v_qty := greatest(coalesce(public.try_int(v_item->>'qty'), 0), 0);

    if v_product_id is null or v_qty <= 0 then
      raise exception 'Invalid order item payload';
    end if;

    select *
    into v_product
    from public.products
    where id = v_product_id
    for update;

    if not found then
      raise exception 'Product not found: %', v_product_id;
    end if;

    if coalesce(public.try_numeric(to_jsonb(v_product)->>'quantity'), 0) < v_qty then
      raise exception 'Insufficient stock for product %', v_product_id;
    end if;

    v_seller_id := coalesce(
      public.try_uuid(to_jsonb(v_product)->>'owner_id'),
      public.try_uuid(to_jsonb(v_product)->>'seller_id'),
      public.try_uuid(v_item->>'owner_id'),
      public.try_uuid(v_item->>'seller_id')
    );
    if v_seller_id is null then
      raise exception 'Seller mapping missing for product %', v_product_id;
    end if;

    v_price := greatest(
      coalesce(
        public.try_numeric(to_jsonb(v_product)->>'price'),
        0
      ),
      0
    );
    v_item_ref := coalesce(nullif(v_item->>'item_ref', ''), encode(gen_random_bytes(12), 'hex'));

    update public.products
    set quantity = coalesce(public.try_numeric(to_jsonb(v_product)->>'quantity'), 0) - v_qty
    where id = v_product_id;

    v_total := v_total + (v_price * v_qty);
    v_items := v_items || jsonb_build_array(
      v_item || jsonb_build_object(
        'item_ref', v_item_ref,
        'id', v_product_id,
        'product_id', v_product_id,
        'owner_id', v_seller_id,
        'seller_id', v_seller_id,
        'qty', v_qty,
        'price', v_price,
        'currency', coalesce(nullif(v_item->>'currency', ''), p_currency, 'USD'),
        'item_status', public.normalize_order_item_status(coalesce(v_item->>'item_status', 'pending_approval'))
      )
    );
  end loop;

  if jsonb_array_length(v_items) = 0 then
    raise exception 'Order must include at least one valid item';
  end if;
  if p_total is not null and abs(p_total - round(v_total::numeric, 2)) > 0.01 then
    raise exception 'Order total mismatch with current catalog pricing';
  end if;

  insert into public.orders (
    user_id,
    email,
    address,
    items,
    total,
    currency,
    status,
    payment_method,
    payment_status
  )
  values (
    p_user_id,
    coalesce(p_email, ''),
    coalesce(p_address, '{}'::jsonb),
    v_items,
    round(v_total::numeric, 2),
    coalesce(nullif(p_currency, ''), 'USD'),
    public.normalize_order_item_status(coalesce(p_status, 'placed')),
    coalesce(nullif(p_payment_method, ''), 'Pending Payment'),
    coalesce(nullif(p_payment_status, ''), 'pending')
  )
  returning * into v_order;

  perform public.sync_order_items_from_order(v_order.id);
  return v_order;
end;
$$;

grant execute on function public.create_order_with_items(uuid, text, jsonb, jsonb, numeric, text, text, text, text) to authenticated;

create or replace function public.cancel_order_item(
  p_order_id uuid,
  p_buyer_id uuid,
  p_item_ref text
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actor uuid := auth.uid();
  v_order record;
  v_item jsonb;
  v_next_items jsonb := '[]'::jsonb;
  v_ref text;
  v_found boolean := false;
  v_product_id uuid;
  v_qty integer;
  v_status text;
  v_next_order_status text;
begin
  if v_actor is null then
    raise exception 'Authentication required';
  end if;
  if p_order_id is null or p_buyer_id is null or coalesce(trim(p_item_ref), '') = '' then
    raise exception 'order_id, buyer_id and item_ref are required';
  end if;
  if v_actor <> p_buyer_id then
    raise exception 'Permission denied';
  end if;

  select *
  into v_order
  from public.orders
  where id = p_order_id
    and user_id = p_buyer_id
  for update;

  if not found then
    raise exception 'Order not found or access denied';
  end if;

  for v_item in
    select value from jsonb_array_elements(coalesce(v_order.items, '[]'::jsonb))
  loop
    v_ref := coalesce(nullif(v_item->>'item_ref', ''), coalesce(v_item->>'id', v_item->>'product_id', 'item'));
    v_status := public.normalize_order_item_status(
      coalesce(v_item->>'item_status', v_item->>'status', v_order.status)
    );

    if v_ref = p_item_ref then
      if v_status not in ('pending_approval', 'pending', 'placed', 'approved', 'seller_approved') then
        raise exception 'Item can no longer be cancelled';
      end if;
      v_found := true;
      v_product_id := nullif(coalesce(v_item->>'id', v_item->>'product_id'), '')::uuid;
      v_qty := greatest(coalesce((v_item->>'qty')::integer, 0), 0);
      if v_product_id is not null and v_qty > 0 then
        update public.products
        set quantity = coalesce(quantity, 0) + v_qty
        where id = v_product_id;
      end if;
      v_next_items := v_next_items || jsonb_build_array(
        v_item || jsonb_build_object('item_ref', p_item_ref, 'item_status', 'cancelled')
      );
    else
      v_next_items := v_next_items || jsonb_build_array(v_item);
    end if;
  end loop;

  if not v_found then
    raise exception 'Order item not found';
  end if;

  v_next_order_status := public.derive_order_status_from_items(v_next_items, v_order.status);

  begin
    update public.orders
    set
      items = v_next_items,
      status = v_next_order_status
    where id = p_order_id;
  exception when check_violation then
    update public.orders
    set
      items = v_next_items,
      status = 'pending_approval'
    where id = p_order_id;
    v_next_order_status := 'pending_approval';
  end;

  perform public.sync_order_items_from_order(p_order_id);
  return jsonb_build_object(
    'ok', true,
    'order_id', p_order_id,
    'status', v_next_order_status,
    'item_ref', p_item_ref
  );
end;
$$;

grant execute on function public.cancel_order_item(uuid, uuid, text) to authenticated;

create or replace function public.seller_set_order_item_status(
  p_order_id uuid,
  p_seller_id uuid,
  p_next_status text
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actor uuid := auth.uid();
  v_order record;
  v_item jsonb;
  v_next_items jsonb := '[]'::jsonb;
  v_changed integer := 0;
  v_seller_match boolean;
  v_current text;
  v_next text := public.normalize_order_item_status(p_next_status);
  v_ref text;
  v_product_id uuid;
  v_qty integer;
  v_next_order_status text;
begin
  if v_actor is null then
    raise exception 'Authentication required';
  end if;
  if p_order_id is null or p_seller_id is null then
    raise exception 'order_id and seller_id are required';
  end if;
  if v_actor <> p_seller_id then
    raise exception 'Permission denied';
  end if;

  select *
  into v_order
  from public.orders
  where id = p_order_id
  for update;

  if not found then
    raise exception 'Order not found';
  end if;

  for v_item in
    select value from jsonb_array_elements(coalesce(v_order.items, '[]'::jsonb))
  loop
    v_seller_match := coalesce(
      nullif(v_item->>'owner_id', '')::uuid,
      nullif(v_item->>'seller_id', '')::uuid
    ) = p_seller_id;

    if not v_seller_match then
      v_next_items := v_next_items || jsonb_build_array(v_item);
      continue;
    end if;

    v_current := public.normalize_order_item_status(
      coalesce(v_item->>'item_status', v_item->>'status', v_order.status)
    );
    if not public.can_transition_order_item(v_current, v_next) then
      v_next_items := v_next_items || jsonb_build_array(v_item);
      continue;
    end if;

    v_changed := v_changed + 1;
    v_ref := coalesce(nullif(v_item->>'item_ref', ''), coalesce(v_item->>'id', v_item->>'product_id', 'item'));

    if v_next in ('rejected', 'return_approved') and v_current not in ('cancelled', 'rejected', 'declined') then
      v_product_id := nullif(coalesce(v_item->>'id', v_item->>'product_id'), '')::uuid;
      v_qty := greatest(coalesce((v_item->>'qty')::integer, 0), 0);
      if v_product_id is not null and v_qty > 0 then
        update public.products
        set quantity = coalesce(quantity, 0) + v_qty
        where id = v_product_id;
      end if;
    end if;

    v_next_items := v_next_items || jsonb_build_array(
      v_item || jsonb_build_object('item_ref', v_ref, 'item_status', v_next)
    );
  end loop;

  if v_changed = 0 then
    raise exception 'No eligible item found for this seller/status transition';
  end if;

  v_next_order_status := public.derive_order_status_from_items(v_next_items, v_order.status);

  begin
    update public.orders
    set
      items = v_next_items,
      status = v_next_order_status
    where id = p_order_id;
  exception when check_violation then
    update public.orders
    set
      items = v_next_items,
      status = 'pending_approval'
    where id = p_order_id;
    v_next_order_status := 'pending_approval';
  end;

  perform public.sync_order_items_from_order(p_order_id);
  return jsonb_build_object(
    'ok', true,
    'order_id', p_order_id,
    'seller_id', p_seller_id,
    'changed_items', v_changed,
    'status', v_next_order_status,
    'buyer_id', v_order.user_id
  );
end;
$$;

grant execute on function public.seller_set_order_item_status(uuid, uuid, text) to authenticated;

drop function if exists public.seller_set_order_item_status_rpc(uuid, uuid, text);
create or replace function public.seller_set_order_item_status_rpc(
  p_order_id uuid,
  p_seller_id uuid,
  p_next_status text
)
returns jsonb
language sql
security definer
set search_path = public
as $$
  select public.seller_set_order_item_status(
    p_order_id,
    p_seller_id,
    p_next_status
  );
$$;

grant execute on function public.seller_set_order_item_status_rpc(uuid, uuid, text) to authenticated;
