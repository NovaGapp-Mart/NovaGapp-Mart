-- Supabase SQL Editor
-- RPC: Cancel exactly one order item for the buyer, restock inventory once, and recompute order status.

create or replace function public.cancel_order_item_rpc(
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
  v_item record;
  v_row jsonb;
  v_ref text;
  v_status text;
  v_next_status text;
  v_target_found boolean := false;
  v_target_already_cancelled boolean := false;
  v_target_product_id uuid := null;
  v_target_qty integer := 0;
  v_next_items jsonb := '[]'::jsonb;
  v_statuses text[] := array[]::text[];
  v_next_order_status text := 'pending_approval';
begin
  if v_actor is null then
    raise exception 'Authentication required';
  end if;
  if p_order_id is null or p_buyer_id is null or nullif(trim(p_item_ref), '') is null then
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

  if jsonb_typeof(coalesce(v_order.items, '[]'::jsonb)) <> 'array' then
    raise exception 'Order items payload is invalid';
  end if;

  for v_item in
    select value, ordinality
    from jsonb_array_elements(coalesce(v_order.items, '[]'::jsonb)) with ordinality
  loop
    v_row := v_item.value;
    v_ref := coalesce(
      nullif(v_row->>'item_ref', ''),
      coalesce(v_row->>'id', v_row->>'product_id', 'item') || '_' || v_item.ordinality::text
    );
    v_status := replace(
      lower(trim(coalesce(v_row->>'item_status', v_row->>'status', v_order.status, 'pending_approval'))),
      ' ',
      '_'
    );

    if v_ref = p_item_ref then
      v_target_found := true;

      if v_status = 'cancelled' then
        v_target_already_cancelled := true;
        v_next_status := 'cancelled';
      elsif v_status in ('pending_approval', 'pending', 'placed', 'approved', 'seller_approved') then
        v_next_status := 'cancelled';
        v_target_product_id := nullif(coalesce(v_row->>'id', v_row->>'product_id'), '')::uuid;
        v_target_qty := greatest(coalesce((v_row->>'qty')::integer, 0), 0);
      else
        raise exception 'Item cannot be cancelled in status %', v_status;
      end if;

      v_row := v_row || jsonb_build_object(
        'item_ref', v_ref,
        'item_status', v_next_status
      );
      v_statuses := array_append(v_statuses, v_next_status);
    else
      v_statuses := array_append(v_statuses, v_status);
    end if;

    v_next_items := v_next_items || jsonb_build_array(v_row);
  end loop;

  if not v_target_found then
    raise exception 'Order item not found';
  end if;

  if not v_target_already_cancelled and v_target_product_id is not null and v_target_qty > 0 then
    update public.products
    set quantity = coalesce(quantity, 0) + v_target_qty
    where id = v_target_product_id;
  end if;

  if exists (select 1 from unnest(v_statuses) s where s = 'delivered') then
    v_next_order_status := 'delivered';
  elsif exists (select 1 from unnest(v_statuses) s where s = 'out_for_delivery') then
    v_next_order_status := 'out_for_delivery';
  elsif exists (select 1 from unnest(v_statuses) s where s = 'hub_reached') then
    v_next_order_status := 'hub_reached';
  elsif exists (select 1 from unnest(v_statuses) s where s = 'shipped') then
    v_next_order_status := 'shipped';
  elsif exists (select 1 from unnest(v_statuses) s where s in ('approved', 'seller_approved')) then
    v_next_order_status := 'approved';
  elsif exists (select 1 from unnest(v_statuses) s where s in ('pending_approval', 'pending', 'placed', 'new_order', '')) then
    v_next_order_status := 'pending_approval';
  elsif not exists (select 1 from unnest(v_statuses) s where s <> 'cancelled') then
    v_next_order_status := 'cancelled';
  elsif not exists (select 1 from unnest(v_statuses) s where s not in ('cancelled', 'rejected', 'declined')) then
    v_next_order_status := 'rejected';
  else
    v_next_order_status := replace(lower(trim(coalesce(v_order.status, 'pending_approval'))), ' ', '_');
  end if;

  begin
    update public.orders
    set
      items = v_next_items,
      status = v_next_order_status
    where id = p_order_id;
  exception
    when check_violation then
      update public.orders
      set
        items = v_next_items,
        status = 'pending_approval'
      where id = p_order_id;
      v_next_order_status := 'pending_approval';
  end;

  if to_regprocedure('public.sync_order_items_from_order(uuid)') is not null then
    perform public.sync_order_items_from_order(p_order_id);
  end if;

  return jsonb_build_object(
    'ok', true,
    'order_id', p_order_id,
    'item_ref', p_item_ref,
    'status', v_next_order_status,
    'changed', (not v_target_already_cancelled),
    'restocked_qty', case when v_target_already_cancelled then 0 else v_target_qty end
  );
end;
$$;

grant execute on function public.cancel_order_item_rpc(uuid, uuid, text) to authenticated;
