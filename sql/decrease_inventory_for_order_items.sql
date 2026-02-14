-- Run this in Supabase SQL Editor.
-- Purpose: strict, atomic inventory reservation and restoration.

create or replace function public.decrease_inventory_for_order_items(
  p_items jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_item jsonb;
  v_product_id uuid;
  v_qty numeric;
  v_updated integer := 0;
  v_product record;
begin
  if jsonb_typeof(coalesce(p_items, '[]'::jsonb)) <> 'array' then
    raise exception 'p_items must be a json array';
  end if;

  for v_item in
    select value from jsonb_array_elements(coalesce(p_items, '[]'::jsonb))
  loop
    v_product_id := nullif(coalesce(v_item->>'id', v_item->>'product_id'), '')::uuid;
    v_qty := greatest(coalesce((v_item->>'qty')::numeric, 0), 0);

    if v_product_id is null or v_qty <= 0 then
      continue;
    end if;

    select id, coalesce(quantity, 0)::numeric as quantity
    into v_product
    from public.products
    where id = v_product_id
    for update;

    if not found then
      raise exception 'Product not found: %', v_product_id;
    end if;

    if v_product.quantity < v_qty then
      raise exception 'Insufficient stock for product % (available %, requested %)',
        v_product_id, v_product.quantity, v_qty;
    end if;

    update public.products
    set quantity = v_product.quantity - v_qty
    where id = v_product_id;

    v_updated := v_updated + 1;
  end loop;

  return jsonb_build_object(
    'ok', true,
    'updated_products', v_updated
  );
end;
$$;

grant execute on function public.decrease_inventory_for_order_items(jsonb) to authenticated;

create or replace function public.increase_inventory_for_order_items(
  p_items jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_item jsonb;
  v_product_id uuid;
  v_qty numeric;
  v_updated integer := 0;
begin
  if jsonb_typeof(coalesce(p_items, '[]'::jsonb)) <> 'array' then
    raise exception 'p_items must be a json array';
  end if;

  for v_item in
    select value from jsonb_array_elements(coalesce(p_items, '[]'::jsonb))
  loop
    v_product_id := nullif(coalesce(v_item->>'id', v_item->>'product_id'), '')::uuid;
    v_qty := greatest(coalesce((v_item->>'qty')::numeric, 0), 0);

    if v_product_id is null or v_qty <= 0 then
      continue;
    end if;

    update public.products
    set quantity = coalesce(quantity, 0) + v_qty
    where id = v_product_id;

    if found then
      v_updated := v_updated + 1;
    end if;
  end loop;

  return jsonb_build_object(
    'ok', true,
    'updated_products', v_updated
  );
end;
$$;

grant execute on function public.increase_inventory_for_order_items(jsonb) to authenticated;
