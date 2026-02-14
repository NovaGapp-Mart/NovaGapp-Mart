-- Run this in Supabase SQL Editor.
-- Purpose: settle paid orders with seller payout + platform commission credit.
-- Prerequisite: run sql/order_items_schema_and_flows.sql first.

create extension if not exists pgcrypto;

create table if not exists public.platform_commissions (
  id bigserial primary key,
  order_id uuid not null,
  seller_id uuid not null,
  payment_id text null,
  payment_method text null,
  currency text not null default 'INR',
  gross_amount numeric not null default 0,
  commission_amount numeric not null default 0,
  seller_net_amount numeric not null default 0,
  created_at timestamptz not null default now(),
  unique (order_id, seller_id)
);

create table if not exists public.platform_wallet_accounts (
  id bigserial primary key,
  code text not null unique,
  currency text not null default 'INR',
  balance numeric not null default 0,
  total_commission numeric not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.platform_wallet_transactions (
  id bigserial primary key,
  account_code text not null references public.platform_wallet_accounts(code),
  order_id uuid not null,
  seller_id uuid not null,
  amount numeric not null default 0,
  currency text not null default 'INR',
  reason text null,
  created_at timestamptz not null default now(),
  unique (order_id, seller_id)
);

insert into public.platform_wallet_accounts (code, currency, balance, total_commission)
values ('PRIMARY', 'INR', 0, 0)
on conflict (code) do nothing;

create or replace function public.normalize_order_item_status(p_status text)
returns text
language sql
immutable
as $$
  select replace(lower(trim(coalesce(p_status, 'pending_approval'))), ' ', '_');
$$;

drop function if exists public.settle_order(uuid, text, numeric);
drop function if exists public.settle_paid_order(uuid, text, numeric);
drop function if exists public.settle_paid_order(uuid, text, text, numeric);

create or replace function public.settle_paid_order_rpc(
  p_order_id uuid,
  p_payment_id text default null,
  p_payment_method text default 'Online Payment',
  p_usd_inr_rate numeric default null
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actor uuid := auth.uid();
  v_order record;
  v_row record;
  v_currency text := 'INR';
  v_usd_inr_rate numeric;
  v_gross_inr numeric;
  v_commission_inr numeric;
  v_seller_net_inr numeric;
  v_source_count integer := 0;
  v_credited integer := 0;
  v_skipped integer := 0;
  v_total_gross numeric := 0;
  v_total_commission numeric := 0;
  v_total_seller_net numeric := 0;
begin
  if v_actor is null then
    raise exception 'Authentication required';
  end if;
  v_usd_inr_rate := coalesce(p_usd_inr_rate, 0);
  if v_usd_inr_rate <= 0 then
    raise exception 'Live USD/INR rate required for settlement';
  end if;

  select *
  into v_order
  from public.orders
  where id = p_order_id
  for update;

  if not found then
    raise exception 'Order not found: %', p_order_id;
  end if;
  if v_actor <> v_order.user_id then
    raise exception 'Permission denied';
  end if;

  update public.orders
  set
    payment_status = 'paid',
    payment_id = coalesce(p_payment_id, payment_id),
    payment_method = coalesce(nullif(p_payment_method, ''), payment_method)
  where id = p_order_id;

  select count(*)
  into v_source_count
  from public.order_items oi
  where oi.order_id = p_order_id
    and public.normalize_order_item_status(oi.item_status) not in ('cancelled', 'rejected', 'declined');

  for v_row in
    with seller_totals as (
      select
        oi.seller_id,
        sum((oi.quantity * oi.unit_price)::numeric) as gross_usd
      from public.order_items oi
      where oi.order_id = p_order_id
        and public.normalize_order_item_status(oi.item_status) not in ('cancelled', 'rejected', 'declined')
      group by oi.seller_id
    )
    select st.seller_id, st.gross_usd
    from seller_totals st
    where v_source_count > 0

    union all

    select
      t.seller_id,
      sum(t.gross_usd)::numeric as gross_usd
    from (
      select
        coalesce(
          nullif(i->>'owner_id', '')::uuid,
          nullif(i->>'seller_id', '')::uuid
        ) as seller_id,
        (greatest(coalesce((i->>'qty')::numeric, 0), 0) *
         greatest(coalesce((i->>'price')::numeric, 0), 0))::numeric as gross_usd,
        public.normalize_order_item_status(coalesce(i->>'item_status', i->>'status', v_order.status)) as item_status
      from jsonb_array_elements(coalesce(v_order.items, '[]'::jsonb)) i
    ) t
    where v_source_count = 0
      and t.seller_id is not null
      and t.gross_usd > 0
      and t.item_status not in ('cancelled', 'rejected', 'declined')
    group by t.seller_id
  loop
    if exists (
      select 1
      from public.platform_commissions pc
      where pc.order_id = p_order_id
        and pc.seller_id = v_row.seller_id
    ) then
      v_skipped := v_skipped + 1;
      continue;
    end if;

    v_gross_inr := round((coalesce(v_row.gross_usd, 0) * v_usd_inr_rate)::numeric, 2);
    v_commission_inr := round((v_gross_inr * 0.05)::numeric, 2);
    v_seller_net_inr := round((v_gross_inr - v_commission_inr)::numeric, 2);

    if v_seller_net_inr <= 0 then
      continue;
    end if;

    insert into public.sellers (user_id, wallet, total_sales)
    values (v_row.seller_id, 0, 0)
    on conflict (user_id) do nothing;

    update public.sellers s
    set
      wallet = coalesce(s.wallet, 0) + v_seller_net_inr,
      total_sales = coalesce(s.total_sales, 0) + v_seller_net_inr
    where s.user_id = v_row.seller_id;

    insert into public.wallet_transactions (user_id, type, amount, reason)
    values (
      v_row.seller_id,
      'credit',
      v_seller_net_inr,
      'Online payment credited for order ' || p_order_id::text ||
      ' (gross INR ' || to_char(v_gross_inr, 'FM9999999990.00') ||
      ', commission INR ' || to_char(v_commission_inr, 'FM9999999990.00') || ')'
    );

    insert into public.notifications (
      receiver_user_id,
      seller_id,
      type,
      title,
      message,
      order_id,
      payment_method,
      is_read,
      is_deleted
    )
    values (
      v_row.seller_id,
      v_row.seller_id,
      'wallet_credit',
      'Wallet Credited',
      to_char(v_seller_net_inr, 'FM9999999990.00') || ' ' || v_currency ||
      ' credited for paid order ' || p_order_id::text ||
      '. (5% commission: ' || to_char(v_commission_inr, 'FM9999999990.00') || ' ' || v_currency || ')',
      p_order_id,
      coalesce(p_payment_method, 'Online Payment'),
      false,
      false
    );

    insert into public.platform_commissions (
      order_id,
      seller_id,
      payment_id,
      payment_method,
      currency,
      gross_amount,
      commission_amount,
      seller_net_amount
    )
    values (
      p_order_id,
      v_row.seller_id,
      p_payment_id,
      coalesce(p_payment_method, 'Online Payment'),
      v_currency,
      v_gross_inr,
      v_commission_inr,
      v_seller_net_inr
    )
    on conflict (order_id, seller_id) do update
    set
      payment_id = excluded.payment_id,
      payment_method = excluded.payment_method,
      currency = excluded.currency,
      gross_amount = excluded.gross_amount,
      commission_amount = excluded.commission_amount,
      seller_net_amount = excluded.seller_net_amount;

    update public.platform_wallet_accounts pwa
    set
      balance = coalesce(pwa.balance, 0) + v_commission_inr,
      total_commission = coalesce(pwa.total_commission, 0) + v_commission_inr,
      updated_at = now()
    where pwa.code = 'PRIMARY';

    insert into public.platform_wallet_transactions (
      account_code,
      order_id,
      seller_id,
      amount,
      currency,
      reason
    )
    values (
      'PRIMARY',
      p_order_id,
      v_row.seller_id,
      v_commission_inr,
      v_currency,
      'Commission credited for order ' || p_order_id::text
    )
    on conflict (order_id, seller_id) do nothing;

    v_credited := v_credited + 1;
    v_total_gross := v_total_gross + v_gross_inr;
    v_total_commission := v_total_commission + v_commission_inr;
    v_total_seller_net := v_total_seller_net + v_seller_net_inr;
  end loop;

  return jsonb_build_object(
    'ok', true,
    'order_id', p_order_id,
    'credited_sellers', v_credited,
    'skipped_sellers', v_skipped,
    'usd_inr_rate', v_usd_inr_rate,
    'gross_amount', round(v_total_gross::numeric, 2),
    'commission_amount', round(v_total_commission::numeric, 2),
    'credited_amount', round(v_total_seller_net::numeric, 2),
    'platform_credited', round(v_total_commission::numeric, 2)
  );
end;
$$;

grant execute on function public.settle_paid_order_rpc(uuid, text, text, numeric) to authenticated;
