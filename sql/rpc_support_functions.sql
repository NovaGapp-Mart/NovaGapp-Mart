-- Supabase SQL Editor
-- Canonical RPC support functions used by frontend pages.

create extension if not exists pgcrypto;

create table if not exists public.withdraw_requests (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null,
  amount numeric not null check (amount > 0),
  status text not null default 'pending',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.wallet_transactions (
  id bigserial primary key,
  user_id uuid not null,
  type text not null check (type in ('credit','debit')),
  amount numeric not null default 0,
  reason text null,
  created_at timestamptz not null default now()
);

alter table if exists public.sellers add column if not exists wallet numeric not null default 0;
alter table if exists public.sellers add column if not exists total_sales numeric not null default 0;
alter table if exists public.sellers add column if not exists is_paid boolean not null default false;
alter table if exists public.sellers add column if not exists seller_plan text null;
alter table if exists public.sellers add column if not exists show_badge boolean not null default false;
alter table if exists public.sellers add column if not exists paid_at timestamptz null;

alter table if exists public.reels add column if not exists likes integer not null default 0;
alter table if exists public.reels add column if not exists dislikes integer not null default 0;

drop function if exists public.request_withdraw_rpc(uuid, numeric);
create or replace function public.request_withdraw_rpc(
  p_user_id uuid,
  p_amount numeric
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actor uuid := auth.uid();
  v_seller record;
begin
  if v_actor is null then
    raise exception 'Authentication required';
  end if;
  if p_user_id is null or p_amount is null or p_amount <= 0 then
    raise exception 'Invalid withdraw payload';
  end if;
  if v_actor <> p_user_id then
    raise exception 'Permission denied';
  end if;

  insert into public.sellers (user_id, wallet, total_sales)
  values (p_user_id, 0, 0)
  on conflict (user_id) do nothing;

  select user_id, coalesce(wallet, 0) as wallet
  into v_seller
  from public.sellers
  where user_id = p_user_id
  for update;

  if not found then
    raise exception 'Seller row not found';
  end if;
  if v_seller.wallet < p_amount then
    raise exception 'Insufficient wallet balance';
  end if;

  insert into public.withdraw_requests (user_id, amount, status)
  values (p_user_id, p_amount, 'pending');

  return jsonb_build_object(
    'ok', true,
    'user_id', p_user_id,
    'amount', p_amount,
    'status', 'pending'
  );
end;
$$;

grant execute on function public.request_withdraw_rpc(uuid, numeric) to authenticated;

drop function if exists public.decrease_wallet_rpc(uuid, numeric);
create or replace function public.decrease_wallet_rpc(
  p_user_id uuid,
  p_amount numeric
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actor uuid := auth.uid();
  v_is_admin boolean := false;
begin
  if v_actor is null then
    raise exception 'Authentication required';
  end if;
  if p_user_id is null or p_amount is null or p_amount <= 0 then
    raise exception 'Invalid wallet deduction payload';
  end if;

  if v_actor <> p_user_id then
    if to_regclass('public.users') is not null then
      select exists(
        select 1
        from public.users u
        where coalesce(
          nullif(to_jsonb(u)->>'user_id', '')::uuid,
          nullif(to_jsonb(u)->>'id', '')::uuid
        ) = v_actor
          and (
            lower(coalesce(to_jsonb(u)->>'role', '')) in ('admin', 'super_admin')
            or coalesce((to_jsonb(u)->>'is_admin')::boolean, false) = true
          )
      )
      into v_is_admin;
    end if;

    if not v_is_admin then
      raise exception 'Permission denied';
    end if;
  end if;

  update public.sellers
  set wallet = coalesce(wallet, 0) - p_amount
  where user_id = p_user_id
    and coalesce(wallet, 0) >= p_amount;

  if not found then
    raise exception 'Insufficient wallet balance';
  end if;

  insert into public.wallet_transactions (user_id, type, amount, reason)
  values (p_user_id, 'debit', p_amount, 'Wallet debited via approved withdraw');

  return jsonb_build_object(
    'ok', true,
    'user_id', p_user_id,
    'amount', p_amount
  );
end;
$$;

grant execute on function public.decrease_wallet_rpc(uuid, numeric) to authenticated;

drop function if exists public.verify_seller_payment_rpc(uuid, text);
create or replace function public.verify_seller_payment_rpc(
  p_user_id uuid,
  p_payment_id text
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actor uuid := auth.uid();
begin
  if v_actor is null then
    raise exception 'Authentication required';
  end if;
  if p_user_id is null or nullif(trim(coalesce(p_payment_id, '')), '') is null then
    raise exception 'Invalid payment verification payload';
  end if;
  if v_actor <> p_user_id then
    raise exception 'Permission denied';
  end if;

  insert into public.sellers (user_id, wallet, total_sales, is_paid, seller_plan, show_badge, paid_at)
  values (p_user_id, 0, 0, true, 'active', true, now())
  on conflict (user_id) do update
  set
    is_paid = true,
    seller_plan = 'active',
    show_badge = true,
    paid_at = now();

  return jsonb_build_object(
    'ok', true,
    'user_id', p_user_id,
    'payment_id', p_payment_id
  );
end;
$$;

grant execute on function public.verify_seller_payment_rpc(uuid, text) to authenticated;

drop function if exists public.increment_reel_like_rpc(text);
create or replace function public.increment_reel_like_rpc(
  rid text
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
begin
  if nullif(trim(coalesce(rid, '')), '') is null then
    raise exception 'rid is required';
  end if;

  update public.reels
  set likes = coalesce(likes, 0) + 1
  where id::text = rid;

  if not found then
    raise exception 'Reel not found';
  end if;

  return jsonb_build_object('ok', true, 'rid', rid);
end;
$$;

grant execute on function public.increment_reel_like_rpc(text) to authenticated;

drop function if exists public.increment_reel_dislike_rpc(text);
create or replace function public.increment_reel_dislike_rpc(
  rid text
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
begin
  if nullif(trim(coalesce(rid, '')), '') is null then
    raise exception 'rid is required';
  end if;

  update public.reels
  set dislikes = coalesce(dislikes, 0) + 1
  where id::text = rid;

  if not found then
    raise exception 'Reel not found';
  end if;

  return jsonb_build_object('ok', true, 'rid', rid);
end;
$$;

grant execute on function public.increment_reel_dislike_rpc(text) to authenticated;

