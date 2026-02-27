-- ============================================================
-- NOVAGAPP m-videos ecosystem schema + security + RPC
-- Run in Supabase SQL Editor.
-- ============================================================

create extension if not exists pgcrypto;

create table if not exists public.video_monetization_config (
  id integer primary key check (id = 1),
  revenue_per_1000_views numeric(12,6) not null default 2.500000,
  updated_at timestamptz not null default now()
);

insert into public.video_monetization_config (id, revenue_per_1000_views)
values (1, 2.500000)
on conflict (id) do nothing;

create table if not exists public.videos (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  title text not null check (char_length(trim(title)) > 0),
  description text not null default '',
  video_url text not null,
  thumbnail_url text,
  category text not null default 'General',
  tags text not null default '',
  duration_seconds integer not null default 0 check (duration_seconds >= 0),
  views bigint not null default 0 check (views >= 0),
  likes_count integer not null default 0 check (likes_count >= 0),
  dislikes_count integer not null default 0 check (dislikes_count >= 0),
  monetized boolean not null default false,
  created_at timestamptz not null default now()
);

alter table if exists public.videos add column if not exists category text not null default 'General';
alter table if exists public.videos add column if not exists tags text not null default '';
alter table if exists public.videos add column if not exists duration_seconds integer not null default 0;
alter table if exists public.videos add column if not exists views bigint not null default 0;
alter table if exists public.videos add column if not exists likes_count integer not null default 0;
alter table if exists public.videos add column if not exists dislikes_count integer not null default 0;
alter table if exists public.videos add column if not exists monetized boolean not null default false;
alter table if exists public.videos add column if not exists created_at timestamptz not null default now();

create table if not exists public.video_likes (
  video_id uuid not null references public.videos(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  type text not null check (type in ('like','dislike')),
  created_at timestamptz not null default now(),
  primary key (video_id, user_id)
);

create table if not exists public.video_comments (
  id uuid primary key default gen_random_uuid(),
  video_id uuid not null references public.videos(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  parent_comment_id uuid references public.video_comments(id) on delete cascade,
  comment_text text not null check (char_length(trim(comment_text)) > 0),
  likes integer not null default 0 check (likes >= 0),
  created_at timestamptz not null default now()
);

alter table if exists public.video_comments add column if not exists parent_comment_id uuid references public.video_comments(id) on delete cascade;
alter table if exists public.video_comments add column if not exists likes integer not null default 0;

create table if not exists public.channel_subscribers (
  channel_id uuid not null references auth.users(id) on delete cascade,
  subscriber_user_id uuid not null references auth.users(id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (channel_id, subscriber_user_id),
  constraint channel_subscribers_no_self check (channel_id <> subscriber_user_id)
);

create table if not exists public.channel_members (
  channel_id uuid not null references auth.users(id) on delete cascade,
  member_user_id uuid not null references auth.users(id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (channel_id, member_user_id),
  constraint channel_members_no_self check (channel_id <> member_user_id)
);

create table if not exists public.channel_membership_plans (
  channel_id uuid primary key references auth.users(id) on delete cascade,
  join_fee_inr integer not null default 0 check (join_fee_inr >= 0),
  currency text not null default 'INR',
  updated_at timestamptz not null default now()
);

create table if not exists public.video_earnings (
  id bigserial primary key,
  video_id uuid not null references public.videos(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  views_count integer not null default 0 check (views_count >= 0),
  ad_revenue numeric(12,6) not null default 0 check (ad_revenue >= 0),
  created_at timestamptz not null default now()
);

create table if not exists public.video_view_events (
  id bigserial primary key,
  video_id uuid not null references public.videos(id) on delete cascade,
  viewer_user_id uuid not null references auth.users(id) on delete cascade,
  viewed_at timestamptz not null default now()
);

create table if not exists public.video_comment_likes (
  comment_id uuid not null references public.video_comments(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (comment_id, user_id)
);

alter table if exists public.users add column if not exists channel_subscribers_count integer not null default 0;

create index if not exists idx_videos_user on public.videos(user_id);
create index if not exists idx_videos_created_at on public.videos(created_at desc);
create index if not exists idx_videos_views on public.videos(views desc);
create index if not exists idx_videos_category on public.videos(category);
create index if not exists idx_video_likes_video_type on public.video_likes(video_id, type);
create index if not exists idx_video_comments_video on public.video_comments(video_id, created_at);
create index if not exists idx_video_comments_parent on public.video_comments(parent_comment_id);
create index if not exists idx_channel_subscribers_channel on public.channel_subscribers(channel_id);
create index if not exists idx_channel_members_channel on public.channel_members(channel_id);
create index if not exists idx_channel_membership_plans_fee on public.channel_membership_plans(join_fee_inr);
create index if not exists idx_video_earnings_user on public.video_earnings(user_id, created_at desc);
create index if not exists idx_video_view_events_user_video on public.video_view_events(viewer_user_id, video_id, viewed_at desc);

create or replace function public.video_set_user_subscriber_count(
  p_channel_id uuid,
  p_count integer
)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  if to_regclass('public.users') is null then
    return;
  end if;

  begin
    execute 'update public.users set channel_subscribers_count = $1 where user_id = $2'
      using greatest(coalesce(p_count,0), 0), p_channel_id;
  exception when undefined_column then
    begin
      execute 'update public.users set channel_subscribers_count = $1 where id = $2'
        using greatest(coalesce(p_count,0), 0), p_channel_id;
    exception when undefined_column then
      return;
    end;
  end;
end;
$$;

create or replace function public.video_sync_like_counts_trg()
returns trigger
language plpgsql
set search_path = public
as $$
declare
  v_video_id uuid := coalesce(new.video_id, old.video_id);
begin
  if v_video_id is null then
    return null;
  end if;

  update public.videos v
  set
    likes_count = (
      select count(*)::integer
      from public.video_likes l
      where l.video_id = v_video_id and l.type = 'like'
    ),
    dislikes_count = (
      select count(*)::integer
      from public.video_likes l
      where l.video_id = v_video_id and l.type = 'dislike'
    )
  where v.id = v_video_id;

  return null;
end;
$$;

drop trigger if exists trg_video_sync_like_counts on public.video_likes;
create trigger trg_video_sync_like_counts
after insert or update or delete on public.video_likes
for each row execute function public.video_sync_like_counts_trg();

create or replace function public.video_sync_comment_like_counts_trg()
returns trigger
language plpgsql
set search_path = public
as $$
declare
  v_comment_id uuid := coalesce(new.comment_id, old.comment_id);
begin
  if v_comment_id is null then
    return null;
  end if;

  update public.video_comments c
  set likes = (
    select count(*)::integer
    from public.video_comment_likes l
    where l.comment_id = v_comment_id
  )
  where c.id = v_comment_id;

  return null;
end;
$$;

drop trigger if exists trg_video_sync_comment_like_counts on public.video_comment_likes;
create trigger trg_video_sync_comment_like_counts
after insert or update or delete on public.video_comment_likes
for each row execute function public.video_sync_comment_like_counts_trg();

create or replace function public.video_sync_subscriber_counts_trg()
returns trigger
language plpgsql
set search_path = public
as $$
declare
  v_channel_id uuid := coalesce(new.channel_id, old.channel_id);
  v_count integer := 0;
begin
  if v_channel_id is null then
    return null;
  end if;

  select count(*)::integer
  into v_count
  from public.channel_subscribers s
  where s.channel_id = v_channel_id;

  perform public.video_set_user_subscriber_count(v_channel_id, v_count);
  return null;
end;
$$;

drop trigger if exists trg_video_sync_subscriber_counts on public.channel_subscribers;
create trigger trg_video_sync_subscriber_counts
after insert or delete on public.channel_subscribers
for each row execute function public.video_sync_subscriber_counts_trg();

drop function if exists public.video_toggle_reaction_rpc(uuid, text);
create or replace function public.video_toggle_reaction_rpc(
  p_video_id uuid,
  p_type text
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actor uuid := auth.uid();
  v_existing text;
  v_like bigint := 0;
  v_dislike bigint := 0;
  v_reaction text := null;
begin
  if v_actor is null then
    raise exception 'Authentication required';
  end if;
  if p_video_id is null then
    raise exception 'video_id is required';
  end if;
  if p_type not in ('like', 'dislike') then
    raise exception 'Invalid reaction type';
  end if;

  perform pg_advisory_xact_lock(hashtextextended(v_actor::text || ':' || p_video_id::text, 43));

  perform 1 from public.videos where id = p_video_id;
  if not found then
    raise exception 'Video not found';
  end if;

  select type into v_existing
  from public.video_likes
  where video_id = p_video_id and user_id = v_actor
  for update;

  if v_existing is null then
    insert into public.video_likes(video_id, user_id, type)
    values (p_video_id, v_actor, p_type);
    v_reaction := p_type;
  elsif v_existing = p_type then
    delete from public.video_likes
    where video_id = p_video_id and user_id = v_actor;
    v_reaction := null;
  else
    update public.video_likes
    set type = p_type
    where video_id = p_video_id and user_id = v_actor;
    v_reaction := p_type;
  end if;

  select likes_count, dislikes_count
  into v_like, v_dislike
  from public.videos
  where id = p_video_id;

  return jsonb_build_object(
    'ok', true,
    'reaction', v_reaction,
    'likes_count', coalesce(v_like, 0),
    'dislikes_count', coalesce(v_dislike, 0)
  );
end;
$$;

drop function if exists public.video_record_view_rpc(uuid);
create or replace function public.video_record_view_rpc(
  p_video_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actor uuid := auth.uid();
  v_owner uuid;
  v_monetized boolean := false;
  v_last_view timestamptz;
  v_views bigint := 0;
  v_rpm numeric(12,6) := 0;
  v_revenue numeric(12,6) := 0;
begin
  if v_actor is null then
    raise exception 'Authentication required';
  end if;
  if p_video_id is null then
    raise exception 'video_id is required';
  end if;

  perform pg_advisory_xact_lock(hashtextextended(v_actor::text || ':' || p_video_id::text, 42));

  select user_id, monetized
  into v_owner, v_monetized
  from public.videos
  where id = p_video_id
  for update;

  if not found then
    raise exception 'Video not found';
  end if;

  select viewed_at
  into v_last_view
  from public.video_view_events
  where video_id = p_video_id and viewer_user_id = v_actor
  order by viewed_at desc
  limit 1;

  if v_last_view is null or now() - v_last_view >= interval '10 seconds' then
    insert into public.video_view_events(video_id, viewer_user_id, viewed_at)
    values (p_video_id, v_actor, now());

    update public.videos as vv
    set views = vv.views + 1
    where vv.id = p_video_id
    returning vv.views into v_views;

    if v_monetized then
      select revenue_per_1000_views
      into v_rpm
      from public.video_monetization_config
      where id = 1;
      v_revenue := greatest(coalesce(v_rpm, 0), 0) / 1000.0;

      insert into public.video_earnings(video_id, user_id, views_count, ad_revenue, created_at)
      values (p_video_id, v_owner, 1, v_revenue, now());
    end if;

    return jsonb_build_object(
      'ok', true,
      'counted', true,
      'views', coalesce(v_views, 0),
      'revenue_added', coalesce(v_revenue, 0)
    );
  end if;

  select vv.views into v_views
  from public.videos vv
  where vv.id = p_video_id;

  return jsonb_build_object(
    'ok', true,
    'counted', false,
    'views', coalesce(v_views, 0),
    'revenue_added', 0
  );
end;
$$;

drop function if exists public.video_toggle_subscribe_rpc(uuid);
create or replace function public.video_toggle_subscribe_rpc(
  p_channel_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actor uuid := auth.uid();
  v_subscribed boolean := false;
  v_count integer := 0;
begin
  if v_actor is null then
    raise exception 'Authentication required';
  end if;
  if p_channel_id is null then
    raise exception 'channel_id is required';
  end if;
  if p_channel_id = v_actor then
    raise exception 'Self subscribe is not allowed';
  end if;

  perform pg_advisory_xact_lock(hashtextextended(v_actor::text || ':' || p_channel_id::text, 44));

  if exists (
    select 1
    from public.channel_subscribers
    where channel_id = p_channel_id and subscriber_user_id = v_actor
  ) then
    delete from public.channel_subscribers
    where channel_id = p_channel_id and subscriber_user_id = v_actor;
    v_subscribed := false;
  else
    insert into public.channel_subscribers(channel_id, subscriber_user_id)
    values (p_channel_id, v_actor)
    on conflict do nothing;
    v_subscribed := true;
  end if;

  select count(*)::integer
  into v_count
  from public.channel_subscribers
  where channel_id = p_channel_id;

  perform public.video_set_user_subscriber_count(p_channel_id, v_count);

  return jsonb_build_object(
    'ok', true,
    'subscribed', v_subscribed,
    'subscribers_count', coalesce(v_count, 0)
  );
end;
$$;

drop function if exists public.video_toggle_comment_like_rpc(uuid);
create or replace function public.video_toggle_comment_like_rpc(
  p_comment_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actor uuid := auth.uid();
  v_liked boolean := false;
  v_likes integer := 0;
begin
  if v_actor is null then
    raise exception 'Authentication required';
  end if;
  if p_comment_id is null then
    raise exception 'comment_id is required';
  end if;

  perform pg_advisory_xact_lock(hashtextextended(v_actor::text || ':' || p_comment_id::text, 45));

  perform 1 from public.video_comments where id = p_comment_id;
  if not found then
    raise exception 'Comment not found';
  end if;

  if exists (
    select 1
    from public.video_comment_likes
    where comment_id = p_comment_id and user_id = v_actor
  ) then
    delete from public.video_comment_likes
    where comment_id = p_comment_id and user_id = v_actor;
    v_liked := false;
  else
    insert into public.video_comment_likes(comment_id, user_id)
    values (p_comment_id, v_actor)
    on conflict do nothing;
    v_liked := true;
  end if;

  select likes
  into v_likes
  from public.video_comments
  where id = p_comment_id;

  return jsonb_build_object(
    'ok', true,
    'liked', v_liked,
    'likes_count', coalesce(v_likes, 0)
  );
end;
$$;

drop function if exists public.video_creator_dashboard_rpc(uuid);
create or replace function public.video_creator_dashboard_rpc(
  p_owner_id uuid default null
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actor uuid := auth.uid();
  v_owner uuid;
  v_total_views bigint := 0;
  v_total_revenue numeric(12,6) := 0;
  v_revenue_30d numeric(12,6) := 0;
  v_rpm numeric(12,6) := 0;
  v_default_rpm numeric(12,6) := 0;
begin
  if v_actor is null then
    raise exception 'Authentication required';
  end if;

  v_owner := coalesce(p_owner_id, v_actor);
  if v_owner <> v_actor then
    raise exception 'Permission denied';
  end if;

  select coalesce(sum(views), 0)
  into v_total_views
  from public.videos
  where user_id = v_owner;

  select coalesce(sum(ad_revenue), 0)
  into v_total_revenue
  from public.video_earnings
  where user_id = v_owner;

  select coalesce(sum(ad_revenue), 0)
  into v_revenue_30d
  from public.video_earnings
  where user_id = v_owner
    and created_at >= now() - interval '30 days';

  select coalesce(revenue_per_1000_views, 0)
  into v_default_rpm
  from public.video_monetization_config
  where id = 1;

  if coalesce(v_total_views, 0) > 0 then
    v_rpm := (coalesce(v_total_revenue, 0) * 1000.0) / v_total_views;
  else
    v_rpm := coalesce(v_default_rpm, 0);
  end if;

  return jsonb_build_object(
    'ok', true,
    'owner_id', v_owner,
    'total_views', coalesce(v_total_views, 0),
    'total_revenue', coalesce(v_total_revenue, 0),
    'rpm', coalesce(v_rpm, 0),
    'estimated_monthly_earnings', coalesce(v_revenue_30d, 0)
  );
end;
$$;

grant execute on function public.video_toggle_reaction_rpc(uuid, text) to authenticated;
grant execute on function public.video_record_view_rpc(uuid) to authenticated;
grant execute on function public.video_toggle_subscribe_rpc(uuid) to authenticated;
grant execute on function public.video_toggle_comment_like_rpc(uuid) to authenticated;
grant execute on function public.video_creator_dashboard_rpc(uuid) to authenticated;

alter table public.videos enable row level security;
alter table public.video_likes enable row level security;
alter table public.video_comments enable row level security;
alter table public.channel_subscribers enable row level security;
alter table public.channel_members enable row level security;
alter table public.channel_membership_plans enable row level security;
alter table public.video_earnings enable row level security;
alter table public.video_view_events enable row level security;
alter table public.video_comment_likes enable row level security;
alter table public.video_monetization_config enable row level security;

drop policy if exists videos_read_public on public.videos;
drop policy if exists videos_insert_own on public.videos;
drop policy if exists videos_delete_own on public.videos;
create policy videos_read_public on public.videos
  for select using (true);
create policy videos_insert_own on public.videos
  for insert with check (auth.uid() = user_id);
create policy videos_delete_own on public.videos
  for delete using (auth.uid() = user_id);

drop policy if exists video_likes_read_public on public.video_likes;
create policy video_likes_read_public on public.video_likes
  for select using (true);

drop policy if exists video_comments_read_public on public.video_comments;
drop policy if exists video_comments_insert_own on public.video_comments;
drop policy if exists video_comments_delete_own on public.video_comments;
create policy video_comments_read_public on public.video_comments
  for select using (true);
create policy video_comments_insert_own on public.video_comments
  for insert with check (auth.uid() = user_id);
create policy video_comments_delete_own on public.video_comments
  for delete using (auth.uid() = user_id);

drop policy if exists video_comment_likes_read_public on public.video_comment_likes;
create policy video_comment_likes_read_public on public.video_comment_likes
  for select using (true);

drop policy if exists channel_subscribers_read_public on public.channel_subscribers;
create policy channel_subscribers_read_public on public.channel_subscribers
  for select using (true);

drop policy if exists channel_members_read_public on public.channel_members;
drop policy if exists channel_members_insert_own on public.channel_members;
drop policy if exists channel_members_delete_own on public.channel_members;
create policy channel_members_read_public on public.channel_members
  for select using (true);
create policy channel_members_insert_own on public.channel_members
  for insert with check (auth.uid() = member_user_id and channel_id <> member_user_id);
create policy channel_members_delete_own on public.channel_members
  for delete using (auth.uid() = member_user_id);

drop policy if exists channel_membership_plans_read_public on public.channel_membership_plans;
drop policy if exists channel_membership_plans_upsert_own on public.channel_membership_plans;
create policy channel_membership_plans_read_public on public.channel_membership_plans
  for select using (true);
create policy channel_membership_plans_upsert_own on public.channel_membership_plans
  for all using (auth.uid() = channel_id) with check (auth.uid() = channel_id);

drop policy if exists video_earnings_select_own on public.video_earnings;
create policy video_earnings_select_own on public.video_earnings
  for select using (auth.uid() = user_id);

drop policy if exists video_view_events_select_own on public.video_view_events;
create policy video_view_events_select_own on public.video_view_events
  for select using (auth.uid() = viewer_user_id);

drop policy if exists video_monetization_config_read on public.video_monetization_config;
create policy video_monetization_config_read on public.video_monetization_config
  for select using (true);

grant select on public.videos to anon, authenticated;
grant insert, delete on public.videos to authenticated;
grant select on public.video_likes to anon, authenticated;
grant select on public.video_comments to anon, authenticated;
grant insert, delete on public.video_comments to authenticated;
grant select on public.channel_subscribers to anon, authenticated;
grant select on public.channel_members to anon, authenticated;
grant insert, delete on public.channel_members to authenticated;
grant select on public.channel_membership_plans to anon, authenticated;
grant insert, update on public.channel_membership_plans to authenticated;
grant select on public.video_earnings to authenticated;
grant select on public.video_view_events to authenticated;
grant select on public.video_comment_likes to anon, authenticated;
grant select on public.video_monetization_config to anon, authenticated;

insert into storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
values
  (
    'long_videos',
    'long_videos',
    true,
    1073741824,
    array['video/mp4','video/webm','video/quicktime','video/x-matroska','video/ogg','video/mpeg']::text[]
  ),
  (
    'thumbnails',
    'thumbnails',
    true,
    8388608,
    array['image/jpeg','image/png','image/webp']::text[]
  )
on conflict (id) do update
set
  public = excluded.public,
  file_size_limit = excluded.file_size_limit,
  allowed_mime_types = excluded.allowed_mime_types;

drop policy if exists long_videos_public_read on storage.objects;
drop policy if exists long_videos_owner_insert on storage.objects;
drop policy if exists long_videos_owner_update on storage.objects;
drop policy if exists long_videos_owner_delete on storage.objects;
create policy long_videos_public_read on storage.objects
  for select using (bucket_id = 'long_videos');
create policy long_videos_owner_insert on storage.objects
  for insert to authenticated
  with check (
    bucket_id = 'long_videos'
    and (storage.foldername(name))[1] = auth.uid()::text
  );
create policy long_videos_owner_update on storage.objects
  for update to authenticated
  using (
    bucket_id = 'long_videos'
    and (storage.foldername(name))[1] = auth.uid()::text
  )
  with check (
    bucket_id = 'long_videos'
    and (storage.foldername(name))[1] = auth.uid()::text
  );
create policy long_videos_owner_delete on storage.objects
  for delete to authenticated
  using (
    bucket_id = 'long_videos'
    and (storage.foldername(name))[1] = auth.uid()::text
  );

drop policy if exists thumbnails_public_read on storage.objects;
drop policy if exists thumbnails_owner_insert on storage.objects;
drop policy if exists thumbnails_owner_update on storage.objects;
drop policy if exists thumbnails_owner_delete on storage.objects;
create policy thumbnails_public_read on storage.objects
  for select using (bucket_id = 'thumbnails');
create policy thumbnails_owner_insert on storage.objects
  for insert to authenticated
  with check (
    bucket_id = 'thumbnails'
    and (storage.foldername(name))[1] = auth.uid()::text
  );
create policy thumbnails_owner_update on storage.objects
  for update to authenticated
  using (
    bucket_id = 'thumbnails'
    and (storage.foldername(name))[1] = auth.uid()::text
  )
  with check (
    bucket_id = 'thumbnails'
    and (storage.foldername(name))[1] = auth.uid()::text
  );
create policy thumbnails_owner_delete on storage.objects
  for delete to authenticated
  using (
    bucket_id = 'thumbnails'
    and (storage.foldername(name))[1] = auth.uid()::text
  );

do $$
begin
  begin
    alter publication supabase_realtime add table public.videos;
  exception when duplicate_object then null;
  when undefined_object then null;
  end;
  begin
    alter publication supabase_realtime add table public.video_likes;
  exception when duplicate_object then null;
  when undefined_object then null;
  end;
  begin
    alter publication supabase_realtime add table public.video_comments;
  exception when duplicate_object then null;
  when undefined_object then null;
  end;
  begin
    alter publication supabase_realtime add table public.channel_subscribers;
  exception when duplicate_object then null;
  when undefined_object then null;
  end;
  begin
    alter publication supabase_realtime add table public.channel_members;
  exception when duplicate_object then null;
  when undefined_object then null;
  end;
  begin
    alter publication supabase_realtime add table public.channel_membership_plans;
  exception when duplicate_object then null;
  when undefined_object then null;
  end;
  begin
    alter publication supabase_realtime add table public.video_earnings;
  exception when duplicate_object then null;
  when undefined_object then null;
  end;
end $$;
