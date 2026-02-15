-- Feed visibility fix
-- Run this in Supabase SQL Editor (once).
-- Goal: feed items should be world-readable (anon + authenticated).

create extension if not exists pgcrypto;

begin;

alter table public.posts enable row level security;
alter table public.reels enable row level security;
alter table public.long_videos enable row level security;
alter table public.stories enable row level security;
alter table public.reactions enable row level security;
alter table public.comments enable row level security;

create table if not exists public.post_shares (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  target_type text not null check (target_type in ('post')),
  target_id uuid not null,
  channel text,
  created_at timestamptz not null default now()
);

create index if not exists idx_post_shares_target
  on public.post_shares(target_type, target_id, created_at desc);
create index if not exists idx_post_shares_user
  on public.post_shares(user_id, created_at desc);

alter table public.post_shares enable row level security;

-- Remove old SELECT policies that may restrict rows to only owner.
do $$
declare p record;
begin
  for p in
    select tablename, policyname
    from pg_policies
    where schemaname = 'public'
      and tablename in ('posts', 'reels', 'long_videos', 'stories', 'reactions', 'comments', 'post_shares')
      and cmd = 'SELECT'
  loop
    execute format('drop policy if exists %I on public.%I', p.policyname, p.tablename);
  end loop;
end
$$;

-- Allow everyone to read feed content.
create policy posts_read_public
  on public.posts
  for select
  using (true);

create policy reels_read_public
  on public.reels
  for select
  using (true);

create policy long_videos_read_public
  on public.long_videos
  for select
  using (true);

create policy stories_read_public
  on public.stories
  for select
  using (true);

create policy reactions_read_public
  on public.reactions
  for select
  using (true);

create policy comments_read_public
  on public.comments
  for select
  using (true);

create policy post_shares_read_public
  on public.post_shares
  for select
  using (true);

drop policy if exists post_shares_insert_authenticated on public.post_shares;
create policy post_shares_insert_authenticated
  on public.post_shares
  for insert
  with check (auth.uid() = user_id);

grant select on public.posts to authenticated;
grant select on public.reels to authenticated;
grant select on public.long_videos to authenticated;
grant select on public.stories to authenticated;
grant select on public.reactions to authenticated;
grant select on public.comments to authenticated;
grant select, insert on public.post_shares to authenticated;
grant select on public.posts to anon;
grant select on public.reels to anon;
grant select on public.long_videos to anon;
grant select on public.stories to anon;
grant select on public.reactions to anon;
grant select on public.comments to anon;
grant select on public.post_shares to anon;

-- Ensure realtime stream includes interaction tables.
do $$
begin
  begin
    execute 'alter publication supabase_realtime add table public.reactions';
  exception when duplicate_object then null;
  end;
  begin
    execute 'alter publication supabase_realtime add table public.comments';
  exception when duplicate_object then null;
  end;
  begin
    execute 'alter publication supabase_realtime add table public.post_shares';
  exception when duplicate_object then null;
  end;
end
$$;

commit;
