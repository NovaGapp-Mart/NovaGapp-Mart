-- Optional compatibility schema for reel interaction tables and chat call presence.
-- Run in Supabase SQL Editor if these objects do not already exist.

create extension if not exists pgcrypto;

alter table if exists public.users
  add column if not exists is_on_call boolean not null default false;

create table if not exists public.reel_likes (
  reel_id uuid not null references public.reels(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  reaction text not null default 'like' check (reaction in ('like', 'dislike')),
  created_at timestamptz not null default now(),
  primary key (reel_id, user_id)
);

create table if not exists public.reel_comments (
  id uuid primary key default gen_random_uuid(),
  reel_id uuid not null references public.reels(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  parent_comment_id uuid references public.reel_comments(id) on delete cascade,
  body text not null check (char_length(trim(body)) > 0),
  is_deleted boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_reel_likes_reel on public.reel_likes(reel_id, reaction);
create index if not exists idx_reel_comments_reel on public.reel_comments(reel_id, created_at desc);
create index if not exists idx_reel_comments_parent on public.reel_comments(parent_comment_id);

alter table public.reel_likes enable row level security;
alter table public.reel_comments enable row level security;

drop policy if exists reel_likes_read on public.reel_likes;
create policy reel_likes_read on public.reel_likes for select using (true);

drop policy if exists reel_likes_insert on public.reel_likes;
create policy reel_likes_insert on public.reel_likes for insert with check (auth.uid() = user_id);

drop policy if exists reel_likes_update on public.reel_likes;
create policy reel_likes_update on public.reel_likes for update using (auth.uid() = user_id);

drop policy if exists reel_likes_delete on public.reel_likes;
create policy reel_likes_delete on public.reel_likes for delete using (auth.uid() = user_id);

drop policy if exists reel_comments_read on public.reel_comments;
create policy reel_comments_read on public.reel_comments for select using (true);

drop policy if exists reel_comments_insert on public.reel_comments;
create policy reel_comments_insert on public.reel_comments for insert with check (auth.uid() = user_id);

drop policy if exists reel_comments_update on public.reel_comments;
create policy reel_comments_update on public.reel_comments for update using (auth.uid() = user_id);

drop policy if exists reel_comments_delete on public.reel_comments;
create policy reel_comments_delete on public.reel_comments for delete using (auth.uid() = user_id);
