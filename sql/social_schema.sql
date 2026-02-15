-- Run this in Supabase SQL Editor

-- Buckets
insert into storage.buckets (id, name, public)
values
  ('reels', 'reels', true),
  ('long_videos', 'long_videos', true),
  ('posts', 'posts', true),
  ('stories', 'stories', true),
  ('thumbnails', 'thumbnails', true),
  ('chat_media', 'chat_media', true)
on conflict (id) do update
set public = excluded.public;

-- Content tables
create table if not exists public.reels (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  video_url text not null,
  thumb_url text,
  title text,
  description text,
  keywords text,
  duration integer,
  width integer,
  height integer,
  created_at timestamptz default now()
);

create table if not exists public.long_videos (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  video_url text not null,
  thumb_url text,
  title text,
  description text,
  keywords text,
  duration integer,
  width integer,
  height integer,
  created_at timestamptz default now()
);

create table if not exists public.posts (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  media_url text not null,
  media_type text not null check (media_type in ('image','video')),
  thumb_url text,
  title text,
  description text,
  keywords text,
  created_at timestamptz default now()
);

create table if not exists public.stories (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  media_url text not null,
  media_type text not null check (media_type in ('image','video')),
  thumb_url text,
  title text,
  description text,
  keywords text,
  created_at timestamptz default now(),
  expires_at timestamptz not null
);

create table if not exists public.reactions (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  target_type text not null check (target_type in ('reel','long','post')),
  target_id uuid not null,
  reaction text not null check (reaction in ('like','dislike')),
  created_at timestamptz default now(),
  unique (user_id, target_type, target_id)
);

create table if not exists public.comments (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  target_type text not null check (target_type in ('reel','long','post')),
  target_id uuid not null,
  body text not null,
  created_at timestamptz default now()
);

create table if not exists public.follows (
  follower_id uuid not null references auth.users(id) on delete cascade,
  following_id uuid not null references auth.users(id) on delete cascade,
  created_at timestamptz default now(),
  primary key (follower_id, following_id)
);

create table if not exists public.story_views (
  id uuid primary key default gen_random_uuid(),
  story_id uuid not null references public.stories(id) on delete cascade,
  viewer_id uuid not null references auth.users(id) on delete cascade,
  viewer_name text,
  created_at timestamptz default now(),
  unique (story_id, viewer_id)
);

-- Chats
create table if not exists public.chats (
  id uuid primary key default gen_random_uuid(),
  sender_id uuid not null references auth.users(id) on delete cascade,
  receiver_id uuid not null references auth.users(id) on delete cascade,
  message text,
  media_url text,
  media_type text default 'text' check (media_type in ('text','image','video')),
  created_at timestamptz default now()
);

create table if not exists public.chat_deletes (
  user_id uuid not null references auth.users(id) on delete cascade,
  other_id uuid not null references auth.users(id) on delete cascade,
  deleted_at timestamptz default now(),
  primary key (user_id, other_id)
);

alter table public.chats
  add column if not exists message text;
alter table public.chats
  add column if not exists media_url text;
alter table public.chats
  add column if not exists media_type text;
alter table public.chats
  add column if not exists created_at timestamptz;

alter table public.story_views
  add column if not exists viewer_name text;

alter table public.stories
  add column if not exists title text;
alter table public.stories
  add column if not exists description text;
alter table public.stories
  add column if not exists keywords text;

alter table public.users
  add column if not exists phone_number text;

create index if not exists idx_reels_user on public.reels(user_id);
create index if not exists idx_long_videos_user on public.long_videos(user_id);
create index if not exists idx_posts_user on public.posts(user_id);
create index if not exists idx_stories_user on public.stories(user_id);
create index if not exists idx_reactions_target on public.reactions(target_type, target_id);
create index if not exists idx_comments_target on public.comments(target_type, target_id);
create index if not exists idx_chats_pair on public.chats(sender_id, receiver_id, created_at);
create index if not exists idx_chats_receiver on public.chats(receiver_id, created_at);
create index if not exists idx_chat_deletes_user on public.chat_deletes(user_id, deleted_at);
create index if not exists idx_users_phone on public.users(phone_number);

-- RLS
alter table public.reels enable row level security;
alter table public.long_videos enable row level security;
alter table public.posts enable row level security;
alter table public.stories enable row level security;
alter table public.reactions enable row level security;
alter table public.comments enable row level security;
alter table public.follows enable row level security;
alter table public.story_views enable row level security;
alter table public.chats enable row level security;
alter table public.chat_deletes enable row level security;

-- Public read
create policy "reels_read" on public.reels for select using (true);
create policy "long_videos_read" on public.long_videos for select using (true);
create policy "posts_read" on public.posts for select using (true);
create policy "stories_read" on public.stories for select using (true);
create policy "reactions_read" on public.reactions for select using (true);
create policy "comments_read" on public.comments for select using (true);
create policy "follows_read" on public.follows for select using (true);
create policy "story_views_read" on public.story_views for select using (true);
create policy "chats_read" on public.chats for select
  using (auth.uid() = sender_id or auth.uid() = receiver_id);
create policy "chat_deletes_read" on public.chat_deletes for select
  using (auth.uid() = user_id);

-- Inserts (authenticated only)
create policy "reels_insert" on public.reels for insert
  with check (auth.uid() = user_id);
create policy "long_videos_insert" on public.long_videos for insert
  with check (auth.uid() = user_id);
create policy "posts_insert" on public.posts for insert
  with check (auth.uid() = user_id);
create policy "stories_insert" on public.stories for insert
  with check (auth.uid() = user_id);
create policy "reactions_insert" on public.reactions for insert
  with check (auth.uid() = user_id);
create policy "comments_insert" on public.comments for insert
  with check (auth.uid() = user_id);
create policy "follows_insert" on public.follows for insert
  with check (auth.uid() = follower_id);
create policy "story_views_insert" on public.story_views for insert
  with check (auth.uid() = viewer_id);
create policy "chats_insert" on public.chats for insert
  with check (auth.uid() = sender_id);
create policy "chat_deletes_insert" on public.chat_deletes for insert
  with check (auth.uid() = user_id);

-- Owner updates/deletes
create policy "reels_update" on public.reels for update
  using (auth.uid() = user_id);
create policy "reels_delete" on public.reels for delete
  using (auth.uid() = user_id);
create policy "long_videos_update" on public.long_videos for update
  using (auth.uid() = user_id);
create policy "long_videos_delete" on public.long_videos for delete
  using (auth.uid() = user_id);
create policy "posts_update" on public.posts for update
  using (auth.uid() = user_id);
create policy "posts_delete" on public.posts for delete
  using (auth.uid() = user_id);
create policy "stories_update" on public.stories for update
  using (auth.uid() = user_id);
create policy "stories_delete" on public.stories for delete
  using (auth.uid() = user_id);
create policy "reactions_delete" on public.reactions for delete
  using (auth.uid() = user_id);
create policy "comments_delete" on public.comments for delete
  using (auth.uid() = user_id);
create policy "follows_delete" on public.follows for delete
  using (auth.uid() = follower_id);
create policy "story_views_delete" on public.story_views for delete
  using (auth.uid() = viewer_id);
create policy "chats_delete" on public.chats for delete
  using (auth.uid() = sender_id);
create policy "chat_deletes_update" on public.chat_deletes for update
  using (auth.uid() = user_id);
create policy "chat_deletes_delete" on public.chat_deletes for delete
  using (auth.uid() = user_id);

grant select on public.long_videos to anon;
grant select on public.long_videos to authenticated;

-- Storage policies
alter table storage.objects enable row level security;

create policy "storage_read_reels" on storage.objects for select
  using (bucket_id = 'reels');
create policy "storage_read_long_videos" on storage.objects for select
  using (bucket_id = 'long_videos');
create policy "storage_read_posts" on storage.objects for select
  using (bucket_id = 'posts');
create policy "storage_read_stories" on storage.objects for select
  using (bucket_id = 'stories');
create policy "storage_read_thumbnails" on storage.objects for select
  using (bucket_id = 'thumbnails');
create policy "storage_read_chat_media" on storage.objects for select
  using (bucket_id = 'chat_media');

create policy "storage_insert_reels" on storage.objects for insert
  with check (bucket_id = 'reels' and auth.role() = 'authenticated');
create policy "storage_insert_long_videos" on storage.objects for insert
  with check (bucket_id = 'long_videos' and auth.role() = 'authenticated');
create policy "storage_insert_posts" on storage.objects for insert
  with check (bucket_id = 'posts' and auth.role() = 'authenticated');
create policy "storage_insert_stories" on storage.objects for insert
  with check (bucket_id = 'stories' and auth.role() = 'authenticated');
create policy "storage_insert_thumbnails" on storage.objects for insert
  with check (bucket_id = 'thumbnails' and auth.role() = 'authenticated');
create policy "storage_insert_chat_media" on storage.objects for insert
  with check (bucket_id = 'chat_media' and auth.role() = 'authenticated');

create policy "storage_delete_own" on storage.objects for delete
  using (auth.uid() = owner);
