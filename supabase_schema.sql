-- ForexAnalyzer Pro Supabase schema
-- Run this in Supabase Dashboard -> SQL Editor before starting the backend.

create table if not exists public.tradevault_kv_store (
  key text primary key,
  value jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now()
);

create table if not exists public.tradevault_account_settings (
  account_id text primary key,
  settings jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now()
);

create table if not exists public.tradevault_account_alerts (
  account_id text primary key,
  payload jsonb not null default '{"alerts":[]}'::jsonb,
  updated_at timestamptz not null default now()
);

create table if not exists public.tradevault_account_snapshots (
  account_id text primary key,
  owner_user_id uuid,
  config jsonb not null default '{}'::jsonb,
  raw_live_data jsonb,
  raw_static_data jsonb,
  processed_data jsonb,
  ea_status jsonb,
  push_counts jsonb not null default '{"live":0,"static":0,"status":0}'::jsonb,
  last_seen_ms bigint,
  last_settings_fetch_ms bigint,
  last_error jsonb,
  updated_at timestamptz not null default now()
);

alter table public.tradevault_account_snapshots
  add column if not exists owner_user_id uuid;

create index if not exists tradevault_account_snapshots_updated_at_idx
  on public.tradevault_account_snapshots (updated_at desc);

create table if not exists public.tradevault_user_profiles (
  user_id uuid primary key references auth.users(id) on delete cascade,
  email text,
  full_name text,
  avatar_url text,
  nickname text,
  updated_at timestamptz not null default now()
);

create table if not exists public.tradevault_user_trials (
  user_id uuid primary key references auth.users(id) on delete cascade,
  email text,
  plan_mode text not null default 'free',
  trial_started_at timestamptz not null default now(),
  trial_ends_at timestamptz not null default (now() + interval '30 days'),
  grace_ends_at timestamptz not null default (now() + interval '33 days'),
  paid_until timestamptz,
  device_id_hash text,
  device_first_seen_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.tradevault_user_trials
  add column if not exists email text,
  add column if not exists plan_mode text not null default 'free',
  add column if not exists trial_started_at timestamptz not null default now(),
  add column if not exists trial_ends_at timestamptz not null default (now() + interval '30 days'),
  add column if not exists grace_ends_at timestamptz not null default (now() + interval '33 days'),
  add column if not exists paid_until timestamptz,
  add column if not exists device_id_hash text,
  add column if not exists device_first_seen_at timestamptz,
  add column if not exists created_at timestamptz not null default now(),
  add column if not exists updated_at timestamptz not null default now();

create index if not exists tradevault_user_trials_device_id_hash_idx
  on public.tradevault_user_trials (device_id_hash)
  where device_id_hash is not null;

create index if not exists tradevault_user_trials_plan_idx
  on public.tradevault_user_trials (plan_mode, grace_ends_at);

create table if not exists public.tradevault_account_owners (
  account_id text primary key,
  user_id uuid not null references auth.users(id) on delete cascade,
  claimed_at timestamptz not null default now()
);

create index if not exists tradevault_account_owners_user_id_idx
  on public.tradevault_account_owners (user_id);

create table if not exists public.tradevault_account_deletions (
  account_id text not null,
  user_id uuid not null references auth.users(id) on delete cascade,
  source text not null default 'ea',
  deleted_at timestamptz not null default now(),
  primary key (account_id, user_id)
);

alter table public.tradevault_account_deletions
  add column if not exists source text not null default 'ea',
  add column if not exists deleted_at timestamptz not null default now();

create index if not exists tradevault_account_deletions_user_id_idx
  on public.tradevault_account_deletions (user_id, deleted_at desc);

create table if not exists public.tradevault_user_ea_keys (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  key_hash text not null unique,
  key_prefix text not null,
  name text not null default 'Default EA key',
  created_at timestamptz not null default now(),
  revoked_at timestamptz
);

create index if not exists tradevault_user_ea_keys_user_id_idx
  on public.tradevault_user_ea_keys (user_id);

create table if not exists public.tradevault_direct_mt_accounts (
  account_id text primary key,
  user_id uuid not null references auth.users(id) on delete cascade,
  metaapi_account_id text not null unique,
  platform text not null default 'mt5',
  login text not null,
  server text not null,
  account_name text,
  password_type text not null default 'investor',
  connection_status text not null default 'deploying',
  state text,
  last_sync_at timestamptz,
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  revoked_at timestamptz
);

create index if not exists tradevault_direct_mt_accounts_user_id_idx
  on public.tradevault_direct_mt_accounts (user_id, created_at desc);

create index if not exists tradevault_direct_mt_accounts_metaapi_idx
  on public.tradevault_direct_mt_accounts (metaapi_account_id);

create table if not exists public.tradevault_share_links (
  token text primary key,
  user_id uuid not null references auth.users(id) on delete cascade,
  account_id text not null,
  label text,
  snapshot jsonb,
  created_at timestamptz not null default now(),
  expires_at timestamptz,
  revoked_at timestamptz
);

create index if not exists tradevault_share_links_user_id_idx
  on public.tradevault_share_links (user_id, created_at desc);

create index if not exists tradevault_share_links_account_id_idx
  on public.tradevault_share_links (account_id);

alter table public.tradevault_kv_store enable row level security;
alter table public.tradevault_account_settings enable row level security;
alter table public.tradevault_account_alerts enable row level security;
alter table public.tradevault_account_snapshots enable row level security;
alter table public.tradevault_user_profiles enable row level security;
alter table public.tradevault_user_trials enable row level security;
alter table public.tradevault_account_owners enable row level security;
alter table public.tradevault_account_deletions enable row level security;
alter table public.tradevault_user_ea_keys enable row level security;
alter table public.tradevault_direct_mt_accounts enable row level security;
alter table public.tradevault_share_links enable row level security;

-- Admin/reporting helper.
-- View this in Supabase Table Editor to see each profile, linked MT5 account,
-- latest balance/equity, broker, role, trade count, and online snapshot status.
-- Keep the underlying tradevault_* table names for compatibility with existing data.
create or replace view public.forexanalyzer_client_overview as
select
  p.user_id,
  p.email,
  p.full_name,
  p.nickname,
  coalesce(t.plan_mode, 'free') as plan_mode,
  t.trial_started_at,
  t.trial_ends_at,
  t.grace_ends_at,
  t.paid_until,
  case
    when t.paid_until is not null and t.paid_until > now() then 'paid'
    when t.grace_ends_at is not null and t.grace_ends_at <= now() then 'expired'
    when t.trial_ends_at is not null and t.trial_ends_at <= now() then 'grace'
    else 'trial'
  end as license_status,
  o.account_id,
  coalesce(s.config ->> 'connectionMethod', s.config ->> 'source', 'ea') as connection_method,
  s.config ->> 'broker' as broker,
  coalesce(s.config ->> 'role', s.processed_data #>> '{meta,account_config,role}', 'STANDALONE') as account_role,
  case
    when (s.processed_data #>> '{account,balance}') ~ '^-?[0-9]+(\.[0-9]+)?$'
      then (s.processed_data #>> '{account,balance}')::numeric
    else null
  end as balance,
  case
    when (s.processed_data #>> '{account,equity}') ~ '^-?[0-9]+(\.[0-9]+)?$'
      then (s.processed_data #>> '{account,equity}')::numeric
    else null
  end as equity,
  case
    when jsonb_typeof(s.processed_data -> 'open_positions') = 'array'
      then jsonb_array_length(s.processed_data -> 'open_positions')
    else 0
  end as open_trades,
  case
    when jsonb_typeof(s.processed_data -> 'trade_history') = 'array'
      then jsonb_array_length(s.processed_data -> 'trade_history')
    else 0
  end as closed_trades,
  s.ea_status ->> 'status' as ea_status,
  case
    when s.last_seen_ms is not null then to_timestamp(s.last_seen_ms / 1000.0)
    else null
  end as last_seen_at,
  s.updated_at as snapshot_updated_at
from public.tradevault_user_profiles p
left join public.tradevault_user_trials t
  on t.user_id = p.user_id
left join public.tradevault_account_owners o
  on o.user_id = p.user_id
left join public.tradevault_account_snapshots s
  on s.account_id = o.account_id;

create or replace view public.forexanalyzer_deleted_accounts as
select
  d.account_id,
  d.user_id,
  p.email,
  p.full_name,
  d.source,
  d.deleted_at
from public.tradevault_account_deletions d
left join public.tradevault_user_profiles p
  on p.user_id = d.user_id;
