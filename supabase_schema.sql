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
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.tradevault_user_profiles
  add column if not exists created_at timestamptz not null default now(),
  add column if not exists updated_at timestamptz not null default now();

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

create table if not exists public.tradevault_trial_identity_claims (
  claim_type text not null,
  claim_hash text not null,
  user_id uuid not null references auth.users(id) on delete cascade,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  metadata jsonb not null default '{}'::jsonb,
  primary key (claim_type, claim_hash)
);

alter table public.tradevault_trial_identity_claims
  add column if not exists user_id uuid references auth.users(id) on delete cascade,
  add column if not exists first_seen_at timestamptz not null default now(),
  add column if not exists last_seen_at timestamptz not null default now(),
  add column if not exists metadata jsonb not null default '{}'::jsonb;

create index if not exists tradevault_trial_identity_claims_user_idx
  on public.tradevault_trial_identity_claims (user_id, last_seen_at desc);

create index if not exists tradevault_trial_identity_claims_type_user_idx
  on public.tradevault_trial_identity_claims (claim_type, user_id);

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

create table if not exists public.tradevault_referral_codes (
  user_id uuid primary key references auth.users(id) on delete cascade,
  code text not null unique,
  created_at timestamptz not null default now()
);

create index if not exists tradevault_referral_codes_code_idx
  on public.tradevault_referral_codes (code);

create table if not exists public.tradevault_referrals (
  id uuid primary key default gen_random_uuid(),
  referrer_user_id uuid not null references auth.users(id) on delete cascade,
  referred_user_id uuid not null references auth.users(id) on delete cascade,
  referral_code text not null,
  awarded_days integer not null default 7,
  awarded_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  unique (referred_user_id)
);

create index if not exists tradevault_referrals_referrer_idx
  on public.tradevault_referrals (referrer_user_id, created_at desc);

create table if not exists public.tradevault_feedback_responses (
  id uuid primary key default gen_random_uuid(),
  user_id uuid references auth.users(id) on delete set null,
  email text,
  device_id_hash text,
  session_id text,
  page_path text,
  score integer,
  responses jsonb not null default '{}'::jsonb,
  user_agent text,
  created_at timestamptz not null default now()
);

create index if not exists tradevault_feedback_responses_created_idx
  on public.tradevault_feedback_responses (created_at desc);

create index if not exists tradevault_feedback_responses_user_idx
  on public.tradevault_feedback_responses (user_id, created_at desc);

create table if not exists public.tradevault_user_activity_events (
  id uuid primary key default gen_random_uuid(),
  user_id uuid references auth.users(id) on delete set null,
  event_type text not null default 'page_view',
  page_path text,
  page_title text,
  referrer text,
  user_agent text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists tradevault_user_activity_events_created_idx
  on public.tradevault_user_activity_events (created_at desc);

create index if not exists tradevault_user_activity_events_user_created_idx
  on public.tradevault_user_activity_events (user_id, created_at desc);

create index if not exists tradevault_user_activity_events_page_created_idx
  on public.tradevault_user_activity_events (page_path, created_at desc);

create table if not exists public.tradevault_user_streaks (
  user_id uuid primary key references auth.users(id) on delete cascade,
  current_streak integer not null default 0,
  longest_streak integer not null default 0,
  last_active_date date,
  last_seen_at timestamptz,
  status text not null default 'inactive',
  monthly_restore_period text,
  monthly_restore_count integer not null default 0,
  total_restores_used integer not null default 0,
  restore_limit integer not null default 3,
  milestones_sent jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.tradevault_user_streaks
  add column if not exists current_streak integer not null default 0,
  add column if not exists longest_streak integer not null default 0,
  add column if not exists last_active_date date,
  add column if not exists last_seen_at timestamptz,
  add column if not exists status text not null default 'inactive',
  add column if not exists monthly_restore_period text,
  add column if not exists monthly_restore_count integer not null default 0,
  add column if not exists total_restores_used integer not null default 0,
  add column if not exists restore_limit integer not null default 3,
  add column if not exists milestones_sent jsonb not null default '[]'::jsonb,
  add column if not exists created_at timestamptz not null default now(),
  add column if not exists updated_at timestamptz not null default now();

create index if not exists tradevault_user_streaks_last_seen_idx
  on public.tradevault_user_streaks (last_seen_at desc);

create index if not exists tradevault_user_streaks_last_active_idx
  on public.tradevault_user_streaks (last_active_date desc);

create table if not exists public.tradevault_streak_events (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  event_type text not null,
  activity_date date,
  streak_count integer not null default 0,
  used_restore boolean not null default false,
  restores_used integer not null default 0,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists tradevault_streak_events_user_created_idx
  on public.tradevault_streak_events (user_id, created_at desc);

create index if not exists tradevault_streak_events_activity_date_idx
  on public.tradevault_streak_events (activity_date desc);

create table if not exists public.tradevault_email_logs (
  id uuid primary key default gen_random_uuid(),
  user_id uuid references auth.users(id) on delete set null,
  email text,
  email_type text not null,
  dedupe_key text not null unique,
  metadata jsonb not null default '{}'::jsonb,
  sent_at timestamptz not null default now()
);

create index if not exists tradevault_email_logs_user_type_idx
  on public.tradevault_email_logs (user_id, email_type, sent_at desc);

create index if not exists tradevault_email_logs_type_sent_idx
  on public.tradevault_email_logs (email_type, sent_at desc);

create table if not exists public.tradevault_support_agents (
  user_id uuid primary key references auth.users(id) on delete cascade,
  role text not null default 'agent',
  display_name text,
  created_at timestamptz not null default now()
);

create table if not exists public.tradevault_support_tickets (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  account_id text,
  subject text not null,
  category text not null default 'general',
  priority text not null default 'normal',
  status text not null default 'open',
  last_message_at timestamptz not null default now(),
  assigned_to uuid references auth.users(id),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.tradevault_support_tickets
  add column if not exists user_id uuid references auth.users(id) on delete cascade,
  add column if not exists account_id text,
  add column if not exists subject text,
  add column if not exists category text not null default 'general',
  add column if not exists priority text not null default 'normal',
  add column if not exists status text not null default 'open',
  add column if not exists last_message_at timestamptz not null default now(),
  add column if not exists assigned_to uuid references auth.users(id),
  add column if not exists created_at timestamptz not null default now(),
  add column if not exists updated_at timestamptz not null default now();

create table if not exists public.tradevault_support_messages (
  id uuid primary key default gen_random_uuid(),
  ticket_id uuid not null references public.tradevault_support_tickets(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  sender_role text not null default 'user',
  body text not null,
  attachment_url text,
  created_at timestamptz not null default now()
);

alter table public.tradevault_support_messages
  add column if not exists ticket_id uuid references public.tradevault_support_tickets(id) on delete cascade,
  add column if not exists user_id uuid references auth.users(id) on delete cascade,
  add column if not exists sender_role text not null default 'user',
  add column if not exists body text,
  add column if not exists attachment_url text,
  add column if not exists created_at timestamptz not null default now();

create index if not exists tradevault_support_tickets_user_status_idx
  on public.tradevault_support_tickets (user_id, status, last_message_at desc);

create index if not exists tradevault_support_tickets_status_priority_idx
  on public.tradevault_support_tickets (status, priority, last_message_at desc);

create index if not exists tradevault_support_messages_ticket_idx
  on public.tradevault_support_messages (ticket_id, created_at asc);

alter table public.tradevault_kv_store enable row level security;
alter table public.tradevault_account_settings enable row level security;
alter table public.tradevault_account_alerts enable row level security;
alter table public.tradevault_account_snapshots enable row level security;
alter table public.tradevault_user_profiles enable row level security;
alter table public.tradevault_user_trials enable row level security;
alter table public.tradevault_trial_identity_claims enable row level security;
alter table public.tradevault_account_owners enable row level security;
alter table public.tradevault_account_deletions enable row level security;
alter table public.tradevault_user_ea_keys enable row level security;
alter table public.tradevault_direct_mt_accounts enable row level security;
alter table public.tradevault_share_links enable row level security;
alter table public.tradevault_referral_codes enable row level security;
alter table public.tradevault_referrals enable row level security;
alter table public.tradevault_feedback_responses enable row level security;
alter table public.tradevault_user_activity_events enable row level security;
alter table public.tradevault_support_agents enable row level security;
alter table public.tradevault_support_tickets enable row level security;
alter table public.tradevault_support_messages enable row level security;

drop policy if exists trial_identity_claims_select_own_or_agent on public.tradevault_trial_identity_claims;
create policy trial_identity_claims_select_own_or_agent
  on public.tradevault_trial_identity_claims
  for select
  using (
    auth.uid() = user_id
    or exists (
      select 1 from public.tradevault_support_agents a
      where a.user_id = auth.uid()
    )
  );

drop policy if exists referral_codes_select_own on public.tradevault_referral_codes;
create policy referral_codes_select_own
  on public.tradevault_referral_codes
  for select
  using (auth.uid() = user_id);

drop policy if exists referral_codes_insert_own on public.tradevault_referral_codes;
create policy referral_codes_insert_own
  on public.tradevault_referral_codes
  for insert
  with check (auth.uid() = user_id);

drop policy if exists referrals_select_own_or_agent on public.tradevault_referrals;
create policy referrals_select_own_or_agent
  on public.tradevault_referrals
  for select
  using (
    auth.uid() = referrer_user_id
    or auth.uid() = referred_user_id
    or exists (
      select 1 from public.tradevault_support_agents a
      where a.user_id = auth.uid()
    )
  );

drop policy if exists feedback_insert_self_or_guest on public.tradevault_feedback_responses;
create policy feedback_insert_self_or_guest
  on public.tradevault_feedback_responses
  for insert
  with check (user_id is null or auth.uid() = user_id);

drop policy if exists feedback_select_agent on public.tradevault_feedback_responses;
create policy feedback_select_agent
  on public.tradevault_feedback_responses
  for select
  using (
    exists (
      select 1 from public.tradevault_support_agents a
      where a.user_id = auth.uid()
    )
  );

drop policy if exists activity_events_insert_self on public.tradevault_user_activity_events;
create policy activity_events_insert_self
  on public.tradevault_user_activity_events
  for insert
  with check (auth.uid() = user_id);

drop policy if exists activity_events_select_own_or_agent on public.tradevault_user_activity_events;
create policy activity_events_select_own_or_agent
  on public.tradevault_user_activity_events
  for select
  using (
    auth.uid() = user_id
    or exists (
      select 1 from public.tradevault_support_agents a
      where a.user_id = auth.uid()
    )
  );

drop policy if exists support_agents_select_own on public.tradevault_support_agents;
create policy support_agents_select_own
  on public.tradevault_support_agents
  for select
  using (auth.uid() = user_id);

drop policy if exists support_tickets_select_own on public.tradevault_support_tickets;
create policy support_tickets_select_own
  on public.tradevault_support_tickets
  for select
  using (
    auth.uid() = user_id
    or exists (
      select 1 from public.tradevault_support_agents a
      where a.user_id = auth.uid()
    )
  );

drop policy if exists support_tickets_insert_own on public.tradevault_support_tickets;
create policy support_tickets_insert_own
  on public.tradevault_support_tickets
  for insert
  with check (auth.uid() = user_id);

drop policy if exists support_tickets_update_own_or_agent on public.tradevault_support_tickets;
create policy support_tickets_update_own_or_agent
  on public.tradevault_support_tickets
  for update
  using (
    auth.uid() = user_id
    or exists (
      select 1 from public.tradevault_support_agents a
      where a.user_id = auth.uid()
    )
  );

drop policy if exists support_messages_select_own_or_agent on public.tradevault_support_messages;
create policy support_messages_select_own_or_agent
  on public.tradevault_support_messages
  for select
  using (
    exists (
      select 1 from public.tradevault_support_tickets t
      where t.id = ticket_id
        and t.user_id = auth.uid()
    )
    or exists (
      select 1 from public.tradevault_support_agents a
      where a.user_id = auth.uid()
    )
  );

drop policy if exists support_messages_insert_own_or_agent on public.tradevault_support_messages;
create policy support_messages_insert_own_or_agent
  on public.tradevault_support_messages
  for insert
  with check (
    exists (
      select 1 from public.tradevault_support_tickets t
      where t.id = ticket_id
        and t.user_id = auth.uid()
    )
    or exists (
      select 1 from public.tradevault_support_agents a
      where a.user_id = auth.uid()
    )
  );

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
  s.updated_at as snapshot_updated_at,
  coalesce(
    s.processed_data #>> '{account,account_type}',
    s.processed_data #>> '{account,type}',
    s.processed_data #>> '{account,trade_mode}',
    s.processed_data #>> '{meta,account_type}',
    s.config ->> 'accountType',
    ''
  ) as account_type,
  case
    when lower(coalesce(
      s.processed_data #>> '{account,account_type}',
      s.processed_data #>> '{account,type}',
      s.processed_data #>> '{account,trade_mode}',
      s.processed_data #>> '{meta,account_type}',
      s.config ->> 'accountType',
      ''
    )) similar to '%(demo|practice|contest)%' then 'demo'
    when lower(coalesce(
      s.processed_data #>> '{account,account_type}',
      s.processed_data #>> '{account,type}',
      s.processed_data #>> '{account,trade_mode}',
      s.processed_data #>> '{meta,account_type}',
      s.config ->> 'accountType',
      ''
    )) similar to '%(real|live)%' then 'live'
    else 'unknown'
  end as account_environment,
  coalesce(s.processed_data #>> '{account,currency}', s.config ->> 'currency', '') as currency,
  coalesce(s.processed_data #>> '{account,leverage}', s.config ->> 'leverage', '') as leverage,
  coalesce(s.processed_data #>> '{account,server}', s.processed_data #>> '{meta,server}', s.config ->> 'server', '') as server_name,
  case
    when (s.processed_data #>> '{account,profit}') ~ '^-?[0-9]+(\.[0-9]+)?$'
      then (s.processed_data #>> '{account,profit}')::numeric
    else null
  end as running_profit
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
