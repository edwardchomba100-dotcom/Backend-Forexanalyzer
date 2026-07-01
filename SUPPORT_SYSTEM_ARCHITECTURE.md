# ForexAnalyzer Pro Support System Architecture

## Goal

Add a secure support section where a logged-in user can open a support chat/ticket, send messages, attach useful account context, and receive replies from ForexAnalyzer Pro support. Every conversation must stay isolated per user so support can reply to each client individually without mixing data.

## User Flow

1. User signs in with Google on ForexAnalyzer Pro.
2. User opens `Support` from the sidebar.
3. User sees their own support tickets only.
4. User creates a new ticket with:
   - subject
   - category
   - priority
   - optional account id
   - first message
5. Backend stores the ticket and first message under `user_id`.
6. Support/admin dashboard shows all tickets, grouped by status and priority.
7. Support opens one ticket and replies.
8. User receives the reply in their ticket thread.
9. Ticket can be marked `open`, `pending`, `resolved`, or `closed`.

## Database Tables

Run these in Supabase SQL Editor.

```sql
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

create table if not exists public.tradevault_support_messages (
  id uuid primary key default gen_random_uuid(),
  ticket_id uuid not null references public.tradevault_support_tickets(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  sender_role text not null default 'user',
  body text not null,
  attachment_url text,
  created_at timestamptz not null default now()
);

create table if not exists public.tradevault_support_agents (
  user_id uuid primary key references auth.users(id) on delete cascade,
  role text not null default 'agent',
  display_name text,
  created_at timestamptz not null default now()
);

alter table public.tradevault_support_tickets enable row level security;
alter table public.tradevault_support_messages enable row level security;
alter table public.tradevault_support_agents enable row level security;

create index if not exists support_tickets_user_status_idx
  on public.tradevault_support_tickets (user_id, status, last_message_at desc);

create index if not exists support_messages_ticket_idx
  on public.tradevault_support_messages (ticket_id, created_at asc);
```

## Security Model

Users should only read/write their own tickets and messages.

Support agents should read/reply to all tickets only if their `auth.users.id` exists in `tradevault_support_agents`.

The safest implementation is:

- normal users use frontend auth token
- backend verifies Supabase user
- backend uses service role key to create/read support rows after checking ownership
- admin/support page also calls backend, not direct public table writes

This keeps the rules simple and avoids exposing service-role powers to the browser.

## Backend API

Add these routes under `/api/support`.

### User routes

```text
GET    /api/support/tickets
POST   /api/support/tickets
GET    /api/support/tickets/:ticketId
POST   /api/support/tickets/:ticketId/messages
PATCH  /api/support/tickets/:ticketId/status
```

User rules:

- `GET /tickets` returns tickets where `ticket.user_id = req.user.id`
- `POST /tickets` creates ticket using `req.user.id`
- `GET /tickets/:id` checks the ticket belongs to `req.user.id`
- `POST /tickets/:id/messages` checks ownership, then inserts a message with `sender_role = 'user'`
- user can set status to `closed` or reopen to `open`

### Admin routes

```text
GET    /api/support/admin/tickets
GET    /api/support/admin/tickets/:ticketId
POST   /api/support/admin/tickets/:ticketId/messages
PATCH  /api/support/admin/tickets/:ticketId
```

Admin rules:

- backend checks `req.user.id` exists in `tradevault_support_agents`
- admin can view all tickets
- admin replies with `sender_role = 'agent'`
- admin can assign, change priority, and change status

## Real-Time Updates

Best option:

- keep user support messages in Supabase
- use backend Socket.IO or Supabase Realtime to push updates

Recommended simple first version:

- poll selected ticket every 5 to 10 seconds
- poll ticket list every 15 to 30 seconds

Recommended premium version:

- join Socket.IO room `support:user:<userId>` for normal users
- join room `support:agents` for support team
- when a user sends a message:
  - emit to `support:agents`
  - emit to `support:user:<userId>`
- when support replies:
  - emit to `support:user:<ticket.user_id>`
  - emit to `support:agents`

## Frontend Pages

Add sidebar item:

```text
Support
```

Routes:

```text
/support
/support/:ticketId
/support-admin
```

### User Support Page

Sections:

- ticket list
- new ticket button
- active conversation panel
- message input
- optional selected account context

Ticket categories:

- Account connection
- EA API key
- Billing / subscription
- Trade data issue
- Copy trading
- Alerts
- General question

Priority:

- low
- normal
- urgent

### Admin Support Page

Sections:

- all open tickets
- filters by status, priority, category
- unread count
- customer email/name
- linked account id
- conversation thread
- reply box
- status controls

## Notification Flow

When user creates a ticket:

- store ticket/message
- notify support email
- optionally notify support Telegram/WhatsApp later

When agent replies:

- store message
- show in user dashboard
- optional email notification to user

Email service options:

- Resend
- SendGrid
- Supabase Edge Function
- SMTP provider from hosting

## Data Isolation

Ticket isolation must always use:

```text
ticket.user_id === authenticated user id
```

Support messages must be loaded only after the ticket ownership check passes.

Never trust `user_id` sent by the frontend. Always use `req.user.id` from the verified Supabase JWT.

## Recommended Build Order

1. Add Supabase tables.
2. Add backend `/api/support` user routes.
3. Add frontend `/support` page.
4. Add backend admin-agent check.
5. Add `/support-admin` page.
6. Add Socket.IO real-time updates.
7. Add email notifications.
8. Add attachments if needed.

## Minimal Version To Launch

For the first launch, build:

- support tickets table
- support messages table
- `/support` user page
- `/support-admin` private admin page
- backend APIs
- polling refresh

This is enough to receive every customer message and reply to each user individually.
