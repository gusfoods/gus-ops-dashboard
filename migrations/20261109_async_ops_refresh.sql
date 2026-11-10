-- 2026-11-09: make the matview refresh asynchronous + lock-guarded.
-- Why: refresh_ops_rollup() via PostgREST hit the 120s client read timeout (work_mem 3.5MB
-- spilled every sort/hash to disk, 602 GB of temp files); the sync's retry loop then fired
-- up to 12 overlapping refreshes that queued behind each other's lock -> Fri/Sun failures.
-- Apply with: supabase MCP apply_migration on project evwfqounmubytvxmlinj, or the SQL editor.

create extension if not exists pg_cron;

create table if not exists public.ops_refresh_log (
  id bigserial primary key,
  requested_at timestamptz not null default now(),
  started_at timestamptz,
  finished_at timestamptz,
  status text not null default 'queued',   -- queued | running | done | skipped | failed
  error text,
  detail text
);
alter table public.ops_refresh_log enable row level security;
revoke all on public.ops_refresh_log from public, anon, authenticated;
grant select on public.ops_refresh_log to service_role;

drop function if exists public.refresh_ops_rollup();
create or replace function public.refresh_ops_rollup(p_log_id bigint default null)
returns text
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
set work_mem to '64MB'
set lock_timeout to '8min'
as $$
declare t0 timestamptz; r text := '';
begin
  if not pg_try_advisory_xact_lock(hashtext('refresh_ops_rollup')) then
    if p_log_id is not null then
      update public.ops_refresh_log set status='skipped', finished_at=clock_timestamp(), error='another refresh is running' where id=p_log_id;
    end if;
    return 'skipped: another refresh is running';
  end if;
  if p_log_id is not null then
    update public.ops_refresh_log set status='running', started_at=clock_timestamp() where id=p_log_id;
  end if;
  t0 := clock_timestamp(); refresh materialized view concurrently public.ops_rollup;  r := r || 'ops='  || round(extract(epoch from clock_timestamp()-t0)) || 's ';
  t0 := clock_timestamp(); refresh materialized view concurrently public.cust_rollup; r := r || 'cust=' || round(extract(epoch from clock_timestamp()-t0)) || 's ';
  t0 := clock_timestamp(); refresh materialized view concurrently public.prod_rollup; r := r || 'prod=' || round(extract(epoch from clock_timestamp()-t0)) || 's ';
  t0 := clock_timestamp(); refresh materialized view concurrently public.b2b_rollup;  r := r || 'b2b='  || round(extract(epoch from clock_timestamp()-t0)) || 's';
  if p_log_id is not null then
    update public.ops_refresh_log set status='done', finished_at=clock_timestamp(), detail=r where id=p_log_id;
  end if;
  return r;
exception when others then
  if p_log_id is not null then
    update public.ops_refresh_log set status='failed', finished_at=clock_timestamp(), error=sqlerrm where id=p_log_id;
  end if;
  raise;
end;
$$;
revoke all on function public.refresh_ops_rollup(bigint) from public, anon, authenticated;
grant execute on function public.refresh_ops_rollup(bigint) to service_role;

-- Worker run by the one-shot pg_cron job: unschedules itself, then refreshes.
create or replace function public.run_ops_refresh(p_log_id bigint)
returns void
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $$
begin
  perform cron.unschedule('ops-refresh-' || p_log_id);
  perform public.refresh_ops_rollup(p_log_id);
exception when others then
  update public.ops_refresh_log set status='failed', finished_at=clock_timestamp(), error=coalesce(error, sqlerrm) where id=p_log_id;
end;
$$;
revoke all on function public.run_ops_refresh(bigint) from public, anon, authenticated;

-- Called by the sync job over PostgREST: returns immediately with a log id to poll.
create or replace function public.request_ops_refresh()
returns bigint
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $$
declare v_id bigint;
begin
  insert into public.ops_refresh_log default values returning id into v_id;
  perform cron.schedule('ops-refresh-' || v_id, '10 seconds',
    format('set statement_timeout = ''45min''; select public.run_ops_refresh(%s);', v_id));
  return v_id;
end;
$$;
revoke all on function public.request_ops_refresh() from public, anon, authenticated;
grant execute on function public.request_ops_refresh() to service_role;

-- Weekly vacuum: concurrent refreshes leave dead tuples; autovacuum had not touched the matviews since Oct 1.
select cron.unschedule(jobid) from cron.job where jobname = 'ops-vacuum-weekly';
select cron.schedule('ops-vacuum-weekly', '0 3 * * 0',
  'vacuum analyze public.ops_rollup, public.cust_rollup, public.prod_rollup, public.b2b_rollup, public.sales_order_lines, public.stock_moves');
