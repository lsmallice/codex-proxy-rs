create table managed_ipv6_settings (
  singleton boolean primary key default true check (singleton),
  controller_id text,
  revision bigint not null default 1 check (revision > 0)
);
insert into managed_ipv6_settings(singleton) values (true);

create table managed_ipv6_leases (
  request_id text primary key,
  controller_id text not null,
  name text not null check (char_length(name) between 1 and 100),
  target_account_id text,
  owner_account_id text,
  resume_enabled boolean not null default false,
  cancelled boolean not null default false,
  automatic boolean not null default false,
  lease_id text,
  address text,
  proxy_url text,
  proxy_id text unique references outbound_proxies(id) on delete restrict,
  state text not null check (state in ('queued','allocating','unknown','pending','active','retiring','retired')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  next_attempt_at timestamptz not null default now(),
  unique (controller_id, lease_id),
  unique (controller_id, address),
  check ((lease_id is null and address is null and proxy_url is null)
      or (lease_id is not null and address is not null and (proxy_url is not null or state = 'retired')))
);
create index managed_ipv6_pending_idx on managed_ipv6_leases(controller_id, updated_at)
  where state in ('queued','allocating','unknown','pending','retiring');
create unique index managed_ipv6_target_pending_idx on managed_ipv6_leases(target_account_id)
  where target_account_id is not null and state not in ('retired','active');

-- 创建入口共享自动分配队列，首次事务内停用待就绪账号，不覆盖显式出口
create function queue_managed_ipv6_import() returns trigger language plpgsql as $$
declare controller text;
begin
  if new.outbound_proxy_id is not null or new.outbound_proxy_url is not null then return new; end if;
  select controller_id into controller from managed_ipv6_settings where singleton;
  if controller is null then return new; end if;
  insert into managed_ipv6_leases(request_id, controller_id, name, target_account_id, state, resume_enabled, automatic)
    values ('ipv6_' || gen_random_uuid()::text, controller, left(new.name,100), new.id, 'queued', new.enabled, true);
  update provider_accounts set enabled = false where id = new.id;
  return new;
end $$;
create trigger managed_ipv6_import_queue after insert on provider_accounts
  for each row execute function queue_managed_ipv6_import();

-- 待就绪期间保存管理员显式设置的启停意图，内部排队停用不覆盖原意图
create function remember_managed_ipv6_enabled() returns trigger language plpgsql as $$
begin
  if pg_trigger_depth() = 1 then
    update managed_ipv6_leases set resume_enabled = new.enabled
      where target_account_id = new.id and automatic and state in ('queued','allocating','unknown','pending');
  end if;
  return new;
end $$;
create trigger a_managed_ipv6_enabled before update of enabled on provider_accounts
  for each row execute function remember_managed_ipv6_enabled();

-- 所有账号写入入口共用归属约束，解绑后的地址不可交给其他账号
create function guard_managed_ipv6_account() returns trigger language plpgsql as $$
declare lease managed_ipv6_leases%rowtype;
begin
  if tg_op <> 'DELETE' then
    if new.outbound_proxy_id is null and new.outbound_proxy_url is not null then
      select proxy_id into new.outbound_proxy_id from managed_ipv6_leases where proxy_url = new.outbound_proxy_url;
    end if;
    if new.outbound_proxy_id is not null then
      select * into lease from managed_ipv6_leases where proxy_id = new.outbound_proxy_id for update;
      if found then
        if lease.state <> 'active' or (lease.owner_account_id is not null and lease.owner_account_id <> new.id) then
          raise exception 'managed IPv6 lease unavailable or owned' using errcode = '23505';
        end if;
        if new.outbound_proxy_url is distinct from lease.proxy_url then
          raise exception 'managed IPv6 URL mismatch' using errcode = '23505';
        end if;
        update managed_ipv6_leases set owner_account_id = new.id, updated_at = now() where request_id = lease.request_id;
      end if;
    end if;
    if exists(select 1 from managed_ipv6_leases where target_account_id = new.id
        and automatic and state in ('queued','allocating','unknown','pending','retiring')) then
      new.enabled := false;
    end if;
  end if;
  if tg_op = 'DELETE' then
    update managed_ipv6_leases set state = 'retiring', updated_at = now()
      where (proxy_id = old.outbound_proxy_id or target_account_id = old.id) and state <> 'retired';
    return old;
  elsif tg_op = 'UPDATE' and old.outbound_proxy_id is distinct from new.outbound_proxy_id then
    update managed_ipv6_leases set state = 'retiring', updated_at = now()
      where proxy_id = old.outbound_proxy_id and owner_account_id = old.id and state = 'active';
  end if;
  return new;
end $$;
create trigger managed_ipv6_account_guard before insert or update of outbound_proxy_id, outbound_proxy_url, enabled or delete on provider_accounts
  for each row execute function guard_managed_ipv6_account();

-- 独占代理不能经普通编辑或删除变成直连，须先解除账号绑定再退役
create function guard_managed_ipv6_proxy() returns trigger language plpgsql as $$
begin
  if exists(select 1 from managed_ipv6_leases where proxy_id = old.id) then
    if tg_op = 'DELETE' then
      raise exception 'managed IPv6 lease must be retired first' using errcode = '23505';
    elsif new.proxy_url is distinct from old.proxy_url or new.enabled is distinct from old.enabled then
      raise exception 'managed IPv6 connection is immutable' using errcode = '23505';
    end if;
  end if;
  if tg_op = 'DELETE' then return old; end if;
  return new;
end $$;
create trigger managed_ipv6_proxy_guard before update or delete on outbound_proxies
  for each row execute function guard_managed_ipv6_proxy();
