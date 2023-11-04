create table if not exists redis_type(
    type text primary key,
    description text
);
insert into redis_type values('s', 'string') on conflict do nothing;