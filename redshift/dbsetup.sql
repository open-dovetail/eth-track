-- create db
create database ethdb;

-- create user
create user ethuser password '';

-- login to ethdb and create schema
create schema eth authorization ethuser;

-- verify user and schema
select * from pg_user;
select * from pg_namespace;