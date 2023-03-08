
\c testdb;

CREATE TABLE IF NOT EXISTS action (
  id int NOT NULL PRIMARY KEY,
  action_name varchar(1000)
);

CREATE TABLE IF NOT EXISTS browser (
  id int NOT NULL PRIMARY KEY,
  browser_name varchar(1000)
);

CREATE TABLE IF NOT EXISTS category (
  id int NOT NULL PRIMARY KEY,
  category_name varchar(1000)
);

CREATE TABLE IF NOT EXISTS users (
  id int NOT NULL PRIMARY KEY,
  first_name varchar(1000),
  last_name varchar(1000)
);

CREATE TABLE IF NOT EXISTS label (
  id int NOT NULL PRIMARY KEY,
  label varchar(1000)
);

CREATE TABLE IF NOT EXISTS event (
  id int NOT NULL PRIMARY KEY,
  ip_address varchar(40),
  event_date timestamp,
  user_id int,
  category_id int,
  action_id int,
  label_id int,
  browser_id int,
  search_term text
);

