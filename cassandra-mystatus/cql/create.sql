CREATE TABLE users (
username text PRIMARY KEY,
email text,
encrypted_password blob,
location text,
version timeuuid
);