CREATE TABLE users (
    username text PRIMARY KEY,
    email text,
    encrypted_password blob,
    location text,
    version timeuuid
);

CREATE TABLE user_status_updates (
	username text,
	id timeuuid,
	body text,
	PRIMARY KEY (username, id)
);


CREATE TABLE home_status_updates (
	timeline_username text,
	status_update_id timeuuid,
	status_update_username text,
	body text,
	PRIMARY KEY (timeline_username, status_update_id)
) WITH CLUSTERING ORDER BY (status_update_id DESC);


CREATE TABLE user_follows (
	followed_username text,
	follower_username text,
	PRIMARY KEY (followed_username, follower_username)
);