DROP TABLE IF EXISTS public.artists;
DROP TABLE IF EXISTS public.songplays;
DROP TABLE IF EXISTS public.songs;
DROP TABLE IF EXISTS public.staging_events;
DROP TABLE IF EXISTS public.staging_songs;
DROP TABLE IF EXISTS public."time";
DROP TABLE IF EXISTS public.users;

CREATE TABLE public.artists (
	artistId varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	lattitude numeric(18,0),
	longitude numeric(18,0)
);

CREATE TABLE public.songplays (
	playId varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	userId int4 NOT NULL,
	"level" varchar(256),
	songId varchar(256),
	artistId varchar(256),
	sessionId int4,
	location varchar(256),
	user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (playId)
);

CREATE TABLE public.songs (
	songId varchar(256) NOT NULL,
	title varchar(256),
	artistId varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songId)
);

CREATE TABLE public.staging_events (
	artist varchar(256),
	auth varchar(256),
	firstName varchar(256),
	gender varchar(256),
	itemInSession int4,
	lastName varchar(256),
	length numeric(18,0),
	"level" varchar(256),
	location varchar(256),
	"method" varchar(256),
	page varchar(256),
	registration numeric(18,0),
	sessionId int4,
	song varchar(256),
	status int4,
	ts int8,
	userAgent varchar(256),
	userId int4
);

CREATE TABLE public.staging_songs (
	num_songs int4,
	artist_id varchar(256),
	artist_name varchar(256),
	artist_latitude numeric(18,0),
	artist_longitude numeric(18,0),
	artist_location varchar(256),
	song_id varchar(256),
	title varchar(256),
	duration numeric(18,0),
	"year" int4
);

CREATE TABLE public."time" (
	start_time timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
);

CREATE TABLE public.users (
	userId int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	"level" varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (userId)
);





