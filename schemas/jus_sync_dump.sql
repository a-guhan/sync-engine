--
-- PostgreSQL database dump
--

\restrict mtDEKJFKE3B8aiHWGOyJqR6k3EOYMM6UbN0sG9mgikprx65eYgCGrsdxEIOgwS2

-- Dumped from database version 18.6
-- Dumped by pg_dump version 18.6

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_roles
    WHERE rolname = 'jus_sync_user'
  ) THEN
    CREATE ROLE jus_sync_user;
  END IF;
END
$$;

ALTER ROLE jus_sync_user WITH SUPERUSER INHERIT CREATEROLE CREATEDB LOGIN REPLICATION NOBYPASSRLS PASSWORD 'jus_sync_pass';

CREATE TABLE public.user_table (
    name text NOT NULL,
    age integer NOT NULL,
    country text NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL
);

ALTER TABLE ONLY public.user_table REPLICA IDENTITY FULL;


ALTER TABLE public.user_table OWNER TO jus_sync_user;

INSERT INTO public.user_table (name, age, country, metadata) VALUES ('a', 1, 'IN', '{}');
INSERT INTO public.user_table (name, age, country, metadata) VALUES ('b', 2, 'US', '{}');
INSERT INTO public.user_table (name, age, country, metadata) VALUES ('c', 3, 'DE', '{}');


--
-- PostgreSQL database dump complete
--

\unrestrict mtDEKJFKE3B8aiHWGOyJqR6k3EOYMM6UbN0sG9mgikprx65eYgCGrsdxEIOgwS2
