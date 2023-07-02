CREATE TABLE IF NOT EXISTS public.users_revenue
(
    user_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    revenue integer NOT NULL,
    CONSTRAINT users_revenue_pkey PRIMARY KEY (user_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.users_revenue
    OWNER to postgres;