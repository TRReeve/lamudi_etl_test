CREATE TABLE IF NOT EXISTS {0}
(
    country_iso character varying(3) COLLATE pg_catalog."default" NOT NULL,
    id bigint NOT NULL,
    date timestamp without time zone,
    sku text COLLATE pg_catalog."default",
    project_id bigint,
    agency_id bigint,
    views bigint,
    email_leads bigint,
    phone_leads bigint,
    sms_leads bigint,
    sid text COLLATE pg_catalog."default",
    eid text COLLATE pg_catalog."default",
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    insertedlog text COLLATE pg_catalog."default",
    "timestamp" bigint,
    PRIMARY KEY (country_iso, id)
)
