--- When running with write-access, metrics-utility will create these dynamically
--- but GitHub CI runs without, need to import these during init

--
-- Name: metrics_utility_is_valid_json(text); Type: FUNCTION; Schema: public; Owner: awx
--

CREATE FUNCTION public.metrics_utility_is_valid_json(p_json text) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
            BEGIN
                RETURN (p_json::json is not null);
            EXCEPTION
                WHEN others THEN
                    RETURN false;
            END;
            $$;


ALTER FUNCTION public.metrics_utility_is_valid_json(p_json text) OWNER TO awx;

--
-- Name: metrics_utility_parse_yaml_field(text, text); Type: FUNCTION; Schema: public; Owner: awx
--

CREATE FUNCTION public.metrics_utility_parse_yaml_field(str text, field text) RETURNS text
    LANGUAGE plpgsql
    AS $_$
            DECLARE
                line_re text;
                field_re text;
            BEGIN
                field_re := ' *[:=] *(.+?) *$';
                line_re := '(?n)^' || field || field_re;
                RETURN trim(both '"' from substring(str from line_re) );
            END;
            $_$;


ALTER FUNCTION public.metrics_utility_parse_yaml_field(str text, field text) OWNER TO awx;

