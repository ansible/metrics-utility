--
-- PostgreSQL database dump
--

-- Dumped from database version 15.10
-- Dumped by pg_dump version 15.10

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

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

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: _unpartitioned_main_adhoccommandevent; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public._unpartitioned_main_adhoccommandevent (
    id bigint NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    host_name character varying(1024) NOT NULL,
    event character varying(100) NOT NULL,
    event_data text NOT NULL,
    failed boolean NOT NULL,
    changed boolean NOT NULL,
    counter integer NOT NULL,
    host_id integer,
    ad_hoc_command_id integer NOT NULL,
    end_line integer NOT NULL,
    start_line integer NOT NULL,
    stdout text NOT NULL,
    uuid character varying(1024) NOT NULL,
    verbosity integer NOT NULL,
    CONSTRAINT main_adhoccommandevent_counter_check CHECK ((counter >= 0)),
    CONSTRAINT main_adhoccommandevent_end_line_check CHECK ((end_line >= 0)),
    CONSTRAINT main_adhoccommandevent_start_line_check CHECK ((start_line >= 0)),
    CONSTRAINT main_adhoccommandevent_verbosity_check CHECK ((verbosity >= 0))
);


ALTER TABLE public._unpartitioned_main_adhoccommandevent OWNER TO awx;

--
-- Name: _unpartitioned_main_inventoryupdateevent; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public._unpartitioned_main_inventoryupdateevent (
    id bigint NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    event_data text NOT NULL,
    uuid character varying(1024) NOT NULL,
    counter integer NOT NULL,
    stdout text NOT NULL,
    verbosity integer NOT NULL,
    start_line integer NOT NULL,
    end_line integer NOT NULL,
    inventory_update_id integer NOT NULL,
    CONSTRAINT main_inventoryupdateevent_counter_check CHECK ((counter >= 0)),
    CONSTRAINT main_inventoryupdateevent_end_line_check CHECK ((end_line >= 0)),
    CONSTRAINT main_inventoryupdateevent_start_line_check CHECK ((start_line >= 0)),
    CONSTRAINT main_inventoryupdateevent_verbosity_check CHECK ((verbosity >= 0))
);


ALTER TABLE public._unpartitioned_main_inventoryupdateevent OWNER TO awx;

--
-- Name: _unpartitioned_main_jobevent; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public._unpartitioned_main_jobevent (
    id bigint NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone NOT NULL,
    event character varying(100) NOT NULL,
    event_data text NOT NULL,
    failed boolean NOT NULL,
    changed boolean NOT NULL,
    host_name character varying(1024) NOT NULL,
    play character varying(1024) NOT NULL,
    role character varying(1024) NOT NULL,
    task character varying(1024) NOT NULL,
    counter integer NOT NULL,
    host_id integer,
    job_id integer NOT NULL,
    uuid character varying(1024) NOT NULL,
    parent_uuid character varying(1024) NOT NULL,
    end_line integer NOT NULL,
    playbook character varying(1024) NOT NULL,
    start_line integer NOT NULL,
    stdout text NOT NULL,
    verbosity integer NOT NULL,
    CONSTRAINT main_jobevent_counter_check CHECK ((counter >= 0)),
    CONSTRAINT main_jobevent_end_line_check CHECK ((end_line >= 0)),
    CONSTRAINT main_jobevent_start_line_check CHECK ((start_line >= 0)),
    CONSTRAINT main_jobevent_verbosity_check CHECK ((verbosity >= 0))
);


ALTER TABLE public._unpartitioned_main_jobevent OWNER TO awx;

--
-- Name: _unpartitioned_main_projectupdateevent; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public._unpartitioned_main_projectupdateevent (
    id bigint NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone NOT NULL,
    event character varying(100) NOT NULL,
    event_data text NOT NULL,
    failed boolean NOT NULL,
    changed boolean NOT NULL,
    uuid character varying(1024) NOT NULL,
    playbook character varying(1024) NOT NULL,
    play character varying(1024) NOT NULL,
    role character varying(1024) NOT NULL,
    task character varying(1024) NOT NULL,
    counter integer NOT NULL,
    stdout text NOT NULL,
    verbosity integer NOT NULL,
    start_line integer NOT NULL,
    end_line integer NOT NULL,
    project_update_id integer NOT NULL,
    CONSTRAINT main_projectupdateevent_counter_check CHECK ((counter >= 0)),
    CONSTRAINT main_projectupdateevent_end_line_check CHECK ((end_line >= 0)),
    CONSTRAINT main_projectupdateevent_start_line_check CHECK ((start_line >= 0)),
    CONSTRAINT main_projectupdateevent_verbosity_check CHECK ((verbosity >= 0))
);


ALTER TABLE public._unpartitioned_main_projectupdateevent OWNER TO awx;

--
-- Name: _unpartitioned_main_systemjobevent; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public._unpartitioned_main_systemjobevent (
    id bigint NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    event_data text NOT NULL,
    uuid character varying(1024) NOT NULL,
    counter integer NOT NULL,
    stdout text NOT NULL,
    verbosity integer NOT NULL,
    start_line integer NOT NULL,
    end_line integer NOT NULL,
    system_job_id integer NOT NULL,
    CONSTRAINT main_systemjobevent_counter_check CHECK ((counter >= 0)),
    CONSTRAINT main_systemjobevent_end_line_check CHECK ((end_line >= 0)),
    CONSTRAINT main_systemjobevent_start_line_check CHECK ((start_line >= 0)),
    CONSTRAINT main_systemjobevent_verbosity_check CHECK ((verbosity >= 0))
);


ALTER TABLE public._unpartitioned_main_systemjobevent OWNER TO awx;

--
-- Name: auth_group; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.auth_group (
    id integer NOT NULL,
    name character varying(150) NOT NULL
);


ALTER TABLE public.auth_group OWNER TO awx;

--
-- Name: auth_group_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.auth_group ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.auth_group_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: auth_group_permissions; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.auth_group_permissions (
    id integer NOT NULL,
    group_id integer NOT NULL,
    permission_id integer NOT NULL
);


ALTER TABLE public.auth_group_permissions OWNER TO awx;

--
-- Name: auth_group_permissions_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.auth_group_permissions ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.auth_group_permissions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: auth_permission; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.auth_permission (
    id integer NOT NULL,
    name character varying(255) NOT NULL,
    content_type_id integer NOT NULL,
    codename character varying(100) NOT NULL
);


ALTER TABLE public.auth_permission OWNER TO awx;

--
-- Name: auth_permission_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.auth_permission ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.auth_permission_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: auth_user; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.auth_user (
    id integer NOT NULL,
    password character varying(128) NOT NULL,
    last_login timestamp with time zone,
    is_superuser boolean NOT NULL,
    username character varying(150) NOT NULL,
    first_name character varying(150) NOT NULL,
    last_name character varying(150) NOT NULL,
    email character varying(254) NOT NULL,
    is_staff boolean NOT NULL,
    is_active boolean NOT NULL,
    date_joined timestamp with time zone NOT NULL
);


ALTER TABLE public.auth_user OWNER TO awx;

--
-- Name: auth_user_groups; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.auth_user_groups (
    id integer NOT NULL,
    user_id integer NOT NULL,
    group_id integer NOT NULL
);


ALTER TABLE public.auth_user_groups OWNER TO awx;

--
-- Name: auth_user_groups_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.auth_user_groups ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.auth_user_groups_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: auth_user_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.auth_user ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.auth_user_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: auth_user_user_permissions; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.auth_user_user_permissions (
    id integer NOT NULL,
    user_id integer NOT NULL,
    permission_id integer NOT NULL
);


ALTER TABLE public.auth_user_user_permissions OWNER TO awx;

--
-- Name: auth_user_user_permissions_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.auth_user_user_permissions ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.auth_user_user_permissions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: conf_setting; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.conf_setting (
    id integer NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    key character varying(255) NOT NULL,
    value jsonb,
    user_id integer
);


ALTER TABLE public.conf_setting OWNER TO awx;

--
-- Name: conf_setting_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.conf_setting ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.conf_setting_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: dab_rbac_dabpermission; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.dab_rbac_dabpermission (
    id bigint NOT NULL,
    name character varying(255) NOT NULL,
    codename character varying(100) NOT NULL,
    content_type_id integer NOT NULL
);


ALTER TABLE public.dab_rbac_dabpermission OWNER TO awx;

--
-- Name: dab_rbac_dabpermission_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.dab_rbac_dabpermission ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.dab_rbac_dabpermission_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: dab_rbac_objectrole; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.dab_rbac_objectrole (
    id bigint NOT NULL,
    object_id text NOT NULL,
    content_type_id integer NOT NULL,
    role_definition_id bigint NOT NULL
);


ALTER TABLE public.dab_rbac_objectrole OWNER TO awx;

--
-- Name: dab_rbac_objectrole_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.dab_rbac_objectrole ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.dab_rbac_objectrole_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: dab_rbac_objectrole_provides_teams; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.dab_rbac_objectrole_provides_teams (
    id integer NOT NULL,
    objectrole_id bigint NOT NULL,
    team_id integer NOT NULL
);


ALTER TABLE public.dab_rbac_objectrole_provides_teams OWNER TO awx;

--
-- Name: dab_rbac_objectrole_provides_teams_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.dab_rbac_objectrole_provides_teams ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.dab_rbac_objectrole_provides_teams_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: dab_rbac_roledefinition; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.dab_rbac_roledefinition (
    id bigint NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    managed boolean NOT NULL,
    content_type_id integer,
    created_by_id integer,
    created timestamp with time zone NOT NULL,
    modified_by_id integer,
    modified timestamp with time zone NOT NULL
);


ALTER TABLE public.dab_rbac_roledefinition OWNER TO awx;

--
-- Name: dab_rbac_roledefinition_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.dab_rbac_roledefinition ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.dab_rbac_roledefinition_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: dab_rbac_roledefinition_permissions; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.dab_rbac_roledefinition_permissions (
    id integer NOT NULL,
    roledefinition_id bigint NOT NULL,
    dabpermission_id bigint NOT NULL
);


ALTER TABLE public.dab_rbac_roledefinition_permissions OWNER TO awx;

--
-- Name: dab_rbac_roledefinition_permissions_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.dab_rbac_roledefinition_permissions ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.dab_rbac_roledefinition_permissions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: dab_rbac_roleevaluation; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.dab_rbac_roleevaluation (
    id bigint NOT NULL,
    codename text NOT NULL,
    content_type_id integer NOT NULL,
    object_id integer NOT NULL,
    role_id bigint NOT NULL,
    CONSTRAINT dab_rbac_roleevaluation_content_type_id_check CHECK ((content_type_id >= 0)),
    CONSTRAINT dab_rbac_roleevaluation_object_id_check CHECK ((object_id >= 0))
);


ALTER TABLE public.dab_rbac_roleevaluation OWNER TO awx;

--
-- Name: dab_rbac_roleevaluation_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.dab_rbac_roleevaluation ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.dab_rbac_roleevaluation_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: dab_rbac_roleevaluationuuid; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.dab_rbac_roleevaluationuuid (
    id bigint NOT NULL,
    codename text NOT NULL,
    content_type_id integer NOT NULL,
    object_id uuid NOT NULL,
    role_id bigint NOT NULL,
    CONSTRAINT dab_rbac_roleevaluationuuid_content_type_id_check CHECK ((content_type_id >= 0))
);


ALTER TABLE public.dab_rbac_roleevaluationuuid OWNER TO awx;

--
-- Name: dab_rbac_roleevaluationuuid_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.dab_rbac_roleevaluationuuid ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.dab_rbac_roleevaluationuuid_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: dab_rbac_roleteamassignment; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.dab_rbac_roleteamassignment (
    id bigint NOT NULL,
    created timestamp with time zone NOT NULL,
    content_type_id integer,
    object_id text,
    role_definition_id bigint NOT NULL,
    created_by_id integer,
    team_id integer NOT NULL,
    object_role_id bigint
);


ALTER TABLE public.dab_rbac_roleteamassignment OWNER TO awx;

--
-- Name: dab_rbac_roleteamassignment_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.dab_rbac_roleteamassignment ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.dab_rbac_roleteamassignment_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: dab_rbac_roleuserassignment; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.dab_rbac_roleuserassignment (
    id bigint NOT NULL,
    created timestamp with time zone NOT NULL,
    content_type_id integer,
    object_id text,
    role_definition_id bigint NOT NULL,
    created_by_id integer,
    user_id integer NOT NULL,
    object_role_id bigint
);


ALTER TABLE public.dab_rbac_roleuserassignment OWNER TO awx;

--
-- Name: dab_rbac_roleuserassignment_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.dab_rbac_roleuserassignment ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.dab_rbac_roleuserassignment_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: dab_resource_registry_resource; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.dab_resource_registry_resource (
    id bigint NOT NULL,
    object_id text NOT NULL,
    service_id uuid NOT NULL,
    ansible_id uuid NOT NULL,
    name character varying(512),
    content_type_id integer NOT NULL,
    is_partially_migrated boolean NOT NULL
);


ALTER TABLE public.dab_resource_registry_resource OWNER TO awx;

--
-- Name: dab_resource_registry_resource_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.dab_resource_registry_resource ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.dab_resource_registry_resource_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: dab_resource_registry_resourcetype; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.dab_resource_registry_resourcetype (
    id bigint NOT NULL,
    externally_managed boolean NOT NULL,
    name character varying(256) NOT NULL,
    content_type_id integer NOT NULL
);


ALTER TABLE public.dab_resource_registry_resourcetype OWNER TO awx;

--
-- Name: dab_resource_registry_resourcetype_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.dab_resource_registry_resourcetype ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.dab_resource_registry_resourcetype_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: dab_resource_registry_serviceid; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.dab_resource_registry_serviceid (
    id uuid NOT NULL
);


ALTER TABLE public.dab_resource_registry_serviceid OWNER TO awx;

--
-- Name: django_content_type; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.django_content_type (
    id integer NOT NULL,
    app_label character varying(100) NOT NULL,
    model character varying(100) NOT NULL
);


ALTER TABLE public.django_content_type OWNER TO awx;

--
-- Name: django_content_type_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.django_content_type ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.django_content_type_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: django_migrations; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.django_migrations (
    id integer NOT NULL,
    app character varying(255) NOT NULL,
    name character varying(255) NOT NULL,
    applied timestamp with time zone NOT NULL
);


ALTER TABLE public.django_migrations OWNER TO awx;

--
-- Name: django_migrations_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.django_migrations ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.django_migrations_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: django_session; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.django_session (
    session_key character varying(40) NOT NULL,
    session_data text NOT NULL,
    expire_date timestamp with time zone NOT NULL
);


ALTER TABLE public.django_session OWNER TO awx;

--
-- Name: django_site; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.django_site (
    id integer NOT NULL,
    domain character varying(100) NOT NULL,
    name character varying(50) NOT NULL
);


ALTER TABLE public.django_site OWNER TO awx;

--
-- Name: django_site_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.django_site ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.django_site_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: flags_flagstate; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.flags_flagstate (
    id integer NOT NULL,
    name character varying(64) NOT NULL,
    condition character varying(64) NOT NULL,
    value character varying(127) NOT NULL,
    required boolean NOT NULL
);


ALTER TABLE public.flags_flagstate OWNER TO awx;

--
-- Name: flags_flagstate_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.flags_flagstate ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.flags_flagstate_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream (
    id integer NOT NULL,
    operation character varying(13) NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    changes text NOT NULL,
    object_relationship_type text NOT NULL,
    object1 text NOT NULL,
    object2 text NOT NULL,
    actor_id integer,
    action_node character varying(512) NOT NULL,
    deleted_actor jsonb,
    setting jsonb NOT NULL
);


ALTER TABLE public.main_activitystream OWNER TO awx;

--
-- Name: main_activitystream_ad_hoc_command; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_ad_hoc_command (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    adhoccommand_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_ad_hoc_command OWNER TO awx;

--
-- Name: main_activitystream_ad_hoc_command_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_ad_hoc_command ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_ad_hoc_command_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_credential; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_credential (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    credential_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_credential OWNER TO awx;

--
-- Name: main_activitystream_credential_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_credential ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_credential_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_credential_type; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_credential_type (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    credentialtype_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_credential_type OWNER TO awx;

--
-- Name: main_activitystream_credential_type_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_credential_type ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_credential_type_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_execution_environment; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_execution_environment (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    executionenvironment_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_execution_environment OWNER TO awx;

--
-- Name: main_activitystream_execution_environment_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_execution_environment ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_execution_environment_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_group; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_group (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    group_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_group OWNER TO awx;

--
-- Name: main_activitystream_group_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_group ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_group_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_host; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_host (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    host_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_host OWNER TO awx;

--
-- Name: main_activitystream_host_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_host ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_host_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_instance; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_instance (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    instance_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_instance OWNER TO awx;

--
-- Name: main_activitystream_instance_group; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_instance_group (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    instancegroup_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_instance_group OWNER TO awx;

--
-- Name: main_activitystream_instance_group_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_instance_group ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_instance_group_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_instance_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_instance ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_instance_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_inventory; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_inventory (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    inventory_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_inventory OWNER TO awx;

--
-- Name: main_activitystream_inventory_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_inventory ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_inventory_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_inventory_source; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_inventory_source (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    inventorysource_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_inventory_source OWNER TO awx;

--
-- Name: main_activitystream_inventory_source_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_inventory_source ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_inventory_source_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_inventory_update; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_inventory_update (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    inventoryupdate_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_inventory_update OWNER TO awx;

--
-- Name: main_activitystream_inventory_update_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_inventory_update ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_inventory_update_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_job; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_job (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    job_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_job OWNER TO awx;

--
-- Name: main_activitystream_job_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_job ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_job_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_job_template; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_job_template (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    jobtemplate_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_job_template OWNER TO awx;

--
-- Name: main_activitystream_job_template_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_job_template ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_job_template_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_label; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_label (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    label_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_label OWNER TO awx;

--
-- Name: main_activitystream_label_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_label ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_label_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_notification; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_notification (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    notification_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_notification OWNER TO awx;

--
-- Name: main_activitystream_notification_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_notification ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_notification_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_notification_template; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_notification_template (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    notificationtemplate_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_notification_template OWNER TO awx;

--
-- Name: main_activitystream_notification_template_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_notification_template ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_notification_template_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_organization; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_organization (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    organization_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_organization OWNER TO awx;

--
-- Name: main_activitystream_organization_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_organization ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_organization_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_project; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_project (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    project_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_project OWNER TO awx;

--
-- Name: main_activitystream_project_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_project ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_project_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_project_update; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_project_update (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    projectupdate_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_project_update OWNER TO awx;

--
-- Name: main_activitystream_project_update_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_project_update ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_project_update_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_receptor_address; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_receptor_address (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    receptoraddress_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_receptor_address OWNER TO awx;

--
-- Name: main_activitystream_receptor_address_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_receptor_address ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_receptor_address_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_role; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_role (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    role_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_role OWNER TO awx;

--
-- Name: main_activitystream_role_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_role ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_role_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_schedule; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_schedule (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    schedule_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_schedule OWNER TO awx;

--
-- Name: main_activitystream_schedule_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_schedule ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_schedule_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_team; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_team (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    team_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_team OWNER TO awx;

--
-- Name: main_activitystream_team_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_team ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_team_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_unified_job; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_unified_job (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    unifiedjob_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_unified_job OWNER TO awx;

--
-- Name: main_activitystream_unified_job_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_unified_job ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_unified_job_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_unified_job_template; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_unified_job_template (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    unifiedjobtemplate_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_unified_job_template OWNER TO awx;

--
-- Name: main_activitystream_unified_job_template_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_unified_job_template ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_unified_job_template_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_user; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_user (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    user_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_user OWNER TO awx;

--
-- Name: main_activitystream_user_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_user ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_user_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_workflow_approval; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_workflow_approval (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    workflowapproval_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_workflow_approval OWNER TO awx;

--
-- Name: main_activitystream_workflow_approval_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_workflow_approval ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_workflow_approval_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_workflow_approval_template; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_workflow_approval_template (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    workflowapprovaltemplate_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_workflow_approval_template OWNER TO awx;

--
-- Name: main_activitystream_workflow_approval_template_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_workflow_approval_template ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_workflow_approval_template_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_workflow_job; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_workflow_job (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    workflowjob_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_workflow_job OWNER TO awx;

--
-- Name: main_activitystream_workflow_job_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_workflow_job ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_workflow_job_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_workflow_job_node; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_workflow_job_node (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    workflowjobnode_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_workflow_job_node OWNER TO awx;

--
-- Name: main_activitystream_workflow_job_node_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_workflow_job_node ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_workflow_job_node_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_workflow_job_template; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_workflow_job_template (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    workflowjobtemplate_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_workflow_job_template OWNER TO awx;

--
-- Name: main_activitystream_workflow_job_template_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_workflow_job_template ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_workflow_job_template_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_activitystream_workflow_job_template_node; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_activitystream_workflow_job_template_node (
    id integer NOT NULL,
    activitystream_id integer NOT NULL,
    workflowjobtemplatenode_id integer NOT NULL
);


ALTER TABLE public.main_activitystream_workflow_job_template_node OWNER TO awx;

--
-- Name: main_activitystream_workflow_job_template_node_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_workflow_job_template_node ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_activitystream_workflow_job_template_node_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_adhoccommand; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_adhoccommand (
    unifiedjob_ptr_id integer NOT NULL,
    job_type character varying(64) NOT NULL,
    "limit" text NOT NULL,
    module_name character varying(1024) NOT NULL,
    module_args text NOT NULL,
    forks integer NOT NULL,
    verbosity integer NOT NULL,
    become_enabled boolean NOT NULL,
    credential_id integer,
    inventory_id integer,
    extra_vars text NOT NULL,
    diff_mode boolean NOT NULL,
    CONSTRAINT main_adhoccommand_forks_check CHECK ((forks >= 0)),
    CONSTRAINT main_adhoccommand_verbosity_check CHECK ((verbosity >= 0))
);


ALTER TABLE public.main_adhoccommand OWNER TO awx;

--
-- Name: main_adhoccommandevent; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_adhoccommandevent (
    id bigint NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone NOT NULL,
    host_name character varying(1024) NOT NULL,
    event character varying(100) NOT NULL,
    event_data text NOT NULL,
    failed boolean NOT NULL,
    changed boolean NOT NULL,
    counter integer NOT NULL,
    host_id integer,
    ad_hoc_command_id integer NOT NULL,
    end_line integer NOT NULL,
    start_line integer NOT NULL,
    stdout text NOT NULL,
    uuid character varying(1024) NOT NULL,
    verbosity integer NOT NULL,
    job_created timestamp with time zone NOT NULL,
    CONSTRAINT main_adhoccommandevent_counter_check CHECK ((counter >= 0)),
    CONSTRAINT main_adhoccommandevent_end_line_check CHECK ((end_line >= 0)),
    CONSTRAINT main_adhoccommandevent_start_line_check CHECK ((start_line >= 0)),
    CONSTRAINT main_adhoccommandevent_verbosity_check CHECK ((verbosity >= 0))
)
PARTITION BY RANGE (job_created);


ALTER TABLE public.main_adhoccommandevent OWNER TO awx;

--
-- Name: main_adhoccommandevent_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public._unpartitioned_main_adhoccommandevent ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_adhoccommandevent_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_adhoccommandevent_id_seq1; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_adhoccommandevent ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_adhoccommandevent_id_seq1
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_credential; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_credential (
    id integer NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    description text NOT NULL,
    name character varying(512) NOT NULL,
    created_by_id integer,
    modified_by_id integer,
    organization_id integer,
    admin_role_id integer,
    use_role_id integer,
    read_role_id integer,
    inputs jsonb NOT NULL,
    credential_type_id integer NOT NULL,
    managed boolean NOT NULL
);


ALTER TABLE public.main_credential OWNER TO awx;

--
-- Name: main_credential_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_credential ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_credential_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_credentialinputsource; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_credentialinputsource (
    id integer NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    description text NOT NULL,
    input_field_name character varying(1024) NOT NULL,
    metadata jsonb NOT NULL,
    created_by_id integer,
    modified_by_id integer,
    source_credential_id integer,
    target_credential_id integer
);


ALTER TABLE public.main_credentialinputsource OWNER TO awx;

--
-- Name: main_credentialinputsource_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_credentialinputsource ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_credentialinputsource_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_credentialtype; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_credentialtype (
    id integer NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    description text NOT NULL,
    name character varying(512) NOT NULL,
    kind character varying(32) NOT NULL,
    managed boolean NOT NULL,
    inputs jsonb NOT NULL,
    injectors jsonb NOT NULL,
    created_by_id integer,
    modified_by_id integer,
    namespace character varying(1024)
);


ALTER TABLE public.main_credentialtype OWNER TO awx;

--
-- Name: main_credentialtype_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_credentialtype ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_credentialtype_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_custominventoryscript; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_custominventoryscript (
    id integer NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    description text NOT NULL,
    name character varying(512) NOT NULL,
    script text NOT NULL,
    created_by_id integer,
    modified_by_id integer
);


ALTER TABLE public.main_custominventoryscript OWNER TO awx;

--
-- Name: main_custominventoryscript_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_custominventoryscript ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_custominventoryscript_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_executionenvironment; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_executionenvironment (
    id integer NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    description text NOT NULL,
    image character varying(1024) NOT NULL,
    managed boolean NOT NULL,
    created_by_id integer,
    credential_id integer,
    modified_by_id integer,
    organization_id integer,
    name character varying(512) NOT NULL,
    pull character varying(16) NOT NULL
);


ALTER TABLE public.main_executionenvironment OWNER TO awx;

--
-- Name: main_executionenvironment_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_executionenvironment ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_executionenvironment_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_group; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_group (
    id integer NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    description text NOT NULL,
    name character varying(512) NOT NULL,
    variables text NOT NULL,
    created_by_id integer,
    inventory_id integer NOT NULL,
    modified_by_id integer
);


ALTER TABLE public.main_group OWNER TO awx;

--
-- Name: main_group_hosts; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_group_hosts (
    id integer NOT NULL,
    group_id integer NOT NULL,
    host_id integer NOT NULL
);


ALTER TABLE public.main_group_hosts OWNER TO awx;

--
-- Name: main_group_hosts_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_group_hosts ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_group_hosts_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_group_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_group ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_group_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_group_inventory_sources; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_group_inventory_sources (
    id integer NOT NULL,
    group_id integer NOT NULL,
    inventorysource_id integer NOT NULL
);


ALTER TABLE public.main_group_inventory_sources OWNER TO awx;

--
-- Name: main_group_inventory_sources_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_group_inventory_sources ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_group_inventory_sources_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_group_parents; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_group_parents (
    id integer NOT NULL,
    from_group_id integer NOT NULL,
    to_group_id integer NOT NULL
);


ALTER TABLE public.main_group_parents OWNER TO awx;

--
-- Name: main_group_parents_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_group_parents ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_group_parents_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_host; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_host (
    id integer NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    description text NOT NULL,
    name character varying(512) NOT NULL,
    enabled boolean NOT NULL,
    instance_id character varying(1024) NOT NULL,
    variables text NOT NULL,
    created_by_id integer,
    inventory_id integer NOT NULL,
    last_job_host_summary_id integer,
    modified_by_id integer,
    last_job_id integer,
    ansible_facts jsonb NOT NULL,
    ansible_facts_modified timestamp with time zone
);


ALTER TABLE public.main_host OWNER TO awx;

--
-- Name: main_host_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_host ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_host_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_host_inventory_sources; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_host_inventory_sources (
    id integer NOT NULL,
    host_id integer NOT NULL,
    inventorysource_id integer NOT NULL
);


ALTER TABLE public.main_host_inventory_sources OWNER TO awx;

--
-- Name: main_host_inventory_sources_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_host_inventory_sources ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_host_inventory_sources_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_hostmetric; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_hostmetric (
    hostname character varying(512) NOT NULL,
    first_automation timestamp with time zone NOT NULL,
    last_automation timestamp with time zone NOT NULL,
    last_deleted timestamp with time zone,
    automated_counter bigint NOT NULL,
    deleted_counter integer NOT NULL,
    deleted boolean NOT NULL,
    used_in_inventories integer,
    id integer NOT NULL
);


ALTER TABLE public.main_hostmetric OWNER TO awx;

--
-- Name: main_hostmetric_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_hostmetric ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_hostmetric_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_hostmetricsummarymonthly; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_hostmetricsummarymonthly (
    id integer NOT NULL,
    date date NOT NULL,
    license_consumed bigint NOT NULL,
    license_capacity bigint NOT NULL,
    hosts_added integer NOT NULL,
    hosts_deleted integer NOT NULL,
    indirectly_managed_hosts integer NOT NULL
);


ALTER TABLE public.main_hostmetricsummarymonthly OWNER TO awx;

--
-- Name: main_hostmetricsummarymonthly_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_hostmetricsummarymonthly ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_hostmetricsummarymonthly_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_instance; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_instance (
    id integer NOT NULL,
    uuid character varying(40) NOT NULL,
    hostname character varying(250) NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    capacity integer NOT NULL,
    version character varying(120) NOT NULL,
    capacity_adjustment numeric(3,2) NOT NULL,
    cpu numeric(4,1) NOT NULL,
    memory bigint NOT NULL,
    cpu_capacity integer NOT NULL,
    mem_capacity integer NOT NULL,
    enabled boolean NOT NULL,
    managed_by_policy boolean NOT NULL,
    ip_address character varying(50) NOT NULL,
    node_type character varying(16) NOT NULL,
    last_seen timestamp with time zone,
    errors text NOT NULL,
    last_health_check timestamp with time zone,
    node_state character varying(16) NOT NULL,
    health_check_started timestamp with time zone,
    managed boolean NOT NULL,
    CONSTRAINT main_instance_capacity_check CHECK ((capacity >= 0))
);


ALTER TABLE public.main_instance OWNER TO awx;

--
-- Name: main_instance_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_instance ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_instance_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_instancegroup; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_instancegroup (
    id integer NOT NULL,
    name character varying(250) NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    policy_instance_list jsonb NOT NULL,
    policy_instance_minimum integer NOT NULL,
    policy_instance_percentage integer NOT NULL,
    credential_id integer,
    pod_spec_override text NOT NULL,
    is_container_group boolean NOT NULL,
    max_concurrent_jobs integer NOT NULL,
    max_forks integer NOT NULL,
    admin_role_id integer,
    read_role_id integer,
    use_role_id integer
);


ALTER TABLE public.main_instancegroup OWNER TO awx;

--
-- Name: main_instancegroup_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_instancegroup ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_instancegroup_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_instancegroup_instances; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_instancegroup_instances (
    id integer NOT NULL,
    instancegroup_id integer NOT NULL,
    instance_id integer NOT NULL
);


ALTER TABLE public.main_instancegroup_instances OWNER TO awx;

--
-- Name: main_instancegroup_instances_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_instancegroup_instances ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_instancegroup_instances_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_instancelink; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_instancelink (
    id integer NOT NULL,
    source_id integer NOT NULL,
    link_state character varying(16) NOT NULL,
    target_id integer NOT NULL
);


ALTER TABLE public.main_instancelink OWNER TO awx;

--
-- Name: main_instancelink_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_instancelink ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_instancelink_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_inventory; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_inventory (
    id integer NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    description text NOT NULL,
    name character varying(512) NOT NULL,
    variables text NOT NULL,
    has_active_failures boolean NOT NULL,
    total_hosts integer NOT NULL,
    hosts_with_active_failures integer NOT NULL,
    total_groups integer NOT NULL,
    has_inventory_sources boolean NOT NULL,
    total_inventory_sources integer NOT NULL,
    inventory_sources_with_failures integer NOT NULL,
    created_by_id integer,
    modified_by_id integer,
    organization_id integer,
    admin_role_id integer,
    adhoc_role_id integer,
    update_role_id integer,
    use_role_id integer,
    read_role_id integer,
    host_filter text,
    kind character varying(32) NOT NULL,
    pending_deletion boolean NOT NULL,
    prevent_instance_group_fallback boolean NOT NULL,
    CONSTRAINT main_inventory_hosts_with_active_failures_check CHECK ((hosts_with_active_failures >= 0)),
    CONSTRAINT main_inventory_inventory_sources_with_failures_check CHECK ((inventory_sources_with_failures >= 0)),
    CONSTRAINT main_inventory_total_groups_check CHECK ((total_groups >= 0)),
    CONSTRAINT main_inventory_total_hosts_check CHECK ((total_hosts >= 0)),
    CONSTRAINT main_inventory_total_inventory_sources_check CHECK ((total_inventory_sources >= 0))
);


ALTER TABLE public.main_inventory OWNER TO awx;

--
-- Name: main_inventory_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_inventory ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_inventory_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_inventory_labels; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_inventory_labels (
    id integer NOT NULL,
    inventory_id integer NOT NULL,
    label_id integer NOT NULL
);


ALTER TABLE public.main_inventory_labels OWNER TO awx;

--
-- Name: main_inventory_labels_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_inventory_labels ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_inventory_labels_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_inventoryconstructedinventorymembership; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_inventoryconstructedinventorymembership (
    id integer NOT NULL,
    "position" integer,
    constructed_inventory_id integer NOT NULL,
    input_inventory_id integer NOT NULL,
    CONSTRAINT main_inventoryconstructedinventorymembership_position_check CHECK (("position" >= 0))
);


ALTER TABLE public.main_inventoryconstructedinventorymembership OWNER TO awx;

--
-- Name: main_inventoryconstructedinventorymembership_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_inventoryconstructedinventorymembership ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_inventoryconstructedinventorymembership_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_inventoryinstancegroupmembership; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_inventoryinstancegroupmembership (
    id integer NOT NULL,
    "position" integer,
    instancegroup_id integer NOT NULL,
    inventory_id integer NOT NULL,
    CONSTRAINT main_inventoryinstancegroupmembership_position_check CHECK (("position" >= 0))
);


ALTER TABLE public.main_inventoryinstancegroupmembership OWNER TO awx;

--
-- Name: main_inventoryinstancegroupmembership_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_inventoryinstancegroupmembership ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_inventoryinstancegroupmembership_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_inventorysource; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_inventorysource (
    unifiedjobtemplate_ptr_id integer NOT NULL,
    source character varying(32) NOT NULL,
    source_path character varying(1024) NOT NULL,
    source_vars text NOT NULL,
    overwrite boolean NOT NULL,
    overwrite_vars boolean NOT NULL,
    update_on_launch boolean NOT NULL,
    update_cache_timeout integer NOT NULL,
    inventory_id integer,
    timeout integer NOT NULL,
    source_project_id integer,
    verbosity integer NOT NULL,
    custom_virtualenv character varying(100),
    enabled_value text NOT NULL,
    enabled_var text NOT NULL,
    host_filter text NOT NULL,
    scm_branch character varying(1024) NOT NULL,
    "limit" text NOT NULL,
    CONSTRAINT main_inventorysource_update_cache_timeout_check CHECK ((update_cache_timeout >= 0)),
    CONSTRAINT main_inventorysource_verbosity_check CHECK ((verbosity >= 0))
);


ALTER TABLE public.main_inventorysource OWNER TO awx;

--
-- Name: main_inventoryupdate; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_inventoryupdate (
    unifiedjob_ptr_id integer NOT NULL,
    source character varying(32) NOT NULL,
    source_path character varying(1024) NOT NULL,
    source_vars text NOT NULL,
    overwrite boolean NOT NULL,
    overwrite_vars boolean NOT NULL,
    license_error boolean NOT NULL,
    inventory_source_id integer NOT NULL,
    timeout integer NOT NULL,
    source_project_update_id integer,
    verbosity integer NOT NULL,
    inventory_id integer,
    custom_virtualenv character varying(100),
    org_host_limit_error boolean NOT NULL,
    enabled_value text NOT NULL,
    enabled_var text NOT NULL,
    host_filter text NOT NULL,
    scm_revision character varying(1024) NOT NULL,
    scm_branch character varying(1024) NOT NULL,
    "limit" text NOT NULL,
    CONSTRAINT main_inventoryupdate_verbosity_check CHECK ((verbosity >= 0))
);


ALTER TABLE public.main_inventoryupdate OWNER TO awx;

--
-- Name: main_inventoryupdateevent; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_inventoryupdateevent (
    id bigint NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone NOT NULL,
    event_data text NOT NULL,
    uuid character varying(1024) NOT NULL,
    counter integer NOT NULL,
    stdout text NOT NULL,
    verbosity integer NOT NULL,
    start_line integer NOT NULL,
    end_line integer NOT NULL,
    inventory_update_id integer NOT NULL,
    job_created timestamp with time zone NOT NULL,
    CONSTRAINT main_inventoryupdateevent_counter_check CHECK ((counter >= 0)),
    CONSTRAINT main_inventoryupdateevent_end_line_check CHECK ((end_line >= 0)),
    CONSTRAINT main_inventoryupdateevent_start_line_check CHECK ((start_line >= 0)),
    CONSTRAINT main_inventoryupdateevent_verbosity_check CHECK ((verbosity >= 0))
)
PARTITION BY RANGE (job_created);


ALTER TABLE public.main_inventoryupdateevent OWNER TO awx;

--
-- Name: main_inventoryupdateevent_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public._unpartitioned_main_inventoryupdateevent ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_inventoryupdateevent_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_inventoryupdateevent_id_seq1; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_inventoryupdateevent ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_inventoryupdateevent_id_seq1
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_job; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_job (
    unifiedjob_ptr_id integer NOT NULL,
    job_type character varying(64) NOT NULL,
    playbook character varying(1024) NOT NULL,
    forks integer NOT NULL,
    "limit" text NOT NULL,
    verbosity integer NOT NULL,
    extra_vars text NOT NULL,
    job_tags text NOT NULL,
    force_handlers boolean NOT NULL,
    skip_tags character varying(1024) NOT NULL,
    start_at_task character varying(1024) NOT NULL,
    become_enabled boolean NOT NULL,
    inventory_id integer,
    job_template_id integer,
    project_id integer,
    allow_simultaneous boolean NOT NULL,
    artifacts text NOT NULL,
    timeout integer NOT NULL,
    scm_revision character varying(1024) NOT NULL,
    project_update_id integer,
    use_fact_cache boolean NOT NULL,
    diff_mode boolean NOT NULL,
    job_slice_count integer NOT NULL,
    job_slice_number integer NOT NULL,
    custom_virtualenv character varying(100),
    scm_branch character varying(1024) NOT NULL,
    webhook_credential_id integer,
    webhook_guid character varying(128) NOT NULL,
    webhook_service character varying(16) NOT NULL,
    survey_passwords jsonb NOT NULL,
    CONSTRAINT main_job_forks_check CHECK ((forks >= 0)),
    CONSTRAINT main_job_job_slice_count_check CHECK ((job_slice_count >= 0)),
    CONSTRAINT main_job_job_slice_number_check CHECK ((job_slice_number >= 0)),
    CONSTRAINT main_job_verbosity_check CHECK ((verbosity >= 0))
);


ALTER TABLE public.main_job OWNER TO awx;

--
-- Name: main_jobevent; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_jobevent (
    id bigint NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone NOT NULL,
    event character varying(100) NOT NULL,
    event_data text NOT NULL,
    failed boolean NOT NULL,
    changed boolean NOT NULL,
    host_name character varying(1024) NOT NULL,
    play character varying(1024) NOT NULL,
    role character varying(1024) NOT NULL,
    task character varying(1024) NOT NULL,
    counter integer NOT NULL,
    host_id integer,
    job_id integer,
    uuid character varying(1024) NOT NULL,
    parent_uuid character varying(1024) NOT NULL,
    end_line integer NOT NULL,
    playbook character varying(1024) NOT NULL,
    start_line integer NOT NULL,
    stdout text NOT NULL,
    verbosity integer NOT NULL,
    job_created timestamp with time zone NOT NULL,
    CONSTRAINT main_jobevent_counter_check CHECK ((counter >= 0)),
    CONSTRAINT main_jobevent_end_line_check CHECK ((end_line >= 0)),
    CONSTRAINT main_jobevent_start_line_check CHECK ((start_line >= 0)),
    CONSTRAINT main_jobevent_verbosity_check CHECK ((verbosity >= 0))
)
PARTITION BY RANGE (job_created);


ALTER TABLE public.main_jobevent OWNER TO awx;

--
-- Name: main_jobevent_20241219_17; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_jobevent_20241219_17 (
    id bigint NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone NOT NULL,
    event character varying(100) NOT NULL,
    event_data text NOT NULL,
    failed boolean NOT NULL,
    changed boolean NOT NULL,
    host_name character varying(1024) NOT NULL,
    play character varying(1024) NOT NULL,
    role character varying(1024) NOT NULL,
    task character varying(1024) NOT NULL,
    counter integer NOT NULL,
    host_id integer,
    job_id integer,
    uuid character varying(1024) NOT NULL,
    parent_uuid character varying(1024) NOT NULL,
    end_line integer NOT NULL,
    playbook character varying(1024) NOT NULL,
    start_line integer NOT NULL,
    stdout text NOT NULL,
    verbosity integer NOT NULL,
    job_created timestamp with time zone NOT NULL,
    CONSTRAINT main_jobevent_counter_check CHECK ((counter >= 0)),
    CONSTRAINT main_jobevent_end_line_check CHECK ((end_line >= 0)),
    CONSTRAINT main_jobevent_start_line_check CHECK ((start_line >= 0)),
    CONSTRAINT main_jobevent_verbosity_check CHECK ((verbosity >= 0))
);


ALTER TABLE public.main_jobevent_20241219_17 OWNER TO awx;

--
-- Name: main_jobevent_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public._unpartitioned_main_jobevent ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_jobevent_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_jobevent_id_seq1; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_jobevent ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_jobevent_id_seq1
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_jobhostsummary; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_jobhostsummary (
    id integer NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    host_name character varying(1024) NOT NULL,
    changed integer NOT NULL,
    dark integer NOT NULL,
    failures integer NOT NULL,
    ok integer NOT NULL,
    processed integer NOT NULL,
    skipped integer NOT NULL,
    failed boolean NOT NULL,
    host_id integer,
    job_id integer NOT NULL,
    ignored integer NOT NULL,
    rescued integer NOT NULL,
    constructed_host_id integer,
    CONSTRAINT main_jobhostsummary_changed_check CHECK ((changed >= 0)),
    CONSTRAINT main_jobhostsummary_dark_check CHECK ((dark >= 0)),
    CONSTRAINT main_jobhostsummary_failures_check CHECK ((failures >= 0)),
    CONSTRAINT main_jobhostsummary_ignored_check CHECK ((ignored >= 0)),
    CONSTRAINT main_jobhostsummary_ok_check CHECK ((ok >= 0)),
    CONSTRAINT main_jobhostsummary_processed_check CHECK ((processed >= 0)),
    CONSTRAINT main_jobhostsummary_rescued_check CHECK ((rescued >= 0)),
    CONSTRAINT main_jobhostsummary_skipped_check CHECK ((skipped >= 0))
);


ALTER TABLE public.main_jobhostsummary OWNER TO awx;

--
-- Name: main_jobhostsummary_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_jobhostsummary ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_jobhostsummary_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_joblaunchconfig; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_joblaunchconfig (
    id integer NOT NULL,
    extra_data text NOT NULL,
    inventory_id integer,
    job_id integer NOT NULL,
    execution_environment_id integer,
    char_prompts jsonb NOT NULL,
    survey_passwords jsonb NOT NULL
);


ALTER TABLE public.main_joblaunchconfig OWNER TO awx;

--
-- Name: main_joblaunchconfig_credentials; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_joblaunchconfig_credentials (
    id integer NOT NULL,
    joblaunchconfig_id integer NOT NULL,
    credential_id integer NOT NULL
);


ALTER TABLE public.main_joblaunchconfig_credentials OWNER TO awx;

--
-- Name: main_joblaunchconfig_credentials_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_joblaunchconfig_credentials ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_joblaunchconfig_credentials_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_joblaunchconfig_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_joblaunchconfig ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_joblaunchconfig_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_joblaunchconfig_labels; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_joblaunchconfig_labels (
    id integer NOT NULL,
    joblaunchconfig_id integer NOT NULL,
    label_id integer NOT NULL
);


ALTER TABLE public.main_joblaunchconfig_labels OWNER TO awx;

--
-- Name: main_joblaunchconfig_labels_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_joblaunchconfig_labels ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_joblaunchconfig_labels_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_joblaunchconfiginstancegroupmembership; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_joblaunchconfiginstancegroupmembership (
    id integer NOT NULL,
    "position" integer,
    instancegroup_id integer NOT NULL,
    joblaunchconfig_id integer NOT NULL,
    CONSTRAINT main_joblaunchconfiginstancegroupmembership_position_check CHECK (("position" >= 0))
);


ALTER TABLE public.main_joblaunchconfiginstancegroupmembership OWNER TO awx;

--
-- Name: main_joblaunchconfiginstancegroupmembership_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_joblaunchconfiginstancegroupmembership ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_joblaunchconfiginstancegroupmembership_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_jobtemplate; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_jobtemplate (
    unifiedjobtemplate_ptr_id integer NOT NULL,
    job_type character varying(64) NOT NULL,
    playbook character varying(1024) NOT NULL,
    forks integer NOT NULL,
    "limit" text NOT NULL,
    verbosity integer NOT NULL,
    extra_vars text NOT NULL,
    job_tags text NOT NULL,
    force_handlers boolean NOT NULL,
    skip_tags character varying(1024) NOT NULL,
    start_at_task character varying(1024) NOT NULL,
    become_enabled boolean NOT NULL,
    host_config_key character varying(1024) NOT NULL,
    ask_variables_on_launch boolean NOT NULL,
    survey_enabled boolean NOT NULL,
    survey_spec jsonb NOT NULL,
    inventory_id integer,
    project_id integer,
    admin_role_id integer,
    execute_role_id integer,
    read_role_id integer,
    ask_limit_on_launch boolean NOT NULL,
    ask_inventory_on_launch boolean NOT NULL,
    ask_credential_on_launch boolean NOT NULL,
    ask_job_type_on_launch boolean NOT NULL,
    ask_tags_on_launch boolean NOT NULL,
    allow_simultaneous boolean NOT NULL,
    ask_skip_tags_on_launch boolean NOT NULL,
    timeout integer NOT NULL,
    use_fact_cache boolean NOT NULL,
    ask_verbosity_on_launch boolean NOT NULL,
    ask_diff_mode_on_launch boolean NOT NULL,
    diff_mode boolean NOT NULL,
    custom_virtualenv character varying(100),
    job_slice_count integer NOT NULL,
    ask_scm_branch_on_launch boolean NOT NULL,
    scm_branch character varying(1024) NOT NULL,
    webhook_credential_id integer,
    webhook_key character varying(64) NOT NULL,
    webhook_service character varying(16) NOT NULL,
    ask_execution_environment_on_launch boolean NOT NULL,
    ask_forks_on_launch boolean NOT NULL,
    ask_instance_groups_on_launch boolean NOT NULL,
    ask_job_slice_count_on_launch boolean NOT NULL,
    ask_labels_on_launch boolean NOT NULL,
    ask_timeout_on_launch boolean NOT NULL,
    prevent_instance_group_fallback boolean NOT NULL,
    CONSTRAINT main_jobtemplate_forks_check CHECK ((forks >= 0)),
    CONSTRAINT main_jobtemplate_job_slice_count_check CHECK ((job_slice_count >= 0)),
    CONSTRAINT main_jobtemplate_verbosity_check CHECK ((verbosity >= 0))
);


ALTER TABLE public.main_jobtemplate OWNER TO awx;

--
-- Name: main_label; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_label (
    id integer NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    description text NOT NULL,
    name character varying(512) NOT NULL,
    created_by_id integer,
    modified_by_id integer,
    organization_id integer NOT NULL
);


ALTER TABLE public.main_label OWNER TO awx;

--
-- Name: main_label_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_label ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_label_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_notification; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_notification (
    id integer NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    status character varying(20) NOT NULL,
    error text NOT NULL,
    notifications_sent integer NOT NULL,
    notification_type character varying(32) NOT NULL,
    recipients text NOT NULL,
    subject text NOT NULL,
    notification_template_id integer NOT NULL,
    body jsonb NOT NULL
);


ALTER TABLE public.main_notification OWNER TO awx;

--
-- Name: main_notification_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_notification ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_notification_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_notificationtemplate; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_notificationtemplate (
    id integer NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    description text NOT NULL,
    name character varying(512) NOT NULL,
    notification_type character varying(32) NOT NULL,
    notification_configuration jsonb NOT NULL,
    created_by_id integer,
    modified_by_id integer,
    organization_id integer,
    messages jsonb
);


ALTER TABLE public.main_notificationtemplate OWNER TO awx;

--
-- Name: main_notificationtemplate_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_notificationtemplate ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_notificationtemplate_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_organization; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_organization (
    id integer NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    description text NOT NULL,
    name character varying(512) NOT NULL,
    created_by_id integer,
    modified_by_id integer,
    admin_role_id integer,
    auditor_role_id integer,
    member_role_id integer,
    read_role_id integer,
    custom_virtualenv character varying(100),
    execute_role_id integer,
    job_template_admin_role_id integer,
    credential_admin_role_id integer,
    inventory_admin_role_id integer,
    project_admin_role_id integer,
    workflow_admin_role_id integer,
    notification_admin_role_id integer,
    max_hosts integer NOT NULL,
    approval_role_id integer,
    default_environment_id integer,
    execution_environment_admin_role_id integer,
    CONSTRAINT main_organization_max_hosts_check CHECK ((max_hosts >= 0))
);


ALTER TABLE public.main_organization OWNER TO awx;

--
-- Name: main_organization_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_organization ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_organization_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_organization_notification_templates_approvals; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_organization_notification_templates_approvals (
    id integer NOT NULL,
    organization_id integer NOT NULL,
    notificationtemplate_id integer NOT NULL
);


ALTER TABLE public.main_organization_notification_templates_approvals OWNER TO awx;

--
-- Name: main_organization_notification_templates_approvals_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_organization_notification_templates_approvals ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_organization_notification_templates_approvals_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_organization_notification_templates_error; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_organization_notification_templates_error (
    id integer NOT NULL,
    organization_id integer NOT NULL,
    notificationtemplate_id integer NOT NULL
);


ALTER TABLE public.main_organization_notification_templates_error OWNER TO awx;

--
-- Name: main_organization_notification_templates_error_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_organization_notification_templates_error ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_organization_notification_templates_error_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_organization_notification_templates_started; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_organization_notification_templates_started (
    id integer NOT NULL,
    organization_id integer NOT NULL,
    notificationtemplate_id integer NOT NULL
);


ALTER TABLE public.main_organization_notification_templates_started OWNER TO awx;

--
-- Name: main_organization_notification_templates_started_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_organization_notification_templates_started ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_organization_notification_templates_started_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_organization_notification_templates_success; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_organization_notification_templates_success (
    id integer NOT NULL,
    organization_id integer NOT NULL,
    notificationtemplate_id integer NOT NULL
);


ALTER TABLE public.main_organization_notification_templates_success OWNER TO awx;

--
-- Name: main_organization_notification_templates_success_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_organization_notification_templates_success ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_organization_notification_templates_success_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_organizationgalaxycredentialmembership; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_organizationgalaxycredentialmembership (
    id integer NOT NULL,
    "position" integer,
    credential_id integer NOT NULL,
    organization_id integer NOT NULL,
    CONSTRAINT main_organizationgalaxycredentialmembership_position_check CHECK (("position" >= 0))
);


ALTER TABLE public.main_organizationgalaxycredentialmembership OWNER TO awx;

--
-- Name: main_organizationgalaxycredentialmembership_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_organizationgalaxycredentialmembership ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_organizationgalaxycredentialmembership_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_organizationinstancegroupmembership; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_organizationinstancegroupmembership (
    id integer NOT NULL,
    "position" integer,
    instancegroup_id integer NOT NULL,
    organization_id integer NOT NULL,
    CONSTRAINT main_organizationinstancegroupmembership_position_check CHECK (("position" >= 0))
);


ALTER TABLE public.main_organizationinstancegroupmembership OWNER TO awx;

--
-- Name: main_organizationinstancegroupmembership_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_organizationinstancegroupmembership ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_organizationinstancegroupmembership_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_project; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_project (
    unifiedjobtemplate_ptr_id integer NOT NULL,
    local_path character varying(1024) NOT NULL,
    scm_type character varying(8) NOT NULL,
    scm_url character varying(1024) NOT NULL,
    scm_branch character varying(256) NOT NULL,
    scm_clean boolean NOT NULL,
    scm_delete_on_update boolean NOT NULL,
    scm_update_on_launch boolean NOT NULL,
    scm_update_cache_timeout integer NOT NULL,
    credential_id integer,
    admin_role_id integer,
    use_role_id integer,
    update_role_id integer,
    read_role_id integer,
    timeout integer NOT NULL,
    scm_revision character varying(1024) NOT NULL,
    playbook_files jsonb NOT NULL,
    inventory_files jsonb NOT NULL,
    custom_virtualenv character varying(100),
    scm_refspec character varying(1024) NOT NULL,
    allow_override boolean NOT NULL,
    default_environment_id integer,
    scm_track_submodules boolean NOT NULL,
    signature_validation_credential_id integer,
    CONSTRAINT main_project_scm_update_cache_timeout_check CHECK ((scm_update_cache_timeout >= 0))
);


ALTER TABLE public.main_project OWNER TO awx;

--
-- Name: main_projectupdate; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_projectupdate (
    unifiedjob_ptr_id integer NOT NULL,
    local_path character varying(1024) NOT NULL,
    scm_type character varying(8) NOT NULL,
    scm_url character varying(1024) NOT NULL,
    scm_branch character varying(256) NOT NULL,
    scm_clean boolean NOT NULL,
    scm_delete_on_update boolean NOT NULL,
    credential_id integer,
    project_id integer NOT NULL,
    timeout integer NOT NULL,
    job_type character varying(64) NOT NULL,
    scm_refspec character varying(1024) NOT NULL,
    scm_revision character varying(1024) NOT NULL,
    job_tags character varying(1024) NOT NULL,
    scm_track_submodules boolean NOT NULL
);


ALTER TABLE public.main_projectupdate OWNER TO awx;

--
-- Name: main_projectupdateevent; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_projectupdateevent (
    id bigint NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone NOT NULL,
    event character varying(100) NOT NULL,
    event_data text NOT NULL,
    failed boolean NOT NULL,
    changed boolean NOT NULL,
    uuid character varying(1024) NOT NULL,
    playbook character varying(1024) NOT NULL,
    play character varying(1024) NOT NULL,
    role character varying(1024) NOT NULL,
    task character varying(1024) NOT NULL,
    counter integer NOT NULL,
    stdout text NOT NULL,
    verbosity integer NOT NULL,
    start_line integer NOT NULL,
    end_line integer NOT NULL,
    project_update_id integer NOT NULL,
    job_created timestamp with time zone NOT NULL,
    CONSTRAINT main_projectupdateevent_counter_check CHECK ((counter >= 0)),
    CONSTRAINT main_projectupdateevent_end_line_check CHECK ((end_line >= 0)),
    CONSTRAINT main_projectupdateevent_start_line_check CHECK ((start_line >= 0)),
    CONSTRAINT main_projectupdateevent_verbosity_check CHECK ((verbosity >= 0))
)
PARTITION BY RANGE (job_created);


ALTER TABLE public.main_projectupdateevent OWNER TO awx;

--
-- Name: main_projectupdateevent_20241219_17; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_projectupdateevent_20241219_17 (
    id bigint NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone NOT NULL,
    event character varying(100) NOT NULL,
    event_data text NOT NULL,
    failed boolean NOT NULL,
    changed boolean NOT NULL,
    uuid character varying(1024) NOT NULL,
    playbook character varying(1024) NOT NULL,
    play character varying(1024) NOT NULL,
    role character varying(1024) NOT NULL,
    task character varying(1024) NOT NULL,
    counter integer NOT NULL,
    stdout text NOT NULL,
    verbosity integer NOT NULL,
    start_line integer NOT NULL,
    end_line integer NOT NULL,
    project_update_id integer NOT NULL,
    job_created timestamp with time zone NOT NULL,
    CONSTRAINT main_projectupdateevent_counter_check CHECK ((counter >= 0)),
    CONSTRAINT main_projectupdateevent_end_line_check CHECK ((end_line >= 0)),
    CONSTRAINT main_projectupdateevent_start_line_check CHECK ((start_line >= 0)),
    CONSTRAINT main_projectupdateevent_verbosity_check CHECK ((verbosity >= 0))
);


ALTER TABLE public.main_projectupdateevent_20241219_17 OWNER TO awx;

--
-- Name: main_projectupdateevent_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public._unpartitioned_main_projectupdateevent ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_projectupdateevent_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_projectupdateevent_id_seq1; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_projectupdateevent ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_projectupdateevent_id_seq1
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_rbac_role_ancestors; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_rbac_role_ancestors (
    id integer NOT NULL,
    role_field text NOT NULL,
    content_type_id integer NOT NULL,
    object_id integer NOT NULL,
    ancestor_id integer NOT NULL,
    descendent_id integer NOT NULL,
    CONSTRAINT main_rbac_role_ancestors_content_type_id_check CHECK ((content_type_id >= 0)),
    CONSTRAINT main_rbac_role_ancestors_object_id_check CHECK ((object_id >= 0))
);


ALTER TABLE public.main_rbac_role_ancestors OWNER TO awx;

--
-- Name: main_rbac_role_ancestors_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_rbac_role_ancestors ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_rbac_role_ancestors_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_rbac_roles; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_rbac_roles (
    id integer NOT NULL,
    role_field text NOT NULL,
    singleton_name text,
    implicit_parents text NOT NULL,
    content_type_id integer,
    object_id integer,
    CONSTRAINT main_rbac_roles_object_id_check CHECK ((object_id >= 0))
);


ALTER TABLE public.main_rbac_roles OWNER TO awx;

--
-- Name: main_rbac_roles_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_rbac_roles ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_rbac_roles_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_rbac_roles_members; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_rbac_roles_members (
    id integer NOT NULL,
    role_id integer NOT NULL,
    user_id integer NOT NULL
);


ALTER TABLE public.main_rbac_roles_members OWNER TO awx;

--
-- Name: main_rbac_roles_members_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_rbac_roles_members ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_rbac_roles_members_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_rbac_roles_parents; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_rbac_roles_parents (
    id integer NOT NULL,
    from_role_id integer NOT NULL,
    to_role_id integer NOT NULL
);


ALTER TABLE public.main_rbac_roles_parents OWNER TO awx;

--
-- Name: main_rbac_roles_parents_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_rbac_roles_parents ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_rbac_roles_parents_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_receptoraddress; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_receptoraddress (
    id integer NOT NULL,
    address character varying(255) NOT NULL,
    port integer NOT NULL,
    websocket_path character varying(255) NOT NULL,
    protocol character varying(10) NOT NULL,
    is_internal boolean NOT NULL,
    canonical boolean NOT NULL,
    peers_from_control_nodes boolean NOT NULL,
    instance_id integer NOT NULL
);


ALTER TABLE public.main_receptoraddress OWNER TO awx;

--
-- Name: main_receptoraddress_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_receptoraddress ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_receptoraddress_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_schedule; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_schedule (
    id integer NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    description text NOT NULL,
    name character varying(512) NOT NULL,
    enabled boolean NOT NULL,
    dtstart timestamp with time zone,
    dtend timestamp with time zone,
    rrule text NOT NULL,
    next_run timestamp with time zone,
    extra_data text NOT NULL,
    created_by_id integer,
    modified_by_id integer,
    unified_job_template_id integer NOT NULL,
    char_prompts jsonb NOT NULL,
    inventory_id integer,
    survey_passwords jsonb NOT NULL,
    execution_environment_id integer
);


ALTER TABLE public.main_schedule OWNER TO awx;

--
-- Name: main_schedule_credentials; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_schedule_credentials (
    id integer NOT NULL,
    schedule_id integer NOT NULL,
    credential_id integer NOT NULL
);


ALTER TABLE public.main_schedule_credentials OWNER TO awx;

--
-- Name: main_schedule_credentials_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_schedule_credentials ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_schedule_credentials_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_schedule_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_schedule ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_schedule_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_schedule_labels; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_schedule_labels (
    id integer NOT NULL,
    schedule_id integer NOT NULL,
    label_id integer NOT NULL
);


ALTER TABLE public.main_schedule_labels OWNER TO awx;

--
-- Name: main_schedule_labels_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_schedule_labels ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_schedule_labels_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_scheduleinstancegroupmembership; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_scheduleinstancegroupmembership (
    id integer NOT NULL,
    "position" integer,
    instancegroup_id integer NOT NULL,
    schedule_id integer NOT NULL,
    CONSTRAINT main_scheduleinstancegroupmembership_position_check CHECK (("position" >= 0))
);


ALTER TABLE public.main_scheduleinstancegroupmembership OWNER TO awx;

--
-- Name: main_scheduleinstancegroupmembership_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_scheduleinstancegroupmembership ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_scheduleinstancegroupmembership_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_smartinventorymembership; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_smartinventorymembership (
    id integer NOT NULL,
    host_id integer NOT NULL,
    inventory_id integer NOT NULL
);


ALTER TABLE public.main_smartinventorymembership OWNER TO awx;

--
-- Name: main_smartinventorymembership_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_smartinventorymembership ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_smartinventorymembership_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_systemjob; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_systemjob (
    unifiedjob_ptr_id integer NOT NULL,
    job_type character varying(32) NOT NULL,
    extra_vars text NOT NULL,
    system_job_template_id integer
);


ALTER TABLE public.main_systemjob OWNER TO awx;

--
-- Name: main_systemjobevent; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_systemjobevent (
    id bigint NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone NOT NULL,
    event_data text NOT NULL,
    uuid character varying(1024) NOT NULL,
    counter integer NOT NULL,
    stdout text NOT NULL,
    verbosity integer NOT NULL,
    start_line integer NOT NULL,
    end_line integer NOT NULL,
    system_job_id integer NOT NULL,
    job_created timestamp with time zone NOT NULL,
    CONSTRAINT main_systemjobevent_counter_check CHECK ((counter >= 0)),
    CONSTRAINT main_systemjobevent_end_line_check CHECK ((end_line >= 0)),
    CONSTRAINT main_systemjobevent_start_line_check CHECK ((start_line >= 0)),
    CONSTRAINT main_systemjobevent_verbosity_check CHECK ((verbosity >= 0))
)
PARTITION BY RANGE (job_created);


ALTER TABLE public.main_systemjobevent OWNER TO awx;

--
-- Name: main_systemjobevent_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public._unpartitioned_main_systemjobevent ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_systemjobevent_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_systemjobevent_id_seq1; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_systemjobevent ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_systemjobevent_id_seq1
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_systemjobtemplate; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_systemjobtemplate (
    unifiedjobtemplate_ptr_id integer NOT NULL,
    job_type character varying(32) NOT NULL
);


ALTER TABLE public.main_systemjobtemplate OWNER TO awx;

--
-- Name: main_team; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_team (
    id integer NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    description text NOT NULL,
    name character varying(512) NOT NULL,
    created_by_id integer,
    modified_by_id integer,
    organization_id integer NOT NULL,
    admin_role_id integer,
    member_role_id integer,
    read_role_id integer
);


ALTER TABLE public.main_team OWNER TO awx;

--
-- Name: main_team_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_team ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_team_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_towerschedulestate; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_towerschedulestate (
    id integer NOT NULL,
    schedule_last_run timestamp with time zone NOT NULL
);


ALTER TABLE public.main_towerschedulestate OWNER TO awx;

--
-- Name: main_towerschedulestate_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_towerschedulestate ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_towerschedulestate_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_unifiedjob; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_unifiedjob (
    id integer NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    description text NOT NULL,
    name character varying(512) NOT NULL,
    old_pk integer,
    launch_type character varying(20) NOT NULL,
    cancel_flag boolean NOT NULL,
    status character varying(20) NOT NULL,
    failed boolean NOT NULL,
    started timestamp with time zone,
    finished timestamp with time zone,
    elapsed numeric(12,3) NOT NULL,
    job_args text NOT NULL,
    job_cwd character varying(1024) NOT NULL,
    job_explanation text NOT NULL,
    start_args text NOT NULL,
    result_stdout_text text,
    result_traceback text NOT NULL,
    celery_task_id character varying(100) NOT NULL,
    created_by_id integer,
    modified_by_id integer,
    polymorphic_ctype_id integer,
    schedule_id integer,
    unified_job_template_id integer,
    execution_node text NOT NULL,
    instance_group_id integer,
    emitted_events integer NOT NULL,
    controller_node text NOT NULL,
    canceled_on timestamp with time zone,
    dependencies_processed boolean NOT NULL,
    organization_id integer,
    execution_environment_id integer,
    installed_collections jsonb NOT NULL,
    ansible_version character varying(255) NOT NULL,
    work_unit_id character varying(255),
    host_status_counts jsonb,
    preferred_instance_groups_cache jsonb,
    task_impact integer NOT NULL,
    job_env jsonb NOT NULL,
    CONSTRAINT main_unifiedjob_emitted_events_check CHECK ((emitted_events >= 0)),
    CONSTRAINT main_unifiedjob_old_pk_check CHECK ((old_pk >= 0)),
    CONSTRAINT main_unifiedjob_task_impact_check CHECK ((task_impact >= 0))
);


ALTER TABLE public.main_unifiedjob OWNER TO awx;

--
-- Name: main_unifiedjob_credentials; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_unifiedjob_credentials (
    id integer NOT NULL,
    unifiedjob_id integer NOT NULL,
    credential_id integer NOT NULL
);


ALTER TABLE public.main_unifiedjob_credentials OWNER TO awx;

--
-- Name: main_unifiedjob_credentials_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjob_credentials ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_unifiedjob_credentials_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_unifiedjob_dependent_jobs; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_unifiedjob_dependent_jobs (
    id integer NOT NULL,
    from_unifiedjob_id integer NOT NULL,
    to_unifiedjob_id integer NOT NULL
);


ALTER TABLE public.main_unifiedjob_dependent_jobs OWNER TO awx;

--
-- Name: main_unifiedjob_dependent_jobs_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjob_dependent_jobs ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_unifiedjob_dependent_jobs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_unifiedjob_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjob ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_unifiedjob_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_unifiedjob_labels; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_unifiedjob_labels (
    id integer NOT NULL,
    unifiedjob_id integer NOT NULL,
    label_id integer NOT NULL
);


ALTER TABLE public.main_unifiedjob_labels OWNER TO awx;

--
-- Name: main_unifiedjob_labels_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjob_labels ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_unifiedjob_labels_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_unifiedjob_notifications; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_unifiedjob_notifications (
    id integer NOT NULL,
    unifiedjob_id integer NOT NULL,
    notification_id integer NOT NULL
);


ALTER TABLE public.main_unifiedjob_notifications OWNER TO awx;

--
-- Name: main_unifiedjob_notifications_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjob_notifications ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_unifiedjob_notifications_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_unifiedjobtemplate; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_unifiedjobtemplate (
    id integer NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    description text NOT NULL,
    name character varying(512) NOT NULL,
    old_pk integer,
    last_job_failed boolean NOT NULL,
    last_job_run timestamp with time zone,
    next_job_run timestamp with time zone,
    status character varying(32) NOT NULL,
    created_by_id integer,
    current_job_id integer,
    last_job_id integer,
    modified_by_id integer,
    next_schedule_id integer,
    polymorphic_ctype_id integer,
    organization_id integer,
    execution_environment_id integer,
    CONSTRAINT main_unifiedjobtemplate_old_pk_check CHECK ((old_pk >= 0))
);


ALTER TABLE public.main_unifiedjobtemplate OWNER TO awx;

--
-- Name: main_unifiedjobtemplate_credentials; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_unifiedjobtemplate_credentials (
    id integer NOT NULL,
    unifiedjobtemplate_id integer NOT NULL,
    credential_id integer NOT NULL
);


ALTER TABLE public.main_unifiedjobtemplate_credentials OWNER TO awx;

--
-- Name: main_unifiedjobtemplate_credentials_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjobtemplate_credentials ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_unifiedjobtemplate_credentials_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_unifiedjobtemplate_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjobtemplate ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_unifiedjobtemplate_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_unifiedjobtemplate_labels; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_unifiedjobtemplate_labels (
    id integer NOT NULL,
    unifiedjobtemplate_id integer NOT NULL,
    label_id integer NOT NULL
);


ALTER TABLE public.main_unifiedjobtemplate_labels OWNER TO awx;

--
-- Name: main_unifiedjobtemplate_labels_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjobtemplate_labels ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_unifiedjobtemplate_labels_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_unifiedjobtemplate_notification_templates_error; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_unifiedjobtemplate_notification_templates_error (
    id integer NOT NULL,
    unifiedjobtemplate_id integer NOT NULL,
    notificationtemplate_id integer NOT NULL
);


ALTER TABLE public.main_unifiedjobtemplate_notification_templates_error OWNER TO awx;

--
-- Name: main_unifiedjobtemplate_notification_templates_error_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjobtemplate_notification_templates_error ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_unifiedjobtemplate_notification_templates_error_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_unifiedjobtemplate_notification_templates_started; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_unifiedjobtemplate_notification_templates_started (
    id integer NOT NULL,
    unifiedjobtemplate_id integer NOT NULL,
    notificationtemplate_id integer NOT NULL
);


ALTER TABLE public.main_unifiedjobtemplate_notification_templates_started OWNER TO awx;

--
-- Name: main_unifiedjobtemplate_notification_templates_started_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjobtemplate_notification_templates_started ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_unifiedjobtemplate_notification_templates_started_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_unifiedjobtemplate_notification_templates_success; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_unifiedjobtemplate_notification_templates_success (
    id integer NOT NULL,
    unifiedjobtemplate_id integer NOT NULL,
    notificationtemplate_id integer NOT NULL
);


ALTER TABLE public.main_unifiedjobtemplate_notification_templates_success OWNER TO awx;

--
-- Name: main_unifiedjobtemplate_notification_templates_success_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjobtemplate_notification_templates_success ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_unifiedjobtemplate_notification_templates_success_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_unifiedjobtemplateinstancegroupmembership; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_unifiedjobtemplateinstancegroupmembership (
    id integer NOT NULL,
    "position" integer,
    instancegroup_id integer NOT NULL,
    unifiedjobtemplate_id integer NOT NULL,
    CONSTRAINT main_unifiedjobtemplateinstancegroupmembership_position_check CHECK (("position" >= 0))
);


ALTER TABLE public.main_unifiedjobtemplateinstancegroupmembership OWNER TO awx;

--
-- Name: main_unifiedjobtemplateinstancegroupmembership_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjobtemplateinstancegroupmembership ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_unifiedjobtemplateinstancegroupmembership_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_usersessionmembership; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_usersessionmembership (
    id integer NOT NULL,
    created timestamp with time zone NOT NULL,
    session_id character varying(40) NOT NULL,
    user_id integer NOT NULL
);


ALTER TABLE public.main_usersessionmembership OWNER TO awx;

--
-- Name: main_usersessionmembership_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_usersessionmembership ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_usersessionmembership_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_workflowapproval; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_workflowapproval (
    unifiedjob_ptr_id integer NOT NULL,
    workflow_approval_template_id integer,
    timeout integer NOT NULL,
    timed_out boolean NOT NULL,
    approved_or_denied_by_id integer,
    expires timestamp with time zone
);


ALTER TABLE public.main_workflowapproval OWNER TO awx;

--
-- Name: main_workflowapprovaltemplate; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_workflowapprovaltemplate (
    unifiedjobtemplate_ptr_id integer NOT NULL,
    timeout integer NOT NULL
);


ALTER TABLE public.main_workflowapprovaltemplate OWNER TO awx;

--
-- Name: main_workflowjob; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_workflowjob (
    unifiedjob_ptr_id integer NOT NULL,
    extra_vars text NOT NULL,
    workflow_job_template_id integer,
    allow_simultaneous boolean NOT NULL,
    is_sliced_job boolean NOT NULL,
    job_template_id integer,
    inventory_id integer,
    webhook_credential_id integer,
    webhook_guid character varying(128) NOT NULL,
    webhook_service character varying(16) NOT NULL,
    is_bulk_job boolean NOT NULL,
    char_prompts jsonb NOT NULL,
    survey_passwords jsonb NOT NULL
);


ALTER TABLE public.main_workflowjob OWNER TO awx;

--
-- Name: main_workflowjobinstancegroupmembership; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_workflowjobinstancegroupmembership (
    id integer NOT NULL,
    "position" integer,
    instancegroup_id integer NOT NULL,
    workflowjobnode_id integer NOT NULL,
    CONSTRAINT main_workflowjobinstancegroupmembership_position_check CHECK (("position" >= 0))
);


ALTER TABLE public.main_workflowjobinstancegroupmembership OWNER TO awx;

--
-- Name: main_workflowjobinstancegroupmembership_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobinstancegroupmembership ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_workflowjobinstancegroupmembership_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_workflowjobnode; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_workflowjobnode (
    id integer NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    job_id integer,
    unified_job_template_id integer,
    workflow_job_id integer,
    inventory_id integer,
    ancestor_artifacts text NOT NULL,
    extra_data text NOT NULL,
    do_not_run boolean NOT NULL,
    all_parents_must_converge boolean NOT NULL,
    identifier character varying(512) NOT NULL,
    execution_environment_id integer,
    char_prompts jsonb NOT NULL,
    survey_passwords jsonb NOT NULL
);


ALTER TABLE public.main_workflowjobnode OWNER TO awx;

--
-- Name: main_workflowjobnode_always_nodes; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_workflowjobnode_always_nodes (
    id integer NOT NULL,
    from_workflowjobnode_id integer NOT NULL,
    to_workflowjobnode_id integer NOT NULL
);


ALTER TABLE public.main_workflowjobnode_always_nodes OWNER TO awx;

--
-- Name: main_workflowjobnode_always_nodes_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobnode_always_nodes ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_workflowjobnode_always_nodes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_workflowjobnode_credentials; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_workflowjobnode_credentials (
    id integer NOT NULL,
    workflowjobnode_id integer NOT NULL,
    credential_id integer NOT NULL
);


ALTER TABLE public.main_workflowjobnode_credentials OWNER TO awx;

--
-- Name: main_workflowjobnode_credentials_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobnode_credentials ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_workflowjobnode_credentials_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_workflowjobnode_failure_nodes; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_workflowjobnode_failure_nodes (
    id integer NOT NULL,
    from_workflowjobnode_id integer NOT NULL,
    to_workflowjobnode_id integer NOT NULL
);


ALTER TABLE public.main_workflowjobnode_failure_nodes OWNER TO awx;

--
-- Name: main_workflowjobnode_failure_nodes_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobnode_failure_nodes ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_workflowjobnode_failure_nodes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_workflowjobnode_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobnode ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_workflowjobnode_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_workflowjobnode_labels; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_workflowjobnode_labels (
    id integer NOT NULL,
    workflowjobnode_id integer NOT NULL,
    label_id integer NOT NULL
);


ALTER TABLE public.main_workflowjobnode_labels OWNER TO awx;

--
-- Name: main_workflowjobnode_labels_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobnode_labels ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_workflowjobnode_labels_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_workflowjobnode_success_nodes; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_workflowjobnode_success_nodes (
    id integer NOT NULL,
    from_workflowjobnode_id integer NOT NULL,
    to_workflowjobnode_id integer NOT NULL
);


ALTER TABLE public.main_workflowjobnode_success_nodes OWNER TO awx;

--
-- Name: main_workflowjobnode_success_nodes_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobnode_success_nodes ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_workflowjobnode_success_nodes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_workflowjobnodebaseinstancegroupmembership; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_workflowjobnodebaseinstancegroupmembership (
    id integer NOT NULL,
    "position" integer,
    instancegroup_id integer NOT NULL,
    workflowjobnode_id integer NOT NULL,
    CONSTRAINT main_workflowjobnodebaseinstancegroupmembership_position_check CHECK (("position" >= 0))
);


ALTER TABLE public.main_workflowjobnodebaseinstancegroupmembership OWNER TO awx;

--
-- Name: main_workflowjobnodebaseinstancegroupmembership_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobnodebaseinstancegroupmembership ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_workflowjobnodebaseinstancegroupmembership_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_workflowjobtemplate; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_workflowjobtemplate (
    unifiedjobtemplate_ptr_id integer NOT NULL,
    extra_vars text NOT NULL,
    admin_role_id integer,
    execute_role_id integer,
    read_role_id integer,
    survey_enabled boolean NOT NULL,
    survey_spec jsonb NOT NULL,
    allow_simultaneous boolean NOT NULL,
    ask_variables_on_launch boolean NOT NULL,
    ask_inventory_on_launch boolean NOT NULL,
    inventory_id integer,
    approval_role_id integer,
    ask_limit_on_launch boolean NOT NULL,
    ask_scm_branch_on_launch boolean NOT NULL,
    char_prompts jsonb NOT NULL,
    webhook_credential_id integer,
    webhook_key character varying(64) NOT NULL,
    webhook_service character varying(16) NOT NULL,
    ask_labels_on_launch boolean NOT NULL,
    ask_skip_tags_on_launch boolean NOT NULL,
    ask_tags_on_launch boolean NOT NULL
);


ALTER TABLE public.main_workflowjobtemplate OWNER TO awx;

--
-- Name: main_workflowjobtemplate_notification_templates_approvals; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_workflowjobtemplate_notification_templates_approvals (
    id integer NOT NULL,
    workflowjobtemplate_id integer NOT NULL,
    notificationtemplate_id integer NOT NULL
);


ALTER TABLE public.main_workflowjobtemplate_notification_templates_approvals OWNER TO awx;

--
-- Name: main_workflowjobtemplate_notification_templates_approval_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobtemplate_notification_templates_approvals ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_workflowjobtemplate_notification_templates_approval_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_workflowjobtemplatenode; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_workflowjobtemplatenode (
    id integer NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    unified_job_template_id integer,
    workflow_job_template_id integer NOT NULL,
    char_prompts jsonb NOT NULL,
    inventory_id integer,
    extra_data text NOT NULL,
    survey_passwords jsonb NOT NULL,
    all_parents_must_converge boolean NOT NULL,
    identifier character varying(512) NOT NULL,
    execution_environment_id integer
);


ALTER TABLE public.main_workflowjobtemplatenode OWNER TO awx;

--
-- Name: main_workflowjobtemplatenode_always_nodes; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_workflowjobtemplatenode_always_nodes (
    id integer NOT NULL,
    from_workflowjobtemplatenode_id integer NOT NULL,
    to_workflowjobtemplatenode_id integer NOT NULL
);


ALTER TABLE public.main_workflowjobtemplatenode_always_nodes OWNER TO awx;

--
-- Name: main_workflowjobtemplatenode_always_nodes_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobtemplatenode_always_nodes ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_workflowjobtemplatenode_always_nodes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_workflowjobtemplatenode_credentials; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_workflowjobtemplatenode_credentials (
    id integer NOT NULL,
    workflowjobtemplatenode_id integer NOT NULL,
    credential_id integer NOT NULL
);


ALTER TABLE public.main_workflowjobtemplatenode_credentials OWNER TO awx;

--
-- Name: main_workflowjobtemplatenode_credentials_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobtemplatenode_credentials ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_workflowjobtemplatenode_credentials_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_workflowjobtemplatenode_failure_nodes; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_workflowjobtemplatenode_failure_nodes (
    id integer NOT NULL,
    from_workflowjobtemplatenode_id integer NOT NULL,
    to_workflowjobtemplatenode_id integer NOT NULL
);


ALTER TABLE public.main_workflowjobtemplatenode_failure_nodes OWNER TO awx;

--
-- Name: main_workflowjobtemplatenode_failure_nodes_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobtemplatenode_failure_nodes ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_workflowjobtemplatenode_failure_nodes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_workflowjobtemplatenode_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobtemplatenode ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_workflowjobtemplatenode_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_workflowjobtemplatenode_labels; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_workflowjobtemplatenode_labels (
    id integer NOT NULL,
    workflowjobtemplatenode_id integer NOT NULL,
    label_id integer NOT NULL
);


ALTER TABLE public.main_workflowjobtemplatenode_labels OWNER TO awx;

--
-- Name: main_workflowjobtemplatenode_labels_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobtemplatenode_labels ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_workflowjobtemplatenode_labels_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_workflowjobtemplatenode_success_nodes; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_workflowjobtemplatenode_success_nodes (
    id integer NOT NULL,
    from_workflowjobtemplatenode_id integer NOT NULL,
    to_workflowjobtemplatenode_id integer NOT NULL
);


ALTER TABLE public.main_workflowjobtemplatenode_success_nodes OWNER TO awx;

--
-- Name: main_workflowjobtemplatenode_success_nodes_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobtemplatenode_success_nodes ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_workflowjobtemplatenode_success_nodes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_workflowjobtemplatenodebaseinstancegroupmembership; Type: TABLE; Schema: public; Owner: awx
--

CREATE TABLE public.main_workflowjobtemplatenodebaseinstancegroupmembership (
    id integer NOT NULL,
    "position" integer,
    instancegroup_id integer NOT NULL,
    workflowjobtemplatenode_id integer NOT NULL,
    CONSTRAINT main_workflowjobtemplatenodebaseinstancegroupmem_position_check CHECK (("position" >= 0))
);


ALTER TABLE public.main_workflowjobtemplatenodebaseinstancegroupmembership OWNER TO awx;

--
-- Name: main_workflowjobtemplatenodebaseinstancegroupmembership_id_seq; Type: SEQUENCE; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobtemplatenodebaseinstancegroupmembership ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.main_workflowjobtemplatenodebaseinstancegroupmembership_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: main_jobevent_20241219_17; Type: TABLE ATTACH; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_jobevent ATTACH PARTITION public.main_jobevent_20241219_17 FOR VALUES FROM ('2024-12-19 17:00:00+00') TO ('2024-12-19 18:00:00+00');


--
-- Name: main_projectupdateevent_20241219_17; Type: TABLE ATTACH; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_projectupdateevent ATTACH PARTITION public.main_projectupdateevent_20241219_17 FOR VALUES FROM ('2024-12-19 17:00:00+00') TO ('2024-12-19 18:00:00+00');


--
-- Name: auth_group auth_group_name_key; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.auth_group
    ADD CONSTRAINT auth_group_name_key UNIQUE (name);


--
-- Name: auth_group_permissions auth_group_permissions_group_id_permission_id_0cd325b0_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.auth_group_permissions
    ADD CONSTRAINT auth_group_permissions_group_id_permission_id_0cd325b0_uniq UNIQUE (group_id, permission_id);


--
-- Name: auth_group_permissions auth_group_permissions_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.auth_group_permissions
    ADD CONSTRAINT auth_group_permissions_pkey PRIMARY KEY (id);


--
-- Name: auth_group auth_group_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.auth_group
    ADD CONSTRAINT auth_group_pkey PRIMARY KEY (id);


--
-- Name: auth_permission auth_permission_content_type_id_codename_01ab375a_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.auth_permission
    ADD CONSTRAINT auth_permission_content_type_id_codename_01ab375a_uniq UNIQUE (content_type_id, codename);


--
-- Name: auth_permission auth_permission_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.auth_permission
    ADD CONSTRAINT auth_permission_pkey PRIMARY KEY (id);


--
-- Name: auth_user_groups auth_user_groups_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.auth_user_groups
    ADD CONSTRAINT auth_user_groups_pkey PRIMARY KEY (id);


--
-- Name: auth_user_groups auth_user_groups_user_id_group_id_94350c0c_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.auth_user_groups
    ADD CONSTRAINT auth_user_groups_user_id_group_id_94350c0c_uniq UNIQUE (user_id, group_id);


--
-- Name: auth_user auth_user_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.auth_user
    ADD CONSTRAINT auth_user_pkey PRIMARY KEY (id);


--
-- Name: auth_user_user_permissions auth_user_user_permissions_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.auth_user_user_permissions
    ADD CONSTRAINT auth_user_user_permissions_pkey PRIMARY KEY (id);


--
-- Name: auth_user_user_permissions auth_user_user_permissions_user_id_permission_id_14a6b632_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.auth_user_user_permissions
    ADD CONSTRAINT auth_user_user_permissions_user_id_permission_id_14a6b632_uniq UNIQUE (user_id, permission_id);


--
-- Name: auth_user auth_user_username_key; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.auth_user
    ADD CONSTRAINT auth_user_username_key UNIQUE (username);


--
-- Name: conf_setting conf_setting_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.conf_setting
    ADD CONSTRAINT conf_setting_pkey PRIMARY KEY (id);


--
-- Name: dab_rbac_dabpermission dab_rbac_dabpermission_content_type_id_codename_d2d5634d_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_dabpermission
    ADD CONSTRAINT dab_rbac_dabpermission_content_type_id_codename_d2d5634d_uniq UNIQUE (content_type_id, codename);


--
-- Name: dab_rbac_dabpermission dab_rbac_dabpermission_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_dabpermission
    ADD CONSTRAINT dab_rbac_dabpermission_pkey PRIMARY KEY (id);


--
-- Name: dab_rbac_objectrole dab_rbac_objectrole_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_objectrole
    ADD CONSTRAINT dab_rbac_objectrole_pkey PRIMARY KEY (id);


--
-- Name: dab_rbac_objectrole_provides_teams dab_rbac_objectrole_prov_objectrole_id_team_id_9e4f4690_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_objectrole_provides_teams
    ADD CONSTRAINT dab_rbac_objectrole_prov_objectrole_id_team_id_9e4f4690_uniq UNIQUE (objectrole_id, team_id);


--
-- Name: dab_rbac_objectrole_provides_teams dab_rbac_objectrole_provides_teams_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_objectrole_provides_teams
    ADD CONSTRAINT dab_rbac_objectrole_provides_teams_pkey PRIMARY KEY (id);


--
-- Name: dab_rbac_roledefinition_permissions dab_rbac_roledefinition__roledefinition_id_dabper_a47a3d13_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roledefinition_permissions
    ADD CONSTRAINT dab_rbac_roledefinition__roledefinition_id_dabper_a47a3d13_uniq UNIQUE (roledefinition_id, dabpermission_id);


--
-- Name: dab_rbac_roledefinition dab_rbac_roledefinition_name_key; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roledefinition
    ADD CONSTRAINT dab_rbac_roledefinition_name_key UNIQUE (name);


--
-- Name: dab_rbac_roledefinition_permissions dab_rbac_roledefinition_permissions_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roledefinition_permissions
    ADD CONSTRAINT dab_rbac_roledefinition_permissions_pkey PRIMARY KEY (id);


--
-- Name: dab_rbac_roledefinition dab_rbac_roledefinition_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roledefinition
    ADD CONSTRAINT dab_rbac_roledefinition_pkey PRIMARY KEY (id);


--
-- Name: dab_rbac_roleevaluation dab_rbac_roleevaluation_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roleevaluation
    ADD CONSTRAINT dab_rbac_roleevaluation_pkey PRIMARY KEY (id);


--
-- Name: dab_rbac_roleevaluationuuid dab_rbac_roleevaluationuuid_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roleevaluationuuid
    ADD CONSTRAINT dab_rbac_roleevaluationuuid_pkey PRIMARY KEY (id);


--
-- Name: dab_rbac_roleteamassignment dab_rbac_roleteamassignm_team_id_object_role_id_44072651_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roleteamassignment
    ADD CONSTRAINT dab_rbac_roleteamassignm_team_id_object_role_id_44072651_uniq UNIQUE (team_id, object_role_id);


--
-- Name: dab_rbac_roleteamassignment dab_rbac_roleteamassignment_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roleteamassignment
    ADD CONSTRAINT dab_rbac_roleteamassignment_pkey PRIMARY KEY (id);


--
-- Name: dab_rbac_roleuserassignment dab_rbac_roleuserassignm_user_id_object_role_id_446123e4_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roleuserassignment
    ADD CONSTRAINT dab_rbac_roleuserassignm_user_id_object_role_id_446123e4_uniq UNIQUE (user_id, object_role_id);


--
-- Name: dab_rbac_roleuserassignment dab_rbac_roleuserassignment_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roleuserassignment
    ADD CONSTRAINT dab_rbac_roleuserassignment_pkey PRIMARY KEY (id);


--
-- Name: dab_resource_registry_resource dab_resource_registry_re_content_type_id_object_i_c41ad5bd_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_resource_registry_resource
    ADD CONSTRAINT dab_resource_registry_re_content_type_id_object_i_c41ad5bd_uniq UNIQUE (content_type_id, object_id);


--
-- Name: dab_resource_registry_resource dab_resource_registry_resource_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_resource_registry_resource
    ADD CONSTRAINT dab_resource_registry_resource_pkey PRIMARY KEY (id);


--
-- Name: dab_resource_registry_resource dab_resource_registry_resource_resource_id_key; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_resource_registry_resource
    ADD CONSTRAINT dab_resource_registry_resource_resource_id_key UNIQUE (ansible_id);


--
-- Name: dab_resource_registry_resourcetype dab_resource_registry_resourcetype_content_type_id_key; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_resource_registry_resourcetype
    ADD CONSTRAINT dab_resource_registry_resourcetype_content_type_id_key UNIQUE (content_type_id);


--
-- Name: dab_resource_registry_resourcetype dab_resource_registry_resourcetype_name_key; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_resource_registry_resourcetype
    ADD CONSTRAINT dab_resource_registry_resourcetype_name_key UNIQUE (name);


--
-- Name: dab_resource_registry_resourcetype dab_resource_registry_resourcetype_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_resource_registry_resourcetype
    ADD CONSTRAINT dab_resource_registry_resourcetype_pkey PRIMARY KEY (id);


--
-- Name: dab_resource_registry_serviceid dab_resource_registry_serviceid_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_resource_registry_serviceid
    ADD CONSTRAINT dab_resource_registry_serviceid_pkey PRIMARY KEY (id);


--
-- Name: django_content_type django_content_type_app_label_model_76bd3d3b_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.django_content_type
    ADD CONSTRAINT django_content_type_app_label_model_76bd3d3b_uniq UNIQUE (app_label, model);


--
-- Name: django_content_type django_content_type_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.django_content_type
    ADD CONSTRAINT django_content_type_pkey PRIMARY KEY (id);


--
-- Name: django_migrations django_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.django_migrations
    ADD CONSTRAINT django_migrations_pkey PRIMARY KEY (id);


--
-- Name: django_session django_session_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.django_session
    ADD CONSTRAINT django_session_pkey PRIMARY KEY (session_key);


--
-- Name: django_site django_site_domain_a2e37b91_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.django_site
    ADD CONSTRAINT django_site_domain_a2e37b91_uniq UNIQUE (domain);


--
-- Name: django_site django_site_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.django_site
    ADD CONSTRAINT django_site_pkey PRIMARY KEY (id);


--
-- Name: flags_flagstate flags_flagstate_name_condition_value_4e81ec48_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.flags_flagstate
    ADD CONSTRAINT flags_flagstate_name_condition_value_4e81ec48_uniq UNIQUE (name, condition, value);


--
-- Name: flags_flagstate flags_flagstate_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.flags_flagstate
    ADD CONSTRAINT flags_flagstate_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_ad_hoc_command main_activitystream_ad_h_activitystream_id_adhocc_710d9648_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_ad_hoc_command
    ADD CONSTRAINT main_activitystream_ad_h_activitystream_id_adhocc_710d9648_uniq UNIQUE (activitystream_id, adhoccommand_id);


--
-- Name: main_activitystream_ad_hoc_command main_activitystream_ad_hoc_command_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_ad_hoc_command
    ADD CONSTRAINT main_activitystream_ad_hoc_command_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_credential main_activitystream_cred_activitystream_id_creden_6b3be6d5_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_credential
    ADD CONSTRAINT main_activitystream_cred_activitystream_id_creden_6b3be6d5_uniq UNIQUE (activitystream_id, credential_id);


--
-- Name: main_activitystream_credential_type main_activitystream_cred_activitystream_id_creden_85746647_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_credential_type
    ADD CONSTRAINT main_activitystream_cred_activitystream_id_creden_85746647_uniq UNIQUE (activitystream_id, credentialtype_id);


--
-- Name: main_activitystream_credential main_activitystream_credential_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_credential
    ADD CONSTRAINT main_activitystream_credential_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_credential_type main_activitystream_credential_type_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_credential_type
    ADD CONSTRAINT main_activitystream_credential_type_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_execution_environment main_activitystream_exec_activitystream_id_execut_e6698de7_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_execution_environment
    ADD CONSTRAINT main_activitystream_exec_activitystream_id_execut_e6698de7_uniq UNIQUE (activitystream_id, executionenvironment_id);


--
-- Name: main_activitystream_execution_environment main_activitystream_execution_environment_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_execution_environment
    ADD CONSTRAINT main_activitystream_execution_environment_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_group main_activitystream_grou_activitystream_id_group__3068b98d_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_group
    ADD CONSTRAINT main_activitystream_grou_activitystream_id_group__3068b98d_uniq UNIQUE (activitystream_id, group_id);


--
-- Name: main_activitystream_group main_activitystream_group_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_group
    ADD CONSTRAINT main_activitystream_group_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_host main_activitystream_host_activitystream_id_host_i_7ec5e62e_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_host
    ADD CONSTRAINT main_activitystream_host_activitystream_id_host_i_7ec5e62e_uniq UNIQUE (activitystream_id, host_id);


--
-- Name: main_activitystream_host main_activitystream_host_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_host
    ADD CONSTRAINT main_activitystream_host_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_instance_group main_activitystream_inst_activitystream_id_instan_173bfccd_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_instance_group
    ADD CONSTRAINT main_activitystream_inst_activitystream_id_instan_173bfccd_uniq UNIQUE (activitystream_id, instancegroup_id);


--
-- Name: main_activitystream_instance main_activitystream_inst_activitystream_id_instan_eba71ee1_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_instance
    ADD CONSTRAINT main_activitystream_inst_activitystream_id_instan_eba71ee1_uniq UNIQUE (activitystream_id, instance_id);


--
-- Name: main_activitystream_instance_group main_activitystream_instance_group_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_instance_group
    ADD CONSTRAINT main_activitystream_instance_group_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_instance main_activitystream_instance_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_instance
    ADD CONSTRAINT main_activitystream_instance_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_inventory_update main_activitystream_inve_activitystream_id_invent_28edee6e_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_inventory_update
    ADD CONSTRAINT main_activitystream_inve_activitystream_id_invent_28edee6e_uniq UNIQUE (activitystream_id, inventoryupdate_id);


--
-- Name: main_activitystream_inventory main_activitystream_inve_activitystream_id_invent_410769d5_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_inventory
    ADD CONSTRAINT main_activitystream_inve_activitystream_id_invent_410769d5_uniq UNIQUE (activitystream_id, inventory_id);


--
-- Name: main_activitystream_inventory_source main_activitystream_inve_activitystream_id_invent_e9d8f675_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_inventory_source
    ADD CONSTRAINT main_activitystream_inve_activitystream_id_invent_e9d8f675_uniq UNIQUE (activitystream_id, inventorysource_id);


--
-- Name: main_activitystream_inventory main_activitystream_inventory_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_inventory
    ADD CONSTRAINT main_activitystream_inventory_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_inventory_source main_activitystream_inventory_source_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_inventory_source
    ADD CONSTRAINT main_activitystream_inventory_source_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_inventory_update main_activitystream_inventory_update_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_inventory_update
    ADD CONSTRAINT main_activitystream_inventory_update_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_job_template main_activitystream_job__activitystream_id_jobtem_ca7c997a_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_job_template
    ADD CONSTRAINT main_activitystream_job__activitystream_id_jobtem_ca7c997a_uniq UNIQUE (activitystream_id, jobtemplate_id);


--
-- Name: main_activitystream_job main_activitystream_job_activitystream_id_job_id_dbb86499_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_job
    ADD CONSTRAINT main_activitystream_job_activitystream_id_job_id_dbb86499_uniq UNIQUE (activitystream_id, job_id);


--
-- Name: main_activitystream_job main_activitystream_job_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_job
    ADD CONSTRAINT main_activitystream_job_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_job_template main_activitystream_job_template_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_job_template
    ADD CONSTRAINT main_activitystream_job_template_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_label main_activitystream_labe_activitystream_id_label__04ca98fb_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_label
    ADD CONSTRAINT main_activitystream_labe_activitystream_id_label__04ca98fb_uniq UNIQUE (activitystream_id, label_id);


--
-- Name: main_activitystream_label main_activitystream_label_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_label
    ADD CONSTRAINT main_activitystream_label_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_notification_template main_activitystream_noti_activitystream_id_notifi_2ecdc66e_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_notification_template
    ADD CONSTRAINT main_activitystream_noti_activitystream_id_notifi_2ecdc66e_uniq UNIQUE (activitystream_id, notificationtemplate_id);


--
-- Name: main_activitystream_notification main_activitystream_noti_activitystream_id_notifi_3f05835f_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_notification
    ADD CONSTRAINT main_activitystream_noti_activitystream_id_notifi_3f05835f_uniq UNIQUE (activitystream_id, notification_id);


--
-- Name: main_activitystream_notification main_activitystream_notification_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_notification
    ADD CONSTRAINT main_activitystream_notification_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_notification_template main_activitystream_notification_template_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_notification_template
    ADD CONSTRAINT main_activitystream_notification_template_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_organization main_activitystream_orga_activitystream_id_organi_ad587114_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_organization
    ADD CONSTRAINT main_activitystream_orga_activitystream_id_organi_ad587114_uniq UNIQUE (activitystream_id, organization_id);


--
-- Name: main_activitystream_organization main_activitystream_organization_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_organization
    ADD CONSTRAINT main_activitystream_organization_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream main_activitystream_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream
    ADD CONSTRAINT main_activitystream_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_project main_activitystream_proj_activitystream_id_projec_25dcced8_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_project
    ADD CONSTRAINT main_activitystream_proj_activitystream_id_projec_25dcced8_uniq UNIQUE (activitystream_id, project_id);


--
-- Name: main_activitystream_project_update main_activitystream_proj_activitystream_id_projec_a3be3a08_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_project_update
    ADD CONSTRAINT main_activitystream_proj_activitystream_id_projec_a3be3a08_uniq UNIQUE (activitystream_id, projectupdate_id);


--
-- Name: main_activitystream_project main_activitystream_project_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_project
    ADD CONSTRAINT main_activitystream_project_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_project_update main_activitystream_project_update_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_project_update
    ADD CONSTRAINT main_activitystream_project_update_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_receptor_address main_activitystream_rece_activitystream_id_recept_ab4d56bb_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_receptor_address
    ADD CONSTRAINT main_activitystream_rece_activitystream_id_recept_ab4d56bb_uniq UNIQUE (activitystream_id, receptoraddress_id);


--
-- Name: main_activitystream_receptor_address main_activitystream_receptor_address_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_receptor_address
    ADD CONSTRAINT main_activitystream_receptor_address_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_role main_activitystream_role_activitystream_id_role_i_b51f6b40_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_role
    ADD CONSTRAINT main_activitystream_role_activitystream_id_role_i_b51f6b40_uniq UNIQUE (activitystream_id, role_id);


--
-- Name: main_activitystream_role main_activitystream_role_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_role
    ADD CONSTRAINT main_activitystream_role_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_schedule main_activitystream_sche_activitystream_id_schedu_a871c992_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_schedule
    ADD CONSTRAINT main_activitystream_sche_activitystream_id_schedu_a871c992_uniq UNIQUE (activitystream_id, schedule_id);


--
-- Name: main_activitystream_schedule main_activitystream_schedule_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_schedule
    ADD CONSTRAINT main_activitystream_schedule_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_team main_activitystream_team_activitystream_id_team_i_89af4b2a_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_team
    ADD CONSTRAINT main_activitystream_team_activitystream_id_team_i_89af4b2a_uniq UNIQUE (activitystream_id, team_id);


--
-- Name: main_activitystream_team main_activitystream_team_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_team
    ADD CONSTRAINT main_activitystream_team_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_unified_job main_activitystream_unif_activitystream_id_unifie_0fc17da3_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_unified_job
    ADD CONSTRAINT main_activitystream_unif_activitystream_id_unifie_0fc17da3_uniq UNIQUE (activitystream_id, unifiedjob_id);


--
-- Name: main_activitystream_unified_job_template main_activitystream_unif_activitystream_id_unifie_e4b906b4_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_unified_job_template
    ADD CONSTRAINT main_activitystream_unif_activitystream_id_unifie_e4b906b4_uniq UNIQUE (activitystream_id, unifiedjobtemplate_id);


--
-- Name: main_activitystream_unified_job main_activitystream_unified_job_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_unified_job
    ADD CONSTRAINT main_activitystream_unified_job_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_unified_job_template main_activitystream_unified_job_template_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_unified_job_template
    ADD CONSTRAINT main_activitystream_unified_job_template_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_user main_activitystream_user_activitystream_id_user_i_3fa08b1e_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_user
    ADD CONSTRAINT main_activitystream_user_activitystream_id_user_i_3fa08b1e_uniq UNIQUE (activitystream_id, user_id);


--
-- Name: main_activitystream_user main_activitystream_user_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_user
    ADD CONSTRAINT main_activitystream_user_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_workflow_approval_template main_activitystream_work_activitystream_id_workfl_6145f2cd_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_approval_template
    ADD CONSTRAINT main_activitystream_work_activitystream_id_workfl_6145f2cd_uniq UNIQUE (activitystream_id, workflowapprovaltemplate_id);


--
-- Name: main_activitystream_workflow_approval main_activitystream_work_activitystream_id_workfl_7c76df21_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_approval
    ADD CONSTRAINT main_activitystream_work_activitystream_id_workfl_7c76df21_uniq UNIQUE (activitystream_id, workflowapproval_id);


--
-- Name: main_activitystream_workflow_job_template main_activitystream_work_activitystream_id_workfl_9cf83c74_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_job_template
    ADD CONSTRAINT main_activitystream_work_activitystream_id_workfl_9cf83c74_uniq UNIQUE (activitystream_id, workflowjobtemplate_id);


--
-- Name: main_activitystream_workflow_job main_activitystream_work_activitystream_id_workfl_bfe2d0c3_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_job
    ADD CONSTRAINT main_activitystream_work_activitystream_id_workfl_bfe2d0c3_uniq UNIQUE (activitystream_id, workflowjob_id);


--
-- Name: main_activitystream_workflow_job_template_node main_activitystream_work_activitystream_id_workfl_c3080a18_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_job_template_node
    ADD CONSTRAINT main_activitystream_work_activitystream_id_workfl_c3080a18_uniq UNIQUE (activitystream_id, workflowjobtemplatenode_id);


--
-- Name: main_activitystream_workflow_job_node main_activitystream_work_activitystream_id_workfl_d615af7e_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_job_node
    ADD CONSTRAINT main_activitystream_work_activitystream_id_workfl_d615af7e_uniq UNIQUE (activitystream_id, workflowjobnode_id);


--
-- Name: main_activitystream_workflow_approval main_activitystream_workflow_approval_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_approval
    ADD CONSTRAINT main_activitystream_workflow_approval_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_workflow_approval_template main_activitystream_workflow_approval_template_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_approval_template
    ADD CONSTRAINT main_activitystream_workflow_approval_template_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_workflow_job_node main_activitystream_workflow_job_node_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_job_node
    ADD CONSTRAINT main_activitystream_workflow_job_node_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_workflow_job main_activitystream_workflow_job_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_job
    ADD CONSTRAINT main_activitystream_workflow_job_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_workflow_job_template_node main_activitystream_workflow_job_template_node_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_job_template_node
    ADD CONSTRAINT main_activitystream_workflow_job_template_node_pkey PRIMARY KEY (id);


--
-- Name: main_activitystream_workflow_job_template main_activitystream_workflow_job_template_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_job_template
    ADD CONSTRAINT main_activitystream_workflow_job_template_pkey PRIMARY KEY (id);


--
-- Name: main_adhoccommand main_adhoccommand_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_adhoccommand
    ADD CONSTRAINT main_adhoccommand_pkey PRIMARY KEY (unifiedjob_ptr_id);


--
-- Name: _unpartitioned_main_adhoccommandevent main_adhoccommandevent_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public._unpartitioned_main_adhoccommandevent
    ADD CONSTRAINT main_adhoccommandevent_pkey PRIMARY KEY (id);


--
-- Name: main_adhoccommandevent main_adhoccommandevent_pkey_new; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_adhoccommandevent
    ADD CONSTRAINT main_adhoccommandevent_pkey_new PRIMARY KEY (id, job_created);


--
-- Name: main_credential main_credential_organization_id_name_cre_55ee19c5_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_credential
    ADD CONSTRAINT main_credential_organization_id_name_cre_55ee19c5_uniq UNIQUE (organization_id, name, credential_type_id);


--
-- Name: main_credential main_credential_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_credential
    ADD CONSTRAINT main_credential_pkey PRIMARY KEY (id);


--
-- Name: main_credentialinputsource main_credentialinputsour_target_credential_id_inp_8e297f1b_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_credentialinputsource
    ADD CONSTRAINT main_credentialinputsour_target_credential_id_inp_8e297f1b_uniq UNIQUE (target_credential_id, input_field_name);


--
-- Name: main_credentialinputsource main_credentialinputsource_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_credentialinputsource
    ADD CONSTRAINT main_credentialinputsource_pkey PRIMARY KEY (id);


--
-- Name: main_credentialtype main_credentialtype_name_kind_af26d717_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_credentialtype
    ADD CONSTRAINT main_credentialtype_name_kind_af26d717_uniq UNIQUE (name, kind);


--
-- Name: main_credentialtype main_credentialtype_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_credentialtype
    ADD CONSTRAINT main_credentialtype_pkey PRIMARY KEY (id);


--
-- Name: main_custominventoryscript main_custominventoryscript_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_custominventoryscript
    ADD CONSTRAINT main_custominventoryscript_pkey PRIMARY KEY (id);


--
-- Name: main_executionenvironment main_executionenvironment_name_key; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_executionenvironment
    ADD CONSTRAINT main_executionenvironment_name_key UNIQUE (name);


--
-- Name: main_executionenvironment main_executionenvironment_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_executionenvironment
    ADD CONSTRAINT main_executionenvironment_pkey PRIMARY KEY (id);


--
-- Name: main_group_hosts main_group_hosts_group_id_host_id_0713d0ac_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_group_hosts
    ADD CONSTRAINT main_group_hosts_group_id_host_id_0713d0ac_uniq UNIQUE (group_id, host_id);


--
-- Name: main_group_hosts main_group_hosts_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_group_hosts
    ADD CONSTRAINT main_group_hosts_pkey PRIMARY KEY (id);


--
-- Name: main_group_inventory_sources main_group_inventory_sou_group_id_inventorysource_dcb51e86_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_group_inventory_sources
    ADD CONSTRAINT main_group_inventory_sou_group_id_inventorysource_dcb51e86_uniq UNIQUE (group_id, inventorysource_id);


--
-- Name: main_group_inventory_sources main_group_inventory_sources_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_group_inventory_sources
    ADD CONSTRAINT main_group_inventory_sources_pkey PRIMARY KEY (id);


--
-- Name: main_group main_group_name_inventory_id_459cfada_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_group
    ADD CONSTRAINT main_group_name_inventory_id_459cfada_uniq UNIQUE (name, inventory_id);


--
-- Name: main_group_parents main_group_parents_from_group_id_to_group_id_8c9a3fcb_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_group_parents
    ADD CONSTRAINT main_group_parents_from_group_id_to_group_id_8c9a3fcb_uniq UNIQUE (from_group_id, to_group_id);


--
-- Name: main_group_parents main_group_parents_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_group_parents
    ADD CONSTRAINT main_group_parents_pkey PRIMARY KEY (id);


--
-- Name: main_group main_group_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_group
    ADD CONSTRAINT main_group_pkey PRIMARY KEY (id);


--
-- Name: main_host_inventory_sources main_host_inventory_sour_host_id_inventorysource__bdf6a207_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_host_inventory_sources
    ADD CONSTRAINT main_host_inventory_sour_host_id_inventorysource__bdf6a207_uniq UNIQUE (host_id, inventorysource_id);


--
-- Name: main_host_inventory_sources main_host_inventory_sources_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_host_inventory_sources
    ADD CONSTRAINT main_host_inventory_sources_pkey PRIMARY KEY (id);


--
-- Name: main_host main_host_name_inventory_id_45aecd68_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_host
    ADD CONSTRAINT main_host_name_inventory_id_45aecd68_uniq UNIQUE (name, inventory_id);


--
-- Name: main_host main_host_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_host
    ADD CONSTRAINT main_host_pkey PRIMARY KEY (id);


--
-- Name: main_hostmetric main_hostmetric_hostname_87ac3c1f_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_hostmetric
    ADD CONSTRAINT main_hostmetric_hostname_87ac3c1f_uniq UNIQUE (hostname);


--
-- Name: main_hostmetric main_hostmetric_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_hostmetric
    ADD CONSTRAINT main_hostmetric_pkey PRIMARY KEY (id);


--
-- Name: main_hostmetricsummarymonthly main_hostmetricsummarymonthly_date_key; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_hostmetricsummarymonthly
    ADD CONSTRAINT main_hostmetricsummarymonthly_date_key UNIQUE (date);


--
-- Name: main_hostmetricsummarymonthly main_hostmetricsummarymonthly_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_hostmetricsummarymonthly
    ADD CONSTRAINT main_hostmetricsummarymonthly_pkey PRIMARY KEY (id);


--
-- Name: main_instance main_instance_hostname_key; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_instance
    ADD CONSTRAINT main_instance_hostname_key UNIQUE (hostname);


--
-- Name: main_instance main_instance_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_instance
    ADD CONSTRAINT main_instance_pkey PRIMARY KEY (id);


--
-- Name: main_instancegroup_instances main_instancegroup_insta_instancegroup_id_instanc_d224c278_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_instancegroup_instances
    ADD CONSTRAINT main_instancegroup_insta_instancegroup_id_instanc_d224c278_uniq UNIQUE (instancegroup_id, instance_id);


--
-- Name: main_instancegroup_instances main_instancegroup_instances_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_instancegroup_instances
    ADD CONSTRAINT main_instancegroup_instances_pkey PRIMARY KEY (id);


--
-- Name: main_instancegroup main_instancegroup_name_key; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_instancegroup
    ADD CONSTRAINT main_instancegroup_name_key UNIQUE (name);


--
-- Name: main_instancegroup main_instancegroup_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_instancegroup
    ADD CONSTRAINT main_instancegroup_pkey PRIMARY KEY (id);


--
-- Name: main_instancelink main_instancelink_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_instancelink
    ADD CONSTRAINT main_instancelink_pkey PRIMARY KEY (id);


--
-- Name: main_inventory_labels main_inventory_labels_inventory_id_label_id_b527d1a3_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventory_labels
    ADD CONSTRAINT main_inventory_labels_inventory_id_label_id_b527d1a3_uniq UNIQUE (inventory_id, label_id);


--
-- Name: main_inventory_labels main_inventory_labels_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventory_labels
    ADD CONSTRAINT main_inventory_labels_pkey PRIMARY KEY (id);


--
-- Name: main_inventory main_inventory_name_organization_id_5137f34c_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventory
    ADD CONSTRAINT main_inventory_name_organization_id_5137f34c_uniq UNIQUE (name, organization_id);


--
-- Name: main_inventory main_inventory_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventory
    ADD CONSTRAINT main_inventory_pkey PRIMARY KEY (id);


--
-- Name: main_inventoryconstructedinventorymembership main_inventoryconstructedinventorymembership_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventoryconstructedinventorymembership
    ADD CONSTRAINT main_inventoryconstructedinventorymembership_pkey PRIMARY KEY (id);


--
-- Name: main_inventoryinstancegroupmembership main_inventoryinstancegroupmembership_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventoryinstancegroupmembership
    ADD CONSTRAINT main_inventoryinstancegroupmembership_pkey PRIMARY KEY (id);


--
-- Name: main_inventorysource main_inventorysource_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventorysource
    ADD CONSTRAINT main_inventorysource_pkey PRIMARY KEY (unifiedjobtemplate_ptr_id);


--
-- Name: main_inventoryupdate main_inventoryupdate_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventoryupdate
    ADD CONSTRAINT main_inventoryupdate_pkey PRIMARY KEY (unifiedjob_ptr_id);


--
-- Name: _unpartitioned_main_inventoryupdateevent main_inventoryupdateevent_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public._unpartitioned_main_inventoryupdateevent
    ADD CONSTRAINT main_inventoryupdateevent_pkey PRIMARY KEY (id);


--
-- Name: main_inventoryupdateevent main_inventoryupdateevent_pkey_new; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventoryupdateevent
    ADD CONSTRAINT main_inventoryupdateevent_pkey_new PRIMARY KEY (id, job_created);


--
-- Name: main_job main_job_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_job
    ADD CONSTRAINT main_job_pkey PRIMARY KEY (unifiedjob_ptr_id);


--
-- Name: main_jobevent main_jobevent_pkey_new; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_jobevent
    ADD CONSTRAINT main_jobevent_pkey_new PRIMARY KEY (id, job_created);


--
-- Name: main_jobevent_20241219_17 main_jobevent_20241219_17_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_jobevent_20241219_17
    ADD CONSTRAINT main_jobevent_20241219_17_pkey PRIMARY KEY (id, job_created);


--
-- Name: _unpartitioned_main_jobevent main_jobevent_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public._unpartitioned_main_jobevent
    ADD CONSTRAINT main_jobevent_pkey PRIMARY KEY (id);


--
-- Name: main_jobhostsummary main_jobhostsummary_job_id_host_name_eb22f938_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_jobhostsummary
    ADD CONSTRAINT main_jobhostsummary_job_id_host_name_eb22f938_uniq UNIQUE (job_id, host_name);


--
-- Name: main_jobhostsummary main_jobhostsummary_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_jobhostsummary
    ADD CONSTRAINT main_jobhostsummary_pkey PRIMARY KEY (id);


--
-- Name: main_joblaunchconfig_credentials main_joblaunchconfig_cre_joblaunchconfig_id_crede_77f9ef8b_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_joblaunchconfig_credentials
    ADD CONSTRAINT main_joblaunchconfig_cre_joblaunchconfig_id_crede_77f9ef8b_uniq UNIQUE (joblaunchconfig_id, credential_id);


--
-- Name: main_joblaunchconfig_credentials main_joblaunchconfig_credentials_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_joblaunchconfig_credentials
    ADD CONSTRAINT main_joblaunchconfig_credentials_pkey PRIMARY KEY (id);


--
-- Name: main_joblaunchconfig main_joblaunchconfig_job_id_key; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_joblaunchconfig
    ADD CONSTRAINT main_joblaunchconfig_job_id_key UNIQUE (job_id);


--
-- Name: main_joblaunchconfig_labels main_joblaunchconfig_lab_joblaunchconfig_id_label_bddd29c9_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_joblaunchconfig_labels
    ADD CONSTRAINT main_joblaunchconfig_lab_joblaunchconfig_id_label_bddd29c9_uniq UNIQUE (joblaunchconfig_id, label_id);


--
-- Name: main_joblaunchconfig_labels main_joblaunchconfig_labels_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_joblaunchconfig_labels
    ADD CONSTRAINT main_joblaunchconfig_labels_pkey PRIMARY KEY (id);


--
-- Name: main_joblaunchconfig main_joblaunchconfig_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_joblaunchconfig
    ADD CONSTRAINT main_joblaunchconfig_pkey PRIMARY KEY (id);


--
-- Name: main_joblaunchconfiginstancegroupmembership main_joblaunchconfiginstancegroupmembership_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_joblaunchconfiginstancegroupmembership
    ADD CONSTRAINT main_joblaunchconfiginstancegroupmembership_pkey PRIMARY KEY (id);


--
-- Name: main_jobtemplate main_jobtemplate_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_jobtemplate
    ADD CONSTRAINT main_jobtemplate_pkey PRIMARY KEY (unifiedjobtemplate_ptr_id);


--
-- Name: main_label main_label_name_organization_id_f79d7ac4_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_label
    ADD CONSTRAINT main_label_name_organization_id_f79d7ac4_uniq UNIQUE (name, organization_id);


--
-- Name: main_label main_label_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_label
    ADD CONSTRAINT main_label_pkey PRIMARY KEY (id);


--
-- Name: main_notification main_notification_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_notification
    ADD CONSTRAINT main_notification_pkey PRIMARY KEY (id);


--
-- Name: main_notificationtemplate main_notificationtemplate_organization_id_name_07260e01_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_notificationtemplate
    ADD CONSTRAINT main_notificationtemplate_organization_id_name_07260e01_uniq UNIQUE (organization_id, name);


--
-- Name: main_notificationtemplate main_notificationtemplate_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_notificationtemplate
    ADD CONSTRAINT main_notificationtemplate_pkey PRIMARY KEY (id);


--
-- Name: main_organization main_organization_name_key; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization
    ADD CONSTRAINT main_organization_name_key UNIQUE (name);


--
-- Name: main_organization_notification_templates_started main_organization_notifi_organization_id_notifica_2ef43b54_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization_notification_templates_started
    ADD CONSTRAINT main_organization_notifi_organization_id_notifica_2ef43b54_uniq UNIQUE (organization_id, notificationtemplate_id);


--
-- Name: main_organization_notification_templates_success main_organization_notifi_organization_id_notifica_3ccf8832_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization_notification_templates_success
    ADD CONSTRAINT main_organization_notifi_organization_id_notifica_3ccf8832_uniq UNIQUE (organization_id, notificationtemplate_id);


--
-- Name: main_organization_notification_templates_error main_organization_notifi_organization_id_notifica_88aa41f6_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization_notification_templates_error
    ADD CONSTRAINT main_organization_notifi_organization_id_notifica_88aa41f6_uniq UNIQUE (organization_id, notificationtemplate_id);


--
-- Name: main_organization_notification_templates_approvals main_organization_notifi_organization_id_notifica_ec9bb02b_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization_notification_templates_approvals
    ADD CONSTRAINT main_organization_notifi_organization_id_notifica_ec9bb02b_uniq UNIQUE (organization_id, notificationtemplate_id);


--
-- Name: main_organization_notification_templates_approvals main_organization_notification_templates_approvals_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization_notification_templates_approvals
    ADD CONSTRAINT main_organization_notification_templates_approvals_pkey PRIMARY KEY (id);


--
-- Name: main_organization_notification_templates_error main_organization_notification_templates_error_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization_notification_templates_error
    ADD CONSTRAINT main_organization_notification_templates_error_pkey PRIMARY KEY (id);


--
-- Name: main_organization_notification_templates_started main_organization_notification_templates_started_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization_notification_templates_started
    ADD CONSTRAINT main_organization_notification_templates_started_pkey PRIMARY KEY (id);


--
-- Name: main_organization_notification_templates_success main_organization_notification_templates_success_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization_notification_templates_success
    ADD CONSTRAINT main_organization_notification_templates_success_pkey PRIMARY KEY (id);


--
-- Name: main_organization main_organization_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization
    ADD CONSTRAINT main_organization_pkey PRIMARY KEY (id);


--
-- Name: main_organizationgalaxycredentialmembership main_organizationgalaxycredentialmembership_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organizationgalaxycredentialmembership
    ADD CONSTRAINT main_organizationgalaxycredentialmembership_pkey PRIMARY KEY (id);


--
-- Name: main_organizationinstancegroupmembership main_organizationinstancegroupmembership_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organizationinstancegroupmembership
    ADD CONSTRAINT main_organizationinstancegroupmembership_pkey PRIMARY KEY (id);


--
-- Name: main_project main_project_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_project
    ADD CONSTRAINT main_project_pkey PRIMARY KEY (unifiedjobtemplate_ptr_id);


--
-- Name: main_projectupdate main_projectupdate_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_projectupdate
    ADD CONSTRAINT main_projectupdate_pkey PRIMARY KEY (unifiedjob_ptr_id);


--
-- Name: main_projectupdateevent main_projectupdateevent_pkey_new; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_projectupdateevent
    ADD CONSTRAINT main_projectupdateevent_pkey_new PRIMARY KEY (id, job_created);


--
-- Name: main_projectupdateevent_20241219_17 main_projectupdateevent_20241219_17_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_projectupdateevent_20241219_17
    ADD CONSTRAINT main_projectupdateevent_20241219_17_pkey PRIMARY KEY (id, job_created);


--
-- Name: _unpartitioned_main_projectupdateevent main_projectupdateevent_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public._unpartitioned_main_projectupdateevent
    ADD CONSTRAINT main_projectupdateevent_pkey PRIMARY KEY (id);


--
-- Name: main_rbac_role_ancestors main_rbac_role_ancestors_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_rbac_role_ancestors
    ADD CONSTRAINT main_rbac_role_ancestors_pkey PRIMARY KEY (id);


--
-- Name: main_rbac_roles_members main_rbac_roles_members_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_rbac_roles_members
    ADD CONSTRAINT main_rbac_roles_members_pkey PRIMARY KEY (id);


--
-- Name: main_rbac_roles_members main_rbac_roles_members_role_id_user_id_9803c082_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_rbac_roles_members
    ADD CONSTRAINT main_rbac_roles_members_role_id_user_id_9803c082_uniq UNIQUE (role_id, user_id);


--
-- Name: main_rbac_roles_parents main_rbac_roles_parents_from_role_id_to_role_id_1ab75c81_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_rbac_roles_parents
    ADD CONSTRAINT main_rbac_roles_parents_from_role_id_to_role_id_1ab75c81_uniq UNIQUE (from_role_id, to_role_id);


--
-- Name: main_rbac_roles_parents main_rbac_roles_parents_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_rbac_roles_parents
    ADD CONSTRAINT main_rbac_roles_parents_pkey PRIMARY KEY (id);


--
-- Name: main_rbac_roles main_rbac_roles_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_rbac_roles
    ADD CONSTRAINT main_rbac_roles_pkey PRIMARY KEY (id);


--
-- Name: main_rbac_roles main_rbac_roles_singleton_name_key; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_rbac_roles
    ADD CONSTRAINT main_rbac_roles_singleton_name_key UNIQUE (singleton_name);


--
-- Name: main_receptoraddress main_receptoraddress_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_receptoraddress
    ADD CONSTRAINT main_receptoraddress_pkey PRIMARY KEY (id);


--
-- Name: main_schedule_credentials main_schedule_credential_schedule_id_credential_i_11bed4b0_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_schedule_credentials
    ADD CONSTRAINT main_schedule_credential_schedule_id_credential_i_11bed4b0_uniq UNIQUE (schedule_id, credential_id);


--
-- Name: main_schedule_credentials main_schedule_credentials_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_schedule_credentials
    ADD CONSTRAINT main_schedule_credentials_pkey PRIMARY KEY (id);


--
-- Name: main_schedule_labels main_schedule_labels_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_schedule_labels
    ADD CONSTRAINT main_schedule_labels_pkey PRIMARY KEY (id);


--
-- Name: main_schedule_labels main_schedule_labels_schedule_id_label_id_56639469_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_schedule_labels
    ADD CONSTRAINT main_schedule_labels_schedule_id_label_id_56639469_uniq UNIQUE (schedule_id, label_id);


--
-- Name: main_schedule main_schedule_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_schedule
    ADD CONSTRAINT main_schedule_pkey PRIMARY KEY (id);


--
-- Name: main_schedule main_schedule_unified_job_template_id_name_9ba35d7e_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_schedule
    ADD CONSTRAINT main_schedule_unified_job_template_id_name_9ba35d7e_uniq UNIQUE (unified_job_template_id, name);


--
-- Name: main_scheduleinstancegroupmembership main_scheduleinstancegroupmembership_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_scheduleinstancegroupmembership
    ADD CONSTRAINT main_scheduleinstancegroupmembership_pkey PRIMARY KEY (id);


--
-- Name: main_smartinventorymembership main_smartinventorymembe_host_id_inventory_id_58137be6_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_smartinventorymembership
    ADD CONSTRAINT main_smartinventorymembe_host_id_inventory_id_58137be6_uniq UNIQUE (host_id, inventory_id);


--
-- Name: main_smartinventorymembership main_smartinventorymembership_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_smartinventorymembership
    ADD CONSTRAINT main_smartinventorymembership_pkey PRIMARY KEY (id);


--
-- Name: main_systemjob main_systemjob_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_systemjob
    ADD CONSTRAINT main_systemjob_pkey PRIMARY KEY (unifiedjob_ptr_id);


--
-- Name: _unpartitioned_main_systemjobevent main_systemjobevent_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public._unpartitioned_main_systemjobevent
    ADD CONSTRAINT main_systemjobevent_pkey PRIMARY KEY (id);


--
-- Name: main_systemjobevent main_systemjobevent_pkey_new; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_systemjobevent
    ADD CONSTRAINT main_systemjobevent_pkey_new PRIMARY KEY (id, job_created);


--
-- Name: main_systemjobtemplate main_systemjobtemplate_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_systemjobtemplate
    ADD CONSTRAINT main_systemjobtemplate_pkey PRIMARY KEY (unifiedjobtemplate_ptr_id);


--
-- Name: main_team main_team_organization_id_name_70f0184b_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_team
    ADD CONSTRAINT main_team_organization_id_name_70f0184b_uniq UNIQUE (organization_id, name);


--
-- Name: main_team main_team_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_team
    ADD CONSTRAINT main_team_pkey PRIMARY KEY (id);


--
-- Name: main_towerschedulestate main_towerschedulestate_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_towerschedulestate
    ADD CONSTRAINT main_towerschedulestate_pkey PRIMARY KEY (id);


--
-- Name: main_unifiedjob_credentials main_unifiedjob_credenti_unifiedjob_id_credential_f4b12e17_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob_credentials
    ADD CONSTRAINT main_unifiedjob_credenti_unifiedjob_id_credential_f4b12e17_uniq UNIQUE (unifiedjob_id, credential_id);


--
-- Name: main_unifiedjob_credentials main_unifiedjob_credentials_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob_credentials
    ADD CONSTRAINT main_unifiedjob_credentials_pkey PRIMARY KEY (id);


--
-- Name: main_unifiedjob_dependent_jobs main_unifiedjob_dependen_from_unifiedjob_id_to_un_8ee8a967_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob_dependent_jobs
    ADD CONSTRAINT main_unifiedjob_dependen_from_unifiedjob_id_to_un_8ee8a967_uniq UNIQUE (from_unifiedjob_id, to_unifiedjob_id);


--
-- Name: main_unifiedjob_dependent_jobs main_unifiedjob_dependent_jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob_dependent_jobs
    ADD CONSTRAINT main_unifiedjob_dependent_jobs_pkey PRIMARY KEY (id);


--
-- Name: main_unifiedjob_labels main_unifiedjob_labels_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob_labels
    ADD CONSTRAINT main_unifiedjob_labels_pkey PRIMARY KEY (id);


--
-- Name: main_unifiedjob_labels main_unifiedjob_labels_unifiedjob_id_label_id_f6e1dc96_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob_labels
    ADD CONSTRAINT main_unifiedjob_labels_unifiedjob_id_label_id_f6e1dc96_uniq UNIQUE (unifiedjob_id, label_id);


--
-- Name: main_unifiedjob_notifications main_unifiedjob_notifica_unifiedjob_id_notificati_895ae806_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob_notifications
    ADD CONSTRAINT main_unifiedjob_notifica_unifiedjob_id_notificati_895ae806_uniq UNIQUE (unifiedjob_id, notification_id);


--
-- Name: main_unifiedjob_notifications main_unifiedjob_notifications_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob_notifications
    ADD CONSTRAINT main_unifiedjob_notifications_pkey PRIMARY KEY (id);


--
-- Name: main_unifiedjob main_unifiedjob_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob
    ADD CONSTRAINT main_unifiedjob_pkey PRIMARY KEY (id);


--
-- Name: main_unifiedjobtemplate_credentials main_unifiedjobtemplate__unifiedjobtemplate_id_cr_e10bc7a4_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate_credentials
    ADD CONSTRAINT main_unifiedjobtemplate__unifiedjobtemplate_id_cr_e10bc7a4_uniq UNIQUE (unifiedjobtemplate_id, credential_id);


--
-- Name: main_unifiedjobtemplate_labels main_unifiedjobtemplate__unifiedjobtemplate_id_la_ad69a027_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate_labels
    ADD CONSTRAINT main_unifiedjobtemplate__unifiedjobtemplate_id_la_ad69a027_uniq UNIQUE (unifiedjobtemplate_id, label_id);


--
-- Name: main_unifiedjobtemplate_notification_templates_success main_unifiedjobtemplate__unifiedjobtemplate_id_no_113bd2d4_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate_notification_templates_success
    ADD CONSTRAINT main_unifiedjobtemplate__unifiedjobtemplate_id_no_113bd2d4_uniq UNIQUE (unifiedjobtemplate_id, notificationtemplate_id);


--
-- Name: main_unifiedjobtemplate_notification_templates_error main_unifiedjobtemplate__unifiedjobtemplate_id_no_172864be_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate_notification_templates_error
    ADD CONSTRAINT main_unifiedjobtemplate__unifiedjobtemplate_id_no_172864be_uniq UNIQUE (unifiedjobtemplate_id, notificationtemplate_id);


--
-- Name: main_unifiedjobtemplate_notification_templates_started main_unifiedjobtemplate__unifiedjobtemplate_id_no_5b15714c_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate_notification_templates_started
    ADD CONSTRAINT main_unifiedjobtemplate__unifiedjobtemplate_id_no_5b15714c_uniq UNIQUE (unifiedjobtemplate_id, notificationtemplate_id);


--
-- Name: main_unifiedjobtemplate_credentials main_unifiedjobtemplate_credentials_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate_credentials
    ADD CONSTRAINT main_unifiedjobtemplate_credentials_pkey PRIMARY KEY (id);


--
-- Name: main_unifiedjobtemplate_labels main_unifiedjobtemplate_labels_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate_labels
    ADD CONSTRAINT main_unifiedjobtemplate_labels_pkey PRIMARY KEY (id);


--
-- Name: main_unifiedjobtemplate_notification_templates_error main_unifiedjobtemplate_notification_templates_error_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate_notification_templates_error
    ADD CONSTRAINT main_unifiedjobtemplate_notification_templates_error_pkey PRIMARY KEY (id);


--
-- Name: main_unifiedjobtemplate_notification_templates_started main_unifiedjobtemplate_notification_templates_started_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate_notification_templates_started
    ADD CONSTRAINT main_unifiedjobtemplate_notification_templates_started_pkey PRIMARY KEY (id);


--
-- Name: main_unifiedjobtemplate_notification_templates_success main_unifiedjobtemplate_notification_templates_success_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate_notification_templates_success
    ADD CONSTRAINT main_unifiedjobtemplate_notification_templates_success_pkey PRIMARY KEY (id);


--
-- Name: main_unifiedjobtemplate main_unifiedjobtemplate_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate
    ADD CONSTRAINT main_unifiedjobtemplate_pkey PRIMARY KEY (id);


--
-- Name: main_unifiedjobtemplateinstancegroupmembership main_unifiedjobtemplateinstancegroupmembership_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplateinstancegroupmembership
    ADD CONSTRAINT main_unifiedjobtemplateinstancegroupmembership_pkey PRIMARY KEY (id);


--
-- Name: main_usersessionmembership main_usersessionmembership_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_usersessionmembership
    ADD CONSTRAINT main_usersessionmembership_pkey PRIMARY KEY (id);


--
-- Name: main_usersessionmembership main_usersessionmembership_session_id_key; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_usersessionmembership
    ADD CONSTRAINT main_usersessionmembership_session_id_key UNIQUE (session_id);


--
-- Name: main_workflowapproval main_workflowapproval_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowapproval
    ADD CONSTRAINT main_workflowapproval_pkey PRIMARY KEY (unifiedjob_ptr_id);


--
-- Name: main_workflowapprovaltemplate main_workflowapprovaltemplate_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowapprovaltemplate
    ADD CONSTRAINT main_workflowapprovaltemplate_pkey PRIMARY KEY (unifiedjobtemplate_ptr_id);


--
-- Name: main_workflowjob main_workflowjob_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjob
    ADD CONSTRAINT main_workflowjob_pkey PRIMARY KEY (unifiedjob_ptr_id);


--
-- Name: main_workflowjobinstancegroupmembership main_workflowjobinstancegroupmembership_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobinstancegroupmembership
    ADD CONSTRAINT main_workflowjobinstancegroupmembership_pkey PRIMARY KEY (id);


--
-- Name: main_workflowjobnode_always_nodes main_workflowjobnode_alw_from_workflowjobnode_id__550e0051_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode_always_nodes
    ADD CONSTRAINT main_workflowjobnode_alw_from_workflowjobnode_id__550e0051_uniq UNIQUE (from_workflowjobnode_id, to_workflowjobnode_id);


--
-- Name: main_workflowjobnode_always_nodes main_workflowjobnode_always_nodes_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode_always_nodes
    ADD CONSTRAINT main_workflowjobnode_always_nodes_pkey PRIMARY KEY (id);


--
-- Name: main_workflowjobnode_credentials main_workflowjobnode_cre_workflowjobnode_id_crede_75628d2d_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode_credentials
    ADD CONSTRAINT main_workflowjobnode_cre_workflowjobnode_id_crede_75628d2d_uniq UNIQUE (workflowjobnode_id, credential_id);


--
-- Name: main_workflowjobnode_credentials main_workflowjobnode_credentials_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode_credentials
    ADD CONSTRAINT main_workflowjobnode_credentials_pkey PRIMARY KEY (id);


--
-- Name: main_workflowjobnode_failure_nodes main_workflowjobnode_fai_from_workflowjobnode_id__355631cb_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode_failure_nodes
    ADD CONSTRAINT main_workflowjobnode_fai_from_workflowjobnode_id__355631cb_uniq UNIQUE (from_workflowjobnode_id, to_workflowjobnode_id);


--
-- Name: main_workflowjobnode_failure_nodes main_workflowjobnode_failure_nodes_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode_failure_nodes
    ADD CONSTRAINT main_workflowjobnode_failure_nodes_pkey PRIMARY KEY (id);


--
-- Name: main_workflowjobnode main_workflowjobnode_job_id_key; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode
    ADD CONSTRAINT main_workflowjobnode_job_id_key UNIQUE (job_id);


--
-- Name: main_workflowjobnode_labels main_workflowjobnode_lab_workflowjobnode_id_label_f0763257_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode_labels
    ADD CONSTRAINT main_workflowjobnode_lab_workflowjobnode_id_label_f0763257_uniq UNIQUE (workflowjobnode_id, label_id);


--
-- Name: main_workflowjobnode_labels main_workflowjobnode_labels_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode_labels
    ADD CONSTRAINT main_workflowjobnode_labels_pkey PRIMARY KEY (id);


--
-- Name: main_workflowjobnode main_workflowjobnode_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode
    ADD CONSTRAINT main_workflowjobnode_pkey PRIMARY KEY (id);


--
-- Name: main_workflowjobnode_success_nodes main_workflowjobnode_suc_from_workflowjobnode_id__59094efc_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode_success_nodes
    ADD CONSTRAINT main_workflowjobnode_suc_from_workflowjobnode_id__59094efc_uniq UNIQUE (from_workflowjobnode_id, to_workflowjobnode_id);


--
-- Name: main_workflowjobnode_success_nodes main_workflowjobnode_success_nodes_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode_success_nodes
    ADD CONSTRAINT main_workflowjobnode_success_nodes_pkey PRIMARY KEY (id);


--
-- Name: main_workflowjobnodebaseinstancegroupmembership main_workflowjobnodebaseinstancegroupmembership_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnodebaseinstancegroupmembership
    ADD CONSTRAINT main_workflowjobnodebaseinstancegroupmembership_pkey PRIMARY KEY (id);


--
-- Name: main_workflowjobtemplatenode_always_nodes main_workflowjobtemplate_from_workflowjobtemplate_01869c4a_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode_always_nodes
    ADD CONSTRAINT main_workflowjobtemplate_from_workflowjobtemplate_01869c4a_uniq UNIQUE (from_workflowjobtemplatenode_id, to_workflowjobtemplatenode_id);


--
-- Name: main_workflowjobtemplatenode_failure_nodes main_workflowjobtemplate_from_workflowjobtemplate_5f970860_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode_failure_nodes
    ADD CONSTRAINT main_workflowjobtemplate_from_workflowjobtemplate_5f970860_uniq UNIQUE (from_workflowjobtemplatenode_id, to_workflowjobtemplatenode_id);


--
-- Name: main_workflowjobtemplatenode_success_nodes main_workflowjobtemplate_from_workflowjobtemplate_b5f4a54a_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode_success_nodes
    ADD CONSTRAINT main_workflowjobtemplate_from_workflowjobtemplate_b5f4a54a_uniq UNIQUE (from_workflowjobtemplatenode_id, to_workflowjobtemplatenode_id);


--
-- Name: main_workflowjobtemplatenode main_workflowjobtemplate_identifier_workflow_job__03484516_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode
    ADD CONSTRAINT main_workflowjobtemplate_identifier_workflow_job__03484516_uniq UNIQUE (identifier, workflow_job_template_id);


--
-- Name: main_workflowjobtemplate_notification_templates_approvals main_workflowjobtemplate_notification_templates_approvals_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplate_notification_templates_approvals
    ADD CONSTRAINT main_workflowjobtemplate_notification_templates_approvals_pkey PRIMARY KEY (id);


--
-- Name: main_workflowjobtemplate main_workflowjobtemplate_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplate
    ADD CONSTRAINT main_workflowjobtemplate_pkey PRIMARY KEY (unifiedjobtemplate_ptr_id);


--
-- Name: main_workflowjobtemplate_notification_templates_approvals main_workflowjobtemplate_workflowjobtemplate_id_n_4b1a7a0a_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplate_notification_templates_approvals
    ADD CONSTRAINT main_workflowjobtemplate_workflowjobtemplate_id_n_4b1a7a0a_uniq UNIQUE (workflowjobtemplate_id, notificationtemplate_id);


--
-- Name: main_workflowjobtemplatenode_labels main_workflowjobtemplate_workflowjobtemplatenode__119dcc7c_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode_labels
    ADD CONSTRAINT main_workflowjobtemplate_workflowjobtemplatenode__119dcc7c_uniq UNIQUE (workflowjobtemplatenode_id, label_id);


--
-- Name: main_workflowjobtemplatenode_credentials main_workflowjobtemplate_workflowjobtemplatenode__a6ba785b_uniq; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode_credentials
    ADD CONSTRAINT main_workflowjobtemplate_workflowjobtemplatenode__a6ba785b_uniq UNIQUE (workflowjobtemplatenode_id, credential_id);


--
-- Name: main_workflowjobtemplatenode_always_nodes main_workflowjobtemplatenode_always_nodes_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode_always_nodes
    ADD CONSTRAINT main_workflowjobtemplatenode_always_nodes_pkey PRIMARY KEY (id);


--
-- Name: main_workflowjobtemplatenode_credentials main_workflowjobtemplatenode_credentials_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode_credentials
    ADD CONSTRAINT main_workflowjobtemplatenode_credentials_pkey PRIMARY KEY (id);


--
-- Name: main_workflowjobtemplatenode_failure_nodes main_workflowjobtemplatenode_failure_nodes_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode_failure_nodes
    ADD CONSTRAINT main_workflowjobtemplatenode_failure_nodes_pkey PRIMARY KEY (id);


--
-- Name: main_workflowjobtemplatenode_labels main_workflowjobtemplatenode_labels_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode_labels
    ADD CONSTRAINT main_workflowjobtemplatenode_labels_pkey PRIMARY KEY (id);


--
-- Name: main_workflowjobtemplatenode main_workflowjobtemplatenode_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode
    ADD CONSTRAINT main_workflowjobtemplatenode_pkey PRIMARY KEY (id);


--
-- Name: main_workflowjobtemplatenode_success_nodes main_workflowjobtemplatenode_success_nodes_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode_success_nodes
    ADD CONSTRAINT main_workflowjobtemplatenode_success_nodes_pkey PRIMARY KEY (id);


--
-- Name: main_workflowjobtemplatenodebaseinstancegroupmembership main_workflowjobtemplatenodebaseinstancegroupmembership_pkey; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenodebaseinstancegroupmembership
    ADD CONSTRAINT main_workflowjobtemplatenodebaseinstancegroupmembership_pkey PRIMARY KEY (id);


--
-- Name: dab_rbac_roleevaluation one_entry_per_object_permission_and_role; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roleevaluation
    ADD CONSTRAINT one_entry_per_object_permission_and_role UNIQUE (object_id, content_type_id, codename, role_id);


--
-- Name: dab_rbac_roleevaluationuuid one_entry_per_object_permission_and_role_uuid; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roleevaluationuuid
    ADD CONSTRAINT one_entry_per_object_permission_and_role_uuid UNIQUE (object_id, content_type_id, codename, role_id);


--
-- Name: dab_rbac_objectrole one_object_role_per_object_and_role; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_objectrole
    ADD CONSTRAINT one_object_role_per_object_and_role UNIQUE (object_id, content_type_id, role_definition_id);


--
-- Name: main_receptoraddress unique_receptor_address; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_receptoraddress
    ADD CONSTRAINT unique_receptor_address UNIQUE (address);


--
-- Name: main_instancelink unique_source_target; Type: CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_instancelink
    ADD CONSTRAINT unique_source_target UNIQUE (source_id, target_id);


--
-- Name: auth_group_name_a6ea08ec_like; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX auth_group_name_a6ea08ec_like ON public.auth_group USING btree (name varchar_pattern_ops);


--
-- Name: auth_group_permissions_group_id_b120cbf9; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX auth_group_permissions_group_id_b120cbf9 ON public.auth_group_permissions USING btree (group_id);


--
-- Name: auth_group_permissions_permission_id_84c5c92e; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX auth_group_permissions_permission_id_84c5c92e ON public.auth_group_permissions USING btree (permission_id);


--
-- Name: auth_permission_content_type_id_2f476e4b; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX auth_permission_content_type_id_2f476e4b ON public.auth_permission USING btree (content_type_id);


--
-- Name: auth_user_groups_group_id_97559544; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX auth_user_groups_group_id_97559544 ON public.auth_user_groups USING btree (group_id);


--
-- Name: auth_user_groups_user_id_6a12ed8b; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX auth_user_groups_user_id_6a12ed8b ON public.auth_user_groups USING btree (user_id);


--
-- Name: auth_user_user_permissions_permission_id_1fbb5f2c; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX auth_user_user_permissions_permission_id_1fbb5f2c ON public.auth_user_user_permissions USING btree (permission_id);


--
-- Name: auth_user_user_permissions_user_id_a95ead1b; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX auth_user_user_permissions_user_id_a95ead1b ON public.auth_user_user_permissions USING btree (user_id);


--
-- Name: auth_user_username_6821ab7c_like; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX auth_user_username_6821ab7c_like ON public.auth_user USING btree (username varchar_pattern_ops);


--
-- Name: conf_setting_user_id_ce9d5138; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX conf_setting_user_id_ce9d5138 ON public.conf_setting USING btree (user_id);


--
-- Name: dab_rbac_dabpermission_content_type_id_2dbdb964; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_dabpermission_content_type_id_2dbdb964 ON public.dab_rbac_dabpermission USING btree (content_type_id);


--
-- Name: dab_rbac_ob_content_cbd55d_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_ob_content_cbd55d_idx ON public.dab_rbac_objectrole USING btree (content_type_id, object_id);


--
-- Name: dab_rbac_objectrole_content_type_id_a1bc92de; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_objectrole_content_type_id_a1bc92de ON public.dab_rbac_objectrole USING btree (content_type_id);


--
-- Name: dab_rbac_objectrole_provides_teams_objectrole_id_406b577e; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_objectrole_provides_teams_objectrole_id_406b577e ON public.dab_rbac_objectrole_provides_teams USING btree (objectrole_id);


--
-- Name: dab_rbac_objectrole_provides_teams_team_id_5d198983; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_objectrole_provides_teams_team_id_5d198983 ON public.dab_rbac_objectrole_provides_teams USING btree (team_id);


--
-- Name: dab_rbac_objectrole_role_definition_id_0a5a68ee; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_objectrole_role_definition_id_0a5a68ee ON public.dab_rbac_objectrole USING btree (role_definition_id);


--
-- Name: dab_rbac_ro_role_id_237936_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_ro_role_id_237936_idx ON public.dab_rbac_roleevaluationuuid USING btree (role_id, content_type_id, object_id);


--
-- Name: dab_rbac_ro_role_id_4fe905_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_ro_role_id_4fe905_idx ON public.dab_rbac_roleevaluationuuid USING btree (role_id, content_type_id, codename);


--
-- Name: dab_rbac_ro_role_id_604bc4_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_ro_role_id_604bc4_idx ON public.dab_rbac_roleevaluation USING btree (role_id, content_type_id, object_id);


--
-- Name: dab_rbac_ro_role_id_8b9faf_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_ro_role_id_8b9faf_idx ON public.dab_rbac_roleevaluation USING btree (role_id, content_type_id, codename);


--
-- Name: dab_rbac_roledefinition_content_type_id_71c1ad50; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_roledefinition_content_type_id_71c1ad50 ON public.dab_rbac_roledefinition USING btree (content_type_id);


--
-- Name: dab_rbac_roledefinition_created_by_id_42f60326; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_roledefinition_created_by_id_42f60326 ON public.dab_rbac_roledefinition USING btree (created_by_id);


--
-- Name: dab_rbac_roledefinition_modified_by_id_eac4ebcb; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_roledefinition_modified_by_id_eac4ebcb ON public.dab_rbac_roledefinition USING btree (modified_by_id);


--
-- Name: dab_rbac_roledefinition_name_d48d8c12_like; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_roledefinition_name_d48d8c12_like ON public.dab_rbac_roledefinition USING btree (name text_pattern_ops);


--
-- Name: dab_rbac_roledefinition_permissions_dabpermission_id_4f03ecd7; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_roledefinition_permissions_dabpermission_id_4f03ecd7 ON public.dab_rbac_roledefinition_permissions USING btree (dabpermission_id);


--
-- Name: dab_rbac_roledefinition_permissions_roledefinition_id_0bef5090; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_roledefinition_permissions_roledefinition_id_0bef5090 ON public.dab_rbac_roledefinition_permissions USING btree (roledefinition_id);


--
-- Name: dab_rbac_roleevaluation_role_id_93254162; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_roleevaluation_role_id_93254162 ON public.dab_rbac_roleevaluation USING btree (role_id);


--
-- Name: dab_rbac_roleevaluationuuid_role_id_254631b0; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_roleevaluationuuid_role_id_254631b0 ON public.dab_rbac_roleevaluationuuid USING btree (role_id);


--
-- Name: dab_rbac_roleteamassignment_content_type_id_adbc7356; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_roleteamassignment_content_type_id_adbc7356 ON public.dab_rbac_roleteamassignment USING btree (content_type_id);


--
-- Name: dab_rbac_roleteamassignment_created_by_id_ef815504; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_roleteamassignment_created_by_id_ef815504 ON public.dab_rbac_roleteamassignment USING btree (created_by_id);


--
-- Name: dab_rbac_roleteamassignment_object_role_id_4a315264; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_roleteamassignment_object_role_id_4a315264 ON public.dab_rbac_roleteamassignment USING btree (object_role_id);


--
-- Name: dab_rbac_roleteamassignment_role_definition_id_1a2ed43d; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_roleteamassignment_role_definition_id_1a2ed43d ON public.dab_rbac_roleteamassignment USING btree (role_definition_id);


--
-- Name: dab_rbac_roleteamassignment_team_id_af62136b; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_roleteamassignment_team_id_af62136b ON public.dab_rbac_roleteamassignment USING btree (team_id);


--
-- Name: dab_rbac_roleuserassignment_content_type_id_5e447f36; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_roleuserassignment_content_type_id_5e447f36 ON public.dab_rbac_roleuserassignment USING btree (content_type_id);


--
-- Name: dab_rbac_roleuserassignment_created_by_id_5e97e92f; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_roleuserassignment_created_by_id_5e97e92f ON public.dab_rbac_roleuserassignment USING btree (created_by_id);


--
-- Name: dab_rbac_roleuserassignment_object_role_id_47901e46; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_roleuserassignment_object_role_id_47901e46 ON public.dab_rbac_roleuserassignment USING btree (object_role_id);


--
-- Name: dab_rbac_roleuserassignment_role_definition_id_4b3cfad9; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_roleuserassignment_role_definition_id_4b3cfad9 ON public.dab_rbac_roleuserassignment USING btree (role_definition_id);


--
-- Name: dab_rbac_roleuserassignment_user_id_585ff6b3; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_rbac_roleuserassignment_user_id_585ff6b3 ON public.dab_rbac_roleuserassignment USING btree (user_id);


--
-- Name: dab_resourc_content_6d9d9c_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_resourc_content_6d9d9c_idx ON public.dab_resource_registry_resource USING btree (content_type_id, object_id);


--
-- Name: dab_resource_registry_resource_content_type_id_aaf2e6b9; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_resource_registry_resource_content_type_id_aaf2e6b9 ON public.dab_resource_registry_resource USING btree (content_type_id);


--
-- Name: dab_resource_registry_resource_object_id_7e5c7d6e; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_resource_registry_resource_object_id_7e5c7d6e ON public.dab_resource_registry_resource USING btree (object_id);


--
-- Name: dab_resource_registry_resource_object_id_7e5c7d6e_like; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_resource_registry_resource_object_id_7e5c7d6e_like ON public.dab_resource_registry_resource USING btree (object_id text_pattern_ops);


--
-- Name: dab_resource_registry_resourcetype_name_7e173bf2_like; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX dab_resource_registry_resourcetype_name_7e173bf2_like ON public.dab_resource_registry_resourcetype USING btree (name varchar_pattern_ops);


--
-- Name: django_session_expire_date_a5c62663; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX django_session_expire_date_a5c62663 ON public.django_session USING btree (expire_date);


--
-- Name: django_session_session_key_c0390e0f_like; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX django_session_session_key_c0390e0f_like ON public.django_session USING btree (session_key varchar_pattern_ops);


--
-- Name: django_site_domain_a2e37b91_like; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX django_site_domain_a2e37b91_like ON public.django_site USING btree (domain varchar_pattern_ops);


--
-- Name: host_ansible_facts_default_gin; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX host_ansible_facts_default_gin ON public.main_host USING gin (ansible_facts jsonb_path_ops);


--
-- Name: main_activitystream_actor_id_29aafc0f; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_actor_id_29aafc0f ON public.main_activitystream USING btree (actor_id);


--
-- Name: main_activitystream_ad_hoc_command_activitystream_id_870ddb01; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_ad_hoc_command_activitystream_id_870ddb01 ON public.main_activitystream_ad_hoc_command USING btree (activitystream_id);


--
-- Name: main_activitystream_ad_hoc_command_adhoccommand_id_0df7bfcd; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_ad_hoc_command_adhoccommand_id_0df7bfcd ON public.main_activitystream_ad_hoc_command USING btree (adhoccommand_id);


--
-- Name: main_activitystream_credential_activitystream_id_4be1a957; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_credential_activitystream_id_4be1a957 ON public.main_activitystream_credential USING btree (activitystream_id);


--
-- Name: main_activitystream_credential_credential_id_d5911596; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_credential_credential_id_d5911596 ON public.main_activitystream_credential USING btree (credential_id);


--
-- Name: main_activitystream_credential_type_activitystream_id_b7a4b49d; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_credential_type_activitystream_id_b7a4b49d ON public.main_activitystream_credential_type USING btree (activitystream_id);


--
-- Name: main_activitystream_credential_type_credentialtype_id_89572b10; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_credential_type_credentialtype_id_89572b10 ON public.main_activitystream_credential_type USING btree (credentialtype_id);


--
-- Name: main_activitystream_execut_activitystream_id_4938d427; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_execut_activitystream_id_4938d427 ON public.main_activitystream_execution_environment USING btree (activitystream_id);


--
-- Name: main_activitystream_execut_executionenvironment_id_b455fc65; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_execut_executionenvironment_id_b455fc65 ON public.main_activitystream_execution_environment USING btree (executionenvironment_id);


--
-- Name: main_activitystream_group_activitystream_id_94d31559; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_group_activitystream_id_94d31559 ON public.main_activitystream_group USING btree (activitystream_id);


--
-- Name: main_activitystream_group_group_id_fd48b400; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_group_group_id_fd48b400 ON public.main_activitystream_group USING btree (group_id);


--
-- Name: main_activitystream_host_activitystream_id_c4d91cb7; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_host_activitystream_id_c4d91cb7 ON public.main_activitystream_host USING btree (activitystream_id);


--
-- Name: main_activitystream_host_host_id_0e598602; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_host_host_id_0e598602 ON public.main_activitystream_host USING btree (host_id);


--
-- Name: main_activitystream_instance_activitystream_id_04ccbf32; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_instance_activitystream_id_04ccbf32 ON public.main_activitystream_instance USING btree (activitystream_id);


--
-- Name: main_activitystream_instance_group_activitystream_id_e81ef38a; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_instance_group_activitystream_id_e81ef38a ON public.main_activitystream_instance_group USING btree (activitystream_id);


--
-- Name: main_activitystream_instance_group_instancegroup_id_fca49f6c; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_instance_group_instancegroup_id_fca49f6c ON public.main_activitystream_instance_group USING btree (instancegroup_id);


--
-- Name: main_activitystream_instance_instance_id_d10eb669; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_instance_instance_id_d10eb669 ON public.main_activitystream_instance USING btree (instance_id);


--
-- Name: main_activitystream_invent_inventorysource_id_235e699a; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_invent_inventorysource_id_235e699a ON public.main_activitystream_inventory_source USING btree (inventorysource_id);


--
-- Name: main_activitystream_invent_inventoryupdate_id_817749c5; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_invent_inventoryupdate_id_817749c5 ON public.main_activitystream_inventory_update USING btree (inventoryupdate_id);


--
-- Name: main_activitystream_inventory_activitystream_id_4a1242eb; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_inventory_activitystream_id_4a1242eb ON public.main_activitystream_inventory USING btree (activitystream_id);


--
-- Name: main_activitystream_inventory_inventory_id_8daf9251; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_inventory_inventory_id_8daf9251 ON public.main_activitystream_inventory USING btree (inventory_id);


--
-- Name: main_activitystream_inventory_source_activitystream_id_d88c8423; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_inventory_source_activitystream_id_d88c8423 ON public.main_activitystream_inventory_source USING btree (activitystream_id);


--
-- Name: main_activitystream_inventory_update_activitystream_id_732f074a; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_inventory_update_activitystream_id_732f074a ON public.main_activitystream_inventory_update USING btree (activitystream_id);


--
-- Name: main_activitystream_job_activitystream_id_b1f2ab1b; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_job_activitystream_id_b1f2ab1b ON public.main_activitystream_job USING btree (activitystream_id);


--
-- Name: main_activitystream_job_job_id_aa6811b5; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_job_job_id_aa6811b5 ON public.main_activitystream_job USING btree (job_id);


--
-- Name: main_activitystream_job_template_activitystream_id_abd63b6d; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_job_template_activitystream_id_abd63b6d ON public.main_activitystream_job_template USING btree (activitystream_id);


--
-- Name: main_activitystream_job_template_jobtemplate_id_c05e0b6c; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_job_template_jobtemplate_id_c05e0b6c ON public.main_activitystream_job_template USING btree (jobtemplate_id);


--
-- Name: main_activitystream_label_activitystream_id_afd608d7; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_label_activitystream_id_afd608d7 ON public.main_activitystream_label USING btree (activitystream_id);


--
-- Name: main_activitystream_label_label_id_b33683fb; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_label_label_id_b33683fb ON public.main_activitystream_label USING btree (label_id);


--
-- Name: main_activitystream_notifi_activitystream_id_214c1789; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_notifi_activitystream_id_214c1789 ON public.main_activitystream_notification_template USING btree (activitystream_id);


--
-- Name: main_activitystream_notifi_notificationtemplate_id_96d11a5d; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_notifi_notificationtemplate_id_96d11a5d ON public.main_activitystream_notification_template USING btree (notificationtemplate_id);


--
-- Name: main_activitystream_notification_activitystream_id_7d39234a; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_notification_activitystream_id_7d39234a ON public.main_activitystream_notification USING btree (activitystream_id);


--
-- Name: main_activitystream_notification_notification_id_bbfaa8ac; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_notification_notification_id_bbfaa8ac ON public.main_activitystream_notification USING btree (notification_id);


--
-- Name: main_activitystream_organization_activitystream_id_0283e075; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_organization_activitystream_id_0283e075 ON public.main_activitystream_organization USING btree (activitystream_id);


--
-- Name: main_activitystream_organization_organization_id_8ccdfd12; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_organization_organization_id_8ccdfd12 ON public.main_activitystream_organization USING btree (organization_id);


--
-- Name: main_activitystream_project_activitystream_id_f6aa28cc; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_project_activitystream_id_f6aa28cc ON public.main_activitystream_project USING btree (activitystream_id);


--
-- Name: main_activitystream_project_project_id_836f7b93; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_project_project_id_836f7b93 ON public.main_activitystream_project USING btree (project_id);


--
-- Name: main_activitystream_project_update_activitystream_id_2965eda0; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_project_update_activitystream_id_2965eda0 ON public.main_activitystream_project_update USING btree (activitystream_id);


--
-- Name: main_activitystream_project_update_projectupdate_id_8ac4ba92; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_project_update_projectupdate_id_8ac4ba92 ON public.main_activitystream_project_update USING btree (projectupdate_id);


--
-- Name: main_activitystream_recept_receptoraddress_id_dd973082; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_recept_receptoraddress_id_dd973082 ON public.main_activitystream_receptor_address USING btree (receptoraddress_id);


--
-- Name: main_activitystream_receptor_address_activitystream_id_c13e1e5f; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_receptor_address_activitystream_id_c13e1e5f ON public.main_activitystream_receptor_address USING btree (activitystream_id);


--
-- Name: main_activitystream_role_activitystream_id_d591eb98; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_role_activitystream_id_d591eb98 ON public.main_activitystream_role USING btree (activitystream_id);


--
-- Name: main_activitystream_role_role_id_e19fce37; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_role_role_id_e19fce37 ON public.main_activitystream_role USING btree (role_id);


--
-- Name: main_activitystream_schedule_activitystream_id_a5fd87ef; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_schedule_activitystream_id_a5fd87ef ON public.main_activitystream_schedule USING btree (activitystream_id);


--
-- Name: main_activitystream_schedule_schedule_id_9bde99e8; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_schedule_schedule_id_9bde99e8 ON public.main_activitystream_schedule USING btree (schedule_id);


--
-- Name: main_activitystream_team_activitystream_id_c4874e73; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_team_activitystream_id_c4874e73 ON public.main_activitystream_team USING btree (activitystream_id);


--
-- Name: main_activitystream_team_team_id_725f033a; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_team_team_id_725f033a ON public.main_activitystream_team USING btree (team_id);


--
-- Name: main_activitystream_unifie_activitystream_id_e4ce5d15; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_unifie_activitystream_id_e4ce5d15 ON public.main_activitystream_unified_job_template USING btree (activitystream_id);


--
-- Name: main_activitystream_unifie_unifiedjobtemplate_id_71f8a21f; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_unifie_unifiedjobtemplate_id_71f8a21f ON public.main_activitystream_unified_job_template USING btree (unifiedjobtemplate_id);


--
-- Name: main_activitystream_unified_job_activitystream_id_e29d497f; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_unified_job_activitystream_id_e29d497f ON public.main_activitystream_unified_job USING btree (activitystream_id);


--
-- Name: main_activitystream_unified_job_unifiedjob_id_bd9f07c6; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_unified_job_unifiedjob_id_bd9f07c6 ON public.main_activitystream_unified_job USING btree (unifiedjob_id);


--
-- Name: main_activitystream_user_activitystream_id_f120c9d1; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_user_activitystream_id_f120c9d1 ON public.main_activitystream_user USING btree (activitystream_id);


--
-- Name: main_activitystream_user_user_id_435f8320; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_user_user_id_435f8320 ON public.main_activitystream_user USING btree (user_id);


--
-- Name: main_activitystream_workfl_activitystream_id_14401444; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_workfl_activitystream_id_14401444 ON public.main_activitystream_workflow_approval USING btree (activitystream_id);


--
-- Name: main_activitystream_workfl_activitystream_id_259ad363; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_workfl_activitystream_id_259ad363 ON public.main_activitystream_workflow_job_template USING btree (activitystream_id);


--
-- Name: main_activitystream_workfl_activitystream_id_7e8e02aa; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_workfl_activitystream_id_7e8e02aa ON public.main_activitystream_workflow_approval_template USING btree (activitystream_id);


--
-- Name: main_activitystream_workfl_activitystream_id_b3d1beb6; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_workfl_activitystream_id_b3d1beb6 ON public.main_activitystream_workflow_job_template_node USING btree (activitystream_id);


--
-- Name: main_activitystream_workfl_activitystream_id_c8397668; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_workfl_activitystream_id_c8397668 ON public.main_activitystream_workflow_job_node USING btree (activitystream_id);


--
-- Name: main_activitystream_workfl_workflowapproval_id_8d4193a7; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_workfl_workflowapproval_id_8d4193a7 ON public.main_activitystream_workflow_approval USING btree (workflowapproval_id);


--
-- Name: main_activitystream_workfl_workflowapprovaltemplate_i_93e9e097; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_workfl_workflowapprovaltemplate_i_93e9e097 ON public.main_activitystream_workflow_approval_template USING btree (workflowapprovaltemplate_id);


--
-- Name: main_activitystream_workfl_workflowjobnode_id_85bb51d6; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_workfl_workflowjobnode_id_85bb51d6 ON public.main_activitystream_workflow_job_node USING btree (workflowjobnode_id);


--
-- Name: main_activitystream_workfl_workflowjobtemplate_id_efd4c1aa; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_workfl_workflowjobtemplate_id_efd4c1aa ON public.main_activitystream_workflow_job_template USING btree (workflowjobtemplate_id);


--
-- Name: main_activitystream_workfl_workflowjobtemplatenode_id_a2630ab6; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_workfl_workflowjobtemplatenode_id_a2630ab6 ON public.main_activitystream_workflow_job_template_node USING btree (workflowjobtemplatenode_id);


--
-- Name: main_activitystream_workflow_job_activitystream_id_93d66e38; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_workflow_job_activitystream_id_93d66e38 ON public.main_activitystream_workflow_job USING btree (activitystream_id);


--
-- Name: main_activitystream_workflow_job_workflowjob_id_c29366d7; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_activitystream_workflow_job_workflowjob_id_c29366d7 ON public.main_activitystream_workflow_job USING btree (workflowjob_id);


--
-- Name: main_adhocc_ad_hoc__1e4d24_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_adhocc_ad_hoc__1e4d24_idx ON ONLY public.main_adhoccommandevent USING btree (ad_hoc_command_id, job_created, uuid);


--
-- Name: main_adhocc_ad_hoc__a57777_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_adhocc_ad_hoc__a57777_idx ON ONLY public.main_adhoccommandevent USING btree (ad_hoc_command_id, job_created, counter);


--
-- Name: main_adhocc_ad_hoc__e72142_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_adhocc_ad_hoc__e72142_idx ON ONLY public.main_adhoccommandevent USING btree (ad_hoc_command_id, job_created, event);


--
-- Name: main_adhoccommand_credential_id_da6b1c87; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_adhoccommand_credential_id_da6b1c87 ON public.main_adhoccommand USING btree (credential_id);


--
-- Name: main_adhoccommand_inventory_id_b29bba0e; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_adhoccommand_inventory_id_b29bba0e ON public.main_adhoccommand USING btree (inventory_id);


--
-- Name: main_adhoccommandevent_ad_hoc_command_id_1721f1e2; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_adhoccommandevent_ad_hoc_command_id_1721f1e2 ON public._unpartitioned_main_adhoccommandevent USING btree (ad_hoc_command_id);


--
-- Name: main_adhoccommandevent_ad_hoc_command_id_end_line_f08bd1b4_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_adhoccommandevent_ad_hoc_command_id_end_line_f08bd1b4_idx ON public._unpartitioned_main_adhoccommandevent USING btree (ad_hoc_command_id, end_line);


--
-- Name: main_adhoccommandevent_ad_hoc_command_id_event_85c463e3_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_adhoccommandevent_ad_hoc_command_id_event_85c463e3_idx ON public._unpartitioned_main_adhoccommandevent USING btree (ad_hoc_command_id, event);


--
-- Name: main_adhoccommandevent_ad_hoc_command_id_start__6e575dd7_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_adhoccommandevent_ad_hoc_command_id_start__6e575dd7_idx ON public._unpartitioned_main_adhoccommandevent USING btree (ad_hoc_command_id, start_line);


--
-- Name: main_adhoccommandevent_ad_hoc_command_id_uuid_f1fab1c8_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_adhoccommandevent_ad_hoc_command_id_uuid_f1fab1c8_idx ON public._unpartitioned_main_adhoccommandevent USING btree (ad_hoc_command_id, uuid);


--
-- Name: main_adhoccommandevent_host_id_5613e329; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_adhoccommandevent_host_id_5613e329 ON public._unpartitioned_main_adhoccommandevent USING btree (host_id);


--
-- Name: main_adhoccommandevent_host_id_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_adhoccommandevent_host_id_idx ON ONLY public.main_adhoccommandevent USING btree (host_id);


--
-- Name: main_adhoccommandevent_modified_3e4ee2db; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_adhoccommandevent_modified_3e4ee2db ON ONLY public.main_adhoccommandevent USING btree (modified);


--
-- Name: main_credential_admin_role_id_6cd7ab86; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_credential_admin_role_id_6cd7ab86 ON public.main_credential USING btree (admin_role_id);


--
-- Name: main_credential_created_by_id_237add04; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_credential_created_by_id_237add04 ON public.main_credential USING btree (created_by_id);


--
-- Name: main_credential_credential_type_id_0120654c; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_credential_credential_type_id_0120654c ON public.main_credential USING btree (credential_type_id);


--
-- Name: main_credential_modified_by_id_c290955a; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_credential_modified_by_id_c290955a ON public.main_credential USING btree (modified_by_id);


--
-- Name: main_credential_organization_id_18d4ae89; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_credential_organization_id_18d4ae89 ON public.main_credential USING btree (organization_id);


--
-- Name: main_credential_read_role_id_12be41a2; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_credential_read_role_id_12be41a2 ON public.main_credential USING btree (read_role_id);


--
-- Name: main_credential_use_role_id_122159d4; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_credential_use_role_id_122159d4 ON public.main_credential USING btree (use_role_id);


--
-- Name: main_credentialinputsource_created_by_id_d2dc637c; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_credentialinputsource_created_by_id_d2dc637c ON public.main_credentialinputsource USING btree (created_by_id);


--
-- Name: main_credentialinputsource_modified_by_id_e3fd88dd; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_credentialinputsource_modified_by_id_e3fd88dd ON public.main_credentialinputsource USING btree (modified_by_id);


--
-- Name: main_credentialinputsource_source_credential_id_868d93af; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_credentialinputsource_source_credential_id_868d93af ON public.main_credentialinputsource USING btree (source_credential_id);


--
-- Name: main_credentialinputsource_target_credential_id_4bf0e248; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_credentialinputsource_target_credential_id_4bf0e248 ON public.main_credentialinputsource USING btree (target_credential_id);


--
-- Name: main_credentialtype_created_by_id_0f8451ed; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_credentialtype_created_by_id_0f8451ed ON public.main_credentialtype USING btree (created_by_id);


--
-- Name: main_credentialtype_modified_by_id_b425580d; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_credentialtype_modified_by_id_b425580d ON public.main_credentialtype USING btree (modified_by_id);


--
-- Name: main_custominventoryscript_created_by_id_45a39526; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_custominventoryscript_created_by_id_45a39526 ON public.main_custominventoryscript USING btree (created_by_id);


--
-- Name: main_custominventoryscript_modified_by_id_6c74f1d0; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_custominventoryscript_modified_by_id_6c74f1d0 ON public.main_custominventoryscript USING btree (modified_by_id);


--
-- Name: main_executionenvironment_created_by_id_3808c16f; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_executionenvironment_created_by_id_3808c16f ON public.main_executionenvironment USING btree (created_by_id);


--
-- Name: main_executionenvironment_credential_id_e91204b4; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_executionenvironment_credential_id_e91204b4 ON public.main_executionenvironment USING btree (credential_id);


--
-- Name: main_executionenvironment_modified_by_id_fa58a43d; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_executionenvironment_modified_by_id_fa58a43d ON public.main_executionenvironment USING btree (modified_by_id);


--
-- Name: main_executionenvironment_name_c1ad78d0_like; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_executionenvironment_name_c1ad78d0_like ON public.main_executionenvironment USING btree (name varchar_pattern_ops);


--
-- Name: main_executionenvironment_organization_id_66056df5; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_executionenvironment_organization_id_66056df5 ON public.main_executionenvironment USING btree (organization_id);


--
-- Name: main_group_created_by_id_326129d5; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_group_created_by_id_326129d5 ON public.main_group USING btree (created_by_id);


--
-- Name: main_group_hosts_group_id_524c3b29; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_group_hosts_group_id_524c3b29 ON public.main_group_hosts USING btree (group_id);


--
-- Name: main_group_hosts_host_id_672eaed0; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_group_hosts_host_id_672eaed0 ON public.main_group_hosts USING btree (host_id);


--
-- Name: main_group_inventory_id_f9e83725; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_group_inventory_id_f9e83725 ON public.main_group USING btree (inventory_id);


--
-- Name: main_group_inventory_sources_group_id_1be295c4; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_group_inventory_sources_group_id_1be295c4 ON public.main_group_inventory_sources USING btree (group_id);


--
-- Name: main_group_inventory_sources_inventorysource_id_5da14efc; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_group_inventory_sources_inventorysource_id_5da14efc ON public.main_group_inventory_sources USING btree (inventorysource_id);


--
-- Name: main_group_modified_by_id_20a1b654; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_group_modified_by_id_20a1b654 ON public.main_group USING btree (modified_by_id);


--
-- Name: main_group_parents_from_group_id_9d63324d; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_group_parents_from_group_id_9d63324d ON public.main_group_parents USING btree (from_group_id);


--
-- Name: main_group_parents_to_group_id_851cc1ce; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_group_parents_to_group_id_851cc1ce ON public.main_group_parents USING btree (to_group_id);


--
-- Name: main_host_created_by_id_2b5e0abe; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_host_created_by_id_2b5e0abe ON public.main_host USING btree (created_by_id);


--
-- Name: main_host_inventory_id_e5bcdb08; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_host_inventory_id_e5bcdb08 ON public.main_host USING btree (inventory_id);


--
-- Name: main_host_inventory_sources_host_id_03f0dcdc; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_host_inventory_sources_host_id_03f0dcdc ON public.main_host_inventory_sources USING btree (host_id);


--
-- Name: main_host_inventory_sources_inventorysource_id_b25d3959; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_host_inventory_sources_inventorysource_id_b25d3959 ON public.main_host_inventory_sources USING btree (inventorysource_id);


--
-- Name: main_host_last_job_host_summary_id_b8bd727d; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_host_last_job_host_summary_id_b8bd727d ON public.main_host USING btree (last_job_host_summary_id);


--
-- Name: main_host_last_job_id_d247075b; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_host_last_job_id_d247075b ON public.main_host USING btree (last_job_id);


--
-- Name: main_host_modified_by_id_28b76283; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_host_modified_by_id_28b76283 ON public.main_host USING btree (modified_by_id);


--
-- Name: main_hostmetric_first_automation_b747a0a1; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_hostmetric_first_automation_b747a0a1 ON public.main_hostmetric USING btree (first_automation);


--
-- Name: main_hostmetric_hostname_87ac3c1f_like; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_hostmetric_hostname_87ac3c1f_like ON public.main_hostmetric USING btree (hostname varchar_pattern_ops);


--
-- Name: main_hostmetric_last_automation_11010683; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_hostmetric_last_automation_11010683 ON public.main_hostmetric USING btree (last_automation);


--
-- Name: main_hostmetric_last_deleted_d8249820; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_hostmetric_last_deleted_d8249820 ON public.main_hostmetric USING btree (last_deleted);


--
-- Name: main_instance_hostname_f2698dae_like; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_instance_hostname_f2698dae_like ON public.main_instance USING btree (hostname varchar_pattern_ops);


--
-- Name: main_instancegroup_admin_role_id_03760535; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_instancegroup_admin_role_id_03760535 ON public.main_instancegroup USING btree (admin_role_id);


--
-- Name: main_instancegroup_credential_id_98351d10; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_instancegroup_credential_id_98351d10 ON public.main_instancegroup USING btree (credential_id);


--
-- Name: main_instancegroup_instances_instance_id_d41cb05c; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_instancegroup_instances_instance_id_d41cb05c ON public.main_instancegroup_instances USING btree (instance_id);


--
-- Name: main_instancegroup_instances_instancegroup_id_b4b19635; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_instancegroup_instances_instancegroup_id_b4b19635 ON public.main_instancegroup_instances USING btree (instancegroup_id);


--
-- Name: main_instancegroup_name_bde73070_like; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_instancegroup_name_bde73070_like ON public.main_instancegroup USING btree (name varchar_pattern_ops);


--
-- Name: main_instancegroup_read_role_id_139c801e; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_instancegroup_read_role_id_139c801e ON public.main_instancegroup USING btree (read_role_id);


--
-- Name: main_instancegroup_use_role_id_48ea7ecc; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_instancegroup_use_role_id_48ea7ecc ON public.main_instancegroup USING btree (use_role_id);


--
-- Name: main_instancelink_source_id_29f35cad; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_instancelink_source_id_29f35cad ON public.main_instancelink USING btree (source_id);


--
-- Name: main_instancelink_target_id_0ee650b4; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_instancelink_target_id_0ee650b4 ON public.main_instancelink USING btree (target_id);


--
-- Name: main_invent_invento_364dcb_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_invent_invento_364dcb_idx ON ONLY public.main_inventoryupdateevent USING btree (inventory_update_id, job_created, counter);


--
-- Name: main_invent_invento_f72b21_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_invent_invento_f72b21_idx ON ONLY public.main_inventoryupdateevent USING btree (inventory_update_id, job_created, uuid);


--
-- Name: main_inventory_adhoc_role_id_b57042aa; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventory_adhoc_role_id_b57042aa ON public.main_inventory USING btree (adhoc_role_id);


--
-- Name: main_inventory_admin_role_id_3bb301cb; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventory_admin_role_id_3bb301cb ON public.main_inventory USING btree (admin_role_id);


--
-- Name: main_inventory_created_by_id_5d690781; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventory_created_by_id_5d690781 ON public.main_inventory USING btree (created_by_id);


--
-- Name: main_inventory_labels_inventory_id_3c7ecb7a; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventory_labels_inventory_id_3c7ecb7a ON public.main_inventory_labels USING btree (inventory_id);


--
-- Name: main_inventory_labels_label_id_0ab1cd80; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventory_labels_label_id_0ab1cd80 ON public.main_inventory_labels USING btree (label_id);


--
-- Name: main_inventory_modified_by_id_a4a91734; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventory_modified_by_id_a4a91734 ON public.main_inventory USING btree (modified_by_id);


--
-- Name: main_inventory_organization_id_3ee77ea9; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventory_organization_id_3ee77ea9 ON public.main_inventory USING btree (organization_id);


--
-- Name: main_inventory_read_role_id_270dd070; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventory_read_role_id_270dd070 ON public.main_inventory USING btree (read_role_id);


--
-- Name: main_inventory_update_role_id_be0903a1; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventory_update_role_id_be0903a1 ON public.main_inventory USING btree (update_role_id);


--
-- Name: main_inventory_use_role_id_77407b26; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventory_use_role_id_77407b26 ON public.main_inventory USING btree (use_role_id);


--
-- Name: main_inventoryconstructedi_constructed_inventory_id_7f494472; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventoryconstructedi_constructed_inventory_id_7f494472 ON public.main_inventoryconstructedinventorymembership USING btree (constructed_inventory_id);


--
-- Name: main_inventoryconstructedi_input_inventory_id_fc428cbb; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventoryconstructedi_input_inventory_id_fc428cbb ON public.main_inventoryconstructedinventorymembership USING btree (input_inventory_id);


--
-- Name: main_inventoryconstructedinventorymembership_position_7d2caaa0; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventoryconstructedinventorymembership_position_7d2caaa0 ON public.main_inventoryconstructedinventorymembership USING btree ("position");


--
-- Name: main_inventoryinstancegroupmembership_instancegroup_id_8c752e87; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventoryinstancegroupmembership_instancegroup_id_8c752e87 ON public.main_inventoryinstancegroupmembership USING btree (instancegroup_id);


--
-- Name: main_inventoryinstancegroupmembership_inventory_id_76a877b6; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventoryinstancegroupmembership_inventory_id_76a877b6 ON public.main_inventoryinstancegroupmembership USING btree (inventory_id);


--
-- Name: main_inventoryinstancegroupmembership_position_f7487717; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventoryinstancegroupmembership_position_f7487717 ON public.main_inventoryinstancegroupmembership USING btree ("position");


--
-- Name: main_inventorysource_inventory_id_3c1cac19; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventorysource_inventory_id_3c1cac19 ON public.main_inventorysource USING btree (inventory_id);


--
-- Name: main_inventorysource_source_project_id_5b9c4374; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventorysource_source_project_id_5b9c4374 ON public.main_inventorysource USING btree (source_project_id);


--
-- Name: main_inventoryupdate_inventory_id_e60f1f2e; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventoryupdate_inventory_id_e60f1f2e ON public.main_inventoryupdate USING btree (inventory_id);


--
-- Name: main_inventoryupdate_inventory_source_id_bc4b2567; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventoryupdate_inventory_source_id_bc4b2567 ON public.main_inventoryupdate USING btree (inventory_source_id);


--
-- Name: main_inventoryupdate_source_project_update_id_b896d555; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventoryupdate_source_project_update_id_b896d555 ON public.main_inventoryupdate USING btree (source_project_update_id);


--
-- Name: main_inventoryupdateeven_inventory_update_id_end__da3bcc42_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventoryupdateeven_inventory_update_id_end__da3bcc42_idx ON public._unpartitioned_main_inventoryupdateevent USING btree (inventory_update_id, end_line);


--
-- Name: main_inventoryupdateeven_inventory_update_id_star_ee7580ed_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventoryupdateeven_inventory_update_id_star_ee7580ed_idx ON public._unpartitioned_main_inventoryupdateevent USING btree (inventory_update_id, start_line);


--
-- Name: main_inventoryupdateevent_inventory_update_id_8974f1f7; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventoryupdateevent_inventory_update_id_8974f1f7 ON public._unpartitioned_main_inventoryupdateevent USING btree (inventory_update_id);


--
-- Name: main_inventoryupdateevent_inventory_update_id_uuid_c45a56f6_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventoryupdateevent_inventory_update_id_uuid_c45a56f6_idx ON public._unpartitioned_main_inventoryupdateevent USING btree (inventory_update_id, uuid);


--
-- Name: main_inventoryupdateevent_modified_e8e6da8b; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_inventoryupdateevent_modified_e8e6da8b ON ONLY public.main_inventoryupdateevent USING btree (modified);


--
-- Name: main_job_inventory_id_1b436658; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_job_inventory_id_1b436658 ON public.main_job USING btree (inventory_id);


--
-- Name: main_job_job_template_id_070b0d56; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_job_job_template_id_070b0d56 ON public.main_job USING btree (job_template_id);


--
-- Name: main_job_project_id_a8f63894; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_job_project_id_a8f63894 ON public.main_job USING btree (project_id);


--
-- Name: main_job_project_update_id_5adf90ad; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_job_project_update_id_5adf90ad ON public.main_job USING btree (project_update_id);


--
-- Name: main_job_webhook_credential_id_40ca94fa; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_job_webhook_credential_id_40ca94fa ON public.main_job USING btree (webhook_credential_id);


--
-- Name: main_jobeve_job_id_0ddc6b_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobeve_job_id_0ddc6b_idx ON ONLY public.main_jobevent USING btree (job_id, job_created, event);


--
-- Name: main_jobeve_job_id_3c4a4a_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobeve_job_id_3c4a4a_idx ON ONLY public.main_jobevent USING btree (job_id, job_created, uuid);


--
-- Name: main_jobeve_job_id_40a56d_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobeve_job_id_40a56d_idx ON ONLY public.main_jobevent USING btree (job_id, job_created, parent_uuid);


--
-- Name: main_jobeve_job_id_51c382_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobeve_job_id_51c382_idx ON ONLY public.main_jobevent USING btree (job_id, job_created, counter);


--
-- Name: main_jobevent_host_id_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobevent_host_id_idx ON ONLY public.main_jobevent USING btree (host_id);


--
-- Name: main_jobevent_20241219_17_host_id_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobevent_20241219_17_host_id_idx ON public.main_jobevent_20241219_17 USING btree (host_id);


--
-- Name: main_jobevent_20241219_17_job_id_job_created_counter_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobevent_20241219_17_job_id_job_created_counter_idx ON public.main_jobevent_20241219_17 USING btree (job_id, job_created, counter);


--
-- Name: main_jobevent_20241219_17_job_id_job_created_event_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobevent_20241219_17_job_id_job_created_event_idx ON public.main_jobevent_20241219_17 USING btree (job_id, job_created, event);


--
-- Name: main_jobevent_20241219_17_job_id_job_created_parent_uuid_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobevent_20241219_17_job_id_job_created_parent_uuid_idx ON public.main_jobevent_20241219_17 USING btree (job_id, job_created, parent_uuid);


--
-- Name: main_jobevent_20241219_17_job_id_job_created_uuid_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobevent_20241219_17_job_id_job_created_uuid_idx ON public.main_jobevent_20241219_17 USING btree (job_id, job_created, uuid);


--
-- Name: main_jobevent_modified_52b12bb7; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobevent_modified_52b12bb7 ON ONLY public.main_jobevent USING btree (modified);


--
-- Name: main_jobevent_20241219_17_modified_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobevent_20241219_17_modified_idx ON public.main_jobevent_20241219_17 USING btree (modified);


--
-- Name: main_jobevent_created_1976e874; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobevent_created_1976e874 ON public._unpartitioned_main_jobevent USING btree (created);


--
-- Name: main_jobevent_host_id_b03b6059; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobevent_host_id_b03b6059 ON public._unpartitioned_main_jobevent USING btree (host_id);


--
-- Name: main_jobevent_job_id_571587e8; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobevent_job_id_571587e8 ON public._unpartitioned_main_jobevent USING btree (job_id);


--
-- Name: main_jobevent_job_id_end_line_18215490_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobevent_job_id_end_line_18215490_idx ON public._unpartitioned_main_jobevent USING btree (job_id, end_line);


--
-- Name: main_jobevent_job_id_event_dc5f44fe_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobevent_job_id_event_dc5f44fe_idx ON public._unpartitioned_main_jobevent USING btree (job_id, event);


--
-- Name: main_jobevent_job_id_parent_uuid_8de74312_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobevent_job_id_parent_uuid_8de74312_idx ON public._unpartitioned_main_jobevent USING btree (job_id, parent_uuid);


--
-- Name: main_jobevent_job_id_start_line_76ab73f6_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobevent_job_id_start_line_76ab73f6_idx ON public._unpartitioned_main_jobevent USING btree (job_id, start_line);


--
-- Name: main_jobevent_job_id_uuid_3df694c5_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobevent_job_id_uuid_3df694c5_idx ON public._unpartitioned_main_jobevent USING btree (job_id, uuid);


--
-- Name: main_jobhostsummary_constructed_host_id_8ec8dc05; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobhostsummary_constructed_host_id_8ec8dc05 ON public.main_jobhostsummary USING btree (constructed_host_id);


--
-- Name: main_jobhostsummary_failed_42948cd9; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobhostsummary_failed_42948cd9 ON public.main_jobhostsummary USING btree (failed);


--
-- Name: main_jobhostsummary_host_id_7d9f6bf9; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobhostsummary_host_id_7d9f6bf9 ON public.main_jobhostsummary USING btree (host_id);


--
-- Name: main_jobhostsummary_job_id_8d60afa0; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobhostsummary_job_id_8d60afa0 ON public.main_jobhostsummary USING btree (job_id);


--
-- Name: main_joblaunchconfig_credentials_credential_id_2f5c0487; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_joblaunchconfig_credentials_credential_id_2f5c0487 ON public.main_joblaunchconfig_credentials USING btree (credential_id);


--
-- Name: main_joblaunchconfig_credentials_joblaunchconfig_id_37dc31b9; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_joblaunchconfig_credentials_joblaunchconfig_id_37dc31b9 ON public.main_joblaunchconfig_credentials USING btree (joblaunchconfig_id);


--
-- Name: main_joblaunchconfig_execution_environment_id_ddf8eeec; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_joblaunchconfig_execution_environment_id_ddf8eeec ON public.main_joblaunchconfig USING btree (execution_environment_id);


--
-- Name: main_joblaunchconfig_inventory_id_f905306d; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_joblaunchconfig_inventory_id_f905306d ON public.main_joblaunchconfig USING btree (inventory_id);


--
-- Name: main_joblaunchconfig_labels_joblaunchconfig_id_004bb969; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_joblaunchconfig_labels_joblaunchconfig_id_004bb969 ON public.main_joblaunchconfig_labels USING btree (joblaunchconfig_id);


--
-- Name: main_joblaunchconfig_labels_label_id_5a9a600e; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_joblaunchconfig_labels_label_id_5a9a600e ON public.main_joblaunchconfig_labels USING btree (label_id);


--
-- Name: main_joblaunchconfiginstan_instancegroup_id_e76ac8f9; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_joblaunchconfiginstan_instancegroup_id_e76ac8f9 ON public.main_joblaunchconfiginstancegroupmembership USING btree (instancegroup_id);


--
-- Name: main_joblaunchconfiginstan_joblaunchconfig_id_93eb971f; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_joblaunchconfiginstan_joblaunchconfig_id_93eb971f ON public.main_joblaunchconfiginstancegroupmembership USING btree (joblaunchconfig_id);


--
-- Name: main_joblaunchconfiginstancegroupmembership_position_02d8202f; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_joblaunchconfiginstancegroupmembership_position_02d8202f ON public.main_joblaunchconfiginstancegroupmembership USING btree ("position");


--
-- Name: main_jobtemplate_admin_role_id_f9dc66ce; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobtemplate_admin_role_id_f9dc66ce ON public.main_jobtemplate USING btree (admin_role_id);


--
-- Name: main_jobtemplate_execute_role_id_c2f0db2c; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobtemplate_execute_role_id_c2f0db2c ON public.main_jobtemplate USING btree (execute_role_id);


--
-- Name: main_jobtemplate_inventory_id_9b8df646; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobtemplate_inventory_id_9b8df646 ON public.main_jobtemplate USING btree (inventory_id);


--
-- Name: main_jobtemplate_project_id_36e80985; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobtemplate_project_id_36e80985 ON public.main_jobtemplate USING btree (project_id);


--
-- Name: main_jobtemplate_read_role_id_0e489c81; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobtemplate_read_role_id_0e489c81 ON public.main_jobtemplate USING btree (read_role_id);


--
-- Name: main_jobtemplate_webhook_credential_id_eff7fb4b; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_jobtemplate_webhook_credential_id_eff7fb4b ON public.main_jobtemplate USING btree (webhook_credential_id);


--
-- Name: main_label_created_by_id_201182c0; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_label_created_by_id_201182c0 ON public.main_label USING btree (created_by_id);


--
-- Name: main_label_modified_by_id_7f9aac68; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_label_modified_by_id_7f9aac68 ON public.main_label USING btree (modified_by_id);


--
-- Name: main_label_organization_id_78a1bd27; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_label_organization_id_78a1bd27 ON public.main_label USING btree (organization_id);


--
-- Name: main_notification_notification_template_id_9eed1d65; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_notification_notification_template_id_9eed1d65 ON public.main_notification USING btree (notification_template_id);


--
-- Name: main_notificationtemplate_created_by_id_1f77983a; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_notificationtemplate_created_by_id_1f77983a ON public.main_notificationtemplate USING btree (created_by_id);


--
-- Name: main_notificationtemplate_modified_by_id_83c40510; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_notificationtemplate_modified_by_id_83c40510 ON public.main_notificationtemplate USING btree (modified_by_id);


--
-- Name: main_notificationtemplate_organization_id_15933abb; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_notificationtemplate_organization_id_15933abb ON public.main_notificationtemplate USING btree (organization_id);


--
-- Name: main_organization_admin_role_id_e3ffdd41; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_admin_role_id_e3ffdd41 ON public.main_organization USING btree (admin_role_id);


--
-- Name: main_organization_approval_role_id_14c1d96f; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_approval_role_id_14c1d96f ON public.main_organization USING btree (approval_role_id);


--
-- Name: main_organization_auditor_role_id_f912df0a; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_auditor_role_id_f912df0a ON public.main_organization USING btree (auditor_role_id);


--
-- Name: main_organization_created_by_id_141da798; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_created_by_id_141da798 ON public.main_organization USING btree (created_by_id);


--
-- Name: main_organization_credential_admin_role_id_55733eb5; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_credential_admin_role_id_55733eb5 ON public.main_organization USING btree (credential_admin_role_id);


--
-- Name: main_organization_default_environment_id_1696aac2; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_default_environment_id_1696aac2 ON public.main_organization USING btree (default_environment_id);


--
-- Name: main_organization_execute_role_id_76038d3c; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_execute_role_id_76038d3c ON public.main_organization USING btree (execute_role_id);


--
-- Name: main_organization_execution_environment_admin_role_id_f2351549; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_execution_environment_admin_role_id_f2351549 ON public.main_organization USING btree (execution_environment_admin_role_id);


--
-- Name: main_organization_inventory_admin_role_id_dae5c7e2; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_inventory_admin_role_id_dae5c7e2 ON public.main_organization USING btree (inventory_admin_role_id);


--
-- Name: main_organization_job_template_admin_role_id_25a265c4; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_job_template_admin_role_id_25a265c4 ON public.main_organization USING btree (job_template_admin_role_id);


--
-- Name: main_organization_member_role_id_201ff67a; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_member_role_id_201ff67a ON public.main_organization USING btree (member_role_id);


--
-- Name: main_organization_modified_by_id_dec7a500; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_modified_by_id_dec7a500 ON public.main_organization USING btree (modified_by_id);


--
-- Name: main_organization_name_3afd4fc6_like; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_name_3afd4fc6_like ON public.main_organization USING btree (name varchar_pattern_ops);


--
-- Name: main_organization_notifica_notificationtemplate_id_1df2f173; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_notifica_notificationtemplate_id_1df2f173 ON public.main_organization_notification_templates_started USING btree (notificationtemplate_id);


--
-- Name: main_organization_notifica_notificationtemplate_id_392029b7; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_notifica_notificationtemplate_id_392029b7 ON public.main_organization_notification_templates_approvals USING btree (notificationtemplate_id);


--
-- Name: main_organization_notifica_notificationtemplate_id_4edd98c4; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_notifica_notificationtemplate_id_4edd98c4 ON public.main_organization_notification_templates_success USING btree (notificationtemplate_id);


--
-- Name: main_organization_notifica_notificationtemplate_id_7b1480c0; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_notifica_notificationtemplate_id_7b1480c0 ON public.main_organization_notification_templates_error USING btree (notificationtemplate_id);


--
-- Name: main_organization_notifica_organization_id_44a19957; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_notifica_organization_id_44a19957 ON public.main_organization_notification_templates_approvals USING btree (organization_id);


--
-- Name: main_organization_notifica_organization_id_48a058ac; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_notifica_organization_id_48a058ac ON public.main_organization_notification_templates_started USING btree (organization_id);


--
-- Name: main_organization_notifica_organization_id_94b63d49; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_notifica_organization_id_94b63d49 ON public.main_organization_notification_templates_error USING btree (organization_id);


--
-- Name: main_organization_notifica_organization_id_96635cd6; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_notifica_organization_id_96635cd6 ON public.main_organization_notification_templates_success USING btree (organization_id);


--
-- Name: main_organization_notification_admin_role_id_c36d2f0e; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_notification_admin_role_id_c36d2f0e ON public.main_organization USING btree (notification_admin_role_id);


--
-- Name: main_organization_project_admin_role_id_442cfebe; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_project_admin_role_id_442cfebe ON public.main_organization USING btree (project_admin_role_id);


--
-- Name: main_organization_read_role_id_e143c386; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_read_role_id_e143c386 ON public.main_organization USING btree (read_role_id);


--
-- Name: main_organization_workflow_admin_role_id_52011cd3; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organization_workflow_admin_role_id_52011cd3 ON public.main_organization USING btree (workflow_admin_role_id);


--
-- Name: main_organizationgalaxycre_credential_id_7b6334f3; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organizationgalaxycre_credential_id_7b6334f3 ON public.main_organizationgalaxycredentialmembership USING btree (credential_id);


--
-- Name: main_organizationgalaxycre_organization_id_0fd9495c; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organizationgalaxycre_organization_id_0fd9495c ON public.main_organizationgalaxycredentialmembership USING btree (organization_id);


--
-- Name: main_organizationgalaxycredentialmembership_position_9319aefd; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organizationgalaxycredentialmembership_position_9319aefd ON public.main_organizationgalaxycredentialmembership USING btree ("position");


--
-- Name: main_organizationinstanceg_instancegroup_id_526173a9; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organizationinstanceg_instancegroup_id_526173a9 ON public.main_organizationinstancegroupmembership USING btree (instancegroup_id);


--
-- Name: main_organizationinstanceg_organization_id_35633383; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organizationinstanceg_organization_id_35633383 ON public.main_organizationinstancegroupmembership USING btree (organization_id);


--
-- Name: main_organizationinstancegroupmembership_position_00023fb0; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_organizationinstancegroupmembership_position_00023fb0 ON public.main_organizationinstancegroupmembership USING btree ("position");


--
-- Name: main_projec_project_449bbd_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_projec_project_449bbd_idx ON ONLY public.main_projectupdateevent USING btree (project_update_id, job_created, uuid);


--
-- Name: main_projec_project_69559a_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_projec_project_69559a_idx ON ONLY public.main_projectupdateevent USING btree (project_update_id, job_created, counter);


--
-- Name: main_projec_project_c44b7c_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_projec_project_c44b7c_idx ON ONLY public.main_projectupdateevent USING btree (project_update_id, job_created, event);


--
-- Name: main_project_admin_role_id_ba0e70c7; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_project_admin_role_id_ba0e70c7 ON public.main_project USING btree (admin_role_id);


--
-- Name: main_project_credential_id_370ba2a3; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_project_credential_id_370ba2a3 ON public.main_project USING btree (credential_id);


--
-- Name: main_project_default_environment_id_01467429; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_project_default_environment_id_01467429 ON public.main_project USING btree (default_environment_id);


--
-- Name: main_project_read_role_id_39a01fd4; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_project_read_role_id_39a01fd4 ON public.main_project USING btree (read_role_id);


--
-- Name: main_project_signature_validation_credential_id_41e77a69; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_project_signature_validation_credential_id_41e77a69 ON public.main_project USING btree (signature_validation_credential_id);


--
-- Name: main_project_update_role_id_36e33c42; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_project_update_role_id_36e33c42 ON public.main_project USING btree (update_role_id);


--
-- Name: main_project_use_role_id_7b6d9148; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_project_use_role_id_7b6d9148 ON public.main_project USING btree (use_role_id);


--
-- Name: main_projectupdate_credential_id_2f7d826a; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_projectupdate_credential_id_2f7d826a ON public.main_projectupdate USING btree (credential_id);


--
-- Name: main_projectupdate_project_id_bdd73efe; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_projectupdate_project_id_bdd73efe ON public.main_projectupdate USING btree (project_id);


--
-- Name: main_projectupdateevent_modified_9b0b80e7; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_projectupdateevent_modified_9b0b80e7 ON ONLY public.main_projectupdateevent USING btree (modified);


--
-- Name: main_projectupdateevent_20241219_17_modified_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_projectupdateevent_20241219_17_modified_idx ON public.main_projectupdateevent_20241219_17 USING btree (modified);


--
-- Name: main_projectupdateevent_20241_project_update_id_job_create_idx1; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_projectupdateevent_20241_project_update_id_job_create_idx1 ON public.main_projectupdateevent_20241219_17 USING btree (project_update_id, job_created, event);


--
-- Name: main_projectupdateevent_20241_project_update_id_job_create_idx2; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_projectupdateevent_20241_project_update_id_job_create_idx2 ON public.main_projectupdateevent_20241219_17 USING btree (project_update_id, job_created, counter);


--
-- Name: main_projectupdateevent_20241_project_update_id_job_created_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_projectupdateevent_20241_project_update_id_job_created_idx ON public.main_projectupdateevent_20241219_17 USING btree (project_update_id, job_created, uuid);


--
-- Name: main_projectupdateevent_created_55746b86; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_projectupdateevent_created_55746b86 ON public._unpartitioned_main_projectupdateevent USING btree (created);


--
-- Name: main_projectupdateevent_project_update_id_9d4358b2; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_projectupdateevent_project_update_id_9d4358b2 ON public._unpartitioned_main_projectupdateevent USING btree (project_update_id);


--
-- Name: main_projectupdateevent_project_update_id_end_line_59914839_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_projectupdateevent_project_update_id_end_line_59914839_idx ON public._unpartitioned_main_projectupdateevent USING btree (project_update_id, end_line);


--
-- Name: main_projectupdateevent_project_update_id_event_d8c3c5e5_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_projectupdateevent_project_update_id_event_d8c3c5e5_idx ON public._unpartitioned_main_projectupdateevent USING btree (project_update_id, event);


--
-- Name: main_projectupdateevent_project_update_id_start__0447b41c_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_projectupdateevent_project_update_id_start__0447b41c_idx ON public._unpartitioned_main_projectupdateevent USING btree (project_update_id, start_line);


--
-- Name: main_projectupdateevent_project_update_id_uuid_c4ffd915_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_projectupdateevent_project_update_id_uuid_c4ffd915_idx ON public._unpartitioned_main_projectupdateevent USING btree (project_update_id, uuid);


--
-- Name: main_rbac_r_ancesto_22b9f0_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_rbac_r_ancesto_22b9f0_idx ON public.main_rbac_role_ancestors USING btree (ancestor_id, content_type_id, object_id);


--
-- Name: main_rbac_r_ancesto_b44606_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_rbac_r_ancesto_b44606_idx ON public.main_rbac_role_ancestors USING btree (ancestor_id, content_type_id, role_field);


--
-- Name: main_rbac_r_ancesto_c87b87_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_rbac_r_ancesto_c87b87_idx ON public.main_rbac_role_ancestors USING btree (ancestor_id, descendent_id);


--
-- Name: main_rbac_r_content_979bdd_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_rbac_r_content_979bdd_idx ON public.main_rbac_roles USING btree (content_type_id, object_id);


--
-- Name: main_rbac_role_ancestors_ancestor_id_c6aae106; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_rbac_role_ancestors_ancestor_id_c6aae106 ON public.main_rbac_role_ancestors USING btree (ancestor_id);


--
-- Name: main_rbac_role_ancestors_descendent_id_23bfc463; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_rbac_role_ancestors_descendent_id_23bfc463 ON public.main_rbac_role_ancestors USING btree (descendent_id);


--
-- Name: main_rbac_roles_content_type_id_756d6b30; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_rbac_roles_content_type_id_756d6b30 ON public.main_rbac_roles USING btree (content_type_id);


--
-- Name: main_rbac_roles_members_role_id_7318b4b7; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_rbac_roles_members_role_id_7318b4b7 ON public.main_rbac_roles_members USING btree (role_id);


--
-- Name: main_rbac_roles_members_user_id_f5e05418; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_rbac_roles_members_user_id_f5e05418 ON public.main_rbac_roles_members USING btree (user_id);


--
-- Name: main_rbac_roles_parents_from_role_id_a02db9eb; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_rbac_roles_parents_from_role_id_a02db9eb ON public.main_rbac_roles_parents USING btree (from_role_id);


--
-- Name: main_rbac_roles_parents_to_role_id_c00b5087; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_rbac_roles_parents_to_role_id_c00b5087 ON public.main_rbac_roles_parents USING btree (to_role_id);


--
-- Name: main_rbac_roles_singleton_name_3f0df1dd_like; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_rbac_roles_singleton_name_3f0df1dd_like ON public.main_rbac_roles USING btree (singleton_name text_pattern_ops);


--
-- Name: main_receptoraddress_instance_id_988e9845; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_receptoraddress_instance_id_988e9845 ON public.main_receptoraddress USING btree (instance_id);


--
-- Name: main_schedule_created_by_id_4e647be2; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_schedule_created_by_id_4e647be2 ON public.main_schedule USING btree (created_by_id);


--
-- Name: main_schedule_credentials_credential_id_ced5894e; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_schedule_credentials_credential_id_ced5894e ON public.main_schedule_credentials USING btree (credential_id);


--
-- Name: main_schedule_credentials_schedule_id_03ecad04; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_schedule_credentials_schedule_id_03ecad04 ON public.main_schedule_credentials USING btree (schedule_id);


--
-- Name: main_schedule_execution_environment_id_90eefd45; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_schedule_execution_environment_id_90eefd45 ON public.main_schedule USING btree (execution_environment_id);


--
-- Name: main_schedule_inventory_id_43b7b69d; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_schedule_inventory_id_43b7b69d ON public.main_schedule USING btree (inventory_id);


--
-- Name: main_schedule_labels_label_id_79a46df6; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_schedule_labels_label_id_79a46df6 ON public.main_schedule_labels USING btree (label_id);


--
-- Name: main_schedule_labels_schedule_id_e2f00ec6; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_schedule_labels_schedule_id_e2f00ec6 ON public.main_schedule_labels USING btree (schedule_id);


--
-- Name: main_schedule_modified_by_id_3817bc47; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_schedule_modified_by_id_3817bc47 ON public.main_schedule USING btree (modified_by_id);


--
-- Name: main_schedule_unified_job_template_id_a9d931e2; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_schedule_unified_job_template_id_a9d931e2 ON public.main_schedule USING btree (unified_job_template_id);


--
-- Name: main_scheduleinstancegroupmembership_instancegroup_id_2d5f236c; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_scheduleinstancegroupmembership_instancegroup_id_2d5f236c ON public.main_scheduleinstancegroupmembership USING btree (instancegroup_id);


--
-- Name: main_scheduleinstancegroupmembership_position_f3766917; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_scheduleinstancegroupmembership_position_f3766917 ON public.main_scheduleinstancegroupmembership USING btree ("position");


--
-- Name: main_scheduleinstancegroupmembership_schedule_id_d8eb2c41; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_scheduleinstancegroupmembership_schedule_id_d8eb2c41 ON public.main_scheduleinstancegroupmembership USING btree (schedule_id);


--
-- Name: main_smartinventorymembership_host_id_c721cb8a; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_smartinventorymembership_host_id_c721cb8a ON public.main_smartinventorymembership USING btree (host_id);


--
-- Name: main_smartinventorymembership_inventory_id_5e13df96; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_smartinventorymembership_inventory_id_5e13df96 ON public.main_smartinventorymembership USING btree (inventory_id);


--
-- Name: main_system_system__73537a_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_system_system__73537a_idx ON ONLY public.main_systemjobevent USING btree (system_job_id, job_created, counter);


--
-- Name: main_system_system__e39825_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_system_system__e39825_idx ON ONLY public.main_systemjobevent USING btree (system_job_id, job_created, uuid);


--
-- Name: main_systemjob_system_job_template_id_8bba2060; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_systemjob_system_job_template_id_8bba2060 ON public.main_systemjob USING btree (system_job_template_id);


--
-- Name: main_systemjobevent_modified_e4b3f14a; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_systemjobevent_modified_e4b3f14a ON ONLY public.main_systemjobevent USING btree (modified);


--
-- Name: main_systemjobevent_system_job_id_91bbbfc1; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_systemjobevent_system_job_id_91bbbfc1 ON public._unpartitioned_main_systemjobevent USING btree (system_job_id);


--
-- Name: main_systemjobevent_system_job_id_end_line_9bb9848e_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_systemjobevent_system_job_id_end_line_9bb9848e_idx ON public._unpartitioned_main_systemjobevent USING btree (system_job_id, end_line);


--
-- Name: main_systemjobevent_system_job_id_start_line_60445b40_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_systemjobevent_system_job_id_start_line_60445b40_idx ON public._unpartitioned_main_systemjobevent USING btree (system_job_id, start_line);


--
-- Name: main_systemjobevent_system_job_id_uuid_b25996b0_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_systemjobevent_system_job_id_uuid_b25996b0_idx ON public._unpartitioned_main_systemjobevent USING btree (system_job_id, uuid);


--
-- Name: main_team_admin_role_id_a9e09a22; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_team_admin_role_id_a9e09a22 ON public.main_team USING btree (admin_role_id);


--
-- Name: main_team_created_by_id_c370350b; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_team_created_by_id_c370350b ON public.main_team USING btree (created_by_id);


--
-- Name: main_team_member_role_id_a2f93dc9; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_team_member_role_id_a2f93dc9 ON public.main_team USING btree (member_role_id);


--
-- Name: main_team_modified_by_id_9af533cd; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_team_modified_by_id_9af533cd ON public.main_team USING btree (modified_by_id);


--
-- Name: main_team_organization_id_8b31bbc1; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_team_organization_id_8b31bbc1 ON public.main_team USING btree (organization_id);


--
-- Name: main_team_read_role_id_ea02761f; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_team_read_role_id_ea02761f ON public.main_team USING btree (read_role_id);


--
-- Name: main_unifiedjob_canceled_on_8695ca21; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_canceled_on_8695ca21 ON public.main_unifiedjob USING btree (canceled_on);


--
-- Name: main_unifiedjob_created_94704da7; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_created_94704da7 ON public.main_unifiedjob USING btree (created);


--
-- Name: main_unifiedjob_created_by_id_d2a186ab; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_created_by_id_d2a186ab ON public.main_unifiedjob USING btree (created_by_id);


--
-- Name: main_unifiedjob_credentials_credential_id_661c8f49; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_credentials_credential_id_661c8f49 ON public.main_unifiedjob_credentials USING btree (credential_id);


--
-- Name: main_unifiedjob_credentials_unifiedjob_id_4ed7ff5d; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_credentials_unifiedjob_id_4ed7ff5d ON public.main_unifiedjob_credentials USING btree (unifiedjob_id);


--
-- Name: main_unifiedjob_dependent_jobs_from_unifiedjob_id_c8d58e88; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_dependent_jobs_from_unifiedjob_id_c8d58e88 ON public.main_unifiedjob_dependent_jobs USING btree (from_unifiedjob_id);


--
-- Name: main_unifiedjob_dependent_jobs_to_unifiedjob_id_3f04cbcc; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_dependent_jobs_to_unifiedjob_id_3f04cbcc ON public.main_unifiedjob_dependent_jobs USING btree (to_unifiedjob_id);


--
-- Name: main_unifiedjob_execution_environment_id_b2eaf9c0; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_execution_environment_id_b2eaf9c0 ON public.main_unifiedjob USING btree (execution_environment_id);


--
-- Name: main_unifiedjob_finished_eccf6159; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_finished_eccf6159 ON public.main_unifiedjob USING btree (finished);


--
-- Name: main_unifiedjob_instance_group_id_f76a06e2; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_instance_group_id_f76a06e2 ON public.main_unifiedjob USING btree (instance_group_id);


--
-- Name: main_unifiedjob_labels_label_id_98814bad; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_labels_label_id_98814bad ON public.main_unifiedjob_labels USING btree (label_id);


--
-- Name: main_unifiedjob_labels_unifiedjob_id_bd008d37; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_labels_unifiedjob_id_bd008d37 ON public.main_unifiedjob_labels USING btree (unifiedjob_id);


--
-- Name: main_unifiedjob_launch_type_f97c0639; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_launch_type_f97c0639 ON public.main_unifiedjob USING btree (launch_type);


--
-- Name: main_unifiedjob_launch_type_f97c0639_like; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_launch_type_f97c0639_like ON public.main_unifiedjob USING btree (launch_type varchar_pattern_ops);


--
-- Name: main_unifiedjob_modified_by_id_14cbb9bc; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_modified_by_id_14cbb9bc ON public.main_unifiedjob USING btree (modified_by_id);


--
-- Name: main_unifiedjob_notifications_notification_id_cf3498bf; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_notifications_notification_id_cf3498bf ON public.main_unifiedjob_notifications USING btree (notification_id);


--
-- Name: main_unifiedjob_notifications_unifiedjob_id_65ab9c3c; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_notifications_unifiedjob_id_65ab9c3c ON public.main_unifiedjob_notifications USING btree (unifiedjob_id);


--
-- Name: main_unifiedjob_organization_id_cbfa01d3; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_organization_id_cbfa01d3 ON public.main_unifiedjob USING btree (organization_id);


--
-- Name: main_unifiedjob_polymorphic_ctype_id_cb46239b; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_polymorphic_ctype_id_cb46239b ON public.main_unifiedjob USING btree (polymorphic_ctype_id);


--
-- Name: main_unifiedjob_schedule_id_766ca767; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_schedule_id_766ca767 ON public.main_unifiedjob USING btree (schedule_id);


--
-- Name: main_unifiedjob_status_ea421be2; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_status_ea421be2 ON public.main_unifiedjob USING btree (status);


--
-- Name: main_unifiedjob_status_ea421be2_like; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_status_ea421be2_like ON public.main_unifiedjob USING btree (status varchar_pattern_ops);


--
-- Name: main_unifiedjob_unified_job_template_id_a398b197; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjob_unified_job_template_id_a398b197 ON public.main_unifiedjob USING btree (unified_job_template_id);


--
-- Name: main_unifiedjobtemplate_cr_unifiedjobtemplate_id_d98d7c79; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjobtemplate_cr_unifiedjobtemplate_id_d98d7c79 ON public.main_unifiedjobtemplate_credentials USING btree (unifiedjobtemplate_id);


--
-- Name: main_unifiedjobtemplate_created_by_id_1f5fadfa; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjobtemplate_created_by_id_1f5fadfa ON public.main_unifiedjobtemplate USING btree (created_by_id);


--
-- Name: main_unifiedjobtemplate_credentials_credential_id_fd216c80; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjobtemplate_credentials_credential_id_fd216c80 ON public.main_unifiedjobtemplate_credentials USING btree (credential_id);


--
-- Name: main_unifiedjobtemplate_current_job_id_8f449ab0; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjobtemplate_current_job_id_8f449ab0 ON public.main_unifiedjobtemplate USING btree (current_job_id);


--
-- Name: main_unifiedjobtemplate_execution_environment_id_bed25866; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjobtemplate_execution_environment_id_bed25866 ON public.main_unifiedjobtemplate USING btree (execution_environment_id);


--
-- Name: main_unifiedjobtemplate_labels_label_id_d6a5ee75; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjobtemplate_labels_label_id_d6a5ee75 ON public.main_unifiedjobtemplate_labels USING btree (label_id);


--
-- Name: main_unifiedjobtemplate_labels_unifiedjobtemplate_id_c9307a9a; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjobtemplate_labels_unifiedjobtemplate_id_c9307a9a ON public.main_unifiedjobtemplate_labels USING btree (unifiedjobtemplate_id);


--
-- Name: main_unifiedjobtemplate_last_job_id_7e983743; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjobtemplate_last_job_id_7e983743 ON public.main_unifiedjobtemplate USING btree (last_job_id);


--
-- Name: main_unifiedjobtemplate_modified_by_id_a8bf1de0; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjobtemplate_modified_by_id_a8bf1de0 ON public.main_unifiedjobtemplate USING btree (modified_by_id);


--
-- Name: main_unifiedjobtemplate_next_schedule_id_955ff55d; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjobtemplate_next_schedule_id_955ff55d ON public.main_unifiedjobtemplate USING btree (next_schedule_id);


--
-- Name: main_unifiedjobtemplate_no_notificationtemplate_id_9326cdf9; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjobtemplate_no_notificationtemplate_id_9326cdf9 ON public.main_unifiedjobtemplate_notification_templates_success USING btree (notificationtemplate_id);


--
-- Name: main_unifiedjobtemplate_no_notificationtemplate_id_9793a63a; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjobtemplate_no_notificationtemplate_id_9793a63a ON public.main_unifiedjobtemplate_notification_templates_started USING btree (notificationtemplate_id);


--
-- Name: main_unifiedjobtemplate_no_notificationtemplate_id_b19df8ac; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjobtemplate_no_notificationtemplate_id_b19df8ac ON public.main_unifiedjobtemplate_notification_templates_error USING btree (notificationtemplate_id);


--
-- Name: main_unifiedjobtemplate_no_unifiedjobtemplate_id_0ce91b23; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjobtemplate_no_unifiedjobtemplate_id_0ce91b23 ON public.main_unifiedjobtemplate_notification_templates_error USING btree (unifiedjobtemplate_id);


--
-- Name: main_unifiedjobtemplate_no_unifiedjobtemplate_id_3934753d; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjobtemplate_no_unifiedjobtemplate_id_3934753d ON public.main_unifiedjobtemplate_notification_templates_success USING btree (unifiedjobtemplate_id);


--
-- Name: main_unifiedjobtemplate_no_unifiedjobtemplate_id_6e21dce4; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjobtemplate_no_unifiedjobtemplate_id_6e21dce4 ON public.main_unifiedjobtemplate_notification_templates_started USING btree (unifiedjobtemplate_id);


--
-- Name: main_unifiedjobtemplate_organization_id_c63fa1a4; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjobtemplate_organization_id_c63fa1a4 ON public.main_unifiedjobtemplate USING btree (organization_id);


--
-- Name: main_unifiedjobtemplate_polymorphic_ctype_id_ce19bb25; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjobtemplate_polymorphic_ctype_id_ce19bb25 ON public.main_unifiedjobtemplate USING btree (polymorphic_ctype_id);


--
-- Name: main_unifiedjobtemplateins_instancegroup_id_656188b4; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjobtemplateins_instancegroup_id_656188b4 ON public.main_unifiedjobtemplateinstancegroupmembership USING btree (instancegroup_id);


--
-- Name: main_unifiedjobtemplateins_position_fd6edc28; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjobtemplateins_position_fd6edc28 ON public.main_unifiedjobtemplateinstancegroupmembership USING btree ("position");


--
-- Name: main_unifiedjobtemplateins_unifiedjobtemplate_id_e401e3d7; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_unifiedjobtemplateins_unifiedjobtemplate_id_e401e3d7 ON public.main_unifiedjobtemplateinstancegroupmembership USING btree (unifiedjobtemplate_id);


--
-- Name: main_usersessionmembership_session_id_fbab60a5_like; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_usersessionmembership_session_id_fbab60a5_like ON public.main_usersessionmembership USING btree (session_id varchar_pattern_ops);


--
-- Name: main_usersessionmembership_user_id_fe163c98; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_usersessionmembership_user_id_fe163c98 ON public.main_usersessionmembership USING btree (user_id);


--
-- Name: main_workfl_identif_0cc025_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workfl_identif_0cc025_idx ON public.main_workflowjobtemplatenode USING btree (identifier);


--
-- Name: main_workfl_identif_87b752_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workfl_identif_87b752_idx ON public.main_workflowjobnode USING btree (identifier, workflow_job_id);


--
-- Name: main_workfl_identif_efdfe8_idx; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workfl_identif_efdfe8_idx ON public.main_workflowjobnode USING btree (identifier);


--
-- Name: main_workflowapproval_approved_or_denied_by_id_bb3eae41; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowapproval_approved_or_denied_by_id_bb3eae41 ON public.main_workflowapproval USING btree (approved_or_denied_by_id);


--
-- Name: main_workflowapproval_workflow_approval_template_id_b87dda8a; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowapproval_workflow_approval_template_id_b87dda8a ON public.main_workflowapproval USING btree (workflow_approval_template_id);


--
-- Name: main_workflowjob_inventory_id_8c31355b; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjob_inventory_id_8c31355b ON public.main_workflowjob USING btree (inventory_id);


--
-- Name: main_workflowjob_job_template_id_cceff2a3; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjob_job_template_id_cceff2a3 ON public.main_workflowjob USING btree (job_template_id);


--
-- Name: main_workflowjob_webhook_credential_id_57c9fece; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjob_webhook_credential_id_57c9fece ON public.main_workflowjob USING btree (webhook_credential_id);


--
-- Name: main_workflowjob_workflow_job_template_id_0d9a93a0; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjob_workflow_job_template_id_0d9a93a0 ON public.main_workflowjob USING btree (workflow_job_template_id);


--
-- Name: main_workflowjobinstancegr_instancegroup_id_00dbe24d; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobinstancegr_instancegroup_id_00dbe24d ON public.main_workflowjobinstancegroupmembership USING btree (instancegroup_id);


--
-- Name: main_workflowjobinstancegr_workflowjobnode_id_e18bb569; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobinstancegr_workflowjobnode_id_e18bb569 ON public.main_workflowjobinstancegroupmembership USING btree (workflowjobnode_id);


--
-- Name: main_workflowjobinstancegroupmembership_position_d2c9b3f8; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobinstancegroupmembership_position_d2c9b3f8 ON public.main_workflowjobinstancegroupmembership USING btree ("position");


--
-- Name: main_workflowjobnode_alway_from_workflowjobnode_id_19edb9d7; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobnode_alway_from_workflowjobnode_id_19edb9d7 ON public.main_workflowjobnode_always_nodes USING btree (from_workflowjobnode_id);


--
-- Name: main_workflowjobnode_alway_to_workflowjobnode_id_0edcda07; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobnode_alway_to_workflowjobnode_id_0edcda07 ON public.main_workflowjobnode_always_nodes USING btree (to_workflowjobnode_id);


--
-- Name: main_workflowjobnode_credentials_credential_id_6de5a410; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobnode_credentials_credential_id_6de5a410 ON public.main_workflowjobnode_credentials USING btree (credential_id);


--
-- Name: main_workflowjobnode_credentials_workflowjobnode_id_31f8c02b; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobnode_credentials_workflowjobnode_id_31f8c02b ON public.main_workflowjobnode_credentials USING btree (workflowjobnode_id);


--
-- Name: main_workflowjobnode_execution_environment_id_c593ca11; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobnode_execution_environment_id_c593ca11 ON public.main_workflowjobnode USING btree (execution_environment_id);


--
-- Name: main_workflowjobnode_failu_from_workflowjobnode_id_2172a110; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobnode_failu_from_workflowjobnode_id_2172a110 ON public.main_workflowjobnode_failure_nodes USING btree (from_workflowjobnode_id);


--
-- Name: main_workflowjobnode_failu_to_workflowjobnode_id_d2e09d9c; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobnode_failu_to_workflowjobnode_id_d2e09d9c ON public.main_workflowjobnode_failure_nodes USING btree (to_workflowjobnode_id);


--
-- Name: main_workflowjobnode_inventory_id_1dac2da9; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobnode_inventory_id_1dac2da9 ON public.main_workflowjobnode USING btree (inventory_id);


--
-- Name: main_workflowjobnode_labels_label_id_0e6594a7; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobnode_labels_label_id_0e6594a7 ON public.main_workflowjobnode_labels USING btree (label_id);


--
-- Name: main_workflowjobnode_labels_workflowjobnode_id_14f419e1; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobnode_labels_workflowjobnode_id_14f419e1 ON public.main_workflowjobnode_labels USING btree (workflowjobnode_id);


--
-- Name: main_workflowjobnode_succe_from_workflowjobnode_id_e04f9991; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobnode_succe_from_workflowjobnode_id_e04f9991 ON public.main_workflowjobnode_success_nodes USING btree (from_workflowjobnode_id);


--
-- Name: main_workflowjobnode_succe_to_workflowjobnode_id_e6c8cbb4; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobnode_succe_to_workflowjobnode_id_e6c8cbb4 ON public.main_workflowjobnode_success_nodes USING btree (to_workflowjobnode_id);


--
-- Name: main_workflowjobnode_unified_job_template_id_8a30f93e; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobnode_unified_job_template_id_8a30f93e ON public.main_workflowjobnode USING btree (unified_job_template_id);


--
-- Name: main_workflowjobnode_workflow_job_id_dcd715c7; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobnode_workflow_job_id_dcd715c7 ON public.main_workflowjobnode USING btree (workflow_job_id);


--
-- Name: main_workflowjobnodebasein_instancegroup_id_4e4faca5; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobnodebasein_instancegroup_id_4e4faca5 ON public.main_workflowjobnodebaseinstancegroupmembership USING btree (instancegroup_id);


--
-- Name: main_workflowjobnodebasein_position_e440e34a; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobnodebasein_position_e440e34a ON public.main_workflowjobnodebaseinstancegroupmembership USING btree ("position");


--
-- Name: main_workflowjobnodebasein_workflowjobnode_id_47a05c0e; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobnodebasein_workflowjobnode_id_47a05c0e ON public.main_workflowjobnodebaseinstancegroupmembership USING btree (workflowjobnode_id);


--
-- Name: main_workflowjobtemplate_admin_role_id_5675a40e; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplate_admin_role_id_5675a40e ON public.main_workflowjobtemplate USING btree (admin_role_id);


--
-- Name: main_workflowjobtemplate_approval_role_id_220f0de1; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplate_approval_role_id_220f0de1 ON public.main_workflowjobtemplate USING btree (approval_role_id);


--
-- Name: main_workflowjobtemplate_execute_role_id_ad8970f4; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplate_execute_role_id_ad8970f4 ON public.main_workflowjobtemplate USING btree (execute_role_id);


--
-- Name: main_workflowjobtemplate_inventory_id_99929499; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplate_inventory_id_99929499 ON public.main_workflowjobtemplate USING btree (inventory_id);


--
-- Name: main_workflowjobtemplate_n_notificationtemplate_id_3811d35e; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplate_n_notificationtemplate_id_3811d35e ON public.main_workflowjobtemplate_notification_templates_approvals USING btree (notificationtemplate_id);


--
-- Name: main_workflowjobtemplate_n_workflowjobtemplate_id_ce7a17be; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplate_n_workflowjobtemplate_id_ce7a17be ON public.main_workflowjobtemplate_notification_templates_approvals USING btree (workflowjobtemplate_id);


--
-- Name: main_workflowjobtemplate_read_role_id_acdd95ef; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplate_read_role_id_acdd95ef ON public.main_workflowjobtemplate USING btree (read_role_id);


--
-- Name: main_workflowjobtemplate_webhook_credential_id_ced1ad89; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplate_webhook_credential_id_ced1ad89 ON public.main_workflowjobtemplate USING btree (webhook_credential_id);


--
-- Name: main_workflowjobtemplateno_from_workflowjobtemplateno_8af14c32; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplateno_from_workflowjobtemplateno_8af14c32 ON public.main_workflowjobtemplatenode_always_nodes USING btree (from_workflowjobtemplatenode_id);


--
-- Name: main_workflowjobtemplateno_from_workflowjobtemplateno_9e16f49d; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplateno_from_workflowjobtemplateno_9e16f49d ON public.main_workflowjobtemplatenode_success_nodes USING btree (from_workflowjobtemplatenode_id);


--
-- Name: main_workflowjobtemplateno_from_workflowjobtemplateno_fa405230; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplateno_from_workflowjobtemplateno_fa405230 ON public.main_workflowjobtemplatenode_failure_nodes USING btree (from_workflowjobtemplatenode_id);


--
-- Name: main_workflowjobtemplateno_instancegroup_id_0c59a80a; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplateno_instancegroup_id_0c59a80a ON public.main_workflowjobtemplatenodebaseinstancegroupmembership USING btree (instancegroup_id);


--
-- Name: main_workflowjobtemplateno_position_b6e6fca5; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplateno_position_b6e6fca5 ON public.main_workflowjobtemplatenodebaseinstancegroupmembership USING btree ("position");


--
-- Name: main_workflowjobtemplateno_to_workflowjobtemplatenode_2c1db0ae; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplateno_to_workflowjobtemplatenode_2c1db0ae ON public.main_workflowjobtemplatenode_failure_nodes USING btree (to_workflowjobtemplatenode_id);


--
-- Name: main_workflowjobtemplateno_to_workflowjobtemplatenode_6fe11708; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplateno_to_workflowjobtemplatenode_6fe11708 ON public.main_workflowjobtemplatenode_always_nodes USING btree (to_workflowjobtemplatenode_id);


--
-- Name: main_workflowjobtemplateno_to_workflowjobtemplatenode_f16ee478; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplateno_to_workflowjobtemplatenode_f16ee478 ON public.main_workflowjobtemplatenode_success_nodes USING btree (to_workflowjobtemplatenode_id);


--
-- Name: main_workflowjobtemplateno_workflowjobtemplatenode_id_b91efe86; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplateno_workflowjobtemplatenode_id_b91efe86 ON public.main_workflowjobtemplatenode_credentials USING btree (workflowjobtemplatenode_id);


--
-- Name: main_workflowjobtemplateno_workflowjobtemplatenode_id_f75998d4; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplateno_workflowjobtemplatenode_id_f75998d4 ON public.main_workflowjobtemplatenode_labels USING btree (workflowjobtemplatenode_id);


--
-- Name: main_workflowjobtemplateno_workflowjobtemplatenode_id_fa0959c5; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplateno_workflowjobtemplatenode_id_fa0959c5 ON public.main_workflowjobtemplatenodebaseinstancegroupmembership USING btree (workflowjobtemplatenode_id);


--
-- Name: main_workflowjobtemplatenode_credentials_credential_id_e621c8d1; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplatenode_credentials_credential_id_e621c8d1 ON public.main_workflowjobtemplatenode_credentials USING btree (credential_id);


--
-- Name: main_workflowjobtemplatenode_execution_environment_id_ec5bba6d; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplatenode_execution_environment_id_ec5bba6d ON public.main_workflowjobtemplatenode USING btree (execution_environment_id);


--
-- Name: main_workflowjobtemplatenode_inventory_id_2fab864f; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplatenode_inventory_id_2fab864f ON public.main_workflowjobtemplatenode USING btree (inventory_id);


--
-- Name: main_workflowjobtemplatenode_labels_label_id_b3f1a57f; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplatenode_labels_label_id_b3f1a57f ON public.main_workflowjobtemplatenode_labels USING btree (label_id);


--
-- Name: main_workflowjobtemplatenode_unified_job_template_id_98b53e6c; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplatenode_unified_job_template_id_98b53e6c ON public.main_workflowjobtemplatenode USING btree (unified_job_template_id);


--
-- Name: main_workflowjobtemplatenode_workflow_job_template_id_2fd591f0; Type: INDEX; Schema: public; Owner: awx
--

CREATE INDEX main_workflowjobtemplatenode_workflow_job_template_id_2fd591f0 ON public.main_workflowjobtemplatenode USING btree (workflow_job_template_id);


--
-- Name: unique_ip_address_not_empty; Type: INDEX; Schema: public; Owner: awx
--

CREATE UNIQUE INDEX unique_ip_address_not_empty ON public.main_instance USING btree (ip_address) WHERE (NOT ((ip_address)::text = ''::text));


--
-- Name: main_jobevent_20241219_17_host_id_idx; Type: INDEX ATTACH; Schema: public; Owner: awx
--

ALTER INDEX public.main_jobevent_host_id_idx ATTACH PARTITION public.main_jobevent_20241219_17_host_id_idx;


--
-- Name: main_jobevent_20241219_17_job_id_job_created_counter_idx; Type: INDEX ATTACH; Schema: public; Owner: awx
--

ALTER INDEX public.main_jobeve_job_id_51c382_idx ATTACH PARTITION public.main_jobevent_20241219_17_job_id_job_created_counter_idx;


--
-- Name: main_jobevent_20241219_17_job_id_job_created_event_idx; Type: INDEX ATTACH; Schema: public; Owner: awx
--

ALTER INDEX public.main_jobeve_job_id_0ddc6b_idx ATTACH PARTITION public.main_jobevent_20241219_17_job_id_job_created_event_idx;


--
-- Name: main_jobevent_20241219_17_job_id_job_created_parent_uuid_idx; Type: INDEX ATTACH; Schema: public; Owner: awx
--

ALTER INDEX public.main_jobeve_job_id_40a56d_idx ATTACH PARTITION public.main_jobevent_20241219_17_job_id_job_created_parent_uuid_idx;


--
-- Name: main_jobevent_20241219_17_job_id_job_created_uuid_idx; Type: INDEX ATTACH; Schema: public; Owner: awx
--

ALTER INDEX public.main_jobeve_job_id_3c4a4a_idx ATTACH PARTITION public.main_jobevent_20241219_17_job_id_job_created_uuid_idx;


--
-- Name: main_jobevent_20241219_17_modified_idx; Type: INDEX ATTACH; Schema: public; Owner: awx
--

ALTER INDEX public.main_jobevent_modified_52b12bb7 ATTACH PARTITION public.main_jobevent_20241219_17_modified_idx;


--
-- Name: main_jobevent_20241219_17_pkey; Type: INDEX ATTACH; Schema: public; Owner: awx
--

ALTER INDEX public.main_jobevent_pkey_new ATTACH PARTITION public.main_jobevent_20241219_17_pkey;


--
-- Name: main_projectupdateevent_20241219_17_modified_idx; Type: INDEX ATTACH; Schema: public; Owner: awx
--

ALTER INDEX public.main_projectupdateevent_modified_9b0b80e7 ATTACH PARTITION public.main_projectupdateevent_20241219_17_modified_idx;


--
-- Name: main_projectupdateevent_20241219_17_pkey; Type: INDEX ATTACH; Schema: public; Owner: awx
--

ALTER INDEX public.main_projectupdateevent_pkey_new ATTACH PARTITION public.main_projectupdateevent_20241219_17_pkey;


--
-- Name: main_projectupdateevent_20241_project_update_id_job_create_idx1; Type: INDEX ATTACH; Schema: public; Owner: awx
--

ALTER INDEX public.main_projec_project_c44b7c_idx ATTACH PARTITION public.main_projectupdateevent_20241_project_update_id_job_create_idx1;


--
-- Name: main_projectupdateevent_20241_project_update_id_job_create_idx2; Type: INDEX ATTACH; Schema: public; Owner: awx
--

ALTER INDEX public.main_projec_project_69559a_idx ATTACH PARTITION public.main_projectupdateevent_20241_project_update_id_job_create_idx2;


--
-- Name: main_projectupdateevent_20241_project_update_id_job_created_idx; Type: INDEX ATTACH; Schema: public; Owner: awx
--

ALTER INDEX public.main_projec_project_449bbd_idx ATTACH PARTITION public.main_projectupdateevent_20241_project_update_id_job_created_idx;


--
-- Name: auth_group_permissions auth_group_permissio_permission_id_84c5c92e_fk_auth_perm; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.auth_group_permissions
    ADD CONSTRAINT auth_group_permissio_permission_id_84c5c92e_fk_auth_perm FOREIGN KEY (permission_id) REFERENCES public.auth_permission(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: auth_group_permissions auth_group_permissions_group_id_b120cbf9_fk_auth_group_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.auth_group_permissions
    ADD CONSTRAINT auth_group_permissions_group_id_b120cbf9_fk_auth_group_id FOREIGN KEY (group_id) REFERENCES public.auth_group(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: auth_permission auth_permission_content_type_id_2f476e4b_fk_django_co; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.auth_permission
    ADD CONSTRAINT auth_permission_content_type_id_2f476e4b_fk_django_co FOREIGN KEY (content_type_id) REFERENCES public.django_content_type(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: auth_user_groups auth_user_groups_group_id_97559544_fk_auth_group_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.auth_user_groups
    ADD CONSTRAINT auth_user_groups_group_id_97559544_fk_auth_group_id FOREIGN KEY (group_id) REFERENCES public.auth_group(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: auth_user_groups auth_user_groups_user_id_6a12ed8b_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.auth_user_groups
    ADD CONSTRAINT auth_user_groups_user_id_6a12ed8b_fk_auth_user_id FOREIGN KEY (user_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: auth_user_user_permissions auth_user_user_permi_permission_id_1fbb5f2c_fk_auth_perm; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.auth_user_user_permissions
    ADD CONSTRAINT auth_user_user_permi_permission_id_1fbb5f2c_fk_auth_perm FOREIGN KEY (permission_id) REFERENCES public.auth_permission(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: auth_user_user_permissions auth_user_user_permissions_user_id_a95ead1b_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.auth_user_user_permissions
    ADD CONSTRAINT auth_user_user_permissions_user_id_a95ead1b_fk_auth_user_id FOREIGN KEY (user_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: conf_setting conf_setting_user_id_ce9d5138_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.conf_setting
    ADD CONSTRAINT conf_setting_user_id_ce9d5138_fk_auth_user_id FOREIGN KEY (user_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_dabpermission dab_rbac_dabpermissi_content_type_id_2dbdb964_fk_django_co; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_dabpermission
    ADD CONSTRAINT dab_rbac_dabpermissi_content_type_id_2dbdb964_fk_django_co FOREIGN KEY (content_type_id) REFERENCES public.django_content_type(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_objectrole_provides_teams dab_rbac_objectrole__objectrole_id_406b577e_fk_dab_rbac_; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_objectrole_provides_teams
    ADD CONSTRAINT dab_rbac_objectrole__objectrole_id_406b577e_fk_dab_rbac_ FOREIGN KEY (objectrole_id) REFERENCES public.dab_rbac_objectrole(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_objectrole_provides_teams dab_rbac_objectrole__team_id_5d198983_fk_main_team; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_objectrole_provides_teams
    ADD CONSTRAINT dab_rbac_objectrole__team_id_5d198983_fk_main_team FOREIGN KEY (team_id) REFERENCES public.main_team(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_objectrole dab_rbac_objectrole_content_type_id_a1bc92de_fk_django_co; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_objectrole
    ADD CONSTRAINT dab_rbac_objectrole_content_type_id_a1bc92de_fk_django_co FOREIGN KEY (content_type_id) REFERENCES public.django_content_type(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_objectrole dab_rbac_objectrole_role_definition_id_0a5a68ee_fk_dab_rbac_; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_objectrole
    ADD CONSTRAINT dab_rbac_objectrole_role_definition_id_0a5a68ee_fk_dab_rbac_ FOREIGN KEY (role_definition_id) REFERENCES public.dab_rbac_roledefinition(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_roledefinition dab_rbac_roledefinit_content_type_id_71c1ad50_fk_django_co; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roledefinition
    ADD CONSTRAINT dab_rbac_roledefinit_content_type_id_71c1ad50_fk_django_co FOREIGN KEY (content_type_id) REFERENCES public.django_content_type(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_roledefinition_permissions dab_rbac_roledefinit_dabpermission_id_4f03ecd7_fk_dab_rbac_; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roledefinition_permissions
    ADD CONSTRAINT dab_rbac_roledefinit_dabpermission_id_4f03ecd7_fk_dab_rbac_ FOREIGN KEY (dabpermission_id) REFERENCES public.dab_rbac_dabpermission(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_roledefinition_permissions dab_rbac_roledefinit_roledefinition_id_0bef5090_fk_dab_rbac_; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roledefinition_permissions
    ADD CONSTRAINT dab_rbac_roledefinit_roledefinition_id_0bef5090_fk_dab_rbac_ FOREIGN KEY (roledefinition_id) REFERENCES public.dab_rbac_roledefinition(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_roledefinition dab_rbac_roledefinition_created_by_id_42f60326_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roledefinition
    ADD CONSTRAINT dab_rbac_roledefinition_created_by_id_42f60326_fk_auth_user_id FOREIGN KEY (created_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_roledefinition dab_rbac_roledefinition_modified_by_id_eac4ebcb_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roledefinition
    ADD CONSTRAINT dab_rbac_roledefinition_modified_by_id_eac4ebcb_fk_auth_user_id FOREIGN KEY (modified_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_roleevaluationuuid dab_rbac_roleevaluat_role_id_254631b0_fk_dab_rbac_; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roleevaluationuuid
    ADD CONSTRAINT dab_rbac_roleevaluat_role_id_254631b0_fk_dab_rbac_ FOREIGN KEY (role_id) REFERENCES public.dab_rbac_objectrole(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_roleevaluation dab_rbac_roleevaluat_role_id_93254162_fk_dab_rbac_; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roleevaluation
    ADD CONSTRAINT dab_rbac_roleevaluat_role_id_93254162_fk_dab_rbac_ FOREIGN KEY (role_id) REFERENCES public.dab_rbac_objectrole(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_roleteamassignment dab_rbac_roleteamass_content_type_id_adbc7356_fk_django_co; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roleteamassignment
    ADD CONSTRAINT dab_rbac_roleteamass_content_type_id_adbc7356_fk_django_co FOREIGN KEY (content_type_id) REFERENCES public.django_content_type(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_roleteamassignment dab_rbac_roleteamass_created_by_id_ef815504_fk_auth_user; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roleteamassignment
    ADD CONSTRAINT dab_rbac_roleteamass_created_by_id_ef815504_fk_auth_user FOREIGN KEY (created_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_roleteamassignment dab_rbac_roleteamass_object_role_id_4a315264_fk_dab_rbac_; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roleteamassignment
    ADD CONSTRAINT dab_rbac_roleteamass_object_role_id_4a315264_fk_dab_rbac_ FOREIGN KEY (object_role_id) REFERENCES public.dab_rbac_objectrole(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_roleteamassignment dab_rbac_roleteamass_role_definition_id_1a2ed43d_fk_dab_rbac_; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roleteamassignment
    ADD CONSTRAINT dab_rbac_roleteamass_role_definition_id_1a2ed43d_fk_dab_rbac_ FOREIGN KEY (role_definition_id) REFERENCES public.dab_rbac_roledefinition(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_roleteamassignment dab_rbac_roleteamassignment_team_id_af62136b_fk_main_team_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roleteamassignment
    ADD CONSTRAINT dab_rbac_roleteamassignment_team_id_af62136b_fk_main_team_id FOREIGN KEY (team_id) REFERENCES public.main_team(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_roleuserassignment dab_rbac_roleuserass_content_type_id_5e447f36_fk_django_co; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roleuserassignment
    ADD CONSTRAINT dab_rbac_roleuserass_content_type_id_5e447f36_fk_django_co FOREIGN KEY (content_type_id) REFERENCES public.django_content_type(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_roleuserassignment dab_rbac_roleuserass_created_by_id_5e97e92f_fk_auth_user; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roleuserassignment
    ADD CONSTRAINT dab_rbac_roleuserass_created_by_id_5e97e92f_fk_auth_user FOREIGN KEY (created_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_roleuserassignment dab_rbac_roleuserass_object_role_id_47901e46_fk_dab_rbac_; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roleuserassignment
    ADD CONSTRAINT dab_rbac_roleuserass_object_role_id_47901e46_fk_dab_rbac_ FOREIGN KEY (object_role_id) REFERENCES public.dab_rbac_objectrole(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_roleuserassignment dab_rbac_roleuserass_role_definition_id_4b3cfad9_fk_dab_rbac_; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roleuserassignment
    ADD CONSTRAINT dab_rbac_roleuserass_role_definition_id_4b3cfad9_fk_dab_rbac_ FOREIGN KEY (role_definition_id) REFERENCES public.dab_rbac_roledefinition(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_rbac_roleuserassignment dab_rbac_roleuserassignment_user_id_585ff6b3_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_rbac_roleuserassignment
    ADD CONSTRAINT dab_rbac_roleuserassignment_user_id_585ff6b3_fk_auth_user_id FOREIGN KEY (user_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_resource_registry_resourcetype dab_resource_registr_content_type_id_6b0a29b2_fk_django_co; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_resource_registry_resourcetype
    ADD CONSTRAINT dab_resource_registr_content_type_id_6b0a29b2_fk_django_co FOREIGN KEY (content_type_id) REFERENCES public.django_content_type(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dab_resource_registry_resource dab_resource_registr_content_type_id_aaf2e6b9_fk_django_co; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.dab_resource_registry_resource
    ADD CONSTRAINT dab_resource_registr_content_type_id_aaf2e6b9_fk_django_co FOREIGN KEY (content_type_id) REFERENCES public.django_content_type(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_organization main_activitystream__activitystream_id_0283e075_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_organization
    ADD CONSTRAINT main_activitystream__activitystream_id_0283e075_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_instance main_activitystream__activitystream_id_04ccbf32_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_instance
    ADD CONSTRAINT main_activitystream__activitystream_id_04ccbf32_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_workflow_approval main_activitystream__activitystream_id_14401444_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_approval
    ADD CONSTRAINT main_activitystream__activitystream_id_14401444_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_notification_template main_activitystream__activitystream_id_214c1789_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_notification_template
    ADD CONSTRAINT main_activitystream__activitystream_id_214c1789_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_workflow_job_template main_activitystream__activitystream_id_259ad363_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_job_template
    ADD CONSTRAINT main_activitystream__activitystream_id_259ad363_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_project_update main_activitystream__activitystream_id_2965eda0_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_project_update
    ADD CONSTRAINT main_activitystream__activitystream_id_2965eda0_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_execution_environment main_activitystream__activitystream_id_4938d427_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_execution_environment
    ADD CONSTRAINT main_activitystream__activitystream_id_4938d427_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_inventory main_activitystream__activitystream_id_4a1242eb_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_inventory
    ADD CONSTRAINT main_activitystream__activitystream_id_4a1242eb_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_credential main_activitystream__activitystream_id_4be1a957_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_credential
    ADD CONSTRAINT main_activitystream__activitystream_id_4be1a957_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_inventory_update main_activitystream__activitystream_id_732f074a_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_inventory_update
    ADD CONSTRAINT main_activitystream__activitystream_id_732f074a_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_notification main_activitystream__activitystream_id_7d39234a_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_notification
    ADD CONSTRAINT main_activitystream__activitystream_id_7d39234a_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_workflow_approval_template main_activitystream__activitystream_id_7e8e02aa_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_approval_template
    ADD CONSTRAINT main_activitystream__activitystream_id_7e8e02aa_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_ad_hoc_command main_activitystream__activitystream_id_870ddb01_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_ad_hoc_command
    ADD CONSTRAINT main_activitystream__activitystream_id_870ddb01_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_workflow_job main_activitystream__activitystream_id_93d66e38_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_job
    ADD CONSTRAINT main_activitystream__activitystream_id_93d66e38_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_group main_activitystream__activitystream_id_94d31559_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_group
    ADD CONSTRAINT main_activitystream__activitystream_id_94d31559_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_schedule main_activitystream__activitystream_id_a5fd87ef_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_schedule
    ADD CONSTRAINT main_activitystream__activitystream_id_a5fd87ef_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_job_template main_activitystream__activitystream_id_abd63b6d_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_job_template
    ADD CONSTRAINT main_activitystream__activitystream_id_abd63b6d_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_label main_activitystream__activitystream_id_afd608d7_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_label
    ADD CONSTRAINT main_activitystream__activitystream_id_afd608d7_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_job main_activitystream__activitystream_id_b1f2ab1b_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_job
    ADD CONSTRAINT main_activitystream__activitystream_id_b1f2ab1b_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_workflow_job_template_node main_activitystream__activitystream_id_b3d1beb6_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_job_template_node
    ADD CONSTRAINT main_activitystream__activitystream_id_b3d1beb6_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_credential_type main_activitystream__activitystream_id_b7a4b49d_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_credential_type
    ADD CONSTRAINT main_activitystream__activitystream_id_b7a4b49d_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_receptor_address main_activitystream__activitystream_id_c13e1e5f_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_receptor_address
    ADD CONSTRAINT main_activitystream__activitystream_id_c13e1e5f_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_team main_activitystream__activitystream_id_c4874e73_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_team
    ADD CONSTRAINT main_activitystream__activitystream_id_c4874e73_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_host main_activitystream__activitystream_id_c4d91cb7_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_host
    ADD CONSTRAINT main_activitystream__activitystream_id_c4d91cb7_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_workflow_job_node main_activitystream__activitystream_id_c8397668_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_job_node
    ADD CONSTRAINT main_activitystream__activitystream_id_c8397668_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_role main_activitystream__activitystream_id_d591eb98_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_role
    ADD CONSTRAINT main_activitystream__activitystream_id_d591eb98_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_inventory_source main_activitystream__activitystream_id_d88c8423_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_inventory_source
    ADD CONSTRAINT main_activitystream__activitystream_id_d88c8423_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_unified_job main_activitystream__activitystream_id_e29d497f_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_unified_job
    ADD CONSTRAINT main_activitystream__activitystream_id_e29d497f_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_unified_job_template main_activitystream__activitystream_id_e4ce5d15_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_unified_job_template
    ADD CONSTRAINT main_activitystream__activitystream_id_e4ce5d15_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_instance_group main_activitystream__activitystream_id_e81ef38a_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_instance_group
    ADD CONSTRAINT main_activitystream__activitystream_id_e81ef38a_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_user main_activitystream__activitystream_id_f120c9d1_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_user
    ADD CONSTRAINT main_activitystream__activitystream_id_f120c9d1_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_project main_activitystream__activitystream_id_f6aa28cc_fk_main_acti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_project
    ADD CONSTRAINT main_activitystream__activitystream_id_f6aa28cc_fk_main_acti FOREIGN KEY (activitystream_id) REFERENCES public.main_activitystream(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_ad_hoc_command main_activitystream__adhoccommand_id_0df7bfcd_fk_main_adho; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_ad_hoc_command
    ADD CONSTRAINT main_activitystream__adhoccommand_id_0df7bfcd_fk_main_adho FOREIGN KEY (adhoccommand_id) REFERENCES public.main_adhoccommand(unifiedjob_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_credential main_activitystream__credential_id_d5911596_fk_main_cred; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_credential
    ADD CONSTRAINT main_activitystream__credential_id_d5911596_fk_main_cred FOREIGN KEY (credential_id) REFERENCES public.main_credential(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_credential_type main_activitystream__credentialtype_id_89572b10_fk_main_cred; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_credential_type
    ADD CONSTRAINT main_activitystream__credentialtype_id_89572b10_fk_main_cred FOREIGN KEY (credentialtype_id) REFERENCES public.main_credentialtype(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_execution_environment main_activitystream__executionenvironment_b455fc65_fk_main_exec; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_execution_environment
    ADD CONSTRAINT main_activitystream__executionenvironment_b455fc65_fk_main_exec FOREIGN KEY (executionenvironment_id) REFERENCES public.main_executionenvironment(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_instance main_activitystream__instance_id_d10eb669_fk_main_inst; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_instance
    ADD CONSTRAINT main_activitystream__instance_id_d10eb669_fk_main_inst FOREIGN KEY (instance_id) REFERENCES public.main_instance(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_instance_group main_activitystream__instancegroup_id_fca49f6c_fk_main_inst; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_instance_group
    ADD CONSTRAINT main_activitystream__instancegroup_id_fca49f6c_fk_main_inst FOREIGN KEY (instancegroup_id) REFERENCES public.main_instancegroup(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_inventory main_activitystream__inventory_id_8daf9251_fk_main_inve; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_inventory
    ADD CONSTRAINT main_activitystream__inventory_id_8daf9251_fk_main_inve FOREIGN KEY (inventory_id) REFERENCES public.main_inventory(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_inventory_source main_activitystream__inventorysource_id_235e699a_fk_main_inve; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_inventory_source
    ADD CONSTRAINT main_activitystream__inventorysource_id_235e699a_fk_main_inve FOREIGN KEY (inventorysource_id) REFERENCES public.main_inventorysource(unifiedjobtemplate_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_inventory_update main_activitystream__inventoryupdate_id_817749c5_fk_main_inve; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_inventory_update
    ADD CONSTRAINT main_activitystream__inventoryupdate_id_817749c5_fk_main_inve FOREIGN KEY (inventoryupdate_id) REFERENCES public.main_inventoryupdate(unifiedjob_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_job main_activitystream__job_id_aa6811b5_fk_main_job_; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_job
    ADD CONSTRAINT main_activitystream__job_id_aa6811b5_fk_main_job_ FOREIGN KEY (job_id) REFERENCES public.main_job(unifiedjob_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_job_template main_activitystream__jobtemplate_id_c05e0b6c_fk_main_jobt; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_job_template
    ADD CONSTRAINT main_activitystream__jobtemplate_id_c05e0b6c_fk_main_jobt FOREIGN KEY (jobtemplate_id) REFERENCES public.main_jobtemplate(unifiedjobtemplate_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_notification main_activitystream__notification_id_bbfaa8ac_fk_main_noti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_notification
    ADD CONSTRAINT main_activitystream__notification_id_bbfaa8ac_fk_main_noti FOREIGN KEY (notification_id) REFERENCES public.main_notification(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_notification_template main_activitystream__notificationtemplate_96d11a5d_fk_main_noti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_notification_template
    ADD CONSTRAINT main_activitystream__notificationtemplate_96d11a5d_fk_main_noti FOREIGN KEY (notificationtemplate_id) REFERENCES public.main_notificationtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_organization main_activitystream__organization_id_8ccdfd12_fk_main_orga; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_organization
    ADD CONSTRAINT main_activitystream__organization_id_8ccdfd12_fk_main_orga FOREIGN KEY (organization_id) REFERENCES public.main_organization(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_project main_activitystream__project_id_836f7b93_fk_main_proj; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_project
    ADD CONSTRAINT main_activitystream__project_id_836f7b93_fk_main_proj FOREIGN KEY (project_id) REFERENCES public.main_project(unifiedjobtemplate_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_project_update main_activitystream__projectupdate_id_8ac4ba92_fk_main_proj; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_project_update
    ADD CONSTRAINT main_activitystream__projectupdate_id_8ac4ba92_fk_main_proj FOREIGN KEY (projectupdate_id) REFERENCES public.main_projectupdate(unifiedjob_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_receptor_address main_activitystream__receptoraddress_id_dd973082_fk_main_rece; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_receptor_address
    ADD CONSTRAINT main_activitystream__receptoraddress_id_dd973082_fk_main_rece FOREIGN KEY (receptoraddress_id) REFERENCES public.main_receptoraddress(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_schedule main_activitystream__schedule_id_9bde99e8_fk_main_sche; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_schedule
    ADD CONSTRAINT main_activitystream__schedule_id_9bde99e8_fk_main_sche FOREIGN KEY (schedule_id) REFERENCES public.main_schedule(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_unified_job main_activitystream__unifiedjob_id_bd9f07c6_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_unified_job
    ADD CONSTRAINT main_activitystream__unifiedjob_id_bd9f07c6_fk_main_unif FOREIGN KEY (unifiedjob_id) REFERENCES public.main_unifiedjob(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_unified_job_template main_activitystream__unifiedjobtemplate_i_71f8a21f_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_unified_job_template
    ADD CONSTRAINT main_activitystream__unifiedjobtemplate_i_71f8a21f_fk_main_unif FOREIGN KEY (unifiedjobtemplate_id) REFERENCES public.main_unifiedjobtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_workflow_approval main_activitystream__workflowapproval_id_8d4193a7_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_approval
    ADD CONSTRAINT main_activitystream__workflowapproval_id_8d4193a7_fk_main_work FOREIGN KEY (workflowapproval_id) REFERENCES public.main_workflowapproval(unifiedjob_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_workflow_approval_template main_activitystream__workflowapprovaltemp_93e9e097_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_approval_template
    ADD CONSTRAINT main_activitystream__workflowapprovaltemp_93e9e097_fk_main_work FOREIGN KEY (workflowapprovaltemplate_id) REFERENCES public.main_workflowapprovaltemplate(unifiedjobtemplate_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_workflow_job main_activitystream__workflowjob_id_c29366d7_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_job
    ADD CONSTRAINT main_activitystream__workflowjob_id_c29366d7_fk_main_work FOREIGN KEY (workflowjob_id) REFERENCES public.main_workflowjob(unifiedjob_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_workflow_job_node main_activitystream__workflowjobnode_id_85bb51d6_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_job_node
    ADD CONSTRAINT main_activitystream__workflowjobnode_id_85bb51d6_fk_main_work FOREIGN KEY (workflowjobnode_id) REFERENCES public.main_workflowjobnode(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_workflow_job_template main_activitystream__workflowjobtemplate__efd4c1aa_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_job_template
    ADD CONSTRAINT main_activitystream__workflowjobtemplate__efd4c1aa_fk_main_work FOREIGN KEY (workflowjobtemplate_id) REFERENCES public.main_workflowjobtemplate(unifiedjobtemplate_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_workflow_job_template_node main_activitystream__workflowjobtemplaten_a2630ab6_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_workflow_job_template_node
    ADD CONSTRAINT main_activitystream__workflowjobtemplaten_a2630ab6_fk_main_work FOREIGN KEY (workflowjobtemplatenode_id) REFERENCES public.main_workflowjobtemplatenode(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream main_activitystream_actor_id_29aafc0f_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream
    ADD CONSTRAINT main_activitystream_actor_id_29aafc0f_fk_auth_user_id FOREIGN KEY (actor_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_group main_activitystream_group_group_id_fd48b400_fk_main_group_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_group
    ADD CONSTRAINT main_activitystream_group_group_id_fd48b400_fk_main_group_id FOREIGN KEY (group_id) REFERENCES public.main_group(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_host main_activitystream_host_host_id_0e598602_fk_main_host_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_host
    ADD CONSTRAINT main_activitystream_host_host_id_0e598602_fk_main_host_id FOREIGN KEY (host_id) REFERENCES public.main_host(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_label main_activitystream_label_label_id_b33683fb_fk_main_label_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_label
    ADD CONSTRAINT main_activitystream_label_label_id_b33683fb_fk_main_label_id FOREIGN KEY (label_id) REFERENCES public.main_label(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_role main_activitystream_role_role_id_e19fce37_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_role
    ADD CONSTRAINT main_activitystream_role_role_id_e19fce37_fk_main_rbac_roles_id FOREIGN KEY (role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_team main_activitystream_team_team_id_725f033a_fk_main_team_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_team
    ADD CONSTRAINT main_activitystream_team_team_id_725f033a_fk_main_team_id FOREIGN KEY (team_id) REFERENCES public.main_team(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_activitystream_user main_activitystream_user_user_id_435f8320_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_activitystream_user
    ADD CONSTRAINT main_activitystream_user_user_id_435f8320_fk_auth_user_id FOREIGN KEY (user_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_adhoccommand main_adhoccommand_credential_id_da6b1c87_fk_main_credential_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_adhoccommand
    ADD CONSTRAINT main_adhoccommand_credential_id_da6b1c87_fk_main_credential_id FOREIGN KEY (credential_id) REFERENCES public.main_credential(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_adhoccommand main_adhoccommand_inventory_id_b29bba0e_fk_main_inventory_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_adhoccommand
    ADD CONSTRAINT main_adhoccommand_inventory_id_b29bba0e_fk_main_inventory_id FOREIGN KEY (inventory_id) REFERENCES public.main_inventory(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_adhoccommand main_adhoccommand_unifiedjob_ptr_id_ef80f1dd_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_adhoccommand
    ADD CONSTRAINT main_adhoccommand_unifiedjob_ptr_id_ef80f1dd_fk_main_unif FOREIGN KEY (unifiedjob_ptr_id) REFERENCES public.main_unifiedjob(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: _unpartitioned_main_adhoccommandevent main_adhoccommandeve_ad_hoc_command_id_1721f1e2_fk_main_adho; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public._unpartitioned_main_adhoccommandevent
    ADD CONSTRAINT main_adhoccommandeve_ad_hoc_command_id_1721f1e2_fk_main_adho FOREIGN KEY (ad_hoc_command_id) REFERENCES public.main_adhoccommand(unifiedjob_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: _unpartitioned_main_adhoccommandevent main_adhoccommandevent_host_id_5613e329_fk_main_host_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public._unpartitioned_main_adhoccommandevent
    ADD CONSTRAINT main_adhoccommandevent_host_id_5613e329_fk_main_host_id FOREIGN KEY (host_id) REFERENCES public.main_host(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_credential main_credential_admin_role_id_6cd7ab86_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_credential
    ADD CONSTRAINT main_credential_admin_role_id_6cd7ab86_fk_main_rbac_roles_id FOREIGN KEY (admin_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_credential main_credential_created_by_id_237add04_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_credential
    ADD CONSTRAINT main_credential_created_by_id_237add04_fk_auth_user_id FOREIGN KEY (created_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_credential main_credential_credential_type_id_0120654c_fk_main_cred; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_credential
    ADD CONSTRAINT main_credential_credential_type_id_0120654c_fk_main_cred FOREIGN KEY (credential_type_id) REFERENCES public.main_credentialtype(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_credential main_credential_modified_by_id_c290955a_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_credential
    ADD CONSTRAINT main_credential_modified_by_id_c290955a_fk_auth_user_id FOREIGN KEY (modified_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_credential main_credential_organization_id_18d4ae89_fk_main_orga; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_credential
    ADD CONSTRAINT main_credential_organization_id_18d4ae89_fk_main_orga FOREIGN KEY (organization_id) REFERENCES public.main_organization(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_credential main_credential_read_role_id_12be41a2_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_credential
    ADD CONSTRAINT main_credential_read_role_id_12be41a2_fk_main_rbac_roles_id FOREIGN KEY (read_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_credential main_credential_use_role_id_122159d4_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_credential
    ADD CONSTRAINT main_credential_use_role_id_122159d4_fk_main_rbac_roles_id FOREIGN KEY (use_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_credentialinputsource main_credentialinput_created_by_id_d2dc637c_fk_auth_user; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_credentialinputsource
    ADD CONSTRAINT main_credentialinput_created_by_id_d2dc637c_fk_auth_user FOREIGN KEY (created_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_credentialinputsource main_credentialinput_modified_by_id_e3fd88dd_fk_auth_user; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_credentialinputsource
    ADD CONSTRAINT main_credentialinput_modified_by_id_e3fd88dd_fk_auth_user FOREIGN KEY (modified_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_credentialinputsource main_credentialinput_source_credential_id_868d93af_fk_main_cred; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_credentialinputsource
    ADD CONSTRAINT main_credentialinput_source_credential_id_868d93af_fk_main_cred FOREIGN KEY (source_credential_id) REFERENCES public.main_credential(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_credentialinputsource main_credentialinput_target_credential_id_4bf0e248_fk_main_cred; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_credentialinputsource
    ADD CONSTRAINT main_credentialinput_target_credential_id_4bf0e248_fk_main_cred FOREIGN KEY (target_credential_id) REFERENCES public.main_credential(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_credentialtype main_credentialtype_created_by_id_0f8451ed_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_credentialtype
    ADD CONSTRAINT main_credentialtype_created_by_id_0f8451ed_fk_auth_user_id FOREIGN KEY (created_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_credentialtype main_credentialtype_modified_by_id_b425580d_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_credentialtype
    ADD CONSTRAINT main_credentialtype_modified_by_id_b425580d_fk_auth_user_id FOREIGN KEY (modified_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_custominventoryscript main_custominventory_created_by_id_45a39526_fk_auth_user; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_custominventoryscript
    ADD CONSTRAINT main_custominventory_created_by_id_45a39526_fk_auth_user FOREIGN KEY (created_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_custominventoryscript main_custominventory_modified_by_id_6c74f1d0_fk_auth_user; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_custominventoryscript
    ADD CONSTRAINT main_custominventory_modified_by_id_6c74f1d0_fk_auth_user FOREIGN KEY (modified_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_executionenvironment main_executionenviro_created_by_id_3808c16f_fk_auth_user; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_executionenvironment
    ADD CONSTRAINT main_executionenviro_created_by_id_3808c16f_fk_auth_user FOREIGN KEY (created_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_executionenvironment main_executionenviro_credential_id_e91204b4_fk_main_cred; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_executionenvironment
    ADD CONSTRAINT main_executionenviro_credential_id_e91204b4_fk_main_cred FOREIGN KEY (credential_id) REFERENCES public.main_credential(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_executionenvironment main_executionenviro_modified_by_id_fa58a43d_fk_auth_user; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_executionenvironment
    ADD CONSTRAINT main_executionenviro_modified_by_id_fa58a43d_fk_auth_user FOREIGN KEY (modified_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_executionenvironment main_executionenviro_organization_id_66056df5_fk_main_orga; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_executionenvironment
    ADD CONSTRAINT main_executionenviro_organization_id_66056df5_fk_main_orga FOREIGN KEY (organization_id) REFERENCES public.main_organization(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_group main_group_created_by_id_326129d5_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_group
    ADD CONSTRAINT main_group_created_by_id_326129d5_fk_auth_user_id FOREIGN KEY (created_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_group_hosts main_group_hosts_group_id_524c3b29_fk_main_group_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_group_hosts
    ADD CONSTRAINT main_group_hosts_group_id_524c3b29_fk_main_group_id FOREIGN KEY (group_id) REFERENCES public.main_group(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_group_hosts main_group_hosts_host_id_672eaed0_fk_main_host_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_group_hosts
    ADD CONSTRAINT main_group_hosts_host_id_672eaed0_fk_main_host_id FOREIGN KEY (host_id) REFERENCES public.main_host(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_group main_group_inventory_id_f9e83725_fk_main_inventory_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_group
    ADD CONSTRAINT main_group_inventory_id_f9e83725_fk_main_inventory_id FOREIGN KEY (inventory_id) REFERENCES public.main_inventory(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_group_inventory_sources main_group_inventory_inventorysource_id_5da14efc_fk_main_inve; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_group_inventory_sources
    ADD CONSTRAINT main_group_inventory_inventorysource_id_5da14efc_fk_main_inve FOREIGN KEY (inventorysource_id) REFERENCES public.main_inventorysource(unifiedjobtemplate_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_group_inventory_sources main_group_inventory_sources_group_id_1be295c4_fk_main_group_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_group_inventory_sources
    ADD CONSTRAINT main_group_inventory_sources_group_id_1be295c4_fk_main_group_id FOREIGN KEY (group_id) REFERENCES public.main_group(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_group main_group_modified_by_id_20a1b654_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_group
    ADD CONSTRAINT main_group_modified_by_id_20a1b654_fk_auth_user_id FOREIGN KEY (modified_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_group_parents main_group_parents_from_group_id_9d63324d_fk_main_group_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_group_parents
    ADD CONSTRAINT main_group_parents_from_group_id_9d63324d_fk_main_group_id FOREIGN KEY (from_group_id) REFERENCES public.main_group(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_group_parents main_group_parents_to_group_id_851cc1ce_fk_main_group_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_group_parents
    ADD CONSTRAINT main_group_parents_to_group_id_851cc1ce_fk_main_group_id FOREIGN KEY (to_group_id) REFERENCES public.main_group(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_host main_host_created_by_id_2b5e0abe_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_host
    ADD CONSTRAINT main_host_created_by_id_2b5e0abe_fk_auth_user_id FOREIGN KEY (created_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_host_inventory_sources main_host_inventory__inventorysource_id_b25d3959_fk_main_inve; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_host_inventory_sources
    ADD CONSTRAINT main_host_inventory__inventorysource_id_b25d3959_fk_main_inve FOREIGN KEY (inventorysource_id) REFERENCES public.main_inventorysource(unifiedjobtemplate_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_host main_host_inventory_id_e5bcdb08_fk_main_inventory_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_host
    ADD CONSTRAINT main_host_inventory_id_e5bcdb08_fk_main_inventory_id FOREIGN KEY (inventory_id) REFERENCES public.main_inventory(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_host_inventory_sources main_host_inventory_sources_host_id_03f0dcdc_fk_main_host_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_host_inventory_sources
    ADD CONSTRAINT main_host_inventory_sources_host_id_03f0dcdc_fk_main_host_id FOREIGN KEY (host_id) REFERENCES public.main_host(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_host main_host_last_job_host_summar_b8bd727d_fk_main_jobh; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_host
    ADD CONSTRAINT main_host_last_job_host_summar_b8bd727d_fk_main_jobh FOREIGN KEY (last_job_host_summary_id) REFERENCES public.main_jobhostsummary(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_host main_host_last_job_id_d247075b_fk_main_job_unifiedjob_ptr_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_host
    ADD CONSTRAINT main_host_last_job_id_d247075b_fk_main_job_unifiedjob_ptr_id FOREIGN KEY (last_job_id) REFERENCES public.main_job(unifiedjob_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_host main_host_modified_by_id_28b76283_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_host
    ADD CONSTRAINT main_host_modified_by_id_28b76283_fk_auth_user_id FOREIGN KEY (modified_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_instancegroup main_instancegroup_admin_role_id_03760535_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_instancegroup
    ADD CONSTRAINT main_instancegroup_admin_role_id_03760535_fk_main_rbac_roles_id FOREIGN KEY (admin_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_instancegroup main_instancegroup_credential_id_98351d10_fk_main_credential_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_instancegroup
    ADD CONSTRAINT main_instancegroup_credential_id_98351d10_fk_main_credential_id FOREIGN KEY (credential_id) REFERENCES public.main_credential(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_instancegroup_instances main_instancegroup_i_instance_id_d41cb05c_fk_main_inst; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_instancegroup_instances
    ADD CONSTRAINT main_instancegroup_i_instance_id_d41cb05c_fk_main_inst FOREIGN KEY (instance_id) REFERENCES public.main_instance(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_instancegroup_instances main_instancegroup_i_instancegroup_id_b4b19635_fk_main_inst; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_instancegroup_instances
    ADD CONSTRAINT main_instancegroup_i_instancegroup_id_b4b19635_fk_main_inst FOREIGN KEY (instancegroup_id) REFERENCES public.main_instancegroup(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_instancegroup main_instancegroup_read_role_id_139c801e_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_instancegroup
    ADD CONSTRAINT main_instancegroup_read_role_id_139c801e_fk_main_rbac_roles_id FOREIGN KEY (read_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_instancegroup main_instancegroup_use_role_id_48ea7ecc_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_instancegroup
    ADD CONSTRAINT main_instancegroup_use_role_id_48ea7ecc_fk_main_rbac_roles_id FOREIGN KEY (use_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_instancelink main_instancelink_source_id_29f35cad_fk_main_instance_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_instancelink
    ADD CONSTRAINT main_instancelink_source_id_29f35cad_fk_main_instance_id FOREIGN KEY (source_id) REFERENCES public.main_instance(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_instancelink main_instancelink_target_id_0ee650b4_fk_main_receptoraddress_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_instancelink
    ADD CONSTRAINT main_instancelink_target_id_0ee650b4_fk_main_receptoraddress_id FOREIGN KEY (target_id) REFERENCES public.main_receptoraddress(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_inventory main_inventory_adhoc_role_id_b57042aa_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventory
    ADD CONSTRAINT main_inventory_adhoc_role_id_b57042aa_fk_main_rbac_roles_id FOREIGN KEY (adhoc_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_inventory main_inventory_admin_role_id_3bb301cb_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventory
    ADD CONSTRAINT main_inventory_admin_role_id_3bb301cb_fk_main_rbac_roles_id FOREIGN KEY (admin_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_inventory main_inventory_created_by_id_5d690781_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventory
    ADD CONSTRAINT main_inventory_created_by_id_5d690781_fk_auth_user_id FOREIGN KEY (created_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_inventory_labels main_inventory_label_inventory_id_3c7ecb7a_fk_main_inve; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventory_labels
    ADD CONSTRAINT main_inventory_label_inventory_id_3c7ecb7a_fk_main_inve FOREIGN KEY (inventory_id) REFERENCES public.main_inventory(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_inventory_labels main_inventory_labels_label_id_0ab1cd80_fk_main_label_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventory_labels
    ADD CONSTRAINT main_inventory_labels_label_id_0ab1cd80_fk_main_label_id FOREIGN KEY (label_id) REFERENCES public.main_label(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_inventory main_inventory_modified_by_id_a4a91734_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventory
    ADD CONSTRAINT main_inventory_modified_by_id_a4a91734_fk_auth_user_id FOREIGN KEY (modified_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_inventory main_inventory_organization_id_3ee77ea9_fk_main_organization_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventory
    ADD CONSTRAINT main_inventory_organization_id_3ee77ea9_fk_main_organization_id FOREIGN KEY (organization_id) REFERENCES public.main_organization(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_inventory main_inventory_read_role_id_270dd070_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventory
    ADD CONSTRAINT main_inventory_read_role_id_270dd070_fk_main_rbac_roles_id FOREIGN KEY (read_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_inventory main_inventory_update_role_id_be0903a1_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventory
    ADD CONSTRAINT main_inventory_update_role_id_be0903a1_fk_main_rbac_roles_id FOREIGN KEY (update_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_inventory main_inventory_use_role_id_77407b26_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventory
    ADD CONSTRAINT main_inventory_use_role_id_77407b26_fk_main_rbac_roles_id FOREIGN KEY (use_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_inventoryconstructedinventorymembership main_inventoryconstr_constructed_inventor_7f494472_fk_main_inve; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventoryconstructedinventorymembership
    ADD CONSTRAINT main_inventoryconstr_constructed_inventor_7f494472_fk_main_inve FOREIGN KEY (constructed_inventory_id) REFERENCES public.main_inventory(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_inventoryconstructedinventorymembership main_inventoryconstr_input_inventory_id_fc428cbb_fk_main_inve; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventoryconstructedinventorymembership
    ADD CONSTRAINT main_inventoryconstr_input_inventory_id_fc428cbb_fk_main_inve FOREIGN KEY (input_inventory_id) REFERENCES public.main_inventory(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_inventoryinstancegroupmembership main_inventoryinstan_instancegroup_id_8c752e87_fk_main_inst; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventoryinstancegroupmembership
    ADD CONSTRAINT main_inventoryinstan_instancegroup_id_8c752e87_fk_main_inst FOREIGN KEY (instancegroup_id) REFERENCES public.main_instancegroup(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_inventoryinstancegroupmembership main_inventoryinstan_inventory_id_76a877b6_fk_main_inve; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventoryinstancegroupmembership
    ADD CONSTRAINT main_inventoryinstan_inventory_id_76a877b6_fk_main_inve FOREIGN KEY (inventory_id) REFERENCES public.main_inventory(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_inventorysource main_inventorysource_inventory_id_3c1cac19_fk_main_inventory_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventorysource
    ADD CONSTRAINT main_inventorysource_inventory_id_3c1cac19_fk_main_inventory_id FOREIGN KEY (inventory_id) REFERENCES public.main_inventory(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_inventorysource main_inventorysource_source_project_id_5b9c4374_fk_main_proj; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventorysource
    ADD CONSTRAINT main_inventorysource_source_project_id_5b9c4374_fk_main_proj FOREIGN KEY (source_project_id) REFERENCES public.main_project(unifiedjobtemplate_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_inventorysource main_inventorysource_unifiedjobtemplate_p_6a11d509_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventorysource
    ADD CONSTRAINT main_inventorysource_unifiedjobtemplate_p_6a11d509_fk_main_unif FOREIGN KEY (unifiedjobtemplate_ptr_id) REFERENCES public.main_unifiedjobtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_inventoryupdate main_inventoryupdate_inventory_id_e60f1f2e_fk_main_inventory_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventoryupdate
    ADD CONSTRAINT main_inventoryupdate_inventory_id_e60f1f2e_fk_main_inventory_id FOREIGN KEY (inventory_id) REFERENCES public.main_inventory(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_inventoryupdate main_inventoryupdate_inventory_source_id_bc4b2567_fk_main_inve; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventoryupdate
    ADD CONSTRAINT main_inventoryupdate_inventory_source_id_bc4b2567_fk_main_inve FOREIGN KEY (inventory_source_id) REFERENCES public.main_inventorysource(unifiedjobtemplate_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: _unpartitioned_main_inventoryupdateevent main_inventoryupdate_inventory_update_id_8974f1f7_fk_main_inve; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public._unpartitioned_main_inventoryupdateevent
    ADD CONSTRAINT main_inventoryupdate_inventory_update_id_8974f1f7_fk_main_inve FOREIGN KEY (inventory_update_id) REFERENCES public.main_inventoryupdate(unifiedjob_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_inventoryupdate main_inventoryupdate_source_project_updat_b896d555_fk_main_proj; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventoryupdate
    ADD CONSTRAINT main_inventoryupdate_source_project_updat_b896d555_fk_main_proj FOREIGN KEY (source_project_update_id) REFERENCES public.main_projectupdate(unifiedjob_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_inventoryupdate main_inventoryupdate_unifiedjob_ptr_id_a42ff4c2_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_inventoryupdate
    ADD CONSTRAINT main_inventoryupdate_unifiedjob_ptr_id_a42ff4c2_fk_main_unif FOREIGN KEY (unifiedjob_ptr_id) REFERENCES public.main_unifiedjob(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_job main_job_inventory_id_1b436658_fk_main_inventory_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_job
    ADD CONSTRAINT main_job_inventory_id_1b436658_fk_main_inventory_id FOREIGN KEY (inventory_id) REFERENCES public.main_inventory(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_job main_job_job_template_id_070b0d56_fk_main_jobt; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_job
    ADD CONSTRAINT main_job_job_template_id_070b0d56_fk_main_jobt FOREIGN KEY (job_template_id) REFERENCES public.main_jobtemplate(unifiedjobtemplate_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_job main_job_project_id_a8f63894_fk_main_proj; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_job
    ADD CONSTRAINT main_job_project_id_a8f63894_fk_main_proj FOREIGN KEY (project_id) REFERENCES public.main_project(unifiedjobtemplate_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_job main_job_project_update_id_5adf90ad_fk_main_proj; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_job
    ADD CONSTRAINT main_job_project_update_id_5adf90ad_fk_main_proj FOREIGN KEY (project_update_id) REFERENCES public.main_projectupdate(unifiedjob_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_job main_job_unifiedjob_ptr_id_46108a43_fk_main_unifiedjob_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_job
    ADD CONSTRAINT main_job_unifiedjob_ptr_id_46108a43_fk_main_unifiedjob_id FOREIGN KEY (unifiedjob_ptr_id) REFERENCES public.main_unifiedjob(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_job main_job_webhook_credential_id_40ca94fa_fk_main_credential_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_job
    ADD CONSTRAINT main_job_webhook_credential_id_40ca94fa_fk_main_credential_id FOREIGN KEY (webhook_credential_id) REFERENCES public.main_credential(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: _unpartitioned_main_jobevent main_jobevent_host_id_b03b6059_fk_main_host_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public._unpartitioned_main_jobevent
    ADD CONSTRAINT main_jobevent_host_id_b03b6059_fk_main_host_id FOREIGN KEY (host_id) REFERENCES public.main_host(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: _unpartitioned_main_jobevent main_jobevent_job_id_571587e8_fk_main_job_unifiedjob_ptr_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public._unpartitioned_main_jobevent
    ADD CONSTRAINT main_jobevent_job_id_571587e8_fk_main_job_unifiedjob_ptr_id FOREIGN KEY (job_id) REFERENCES public.main_job(unifiedjob_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_jobhostsummary main_jobhostsummary_constructed_host_id_8ec8dc05_fk_main_host; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_jobhostsummary
    ADD CONSTRAINT main_jobhostsummary_constructed_host_id_8ec8dc05_fk_main_host FOREIGN KEY (constructed_host_id) REFERENCES public.main_host(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_jobhostsummary main_jobhostsummary_host_id_7d9f6bf9_fk_main_host_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_jobhostsummary
    ADD CONSTRAINT main_jobhostsummary_host_id_7d9f6bf9_fk_main_host_id FOREIGN KEY (host_id) REFERENCES public.main_host(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_jobhostsummary main_jobhostsummary_job_id_8d60afa0_fk_main_job_; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_jobhostsummary
    ADD CONSTRAINT main_jobhostsummary_job_id_8d60afa0_fk_main_job_ FOREIGN KEY (job_id) REFERENCES public.main_job(unifiedjob_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_joblaunchconfig_credentials main_joblaunchconfig_credential_id_2f5c0487_fk_main_cred; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_joblaunchconfig_credentials
    ADD CONSTRAINT main_joblaunchconfig_credential_id_2f5c0487_fk_main_cred FOREIGN KEY (credential_id) REFERENCES public.main_credential(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_joblaunchconfig main_joblaunchconfig_execution_environmen_ddf8eeec_fk_main_exec; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_joblaunchconfig
    ADD CONSTRAINT main_joblaunchconfig_execution_environmen_ddf8eeec_fk_main_exec FOREIGN KEY (execution_environment_id) REFERENCES public.main_executionenvironment(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_joblaunchconfiginstancegroupmembership main_joblaunchconfig_instancegroup_id_e76ac8f9_fk_main_inst; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_joblaunchconfiginstancegroupmembership
    ADD CONSTRAINT main_joblaunchconfig_instancegroup_id_e76ac8f9_fk_main_inst FOREIGN KEY (instancegroup_id) REFERENCES public.main_instancegroup(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_joblaunchconfig main_joblaunchconfig_inventory_id_f905306d_fk_main_inventory_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_joblaunchconfig
    ADD CONSTRAINT main_joblaunchconfig_inventory_id_f905306d_fk_main_inventory_id FOREIGN KEY (inventory_id) REFERENCES public.main_inventory(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_joblaunchconfig main_joblaunchconfig_job_id_6e18fad4_fk_main_unifiedjob_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_joblaunchconfig
    ADD CONSTRAINT main_joblaunchconfig_job_id_6e18fad4_fk_main_unifiedjob_id FOREIGN KEY (job_id) REFERENCES public.main_unifiedjob(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_joblaunchconfig_labels main_joblaunchconfig_joblaunchconfig_id_004bb969_fk_main_jobl; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_joblaunchconfig_labels
    ADD CONSTRAINT main_joblaunchconfig_joblaunchconfig_id_004bb969_fk_main_jobl FOREIGN KEY (joblaunchconfig_id) REFERENCES public.main_joblaunchconfig(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_joblaunchconfig_credentials main_joblaunchconfig_joblaunchconfig_id_37dc31b9_fk_main_jobl; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_joblaunchconfig_credentials
    ADD CONSTRAINT main_joblaunchconfig_joblaunchconfig_id_37dc31b9_fk_main_jobl FOREIGN KEY (joblaunchconfig_id) REFERENCES public.main_joblaunchconfig(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_joblaunchconfiginstancegroupmembership main_joblaunchconfig_joblaunchconfig_id_93eb971f_fk_main_jobl; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_joblaunchconfiginstancegroupmembership
    ADD CONSTRAINT main_joblaunchconfig_joblaunchconfig_id_93eb971f_fk_main_jobl FOREIGN KEY (joblaunchconfig_id) REFERENCES public.main_joblaunchconfig(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_joblaunchconfig_labels main_joblaunchconfig_labels_label_id_5a9a600e_fk_main_label_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_joblaunchconfig_labels
    ADD CONSTRAINT main_joblaunchconfig_labels_label_id_5a9a600e_fk_main_label_id FOREIGN KEY (label_id) REFERENCES public.main_label(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_jobtemplate main_jobtemplate_admin_role_id_f9dc66ce_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_jobtemplate
    ADD CONSTRAINT main_jobtemplate_admin_role_id_f9dc66ce_fk_main_rbac_roles_id FOREIGN KEY (admin_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_jobtemplate main_jobtemplate_execute_role_id_c2f0db2c_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_jobtemplate
    ADD CONSTRAINT main_jobtemplate_execute_role_id_c2f0db2c_fk_main_rbac_roles_id FOREIGN KEY (execute_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_jobtemplate main_jobtemplate_inventory_id_9b8df646_fk_main_inventory_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_jobtemplate
    ADD CONSTRAINT main_jobtemplate_inventory_id_9b8df646_fk_main_inventory_id FOREIGN KEY (inventory_id) REFERENCES public.main_inventory(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_jobtemplate main_jobtemplate_project_id_36e80985_fk_main_proj; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_jobtemplate
    ADD CONSTRAINT main_jobtemplate_project_id_36e80985_fk_main_proj FOREIGN KEY (project_id) REFERENCES public.main_project(unifiedjobtemplate_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_jobtemplate main_jobtemplate_read_role_id_0e489c81_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_jobtemplate
    ADD CONSTRAINT main_jobtemplate_read_role_id_0e489c81_fk_main_rbac_roles_id FOREIGN KEY (read_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_jobtemplate main_jobtemplate_unifiedjobtemplate_p_4d0a792f_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_jobtemplate
    ADD CONSTRAINT main_jobtemplate_unifiedjobtemplate_p_4d0a792f_fk_main_unif FOREIGN KEY (unifiedjobtemplate_ptr_id) REFERENCES public.main_unifiedjobtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_jobtemplate main_jobtemplate_webhook_credential_i_eff7fb4b_fk_main_cred; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_jobtemplate
    ADD CONSTRAINT main_jobtemplate_webhook_credential_i_eff7fb4b_fk_main_cred FOREIGN KEY (webhook_credential_id) REFERENCES public.main_credential(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_label main_label_created_by_id_201182c0_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_label
    ADD CONSTRAINT main_label_created_by_id_201182c0_fk_auth_user_id FOREIGN KEY (created_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_label main_label_modified_by_id_7f9aac68_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_label
    ADD CONSTRAINT main_label_modified_by_id_7f9aac68_fk_auth_user_id FOREIGN KEY (modified_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_label main_label_organization_id_78a1bd27_fk_main_organization_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_label
    ADD CONSTRAINT main_label_organization_id_78a1bd27_fk_main_organization_id FOREIGN KEY (organization_id) REFERENCES public.main_organization(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_notification main_notification_notification_templat_9eed1d65_fk_main_noti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_notification
    ADD CONSTRAINT main_notification_notification_templat_9eed1d65_fk_main_noti FOREIGN KEY (notification_template_id) REFERENCES public.main_notificationtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_notificationtemplate main_notificationtem_created_by_id_1f77983a_fk_auth_user; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_notificationtemplate
    ADD CONSTRAINT main_notificationtem_created_by_id_1f77983a_fk_auth_user FOREIGN KEY (created_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_notificationtemplate main_notificationtem_modified_by_id_83c40510_fk_auth_user; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_notificationtemplate
    ADD CONSTRAINT main_notificationtem_modified_by_id_83c40510_fk_auth_user FOREIGN KEY (modified_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_notificationtemplate main_notificationtem_organization_id_15933abb_fk_main_orga; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_notificationtemplate
    ADD CONSTRAINT main_notificationtem_organization_id_15933abb_fk_main_orga FOREIGN KEY (organization_id) REFERENCES public.main_organization(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization main_organization_admin_role_id_e3ffdd41_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization
    ADD CONSTRAINT main_organization_admin_role_id_e3ffdd41_fk_main_rbac_roles_id FOREIGN KEY (admin_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization main_organization_approval_role_id_14c1d96f_fk_main_rbac; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization
    ADD CONSTRAINT main_organization_approval_role_id_14c1d96f_fk_main_rbac FOREIGN KEY (approval_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization main_organization_auditor_role_id_f912df0a_fk_main_rbac; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization
    ADD CONSTRAINT main_organization_auditor_role_id_f912df0a_fk_main_rbac FOREIGN KEY (auditor_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization main_organization_created_by_id_141da798_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization
    ADD CONSTRAINT main_organization_created_by_id_141da798_fk_auth_user_id FOREIGN KEY (created_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization main_organization_credential_admin_rol_55733eb5_fk_main_rbac; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization
    ADD CONSTRAINT main_organization_credential_admin_rol_55733eb5_fk_main_rbac FOREIGN KEY (credential_admin_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization main_organization_default_environment__1696aac2_fk_main_exec; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization
    ADD CONSTRAINT main_organization_default_environment__1696aac2_fk_main_exec FOREIGN KEY (default_environment_id) REFERENCES public.main_executionenvironment(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization main_organization_execute_role_id_76038d3c_fk_main_rbac; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization
    ADD CONSTRAINT main_organization_execute_role_id_76038d3c_fk_main_rbac FOREIGN KEY (execute_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization main_organization_execution_environmen_f2351549_fk_main_rbac; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization
    ADD CONSTRAINT main_organization_execution_environmen_f2351549_fk_main_rbac FOREIGN KEY (execution_environment_admin_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization main_organization_inventory_admin_role_dae5c7e2_fk_main_rbac; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization
    ADD CONSTRAINT main_organization_inventory_admin_role_dae5c7e2_fk_main_rbac FOREIGN KEY (inventory_admin_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization main_organization_job_template_admin_r_25a265c4_fk_main_rbac; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization
    ADD CONSTRAINT main_organization_job_template_admin_r_25a265c4_fk_main_rbac FOREIGN KEY (job_template_admin_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization main_organization_member_role_id_201ff67a_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization
    ADD CONSTRAINT main_organization_member_role_id_201ff67a_fk_main_rbac_roles_id FOREIGN KEY (member_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization main_organization_modified_by_id_dec7a500_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization
    ADD CONSTRAINT main_organization_modified_by_id_dec7a500_fk_auth_user_id FOREIGN KEY (modified_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization_notification_templates_started main_organization_no_notificationtemplate_1df2f173_fk_main_noti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization_notification_templates_started
    ADD CONSTRAINT main_organization_no_notificationtemplate_1df2f173_fk_main_noti FOREIGN KEY (notificationtemplate_id) REFERENCES public.main_notificationtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization_notification_templates_approvals main_organization_no_notificationtemplate_392029b7_fk_main_noti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization_notification_templates_approvals
    ADD CONSTRAINT main_organization_no_notificationtemplate_392029b7_fk_main_noti FOREIGN KEY (notificationtemplate_id) REFERENCES public.main_notificationtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization_notification_templates_success main_organization_no_notificationtemplate_4edd98c4_fk_main_noti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization_notification_templates_success
    ADD CONSTRAINT main_organization_no_notificationtemplate_4edd98c4_fk_main_noti FOREIGN KEY (notificationtemplate_id) REFERENCES public.main_notificationtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization_notification_templates_error main_organization_no_notificationtemplate_7b1480c0_fk_main_noti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization_notification_templates_error
    ADD CONSTRAINT main_organization_no_notificationtemplate_7b1480c0_fk_main_noti FOREIGN KEY (notificationtemplate_id) REFERENCES public.main_notificationtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization_notification_templates_approvals main_organization_no_organization_id_44a19957_fk_main_orga; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization_notification_templates_approvals
    ADD CONSTRAINT main_organization_no_organization_id_44a19957_fk_main_orga FOREIGN KEY (organization_id) REFERENCES public.main_organization(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization_notification_templates_started main_organization_no_organization_id_48a058ac_fk_main_orga; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization_notification_templates_started
    ADD CONSTRAINT main_organization_no_organization_id_48a058ac_fk_main_orga FOREIGN KEY (organization_id) REFERENCES public.main_organization(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization_notification_templates_error main_organization_no_organization_id_94b63d49_fk_main_orga; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization_notification_templates_error
    ADD CONSTRAINT main_organization_no_organization_id_94b63d49_fk_main_orga FOREIGN KEY (organization_id) REFERENCES public.main_organization(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization_notification_templates_success main_organization_no_organization_id_96635cd6_fk_main_orga; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization_notification_templates_success
    ADD CONSTRAINT main_organization_no_organization_id_96635cd6_fk_main_orga FOREIGN KEY (organization_id) REFERENCES public.main_organization(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization main_organization_notification_admin_r_c36d2f0e_fk_main_rbac; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization
    ADD CONSTRAINT main_organization_notification_admin_r_c36d2f0e_fk_main_rbac FOREIGN KEY (notification_admin_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization main_organization_project_admin_role_i_442cfebe_fk_main_rbac; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization
    ADD CONSTRAINT main_organization_project_admin_role_i_442cfebe_fk_main_rbac FOREIGN KEY (project_admin_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization main_organization_read_role_id_e143c386_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization
    ADD CONSTRAINT main_organization_read_role_id_e143c386_fk_main_rbac_roles_id FOREIGN KEY (read_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organization main_organization_workflow_admin_role__52011cd3_fk_main_rbac; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organization
    ADD CONSTRAINT main_organization_workflow_admin_role__52011cd3_fk_main_rbac FOREIGN KEY (workflow_admin_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organizationgalaxycredentialmembership main_organizationgal_credential_id_7b6334f3_fk_main_cred; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organizationgalaxycredentialmembership
    ADD CONSTRAINT main_organizationgal_credential_id_7b6334f3_fk_main_cred FOREIGN KEY (credential_id) REFERENCES public.main_credential(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organizationgalaxycredentialmembership main_organizationgal_organization_id_0fd9495c_fk_main_orga; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organizationgalaxycredentialmembership
    ADD CONSTRAINT main_organizationgal_organization_id_0fd9495c_fk_main_orga FOREIGN KEY (organization_id) REFERENCES public.main_organization(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organizationinstancegroupmembership main_organizationins_instancegroup_id_526173a9_fk_main_inst; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organizationinstancegroupmembership
    ADD CONSTRAINT main_organizationins_instancegroup_id_526173a9_fk_main_inst FOREIGN KEY (instancegroup_id) REFERENCES public.main_instancegroup(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_organizationinstancegroupmembership main_organizationins_organization_id_35633383_fk_main_orga; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_organizationinstancegroupmembership
    ADD CONSTRAINT main_organizationins_organization_id_35633383_fk_main_orga FOREIGN KEY (organization_id) REFERENCES public.main_organization(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_project main_project_admin_role_id_ba0e70c7_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_project
    ADD CONSTRAINT main_project_admin_role_id_ba0e70c7_fk_main_rbac_roles_id FOREIGN KEY (admin_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_project main_project_credential_id_370ba2a3_fk_main_credential_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_project
    ADD CONSTRAINT main_project_credential_id_370ba2a3_fk_main_credential_id FOREIGN KEY (credential_id) REFERENCES public.main_credential(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_project main_project_default_environment__01467429_fk_main_exec; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_project
    ADD CONSTRAINT main_project_default_environment__01467429_fk_main_exec FOREIGN KEY (default_environment_id) REFERENCES public.main_executionenvironment(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_project main_project_read_role_id_39a01fd4_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_project
    ADD CONSTRAINT main_project_read_role_id_39a01fd4_fk_main_rbac_roles_id FOREIGN KEY (read_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_project main_project_signature_validation_41e77a69_fk_main_cred; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_project
    ADD CONSTRAINT main_project_signature_validation_41e77a69_fk_main_cred FOREIGN KEY (signature_validation_credential_id) REFERENCES public.main_credential(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_project main_project_unifiedjobtemplate_p_078080eb_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_project
    ADD CONSTRAINT main_project_unifiedjobtemplate_p_078080eb_fk_main_unif FOREIGN KEY (unifiedjobtemplate_ptr_id) REFERENCES public.main_unifiedjobtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_project main_project_update_role_id_36e33c42_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_project
    ADD CONSTRAINT main_project_update_role_id_36e33c42_fk_main_rbac_roles_id FOREIGN KEY (update_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_project main_project_use_role_id_7b6d9148_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_project
    ADD CONSTRAINT main_project_use_role_id_7b6d9148_fk_main_rbac_roles_id FOREIGN KEY (use_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_projectupdate main_projectupdate_credential_id_2f7d826a_fk_main_credential_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_projectupdate
    ADD CONSTRAINT main_projectupdate_credential_id_2f7d826a_fk_main_credential_id FOREIGN KEY (credential_id) REFERENCES public.main_credential(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_projectupdate main_projectupdate_project_id_bdd73efe_fk_main_proj; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_projectupdate
    ADD CONSTRAINT main_projectupdate_project_id_bdd73efe_fk_main_proj FOREIGN KEY (project_id) REFERENCES public.main_project(unifiedjobtemplate_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_projectupdate main_projectupdate_unifiedjob_ptr_id_039312cd_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_projectupdate
    ADD CONSTRAINT main_projectupdate_unifiedjob_ptr_id_039312cd_fk_main_unif FOREIGN KEY (unifiedjob_ptr_id) REFERENCES public.main_unifiedjob(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: _unpartitioned_main_projectupdateevent main_projectupdateev_project_update_id_9d4358b2_fk_main_proj; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public._unpartitioned_main_projectupdateevent
    ADD CONSTRAINT main_projectupdateev_project_update_id_9d4358b2_fk_main_proj FOREIGN KEY (project_update_id) REFERENCES public.main_projectupdate(unifiedjob_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_rbac_role_ancestors main_rbac_role_ances_ancestor_id_c6aae106_fk_main_rbac; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_rbac_role_ancestors
    ADD CONSTRAINT main_rbac_role_ances_ancestor_id_c6aae106_fk_main_rbac FOREIGN KEY (ancestor_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_rbac_role_ancestors main_rbac_role_ances_descendent_id_23bfc463_fk_main_rbac; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_rbac_role_ancestors
    ADD CONSTRAINT main_rbac_role_ances_descendent_id_23bfc463_fk_main_rbac FOREIGN KEY (descendent_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_rbac_roles main_rbac_roles_content_type_id_756d6b30_fk_django_co; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_rbac_roles
    ADD CONSTRAINT main_rbac_roles_content_type_id_756d6b30_fk_django_co FOREIGN KEY (content_type_id) REFERENCES public.django_content_type(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_rbac_roles_members main_rbac_roles_members_role_id_7318b4b7_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_rbac_roles_members
    ADD CONSTRAINT main_rbac_roles_members_role_id_7318b4b7_fk_main_rbac_roles_id FOREIGN KEY (role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_rbac_roles_members main_rbac_roles_members_user_id_f5e05418_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_rbac_roles_members
    ADD CONSTRAINT main_rbac_roles_members_user_id_f5e05418_fk_auth_user_id FOREIGN KEY (user_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_rbac_roles_parents main_rbac_roles_pare_from_role_id_a02db9eb_fk_main_rbac; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_rbac_roles_parents
    ADD CONSTRAINT main_rbac_roles_pare_from_role_id_a02db9eb_fk_main_rbac FOREIGN KEY (from_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_rbac_roles_parents main_rbac_roles_pare_to_role_id_c00b5087_fk_main_rbac; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_rbac_roles_parents
    ADD CONSTRAINT main_rbac_roles_pare_to_role_id_c00b5087_fk_main_rbac FOREIGN KEY (to_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_receptoraddress main_receptoraddress_instance_id_988e9845_fk_main_instance_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_receptoraddress
    ADD CONSTRAINT main_receptoraddress_instance_id_988e9845_fk_main_instance_id FOREIGN KEY (instance_id) REFERENCES public.main_instance(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_schedule main_schedule_created_by_id_4e647be2_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_schedule
    ADD CONSTRAINT main_schedule_created_by_id_4e647be2_fk_auth_user_id FOREIGN KEY (created_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_schedule_credentials main_schedule_creden_credential_id_ced5894e_fk_main_cred; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_schedule_credentials
    ADD CONSTRAINT main_schedule_creden_credential_id_ced5894e_fk_main_cred FOREIGN KEY (credential_id) REFERENCES public.main_credential(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_schedule_credentials main_schedule_creden_schedule_id_03ecad04_fk_main_sche; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_schedule_credentials
    ADD CONSTRAINT main_schedule_creden_schedule_id_03ecad04_fk_main_sche FOREIGN KEY (schedule_id) REFERENCES public.main_schedule(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_schedule main_schedule_execution_environmen_90eefd45_fk_main_exec; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_schedule
    ADD CONSTRAINT main_schedule_execution_environmen_90eefd45_fk_main_exec FOREIGN KEY (execution_environment_id) REFERENCES public.main_executionenvironment(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_schedule main_schedule_inventory_id_43b7b69d_fk_main_inventory_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_schedule
    ADD CONSTRAINT main_schedule_inventory_id_43b7b69d_fk_main_inventory_id FOREIGN KEY (inventory_id) REFERENCES public.main_inventory(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_schedule_labels main_schedule_labels_label_id_79a46df6_fk_main_label_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_schedule_labels
    ADD CONSTRAINT main_schedule_labels_label_id_79a46df6_fk_main_label_id FOREIGN KEY (label_id) REFERENCES public.main_label(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_schedule_labels main_schedule_labels_schedule_id_e2f00ec6_fk_main_schedule_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_schedule_labels
    ADD CONSTRAINT main_schedule_labels_schedule_id_e2f00ec6_fk_main_schedule_id FOREIGN KEY (schedule_id) REFERENCES public.main_schedule(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_schedule main_schedule_modified_by_id_3817bc47_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_schedule
    ADD CONSTRAINT main_schedule_modified_by_id_3817bc47_fk_auth_user_id FOREIGN KEY (modified_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_schedule main_schedule_unified_job_template_a9d931e2_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_schedule
    ADD CONSTRAINT main_schedule_unified_job_template_a9d931e2_fk_main_unif FOREIGN KEY (unified_job_template_id) REFERENCES public.main_unifiedjobtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_scheduleinstancegroupmembership main_scheduleinstanc_instancegroup_id_2d5f236c_fk_main_inst; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_scheduleinstancegroupmembership
    ADD CONSTRAINT main_scheduleinstanc_instancegroup_id_2d5f236c_fk_main_inst FOREIGN KEY (instancegroup_id) REFERENCES public.main_instancegroup(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_scheduleinstancegroupmembership main_scheduleinstanc_schedule_id_d8eb2c41_fk_main_sche; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_scheduleinstancegroupmembership
    ADD CONSTRAINT main_scheduleinstanc_schedule_id_d8eb2c41_fk_main_sche FOREIGN KEY (schedule_id) REFERENCES public.main_schedule(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_smartinventorymembership main_smartinventorym_inventory_id_5e13df96_fk_main_inve; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_smartinventorymembership
    ADD CONSTRAINT main_smartinventorym_inventory_id_5e13df96_fk_main_inve FOREIGN KEY (inventory_id) REFERENCES public.main_inventory(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_smartinventorymembership main_smartinventorymembership_host_id_c721cb8a_fk_main_host_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_smartinventorymembership
    ADD CONSTRAINT main_smartinventorymembership_host_id_c721cb8a_fk_main_host_id FOREIGN KEY (host_id) REFERENCES public.main_host(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_systemjob main_systemjob_system_job_template__8bba2060_fk_main_syst; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_systemjob
    ADD CONSTRAINT main_systemjob_system_job_template__8bba2060_fk_main_syst FOREIGN KEY (system_job_template_id) REFERENCES public.main_systemjobtemplate(unifiedjobtemplate_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_systemjob main_systemjob_unifiedjob_ptr_id_9517b368_fk_main_unifiedjob_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_systemjob
    ADD CONSTRAINT main_systemjob_unifiedjob_ptr_id_9517b368_fk_main_unifiedjob_id FOREIGN KEY (unifiedjob_ptr_id) REFERENCES public.main_unifiedjob(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: _unpartitioned_main_systemjobevent main_systemjobevent_system_job_id_91bbbfc1_fk_main_syst; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public._unpartitioned_main_systemjobevent
    ADD CONSTRAINT main_systemjobevent_system_job_id_91bbbfc1_fk_main_syst FOREIGN KEY (system_job_id) REFERENCES public.main_systemjob(unifiedjob_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_systemjobtemplate main_systemjobtempla_unifiedjobtemplate_p_60f12f55_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_systemjobtemplate
    ADD CONSTRAINT main_systemjobtempla_unifiedjobtemplate_p_60f12f55_fk_main_unif FOREIGN KEY (unifiedjobtemplate_ptr_id) REFERENCES public.main_unifiedjobtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_team main_team_admin_role_id_a9e09a22_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_team
    ADD CONSTRAINT main_team_admin_role_id_a9e09a22_fk_main_rbac_roles_id FOREIGN KEY (admin_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_team main_team_created_by_id_c370350b_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_team
    ADD CONSTRAINT main_team_created_by_id_c370350b_fk_auth_user_id FOREIGN KEY (created_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_team main_team_member_role_id_a2f93dc9_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_team
    ADD CONSTRAINT main_team_member_role_id_a2f93dc9_fk_main_rbac_roles_id FOREIGN KEY (member_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_team main_team_modified_by_id_9af533cd_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_team
    ADD CONSTRAINT main_team_modified_by_id_9af533cd_fk_auth_user_id FOREIGN KEY (modified_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_team main_team_organization_id_8b31bbc1_fk_main_organization_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_team
    ADD CONSTRAINT main_team_organization_id_8b31bbc1_fk_main_organization_id FOREIGN KEY (organization_id) REFERENCES public.main_organization(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_team main_team_read_role_id_ea02761f_fk_main_rbac_roles_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_team
    ADD CONSTRAINT main_team_read_role_id_ea02761f_fk_main_rbac_roles_id FOREIGN KEY (read_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjob main_unifiedjob_created_by_id_d2a186ab_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob
    ADD CONSTRAINT main_unifiedjob_created_by_id_d2a186ab_fk_auth_user_id FOREIGN KEY (created_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjob_credentials main_unifiedjob_cred_credential_id_661c8f49_fk_main_cred; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob_credentials
    ADD CONSTRAINT main_unifiedjob_cred_credential_id_661c8f49_fk_main_cred FOREIGN KEY (credential_id) REFERENCES public.main_credential(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjob_credentials main_unifiedjob_cred_unifiedjob_id_4ed7ff5d_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob_credentials
    ADD CONSTRAINT main_unifiedjob_cred_unifiedjob_id_4ed7ff5d_fk_main_unif FOREIGN KEY (unifiedjob_id) REFERENCES public.main_unifiedjob(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjob_dependent_jobs main_unifiedjob_depe_from_unifiedjob_id_c8d58e88_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob_dependent_jobs
    ADD CONSTRAINT main_unifiedjob_depe_from_unifiedjob_id_c8d58e88_fk_main_unif FOREIGN KEY (from_unifiedjob_id) REFERENCES public.main_unifiedjob(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjob_dependent_jobs main_unifiedjob_depe_to_unifiedjob_id_3f04cbcc_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob_dependent_jobs
    ADD CONSTRAINT main_unifiedjob_depe_to_unifiedjob_id_3f04cbcc_fk_main_unif FOREIGN KEY (to_unifiedjob_id) REFERENCES public.main_unifiedjob(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjob main_unifiedjob_execution_environmen_b2eaf9c0_fk_main_exec; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob
    ADD CONSTRAINT main_unifiedjob_execution_environmen_b2eaf9c0_fk_main_exec FOREIGN KEY (execution_environment_id) REFERENCES public.main_executionenvironment(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjob main_unifiedjob_instance_group_id_f76a06e2_fk_main_inst; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob
    ADD CONSTRAINT main_unifiedjob_instance_group_id_f76a06e2_fk_main_inst FOREIGN KEY (instance_group_id) REFERENCES public.main_instancegroup(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjob_labels main_unifiedjob_labe_unifiedjob_id_bd008d37_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob_labels
    ADD CONSTRAINT main_unifiedjob_labe_unifiedjob_id_bd008d37_fk_main_unif FOREIGN KEY (unifiedjob_id) REFERENCES public.main_unifiedjob(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjob_labels main_unifiedjob_labels_label_id_98814bad_fk_main_label_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob_labels
    ADD CONSTRAINT main_unifiedjob_labels_label_id_98814bad_fk_main_label_id FOREIGN KEY (label_id) REFERENCES public.main_label(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjob main_unifiedjob_modified_by_id_14cbb9bc_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob
    ADD CONSTRAINT main_unifiedjob_modified_by_id_14cbb9bc_fk_auth_user_id FOREIGN KEY (modified_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjob_notifications main_unifiedjob_noti_notification_id_cf3498bf_fk_main_noti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob_notifications
    ADD CONSTRAINT main_unifiedjob_noti_notification_id_cf3498bf_fk_main_noti FOREIGN KEY (notification_id) REFERENCES public.main_notification(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjob_notifications main_unifiedjob_noti_unifiedjob_id_65ab9c3c_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob_notifications
    ADD CONSTRAINT main_unifiedjob_noti_unifiedjob_id_65ab9c3c_fk_main_unif FOREIGN KEY (unifiedjob_id) REFERENCES public.main_unifiedjob(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjob main_unifiedjob_organization_id_cbfa01d3_fk_main_orga; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob
    ADD CONSTRAINT main_unifiedjob_organization_id_cbfa01d3_fk_main_orga FOREIGN KEY (organization_id) REFERENCES public.main_organization(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjob main_unifiedjob_polymorphic_ctype_id_cb46239b_fk_django_co; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob
    ADD CONSTRAINT main_unifiedjob_polymorphic_ctype_id_cb46239b_fk_django_co FOREIGN KEY (polymorphic_ctype_id) REFERENCES public.django_content_type(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjob main_unifiedjob_schedule_id_766ca767_fk_main_schedule_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob
    ADD CONSTRAINT main_unifiedjob_schedule_id_766ca767_fk_main_schedule_id FOREIGN KEY (schedule_id) REFERENCES public.main_schedule(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjob main_unifiedjob_unified_job_template_a398b197_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjob
    ADD CONSTRAINT main_unifiedjob_unified_job_template_a398b197_fk_main_unif FOREIGN KEY (unified_job_template_id) REFERENCES public.main_unifiedjobtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjobtemplate_credentials main_unifiedjobtempl_credential_id_fd216c80_fk_main_cred; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate_credentials
    ADD CONSTRAINT main_unifiedjobtempl_credential_id_fd216c80_fk_main_cred FOREIGN KEY (credential_id) REFERENCES public.main_credential(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjobtemplate main_unifiedjobtempl_current_job_id_8f449ab0_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate
    ADD CONSTRAINT main_unifiedjobtempl_current_job_id_8f449ab0_fk_main_unif FOREIGN KEY (current_job_id) REFERENCES public.main_unifiedjob(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjobtemplate main_unifiedjobtempl_execution_environmen_bed25866_fk_main_exec; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate
    ADD CONSTRAINT main_unifiedjobtempl_execution_environmen_bed25866_fk_main_exec FOREIGN KEY (execution_environment_id) REFERENCES public.main_executionenvironment(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjobtemplateinstancegroupmembership main_unifiedjobtempl_instancegroup_id_656188b4_fk_main_inst; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplateinstancegroupmembership
    ADD CONSTRAINT main_unifiedjobtempl_instancegroup_id_656188b4_fk_main_inst FOREIGN KEY (instancegroup_id) REFERENCES public.main_instancegroup(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjobtemplate_labels main_unifiedjobtempl_label_id_d6a5ee75_fk_main_labe; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate_labels
    ADD CONSTRAINT main_unifiedjobtempl_label_id_d6a5ee75_fk_main_labe FOREIGN KEY (label_id) REFERENCES public.main_label(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjobtemplate main_unifiedjobtempl_last_job_id_7e983743_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate
    ADD CONSTRAINT main_unifiedjobtempl_last_job_id_7e983743_fk_main_unif FOREIGN KEY (last_job_id) REFERENCES public.main_unifiedjob(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjobtemplate main_unifiedjobtempl_next_schedule_id_955ff55d_fk_main_sche; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate
    ADD CONSTRAINT main_unifiedjobtempl_next_schedule_id_955ff55d_fk_main_sche FOREIGN KEY (next_schedule_id) REFERENCES public.main_schedule(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjobtemplate_notification_templates_success main_unifiedjobtempl_notificationtemplate_9326cdf9_fk_main_noti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate_notification_templates_success
    ADD CONSTRAINT main_unifiedjobtempl_notificationtemplate_9326cdf9_fk_main_noti FOREIGN KEY (notificationtemplate_id) REFERENCES public.main_notificationtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjobtemplate_notification_templates_started main_unifiedjobtempl_notificationtemplate_9793a63a_fk_main_noti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate_notification_templates_started
    ADD CONSTRAINT main_unifiedjobtempl_notificationtemplate_9793a63a_fk_main_noti FOREIGN KEY (notificationtemplate_id) REFERENCES public.main_notificationtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjobtemplate_notification_templates_error main_unifiedjobtempl_notificationtemplate_b19df8ac_fk_main_noti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate_notification_templates_error
    ADD CONSTRAINT main_unifiedjobtempl_notificationtemplate_b19df8ac_fk_main_noti FOREIGN KEY (notificationtemplate_id) REFERENCES public.main_notificationtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjobtemplate main_unifiedjobtempl_organization_id_c63fa1a4_fk_main_orga; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate
    ADD CONSTRAINT main_unifiedjobtempl_organization_id_c63fa1a4_fk_main_orga FOREIGN KEY (organization_id) REFERENCES public.main_organization(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjobtemplate main_unifiedjobtempl_polymorphic_ctype_id_ce19bb25_fk_django_co; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate
    ADD CONSTRAINT main_unifiedjobtempl_polymorphic_ctype_id_ce19bb25_fk_django_co FOREIGN KEY (polymorphic_ctype_id) REFERENCES public.django_content_type(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjobtemplate_notification_templates_error main_unifiedjobtempl_unifiedjobtemplate_i_0ce91b23_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate_notification_templates_error
    ADD CONSTRAINT main_unifiedjobtempl_unifiedjobtemplate_i_0ce91b23_fk_main_unif FOREIGN KEY (unifiedjobtemplate_id) REFERENCES public.main_unifiedjobtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjobtemplate_notification_templates_success main_unifiedjobtempl_unifiedjobtemplate_i_3934753d_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate_notification_templates_success
    ADD CONSTRAINT main_unifiedjobtempl_unifiedjobtemplate_i_3934753d_fk_main_unif FOREIGN KEY (unifiedjobtemplate_id) REFERENCES public.main_unifiedjobtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjobtemplate_notification_templates_started main_unifiedjobtempl_unifiedjobtemplate_i_6e21dce4_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate_notification_templates_started
    ADD CONSTRAINT main_unifiedjobtempl_unifiedjobtemplate_i_6e21dce4_fk_main_unif FOREIGN KEY (unifiedjobtemplate_id) REFERENCES public.main_unifiedjobtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjobtemplate_labels main_unifiedjobtempl_unifiedjobtemplate_i_c9307a9a_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate_labels
    ADD CONSTRAINT main_unifiedjobtempl_unifiedjobtemplate_i_c9307a9a_fk_main_unif FOREIGN KEY (unifiedjobtemplate_id) REFERENCES public.main_unifiedjobtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjobtemplate_credentials main_unifiedjobtempl_unifiedjobtemplate_i_d98d7c79_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate_credentials
    ADD CONSTRAINT main_unifiedjobtempl_unifiedjobtemplate_i_d98d7c79_fk_main_unif FOREIGN KEY (unifiedjobtemplate_id) REFERENCES public.main_unifiedjobtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjobtemplateinstancegroupmembership main_unifiedjobtempl_unifiedjobtemplate_i_e401e3d7_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplateinstancegroupmembership
    ADD CONSTRAINT main_unifiedjobtempl_unifiedjobtemplate_i_e401e3d7_fk_main_unif FOREIGN KEY (unifiedjobtemplate_id) REFERENCES public.main_unifiedjobtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjobtemplate main_unifiedjobtemplate_created_by_id_1f5fadfa_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate
    ADD CONSTRAINT main_unifiedjobtemplate_created_by_id_1f5fadfa_fk_auth_user_id FOREIGN KEY (created_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_unifiedjobtemplate main_unifiedjobtemplate_modified_by_id_a8bf1de0_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_unifiedjobtemplate
    ADD CONSTRAINT main_unifiedjobtemplate_modified_by_id_a8bf1de0_fk_auth_user_id FOREIGN KEY (modified_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_usersessionmembership main_usersessionmemb_session_id_fbab60a5_fk_django_se; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_usersessionmembership
    ADD CONSTRAINT main_usersessionmemb_session_id_fbab60a5_fk_django_se FOREIGN KEY (session_id) REFERENCES public.django_session(session_key) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_usersessionmembership main_usersessionmembership_user_id_fe163c98_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_usersessionmembership
    ADD CONSTRAINT main_usersessionmembership_user_id_fe163c98_fk_auth_user_id FOREIGN KEY (user_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowapproval main_workflowapprova_approved_or_denied_b_bb3eae41_fk_auth_user; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowapproval
    ADD CONSTRAINT main_workflowapprova_approved_or_denied_b_bb3eae41_fk_auth_user FOREIGN KEY (approved_or_denied_by_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowapproval main_workflowapprova_unifiedjob_ptr_id_b8cd5385_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowapproval
    ADD CONSTRAINT main_workflowapprova_unifiedjob_ptr_id_b8cd5385_fk_main_unif FOREIGN KEY (unifiedjob_ptr_id) REFERENCES public.main_unifiedjob(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowapprovaltemplate main_workflowapprova_unifiedjobtemplate_p_289f0768_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowapprovaltemplate
    ADD CONSTRAINT main_workflowapprova_unifiedjobtemplate_p_289f0768_fk_main_unif FOREIGN KEY (unifiedjobtemplate_ptr_id) REFERENCES public.main_unifiedjobtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowapproval main_workflowapprova_workflow_approval_te_b87dda8a_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowapproval
    ADD CONSTRAINT main_workflowapprova_workflow_approval_te_b87dda8a_fk_main_work FOREIGN KEY (workflow_approval_template_id) REFERENCES public.main_workflowapprovaltemplate(unifiedjobtemplate_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjob main_workflowjob_inventory_id_8c31355b_fk_main_inventory_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjob
    ADD CONSTRAINT main_workflowjob_inventory_id_8c31355b_fk_main_inventory_id FOREIGN KEY (inventory_id) REFERENCES public.main_inventory(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjob main_workflowjob_job_template_id_cceff2a3_fk_main_jobt; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjob
    ADD CONSTRAINT main_workflowjob_job_template_id_cceff2a3_fk_main_jobt FOREIGN KEY (job_template_id) REFERENCES public.main_jobtemplate(unifiedjobtemplate_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjob main_workflowjob_unifiedjob_ptr_id_2ee92d99_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjob
    ADD CONSTRAINT main_workflowjob_unifiedjob_ptr_id_2ee92d99_fk_main_unif FOREIGN KEY (unifiedjob_ptr_id) REFERENCES public.main_unifiedjob(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjob main_workflowjob_webhook_credential_i_57c9fece_fk_main_cred; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjob
    ADD CONSTRAINT main_workflowjob_webhook_credential_i_57c9fece_fk_main_cred FOREIGN KEY (webhook_credential_id) REFERENCES public.main_credential(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjob main_workflowjob_workflow_job_templat_0d9a93a0_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjob
    ADD CONSTRAINT main_workflowjob_workflow_job_templat_0d9a93a0_fk_main_work FOREIGN KEY (workflow_job_template_id) REFERENCES public.main_workflowjobtemplate(unifiedjobtemplate_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobinstancegroupmembership main_workflowjobinst_instancegroup_id_00dbe24d_fk_main_inst; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobinstancegroupmembership
    ADD CONSTRAINT main_workflowjobinst_instancegroup_id_00dbe24d_fk_main_inst FOREIGN KEY (instancegroup_id) REFERENCES public.main_instancegroup(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobinstancegroupmembership main_workflowjobinst_workflowjobnode_id_e18bb569_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobinstancegroupmembership
    ADD CONSTRAINT main_workflowjobinst_workflowjobnode_id_e18bb569_fk_main_work FOREIGN KEY (workflowjobnode_id) REFERENCES public.main_workflowjob(unifiedjob_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobnode_credentials main_workflowjobnode_credential_id_6de5a410_fk_main_cred; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode_credentials
    ADD CONSTRAINT main_workflowjobnode_credential_id_6de5a410_fk_main_cred FOREIGN KEY (credential_id) REFERENCES public.main_credential(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobnode main_workflowjobnode_execution_environmen_c593ca11_fk_main_exec; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode
    ADD CONSTRAINT main_workflowjobnode_execution_environmen_c593ca11_fk_main_exec FOREIGN KEY (execution_environment_id) REFERENCES public.main_executionenvironment(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobnode_always_nodes main_workflowjobnode_from_workflowjobnode_19edb9d7_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode_always_nodes
    ADD CONSTRAINT main_workflowjobnode_from_workflowjobnode_19edb9d7_fk_main_work FOREIGN KEY (from_workflowjobnode_id) REFERENCES public.main_workflowjobnode(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobnode_failure_nodes main_workflowjobnode_from_workflowjobnode_2172a110_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode_failure_nodes
    ADD CONSTRAINT main_workflowjobnode_from_workflowjobnode_2172a110_fk_main_work FOREIGN KEY (from_workflowjobnode_id) REFERENCES public.main_workflowjobnode(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobnode_success_nodes main_workflowjobnode_from_workflowjobnode_e04f9991_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode_success_nodes
    ADD CONSTRAINT main_workflowjobnode_from_workflowjobnode_e04f9991_fk_main_work FOREIGN KEY (from_workflowjobnode_id) REFERENCES public.main_workflowjobnode(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobnodebaseinstancegroupmembership main_workflowjobnode_instancegroup_id_4e4faca5_fk_main_inst; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnodebaseinstancegroupmembership
    ADD CONSTRAINT main_workflowjobnode_instancegroup_id_4e4faca5_fk_main_inst FOREIGN KEY (instancegroup_id) REFERENCES public.main_instancegroup(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobnode main_workflowjobnode_inventory_id_1dac2da9_fk_main_inventory_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode
    ADD CONSTRAINT main_workflowjobnode_inventory_id_1dac2da9_fk_main_inventory_id FOREIGN KEY (inventory_id) REFERENCES public.main_inventory(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobnode main_workflowjobnode_job_id_7d2de427_fk_main_unifiedjob_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode
    ADD CONSTRAINT main_workflowjobnode_job_id_7d2de427_fk_main_unifiedjob_id FOREIGN KEY (job_id) REFERENCES public.main_unifiedjob(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobnode_labels main_workflowjobnode_labels_label_id_0e6594a7_fk_main_label_id; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode_labels
    ADD CONSTRAINT main_workflowjobnode_labels_label_id_0e6594a7_fk_main_label_id FOREIGN KEY (label_id) REFERENCES public.main_label(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobnode_always_nodes main_workflowjobnode_to_workflowjobnode_i_0edcda07_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode_always_nodes
    ADD CONSTRAINT main_workflowjobnode_to_workflowjobnode_i_0edcda07_fk_main_work FOREIGN KEY (to_workflowjobnode_id) REFERENCES public.main_workflowjobnode(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobnode_failure_nodes main_workflowjobnode_to_workflowjobnode_i_d2e09d9c_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode_failure_nodes
    ADD CONSTRAINT main_workflowjobnode_to_workflowjobnode_i_d2e09d9c_fk_main_work FOREIGN KEY (to_workflowjobnode_id) REFERENCES public.main_workflowjobnode(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobnode_success_nodes main_workflowjobnode_to_workflowjobnode_i_e6c8cbb4_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode_success_nodes
    ADD CONSTRAINT main_workflowjobnode_to_workflowjobnode_i_e6c8cbb4_fk_main_work FOREIGN KEY (to_workflowjobnode_id) REFERENCES public.main_workflowjobnode(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobnode main_workflowjobnode_unified_job_template_8a30f93e_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode
    ADD CONSTRAINT main_workflowjobnode_unified_job_template_8a30f93e_fk_main_unif FOREIGN KEY (unified_job_template_id) REFERENCES public.main_unifiedjobtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobnode main_workflowjobnode_workflow_job_id_dcd715c7_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode
    ADD CONSTRAINT main_workflowjobnode_workflow_job_id_dcd715c7_fk_main_work FOREIGN KEY (workflow_job_id) REFERENCES public.main_workflowjob(unifiedjob_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobnode_labels main_workflowjobnode_workflowjobnode_id_14f419e1_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode_labels
    ADD CONSTRAINT main_workflowjobnode_workflowjobnode_id_14f419e1_fk_main_work FOREIGN KEY (workflowjobnode_id) REFERENCES public.main_workflowjobnode(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobnode_credentials main_workflowjobnode_workflowjobnode_id_31f8c02b_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnode_credentials
    ADD CONSTRAINT main_workflowjobnode_workflowjobnode_id_31f8c02b_fk_main_work FOREIGN KEY (workflowjobnode_id) REFERENCES public.main_workflowjobnode(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobnodebaseinstancegroupmembership main_workflowjobnode_workflowjobnode_id_47a05c0e_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobnodebaseinstancegroupmembership
    ADD CONSTRAINT main_workflowjobnode_workflowjobnode_id_47a05c0e_fk_main_work FOREIGN KEY (workflowjobnode_id) REFERENCES public.main_workflowjobnode(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplate main_workflowjobtemp_admin_role_id_5675a40e_fk_main_rbac; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplate
    ADD CONSTRAINT main_workflowjobtemp_admin_role_id_5675a40e_fk_main_rbac FOREIGN KEY (admin_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplate main_workflowjobtemp_approval_role_id_220f0de1_fk_main_rbac; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplate
    ADD CONSTRAINT main_workflowjobtemp_approval_role_id_220f0de1_fk_main_rbac FOREIGN KEY (approval_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplatenode_credentials main_workflowjobtemp_credential_id_e621c8d1_fk_main_cred; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode_credentials
    ADD CONSTRAINT main_workflowjobtemp_credential_id_e621c8d1_fk_main_cred FOREIGN KEY (credential_id) REFERENCES public.main_credential(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplate main_workflowjobtemp_execute_role_id_ad8970f4_fk_main_rbac; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplate
    ADD CONSTRAINT main_workflowjobtemp_execute_role_id_ad8970f4_fk_main_rbac FOREIGN KEY (execute_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplatenode main_workflowjobtemp_execution_environmen_ec5bba6d_fk_main_exec; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode
    ADD CONSTRAINT main_workflowjobtemp_execution_environmen_ec5bba6d_fk_main_exec FOREIGN KEY (execution_environment_id) REFERENCES public.main_executionenvironment(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplatenode_always_nodes main_workflowjobtemp_from_workflowjobtemp_8af14c32_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode_always_nodes
    ADD CONSTRAINT main_workflowjobtemp_from_workflowjobtemp_8af14c32_fk_main_work FOREIGN KEY (from_workflowjobtemplatenode_id) REFERENCES public.main_workflowjobtemplatenode(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplatenode_success_nodes main_workflowjobtemp_from_workflowjobtemp_9e16f49d_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode_success_nodes
    ADD CONSTRAINT main_workflowjobtemp_from_workflowjobtemp_9e16f49d_fk_main_work FOREIGN KEY (from_workflowjobtemplatenode_id) REFERENCES public.main_workflowjobtemplatenode(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplatenode_failure_nodes main_workflowjobtemp_from_workflowjobtemp_fa405230_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode_failure_nodes
    ADD CONSTRAINT main_workflowjobtemp_from_workflowjobtemp_fa405230_fk_main_work FOREIGN KEY (from_workflowjobtemplatenode_id) REFERENCES public.main_workflowjobtemplatenode(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplatenodebaseinstancegroupmembership main_workflowjobtemp_instancegroup_id_0c59a80a_fk_main_inst; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenodebaseinstancegroupmembership
    ADD CONSTRAINT main_workflowjobtemp_instancegroup_id_0c59a80a_fk_main_inst FOREIGN KEY (instancegroup_id) REFERENCES public.main_instancegroup(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplatenode main_workflowjobtemp_inventory_id_2fab864f_fk_main_inve; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode
    ADD CONSTRAINT main_workflowjobtemp_inventory_id_2fab864f_fk_main_inve FOREIGN KEY (inventory_id) REFERENCES public.main_inventory(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplate main_workflowjobtemp_inventory_id_99929499_fk_main_inve; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplate
    ADD CONSTRAINT main_workflowjobtemp_inventory_id_99929499_fk_main_inve FOREIGN KEY (inventory_id) REFERENCES public.main_inventory(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplatenode_labels main_workflowjobtemp_label_id_b3f1a57f_fk_main_labe; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode_labels
    ADD CONSTRAINT main_workflowjobtemp_label_id_b3f1a57f_fk_main_labe FOREIGN KEY (label_id) REFERENCES public.main_label(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplate_notification_templates_approvals main_workflowjobtemp_notificationtemplate_3811d35e_fk_main_noti; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplate_notification_templates_approvals
    ADD CONSTRAINT main_workflowjobtemp_notificationtemplate_3811d35e_fk_main_noti FOREIGN KEY (notificationtemplate_id) REFERENCES public.main_notificationtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplate main_workflowjobtemp_read_role_id_acdd95ef_fk_main_rbac; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplate
    ADD CONSTRAINT main_workflowjobtemp_read_role_id_acdd95ef_fk_main_rbac FOREIGN KEY (read_role_id) REFERENCES public.main_rbac_roles(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplatenode_failure_nodes main_workflowjobtemp_to_workflowjobtempla_2c1db0ae_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode_failure_nodes
    ADD CONSTRAINT main_workflowjobtemp_to_workflowjobtempla_2c1db0ae_fk_main_work FOREIGN KEY (to_workflowjobtemplatenode_id) REFERENCES public.main_workflowjobtemplatenode(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplatenode_always_nodes main_workflowjobtemp_to_workflowjobtempla_6fe11708_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode_always_nodes
    ADD CONSTRAINT main_workflowjobtemp_to_workflowjobtempla_6fe11708_fk_main_work FOREIGN KEY (to_workflowjobtemplatenode_id) REFERENCES public.main_workflowjobtemplatenode(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplatenode_success_nodes main_workflowjobtemp_to_workflowjobtempla_f16ee478_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode_success_nodes
    ADD CONSTRAINT main_workflowjobtemp_to_workflowjobtempla_f16ee478_fk_main_work FOREIGN KEY (to_workflowjobtemplatenode_id) REFERENCES public.main_workflowjobtemplatenode(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplatenode main_workflowjobtemp_unified_job_template_98b53e6c_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode
    ADD CONSTRAINT main_workflowjobtemp_unified_job_template_98b53e6c_fk_main_unif FOREIGN KEY (unified_job_template_id) REFERENCES public.main_unifiedjobtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplate main_workflowjobtemp_unifiedjobtemplate_p_3854248b_fk_main_unif; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplate
    ADD CONSTRAINT main_workflowjobtemp_unifiedjobtemplate_p_3854248b_fk_main_unif FOREIGN KEY (unifiedjobtemplate_ptr_id) REFERENCES public.main_unifiedjobtemplate(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplate main_workflowjobtemp_webhook_credential_i_ced1ad89_fk_main_cred; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplate
    ADD CONSTRAINT main_workflowjobtemp_webhook_credential_i_ced1ad89_fk_main_cred FOREIGN KEY (webhook_credential_id) REFERENCES public.main_credential(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplatenode main_workflowjobtemp_workflow_job_templat_2fd591f0_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode
    ADD CONSTRAINT main_workflowjobtemp_workflow_job_templat_2fd591f0_fk_main_work FOREIGN KEY (workflow_job_template_id) REFERENCES public.main_workflowjobtemplate(unifiedjobtemplate_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplate_notification_templates_approvals main_workflowjobtemp_workflowjobtemplate__ce7a17be_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplate_notification_templates_approvals
    ADD CONSTRAINT main_workflowjobtemp_workflowjobtemplate__ce7a17be_fk_main_work FOREIGN KEY (workflowjobtemplate_id) REFERENCES public.main_workflowjobtemplate(unifiedjobtemplate_ptr_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplatenode_credentials main_workflowjobtemp_workflowjobtemplaten_b91efe86_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode_credentials
    ADD CONSTRAINT main_workflowjobtemp_workflowjobtemplaten_b91efe86_fk_main_work FOREIGN KEY (workflowjobtemplatenode_id) REFERENCES public.main_workflowjobtemplatenode(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplatenode_labels main_workflowjobtemp_workflowjobtemplaten_f75998d4_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenode_labels
    ADD CONSTRAINT main_workflowjobtemp_workflowjobtemplaten_f75998d4_fk_main_work FOREIGN KEY (workflowjobtemplatenode_id) REFERENCES public.main_workflowjobtemplatenode(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: main_workflowjobtemplatenodebaseinstancegroupmembership main_workflowjobtemp_workflowjobtemplaten_fa0959c5_fk_main_work; Type: FK CONSTRAINT; Schema: public; Owner: awx
--

ALTER TABLE ONLY public.main_workflowjobtemplatenodebaseinstancegroupmembership
    ADD CONSTRAINT main_workflowjobtemp_workflowjobtemplaten_fa0959c5_fk_main_work FOREIGN KEY (workflowjobtemplatenode_id) REFERENCES public.main_workflowjobtemplatenode(id) DEFERRABLE INITIALLY DEFERRED;


--
-- PostgreSQL database dump complete
--

