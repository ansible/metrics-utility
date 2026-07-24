--
-- PostgreSQL database dump
--

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
-- Data for Name: auth_user; Type: TABLE DATA; Schema: public; Owner: awx
--

SET SESSION AUTHORIZATION DEFAULT;

ALTER TABLE public.auth_user DISABLE TRIGGER ALL;

COPY public.auth_user (id, password, last_login, is_superuser, username, first_name, last_name, email, is_staff, is_active, date_joined) FROM stdin;
1	pbkdf2_sha256$1000000$jGFgUcijpfNfWQBqo6A42j$eQLEuWYF0sovXUtiwaJ5tucZWF4HN4wm4b9wmgLIONs=	\N	t	admin			admin@localhost	t	t	2026-07-24 17:34:59.994394+00
\.

ALTER TABLE public.auth_user ENABLE TRIGGER ALL;

--
-- Data for Name: django_content_type; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.django_content_type DISABLE TRIGGER ALL;

COPY public.django_content_type (id, app_label, model) FROM stdin;
1	main	systemjobtemplate
2	auth	user
3	main	inventorysource
4	main	jobtemplate
5	main	project
6	main	workflowjobtemplate
7	main	adhoccommand
8	main	inventoryupdate
9	main	job
10	main	projectupdate
11	main	workflowjob
12	main	activitystream
13	main	adhoccommandevent
14	main	credential
15	main	custominventoryscript
16	main	group
17	main	host
18	main	instance
19	main	inventory
20	main	jobevent
21	main	jobhostsummary
22	main	organization
23	main	profile
24	main	schedule
25	main	team
26	main	unifiedjob
27	main	unifiedjobtemplate
28	main	systemjob
29	main	notification
30	main	notificationtemplate
31	main	role
32	main	roleancestorentry
33	main	label
34	main	workflowjobnode
35	main	workflowjobtemplatenode
36	main	towerschedulestate
37	main	smartinventorymembership
38	main	credentialtype
39	main	instancegroup
40	main	joblaunchconfig
41	main	unifiedjobdeprecatedstdout
42	main	inventoryupdateevent
43	main	projectupdateevent
44	main	systemjobevent
45	main	usersessionmembership
46	main	oauth2application
47	main	oauth2accesstoken
48	main	credentialinputsource
49	main	inventoryinstancegroupmembership
50	main	organizationinstancegroupmembership
51	main	unifiedjobtemplateinstancegroupmembership
52	main	workflowapprovaltemplate
53	main	workflowapproval
54	main	organizationgalaxycredentialmembership
55	main	executionenvironment
56	main	hostmetric
57	main	unpartitionedadhoccommandevent
58	main	unpartitionedinventoryupdateevent
59	main	unpartitionedjobevent
60	main	unpartitionedprojectupdateevent
61	main	unpartitionedsystemjobevent
62	main	instancelink
63	main	workflowjobtemplatenodebaseinstancegroupmembership
64	main	workflowjobnodebaseinstancegroupmembership
65	main	workflowjobinstancegroupmembership
66	main	scheduleinstancegroupmembership
67	main	joblaunchconfiginstancegroupmembership
68	main	hostmetricsummarymonthly
69	main	inventoryconstructedinventorymembership
70	main	receptoraddress
71	auth	permission
72	auth	group
73	contenttypes	contenttype
74	sessions	session
75	sites	site
76	conf	setting
77	main	eventquery
78	main	indirectmanagednodeaudit
79	main	inventorygroupvariableswithhistory
80	dab_resource_registry	serviceid
81	dab_resource_registry	resourcetype
82	dab_resource_registry	resource
83	dab_rbac	roledefinition
84	dab_feature_flags	aapflag
85	dab_rbac	dabpermission
86	dab_rbac	roleteamassignment
87	dab_rbac	roleuserassignment
88	dab_rbac	objectrole
89	dab_rbac	roleevaluation
90	dab_rbac	roleevaluationuuid
91	dab_rbac	dabcontenttype
92	flags	flagstate
\.

ALTER TABLE public.django_content_type ENABLE TRIGGER ALL;

--
-- Data for Name: main_credentialtype; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_credentialtype DISABLE TRIGGER ALL;

COPY public.main_credentialtype (id, created, modified, description, name, kind, managed, inputs, injectors, created_by_id, modified_by_id, namespace) FROM stdin;
1	2026-07-24 17:33:51.529401+00	2026-07-24 17:35:27.203125+00		Red Hat Ansible Automation Platform	cloud	t	{}	{}	\N	\N	controller
2	2026-07-24 17:33:51.531211+00	2026-07-24 17:35:27.238972+00		Ansible Galaxy/Automation Hub API Token	galaxy	t	{}	{}	\N	\N	galaxy_api_token
3	2026-07-24 17:33:51.532356+00	2026-07-24 17:35:27.244657+00		OpenShift or Kubernetes API Bearer Token	kubernetes	t	{}	{}	\N	\N	kubernetes_bearer_token
4	2026-07-24 17:33:51.533363+00	2026-07-24 17:35:27.251387+00		Network	net	t	{}	{}	\N	\N	net
5	2026-07-24 17:33:51.534227+00	2026-07-24 17:35:27.260132+00		Container Registry	registry	t	{}	{}	\N	\N	registry
6	2026-07-24 17:33:51.535241+00	2026-07-24 17:35:27.265455+00		Source Control	scm	t	{}	{}	\N	\N	scm
7	2026-07-24 17:33:51.536169+00	2026-07-24 17:35:27.272315+00		Machine	ssh	t	{}	{}	\N	\N	ssh
8	2026-07-24 17:33:51.537015+00	2026-07-24 17:35:27.278561+00		Vault	vault	t	{}	{}	\N	\N	vault
\.

ALTER TABLE public.main_credentialtype ENABLE TRIGGER ALL;

--
-- Data for Name: main_rbac_roles; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_rbac_roles DISABLE TRIGGER ALL;

COPY public.main_rbac_roles (id, role_field, singleton_name, implicit_parents, content_type_id, object_id) FROM stdin;
1	system_administrator	system_administrator	[]	\N	\N
2	admin_role	\N	[]	22	1
3	execute_role	\N	[]	22	1
4	project_admin_role	\N	[]	22	1
5	inventory_admin_role	\N	[]	22	1
6	credential_admin_role	\N	[]	22	1
7	workflow_admin_role	\N	[]	22	1
8	notification_admin_role	\N	[]	22	1
9	job_template_admin_role	\N	[]	22	1
10	execution_environment_admin_role	\N	[]	22	1
11	auditor_role	\N	[]	22	1
12	member_role	\N	[]	22	1
13	read_role	\N	[]	22	1
14	approval_role	\N	[]	22	1
15	admin_role	\N	[]	5	5
16	use_role	\N	[]	5	5
17	update_role	\N	[]	5	5
18	read_role	\N	[]	5	5
19	admin_role	\N	[]	14	1
20	use_role	\N	[]	14	1
21	read_role	\N	[]	14	1
22	admin_role	\N	[]	14	2
23	use_role	\N	[]	14	2
24	read_role	\N	[]	14	2
25	admin_role	\N	[]	19	1
26	update_role	\N	[]	19	1
27	adhoc_role	\N	[]	19	1
28	use_role	\N	[]	19	1
29	read_role	\N	[]	19	1
30	admin_role	\N	[]	4	6
31	execute_role	\N	[]	4	6
32	read_role	\N	[]	4	6
33	admin_role	\N	[]	39	1
34	use_role	\N	[]	39	1
35	read_role	\N	[]	39	1
36	admin_role	\N	[]	39	2
37	use_role	\N	[]	39	2
38	read_role	\N	[]	39	2
\.

ALTER TABLE public.main_rbac_roles ENABLE TRIGGER ALL;

--
-- Data for Name: main_credential; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_credential DISABLE TRIGGER ALL;

COPY public.main_credential (id, created, modified, description, name, created_by_id, modified_by_id, organization_id, admin_role_id, use_role_id, read_role_id, inputs, credential_type_id, managed) FROM stdin;
1	2026-07-24 17:35:03.345293+00	2026-07-24 17:35:03.345298+00		Demo Credential	1	1	1	19	20	21	{"username": "admin"}	7	f
2	2026-07-24 17:35:03.394911+00	2026-07-24 17:35:03.394915+00		Ansible Galaxy	1	1	\N	22	23	24	{"url": "https://galaxy.ansible.com/"}	2	t
\.

ALTER TABLE public.main_credential ENABLE TRIGGER ALL;

--
-- Data for Name: main_organization; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_organization DISABLE TRIGGER ALL;

COPY public.main_organization (id, created, modified, description, name, created_by_id, modified_by_id, admin_role_id, auditor_role_id, member_role_id, read_role_id, custom_virtualenv, execute_role_id, job_template_admin_role_id, credential_admin_role_id, inventory_admin_role_id, project_admin_role_id, workflow_admin_role_id, notification_admin_role_id, max_hosts, approval_role_id, default_environment_id, execution_environment_admin_role_id, opa_query_path) FROM stdin;
1	2026-07-24 17:35:03.274506+00	2026-07-24 17:35:03.274511+00		Default	1	1	2	11	12	13	\N	3	9	6	5	4	7	8	0	14	\N	10	\N
\.

ALTER TABLE public.main_organization ENABLE TRIGGER ALL;

--
-- Data for Name: main_executionenvironment; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_executionenvironment DISABLE TRIGGER ALL;

COPY public.main_executionenvironment (id, created, modified, description, image, managed, created_by_id, credential_id, modified_by_id, organization_id, name, pull) FROM stdin;
1	2026-07-24 17:35:06.418741+00	2026-07-24 17:35:06.4408+00		quay.io/ansible/awx-ee:latest	f	\N	\N	\N	\N	AWX EE (latest)	
2	2026-07-24 17:35:06.445912+00	2026-07-24 17:35:06.452243+00		quay.io/ansible/awx-ee:latest	t	\N	\N	\N	\N	Control Plane Execution Environment	
\.

ALTER TABLE public.main_executionenvironment ENABLE TRIGGER ALL;

--
-- Data for Name: main_instancegroup; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_instancegroup DISABLE TRIGGER ALL;

COPY public.main_instancegroup (id, name, created, modified, policy_instance_list, policy_instance_minimum, policy_instance_percentage, credential_id, pod_spec_override, is_container_group, max_concurrent_jobs, max_forks, admin_role_id, read_role_id, use_role_id) FROM stdin;
1	controlplane	2026-07-24 17:35:15.442031+00	2026-07-24 17:35:15.45417+00	[]	0	100	\N		f	0	0	33	35	34
2	default	2026-07-24 17:35:18.388435+00	2026-07-24 17:35:18.399606+00	[]	0	100	\N		f	0	0	36	38	37
\.

ALTER TABLE public.main_instancegroup ENABLE TRIGGER ALL;

--
-- Data for Name: main_inventory; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_inventory DISABLE TRIGGER ALL;

COPY public.main_inventory (id, created, modified, description, name, variables, has_active_failures, total_hosts, hosts_with_active_failures, total_groups, has_inventory_sources, total_inventory_sources, inventory_sources_with_failures, created_by_id, modified_by_id, organization_id, admin_role_id, adhoc_role_id, update_role_id, use_role_id, read_role_id, host_filter, kind, pending_deletion, prevent_instance_group_fallback, opa_query_path) FROM stdin;
1	2026-07-24 17:35:03.412157+00	2026-07-24 17:35:03.412161+00		Demo Inventory		f	0	0	0	f	0	0	1	1	1	25	27	26	28	29	\N		f	f	\N
\.

ALTER TABLE public.main_inventory ENABLE TRIGGER ALL;

--
-- Data for Name: main_schedule; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_schedule DISABLE TRIGGER ALL;

COPY public.main_schedule (id, created, modified, description, name, enabled, dtstart, dtend, rrule, next_run, extra_data, created_by_id, modified_by_id, unified_job_template_id, char_prompts, inventory_id, survey_passwords, execution_environment_id) FROM stdin;
1	2026-07-24 17:33:22.242505+00	2026-07-24 17:33:22.242505+00	Automatically Generated Schedule	Cleanup Job Schedule	t	2026-07-26 17:33:22+00	\N	DTSTART:20260724T173322Z RRULE:FREQ=WEEKLY;INTERVAL=1;BYDAY=SU	2026-07-26 17:33:22+00	{"days": "120"}	\N	\N	1	{}	\N	{}	\N
2	2026-07-24 17:33:22.242505+00	2026-07-24 17:33:22.242505+00	Automatically Generated Schedule	Cleanup Activity Schedule	t	2026-07-28 17:33:22+00	\N	DTSTART:20260724T173322Z RRULE:FREQ=WEEKLY;INTERVAL=1;BYDAY=TU	2026-07-28 17:33:22+00	{"days": "355"}	\N	\N	2	{}	\N	{}	\N
4	2026-07-24 17:33:55.201777+00	2026-07-24 17:33:55.201777+00	Cleans out expired browser sessions	Cleanup Expired Sessions	t	2026-07-24 17:33:55+00	\N	DTSTART:20260724T173355Z RRULE:FREQ=WEEKLY;INTERVAL=1	2026-07-31 17:33:55+00	{}	\N	\N	4	{}	\N	{}	\N
\.

ALTER TABLE public.main_schedule ENABLE TRIGGER ALL;

--
-- Data for Name: main_unifiedjob; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjob DISABLE TRIGGER ALL;

COPY public.main_unifiedjob (id, created, modified, description, name, old_pk, launch_type, cancel_flag, status, failed, started, finished, elapsed, job_args, job_cwd, job_explanation, start_args, result_stdout_text, result_traceback, celery_task_id, created_by_id, modified_by_id, polymorphic_ctype_id, schedule_id, unified_job_template_id, execution_node, instance_group_id, emitted_events, controller_node, canceled_on, dependencies_processed, organization_id, execution_environment_id, installed_collections, ansible_version, work_unit_id, host_status_counts, preferred_instance_groups_cache, task_impact, job_env) FROM stdin;
\.

ALTER TABLE public.main_unifiedjob ENABLE TRIGGER ALL;

--
-- Data for Name: main_adhoccommand; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_adhoccommand DISABLE TRIGGER ALL;

COPY public.main_adhoccommand (unifiedjob_ptr_id, job_type, "limit", module_name, module_args, forks, verbosity, become_enabled, credential_id, inventory_id, extra_vars, diff_mode) FROM stdin;
\.

ALTER TABLE public.main_adhoccommand ENABLE TRIGGER ALL;

--
-- Data for Name: main_unifiedjobtemplate; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjobtemplate DISABLE TRIGGER ALL;

COPY public.main_unifiedjobtemplate (id, created, modified, description, name, old_pk, last_job_failed, last_job_run, next_job_run, status, created_by_id, current_job_id, last_job_id, modified_by_id, next_schedule_id, polymorphic_ctype_id, organization_id, execution_environment_id, org_unique) FROM stdin;
5	2026-07-24 17:35:03.289657+00	2026-07-24 17:35:03.289661+00		Demo Project	\N	f	\N	\N	successful	1	\N	\N	1	\N	5	1	\N	t
6	2026-07-24 17:35:03.429795+00	2026-07-24 17:35:03.429799+00		Demo Job Template	\N	f	\N	\N	never updated	1	\N	\N	1	\N	4	1	\N	t
1	2026-07-24 17:33:22.242505+00	2026-07-24 17:33:22.242505+00	Remove job history	Cleanup Job Details	\N	f	\N	2026-07-26 17:33:22+00	ok	\N	\N	\N	\N	1	1	\N	\N	t
2	2026-07-24 17:33:22.242505+00	2026-07-24 17:33:22.242505+00	Remove activity stream history	Cleanup Activity Stream	\N	f	\N	2026-07-28 17:33:22+00	ok	\N	\N	\N	\N	2	1	\N	\N	t
4	2026-07-24 17:33:55.201777+00	2026-07-24 17:33:55.201777+00	Cleans out expired browser sessions	Cleanup Expired Sessions	\N	f	\N	2026-07-31 17:33:55+00	ok	\N	\N	\N	\N	4	1	\N	\N	t
\.

ALTER TABLE public.main_unifiedjobtemplate ENABLE TRIGGER ALL;

--
-- Data for Name: main_project; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_project DISABLE TRIGGER ALL;

COPY public.main_project (unifiedjobtemplate_ptr_id, local_path, scm_type, scm_url, scm_branch, scm_clean, scm_delete_on_update, scm_update_on_launch, scm_update_cache_timeout, credential_id, admin_role_id, use_role_id, update_role_id, read_role_id, timeout, scm_revision, playbook_files, inventory_files, custom_virtualenv, scm_refspec, allow_override, default_environment_id, scm_track_submodules, signature_validation_credential_id) FROM stdin;
5	_5__demo_project	git	https://github.com/ansible/ansible-tower-samples		f	f	f	0	\N	15	16	17	18	0	347e44fea036c94d5f60e544de006453ee5c71ad	["hello_world.yml"]	[]	\N		f	\N	f	\N
\.

ALTER TABLE public.main_project ENABLE TRIGGER ALL;

--
-- Data for Name: main_jobtemplate; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_jobtemplate DISABLE TRIGGER ALL;

COPY public.main_jobtemplate (unifiedjobtemplate_ptr_id, job_type, playbook, forks, "limit", verbosity, extra_vars, job_tags, force_handlers, skip_tags, start_at_task, become_enabled, host_config_key, ask_variables_on_launch, survey_enabled, survey_spec, inventory_id, project_id, admin_role_id, execute_role_id, read_role_id, ask_limit_on_launch, ask_inventory_on_launch, ask_credential_on_launch, ask_job_type_on_launch, ask_tags_on_launch, allow_simultaneous, ask_skip_tags_on_launch, timeout, use_fact_cache, ask_verbosity_on_launch, ask_diff_mode_on_launch, diff_mode, custom_virtualenv, job_slice_count, ask_scm_branch_on_launch, scm_branch, webhook_credential_id, webhook_key, webhook_service, ask_execution_environment_on_launch, ask_forks_on_launch, ask_instance_groups_on_launch, ask_job_slice_count_on_launch, ask_labels_on_launch, ask_timeout_on_launch, prevent_instance_group_fallback, opa_query_path) FROM stdin;
6	run	hello_world.yml	0		0			f			f		f	f	{}	1	5	30	31	32	f	f	f	f	f	f	f	0	f	f	f	f	\N	1	f		\N			f	f	f	f	f	f	f	\N
\.

ALTER TABLE public.main_jobtemplate ENABLE TRIGGER ALL;

--
-- Data for Name: main_projectupdate; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_projectupdate DISABLE TRIGGER ALL;

COPY public.main_projectupdate (unifiedjob_ptr_id, local_path, scm_type, scm_url, scm_branch, scm_clean, scm_delete_on_update, credential_id, project_id, timeout, job_type, scm_refspec, scm_revision, job_tags, scm_track_submodules) FROM stdin;
\.

ALTER TABLE public.main_projectupdate ENABLE TRIGGER ALL;

--
-- Data for Name: main_job; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_job DISABLE TRIGGER ALL;

COPY public.main_job (unifiedjob_ptr_id, job_type, playbook, forks, "limit", verbosity, extra_vars, job_tags, force_handlers, skip_tags, start_at_task, become_enabled, inventory_id, job_template_id, project_id, allow_simultaneous, artifacts, timeout, scm_revision, project_update_id, use_fact_cache, diff_mode, job_slice_count, job_slice_number, custom_virtualenv, scm_branch, webhook_credential_id, webhook_guid, webhook_service, survey_passwords, event_queries_processed) FROM stdin;
\.

ALTER TABLE public.main_job ENABLE TRIGGER ALL;

--
-- Data for Name: main_host; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_host DISABLE TRIGGER ALL;

COPY public.main_host (id, created, modified, description, name, enabled, instance_id, variables, created_by_id, inventory_id, last_job_host_summary_id, modified_by_id, last_job_id, ansible_facts, ansible_facts_modified) FROM stdin;
1	2026-07-24 17:35:03.422701+00	2026-07-24 17:35:03.422705+00		localhost	t		ansible_connection: local\nansible_python_interpreter: '{{ ansible_playbook_python }}'	1	1	\N	1	\N	{}	\N
\.

ALTER TABLE public.main_host ENABLE TRIGGER ALL;

--
-- Data for Name: _unpartitioned_main_adhoccommandevent; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public._unpartitioned_main_adhoccommandevent DISABLE TRIGGER ALL;

COPY public._unpartitioned_main_adhoccommandevent (id, created, modified, host_name, event, event_data, failed, changed, counter, host_id, ad_hoc_command_id, end_line, start_line, stdout, uuid, verbosity) FROM stdin;
\.

ALTER TABLE public._unpartitioned_main_adhoccommandevent ENABLE TRIGGER ALL;

--
-- Data for Name: main_inventorysource; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_inventorysource DISABLE TRIGGER ALL;

COPY public.main_inventorysource (unifiedjobtemplate_ptr_id, source, source_path, source_vars, overwrite, overwrite_vars, update_on_launch, update_cache_timeout, inventory_id, timeout, source_project_id, verbosity, custom_virtualenv, enabled_value, enabled_var, host_filter, scm_branch, "limit") FROM stdin;
\.

ALTER TABLE public.main_inventorysource ENABLE TRIGGER ALL;

--
-- Data for Name: main_inventoryupdate; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_inventoryupdate DISABLE TRIGGER ALL;

COPY public.main_inventoryupdate (unifiedjob_ptr_id, source, source_path, source_vars, overwrite, overwrite_vars, license_error, inventory_source_id, timeout, source_project_update_id, verbosity, inventory_id, custom_virtualenv, org_host_limit_error, enabled_value, enabled_var, host_filter, scm_revision, scm_branch, "limit") FROM stdin;
\.

ALTER TABLE public.main_inventoryupdate ENABLE TRIGGER ALL;

--
-- Data for Name: _unpartitioned_main_inventoryupdateevent; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public._unpartitioned_main_inventoryupdateevent DISABLE TRIGGER ALL;

COPY public._unpartitioned_main_inventoryupdateevent (id, created, modified, event_data, uuid, counter, stdout, verbosity, start_line, end_line, inventory_update_id) FROM stdin;
\.

ALTER TABLE public._unpartitioned_main_inventoryupdateevent ENABLE TRIGGER ALL;

--
-- Data for Name: _unpartitioned_main_jobevent; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public._unpartitioned_main_jobevent DISABLE TRIGGER ALL;

COPY public._unpartitioned_main_jobevent (id, created, modified, event, event_data, failed, changed, host_name, play, role, task, counter, host_id, job_id, uuid, parent_uuid, end_line, playbook, start_line, stdout, verbosity) FROM stdin;
\.

ALTER TABLE public._unpartitioned_main_jobevent ENABLE TRIGGER ALL;

--
-- Data for Name: _unpartitioned_main_projectupdateevent; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public._unpartitioned_main_projectupdateevent DISABLE TRIGGER ALL;

COPY public._unpartitioned_main_projectupdateevent (id, created, modified, event, event_data, failed, changed, uuid, playbook, play, role, task, counter, stdout, verbosity, start_line, end_line, project_update_id) FROM stdin;
\.

ALTER TABLE public._unpartitioned_main_projectupdateevent ENABLE TRIGGER ALL;

--
-- Data for Name: main_systemjobtemplate; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_systemjobtemplate DISABLE TRIGGER ALL;

COPY public.main_systemjobtemplate (unifiedjobtemplate_ptr_id, job_type) FROM stdin;
1	cleanup_jobs
2	cleanup_activitystream
4	cleanup_sessions
\.

ALTER TABLE public.main_systemjobtemplate ENABLE TRIGGER ALL;

--
-- Data for Name: main_systemjob; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_systemjob DISABLE TRIGGER ALL;

COPY public.main_systemjob (unifiedjob_ptr_id, job_type, extra_vars, system_job_template_id) FROM stdin;
\.

ALTER TABLE public.main_systemjob ENABLE TRIGGER ALL;

--
-- Data for Name: _unpartitioned_main_systemjobevent; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public._unpartitioned_main_systemjobevent DISABLE TRIGGER ALL;

COPY public._unpartitioned_main_systemjobevent (id, created, modified, event_data, uuid, counter, stdout, verbosity, start_line, end_line, system_job_id) FROM stdin;
\.

ALTER TABLE public._unpartitioned_main_systemjobevent ENABLE TRIGGER ALL;

--
-- Data for Name: auth_group; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.auth_group DISABLE TRIGGER ALL;

COPY public.auth_group (id, name) FROM stdin;
\.

ALTER TABLE public.auth_group ENABLE TRIGGER ALL;

--
-- Data for Name: auth_permission; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.auth_permission DISABLE TRIGGER ALL;

COPY public.auth_permission (id, name, content_type_id, codename) FROM stdin;
1	Can add permission	71	add_permission
2	Can change permission	71	change_permission
3	Can delete permission	71	delete_permission
4	Can view permission	71	view_permission
5	Can add group	72	add_group
6	Can change group	72	change_group
7	Can delete group	72	delete_group
8	Can view group	72	view_group
9	Can add user	2	add_user
10	Can change user	2	change_user
11	Can delete user	2	delete_user
12	Can view user	2	view_user
13	Can add content type	73	add_contenttype
14	Can change content type	73	change_contenttype
15	Can delete content type	73	delete_contenttype
16	Can view content type	73	view_contenttype
17	Can add session	74	add_session
18	Can change session	74	change_session
19	Can delete session	74	delete_session
20	Can view session	74	view_session
21	Can add site	75	add_site
22	Can change site	75	change_site
23	Can delete site	75	delete_site
24	Can view site	75	view_site
25	Can add setting	76	add_setting
26	Can change setting	76	change_setting
27	Can delete setting	76	delete_setting
28	Can view setting	76	view_setting
29	Can add activity stream	12	add_activitystream
30	Can change activity stream	12	change_activitystream
31	Can delete activity stream	12	delete_activitystream
32	Can view activity stream	12	view_activitystream
33	Can add ad hoc command event	13	add_adhoccommandevent
34	Can change ad hoc command event	13	change_adhoccommandevent
35	Can delete ad hoc command event	13	delete_adhoccommandevent
36	Can view ad hoc command event	13	view_adhoccommandevent
37	Can add credential	14	add_credential
38	Can change credential	14	change_credential
39	Can delete credential	14	delete_credential
40	Can view credential	14	view_credential
41	Can use credential in a job or related resource	14	use_credential
42	Can add custom inventory script	15	add_custominventoryscript
43	Can change custom inventory script	15	change_custominventoryscript
44	Can delete custom inventory script	15	delete_custominventoryscript
45	Can view custom inventory script	15	view_custominventoryscript
46	Can add group	16	add_group
47	Can change group	16	change_group
48	Can delete group	16	delete_group
49	Can view group	16	view_group
50	Can add host	17	add_host
51	Can change host	17	change_host
52	Can delete host	17	delete_host
53	Can view host	17	view_host
54	Can add instance	18	add_instance
55	Can change instance	18	change_instance
56	Can delete instance	18	delete_instance
57	Can view instance	18	view_instance
58	Can add inventory	19	add_inventory
59	Can change inventory	19	change_inventory
60	Can delete inventory	19	delete_inventory
61	Can view inventory	19	view_inventory
62	Can use inventory in a job template	19	use_inventory
63	Can run ad hoc commands	19	adhoc_inventory
64	Can update inventory sources in inventory	19	update_inventory
65	Can add job event	20	add_jobevent
66	Can change job event	20	change_jobevent
67	Can delete job event	20	delete_jobevent
68	Can view job event	20	view_jobevent
69	Can add job host summary	21	add_jobhostsummary
70	Can change job host summary	21	change_jobhostsummary
71	Can delete job host summary	21	delete_jobhostsummary
72	Can view job host summary	21	view_jobhostsummary
73	Can change organization	22	change_organization
74	Can delete organization	22	delete_organization
75	Can view organization	22	view_organization
76	Basic participation permissions for organization	22	member_organization
77	Audit everything inside the organization	22	audit_organization
78	Can add schedule	24	add_schedule
79	Can change schedule	24	change_schedule
80	Can delete schedule	24	delete_schedule
81	Can view schedule	24	view_schedule
82	Can add team	25	add_team
83	Can change team	25	change_team
84	Can delete team	25	delete_team
85	Can view team	25	view_team
86	Inherit all roles assigned to this team	25	member_team
87	Can add unified job	26	add_unifiedjob
88	Can change unified job	26	change_unifiedjob
89	Can delete unified job	26	delete_unifiedjob
90	Can view unified job	26	view_unifiedjob
91	Can add unified job template	27	add_unifiedjobtemplate
92	Can change unified job template	27	change_unifiedjobtemplate
93	Can delete unified job template	27	delete_unifiedjobtemplate
94	Can view unified job template	27	view_unifiedjobtemplate
95	Can add ad hoc command	7	add_adhoccommand
96	Can change ad hoc command	7	change_adhoccommand
97	Can delete ad hoc command	7	delete_adhoccommand
98	Can view ad hoc command	7	view_adhoccommand
99	Can add inventory source	3	add_inventorysource
100	Can change inventory source	3	change_inventorysource
101	Can delete inventory source	3	delete_inventorysource
102	Can view inventory source	3	view_inventorysource
103	Can add inventory update	8	add_inventoryupdate
104	Can change inventory update	8	change_inventoryupdate
105	Can delete inventory update	8	delete_inventoryupdate
106	Can view inventory update	8	view_inventoryupdate
107	Can add job	9	add_job
108	Can change job	9	change_job
109	Can delete job	9	delete_job
110	Can view job	9	view_job
111	Can change job template	4	change_jobtemplate
112	Can delete job template	4	delete_jobtemplate
113	Can view job template	4	view_jobtemplate
114	Can run this job template	4	execute_jobtemplate
115	Can add project	5	add_project
116	Can change project	5	change_project
117	Can delete project	5	delete_project
118	Can view project	5	view_project
119	Can run a project update	5	update_project
120	Can use project in a job template	5	use_project
121	Can add project update	10	add_projectupdate
122	Can change project update	10	change_projectupdate
123	Can delete project update	10	delete_projectupdate
124	Can view project update	10	view_projectupdate
125	Can add system job	28	add_systemjob
126	Can change system job	28	change_systemjob
127	Can delete system job	28	delete_systemjob
128	Can view system job	28	view_systemjob
129	Can add system job template	1	add_systemjobtemplate
130	Can change system job template	1	change_systemjobtemplate
131	Can delete system job template	1	delete_systemjobtemplate
132	Can view system job template	1	view_systemjobtemplate
133	Can add notification	29	add_notification
134	Can change notification	29	change_notification
135	Can delete notification	29	delete_notification
136	Can view notification	29	view_notification
137	Can add notification template	30	add_notificationtemplate
138	Can change notification template	30	change_notificationtemplate
139	Can delete notification template	30	delete_notificationtemplate
140	Can view notification template	30	view_notificationtemplate
141	Can add role	31	add_role
142	Can change role	31	change_role
143	Can delete role	31	delete_role
144	Can view role	31	view_role
145	Can add role ancestor entry	32	add_roleancestorentry
146	Can change role ancestor entry	32	change_roleancestorentry
147	Can delete role ancestor entry	32	delete_roleancestorentry
148	Can view role ancestor entry	32	view_roleancestorentry
149	Can add label	33	add_label
150	Can change label	33	change_label
151	Can delete label	33	delete_label
152	Can view label	33	view_label
153	Can add workflow job	11	add_workflowjob
154	Can change workflow job	11	change_workflowjob
155	Can delete workflow job	11	delete_workflowjob
156	Can view workflow job	11	view_workflowjob
157	Can add workflow job node	34	add_workflowjobnode
158	Can change workflow job node	34	change_workflowjobnode
159	Can delete workflow job node	34	delete_workflowjobnode
160	Can view workflow job node	34	view_workflowjobnode
161	Can add workflow job template	6	add_workflowjobtemplate
162	Can change workflow job template	6	change_workflowjobtemplate
163	Can delete workflow job template	6	delete_workflowjobtemplate
164	Can view workflow job template	6	view_workflowjobtemplate
165	Can run this workflow job template	6	execute_workflowjobtemplate
166	Can approve steps in this workflow job template	6	approve_workflowjobtemplate
167	Can add workflow job template node	35	add_workflowjobtemplatenode
168	Can change workflow job template node	35	change_workflowjobtemplatenode
169	Can delete workflow job template node	35	delete_workflowjobtemplatenode
170	Can view workflow job template node	35	view_workflowjobtemplatenode
171	Can add tower schedule state	36	add_towerschedulestate
172	Can change tower schedule state	36	change_towerschedulestate
173	Can delete tower schedule state	36	delete_towerschedulestate
174	Can view tower schedule state	36	view_towerschedulestate
175	Can add smart inventory membership	37	add_smartinventorymembership
176	Can change smart inventory membership	37	change_smartinventorymembership
177	Can delete smart inventory membership	37	delete_smartinventorymembership
178	Can view smart inventory membership	37	view_smartinventorymembership
179	Can add credential type	38	add_credentialtype
180	Can change credential type	38	change_credentialtype
181	Can delete credential type	38	delete_credentialtype
182	Can view credential type	38	view_credentialtype
183	Can change instance group	39	change_instancegroup
184	Can delete instance group	39	delete_instancegroup
185	Can view instance group	39	view_instancegroup
186	Can use instance group in a preference list of a resource	39	use_instancegroup
187	Can add job launch config	40	add_joblaunchconfig
188	Can change job launch config	40	change_joblaunchconfig
189	Can delete job launch config	40	delete_joblaunchconfig
190	Can view job launch config	40	view_joblaunchconfig
191	Can add unified job deprecated stdout	41	add_unifiedjobdeprecatedstdout
192	Can change unified job deprecated stdout	41	change_unifiedjobdeprecatedstdout
193	Can delete unified job deprecated stdout	41	delete_unifiedjobdeprecatedstdout
194	Can view unified job deprecated stdout	41	view_unifiedjobdeprecatedstdout
195	Can add inventory update event	42	add_inventoryupdateevent
196	Can change inventory update event	42	change_inventoryupdateevent
197	Can delete inventory update event	42	delete_inventoryupdateevent
198	Can view inventory update event	42	view_inventoryupdateevent
199	Can add project update event	43	add_projectupdateevent
200	Can change project update event	43	change_projectupdateevent
201	Can delete project update event	43	delete_projectupdateevent
202	Can view project update event	43	view_projectupdateevent
203	Can add system job event	44	add_systemjobevent
204	Can change system job event	44	change_systemjobevent
205	Can delete system job event	44	delete_systemjobevent
206	Can view system job event	44	view_systemjobevent
207	Can add user session membership	45	add_usersessionmembership
208	Can change user session membership	45	change_usersessionmembership
209	Can delete user session membership	45	delete_usersessionmembership
210	Can view user session membership	45	view_usersessionmembership
211	Can add credential input source	48	add_credentialinputsource
212	Can change credential input source	48	change_credentialinputsource
213	Can delete credential input source	48	delete_credentialinputsource
214	Can view credential input source	48	view_credentialinputsource
215	Can add inventory instance group membership	49	add_inventoryinstancegroupmembership
216	Can change inventory instance group membership	49	change_inventoryinstancegroupmembership
217	Can delete inventory instance group membership	49	delete_inventoryinstancegroupmembership
218	Can view inventory instance group membership	49	view_inventoryinstancegroupmembership
219	Can add organization instance group membership	50	add_organizationinstancegroupmembership
220	Can change organization instance group membership	50	change_organizationinstancegroupmembership
221	Can delete organization instance group membership	50	delete_organizationinstancegroupmembership
222	Can view organization instance group membership	50	view_organizationinstancegroupmembership
223	Can add unified job template instance group membership	51	add_unifiedjobtemplateinstancegroupmembership
224	Can change unified job template instance group membership	51	change_unifiedjobtemplateinstancegroupmembership
225	Can delete unified job template instance group membership	51	delete_unifiedjobtemplateinstancegroupmembership
226	Can view unified job template instance group membership	51	view_unifiedjobtemplateinstancegroupmembership
227	Can add workflow approval template	52	add_workflowapprovaltemplate
228	Can change workflow approval template	52	change_workflowapprovaltemplate
229	Can delete workflow approval template	52	delete_workflowapprovaltemplate
230	Can view workflow approval template	52	view_workflowapprovaltemplate
231	Can add workflow approval	53	add_workflowapproval
232	Can change workflow approval	53	change_workflowapproval
233	Can delete workflow approval	53	delete_workflowapproval
234	Can view workflow approval	53	view_workflowapproval
235	Can add organization galaxy credential membership	54	add_organizationgalaxycredentialmembership
236	Can change organization galaxy credential membership	54	change_organizationgalaxycredentialmembership
237	Can delete organization galaxy credential membership	54	delete_organizationgalaxycredentialmembership
238	Can view organization galaxy credential membership	54	view_organizationgalaxycredentialmembership
239	Can add execution environment	55	add_executionenvironment
240	Can change execution environment	55	change_executionenvironment
241	Can delete execution environment	55	delete_executionenvironment
242	Can add host metric	56	add_hostmetric
243	Can change host metric	56	change_hostmetric
244	Can delete host metric	56	delete_hostmetric
245	Can view host metric	56	view_hostmetric
246	Can add unpartitioned ad hoc command event	57	add_unpartitionedadhoccommandevent
247	Can change unpartitioned ad hoc command event	57	change_unpartitionedadhoccommandevent
248	Can delete unpartitioned ad hoc command event	57	delete_unpartitionedadhoccommandevent
249	Can view unpartitioned ad hoc command event	57	view_unpartitionedadhoccommandevent
250	Can add unpartitioned inventory update event	58	add_unpartitionedinventoryupdateevent
251	Can change unpartitioned inventory update event	58	change_unpartitionedinventoryupdateevent
252	Can delete unpartitioned inventory update event	58	delete_unpartitionedinventoryupdateevent
253	Can view unpartitioned inventory update event	58	view_unpartitionedinventoryupdateevent
254	Can add unpartitioned job event	59	add_unpartitionedjobevent
255	Can change unpartitioned job event	59	change_unpartitionedjobevent
256	Can delete unpartitioned job event	59	delete_unpartitionedjobevent
257	Can view unpartitioned job event	59	view_unpartitionedjobevent
258	Can add unpartitioned project update event	60	add_unpartitionedprojectupdateevent
259	Can change unpartitioned project update event	60	change_unpartitionedprojectupdateevent
260	Can delete unpartitioned project update event	60	delete_unpartitionedprojectupdateevent
261	Can view unpartitioned project update event	60	view_unpartitionedprojectupdateevent
262	Can add unpartitioned system job event	61	add_unpartitionedsystemjobevent
263	Can change unpartitioned system job event	61	change_unpartitionedsystemjobevent
264	Can delete unpartitioned system job event	61	delete_unpartitionedsystemjobevent
265	Can view unpartitioned system job event	61	view_unpartitionedsystemjobevent
266	Can add instance link	62	add_instancelink
267	Can change instance link	62	change_instancelink
268	Can delete instance link	62	delete_instancelink
269	Can view instance link	62	view_instancelink
270	Can add workflow job template node base instance group membership	63	add_workflowjobtemplatenodebaseinstancegroupmembership
271	Can change workflow job template node base instance group membership	63	change_workflowjobtemplatenodebaseinstancegroupmembership
272	Can delete workflow job template node base instance group membership	63	delete_workflowjobtemplatenodebaseinstancegroupmembership
273	Can view workflow job template node base instance group membership	63	view_workflowjobtemplatenodebaseinstancegroupmembership
274	Can add workflow job node base instance group membership	64	add_workflowjobnodebaseinstancegroupmembership
275	Can change workflow job node base instance group membership	64	change_workflowjobnodebaseinstancegroupmembership
276	Can delete workflow job node base instance group membership	64	delete_workflowjobnodebaseinstancegroupmembership
277	Can view workflow job node base instance group membership	64	view_workflowjobnodebaseinstancegroupmembership
278	Can add workflow job instance group membership	65	add_workflowjobinstancegroupmembership
279	Can change workflow job instance group membership	65	change_workflowjobinstancegroupmembership
280	Can delete workflow job instance group membership	65	delete_workflowjobinstancegroupmembership
281	Can view workflow job instance group membership	65	view_workflowjobinstancegroupmembership
282	Can add schedule instance group membership	66	add_scheduleinstancegroupmembership
283	Can change schedule instance group membership	66	change_scheduleinstancegroupmembership
284	Can delete schedule instance group membership	66	delete_scheduleinstancegroupmembership
285	Can view schedule instance group membership	66	view_scheduleinstancegroupmembership
286	Can add job launch config instance group membership	67	add_joblaunchconfiginstancegroupmembership
287	Can change job launch config instance group membership	67	change_joblaunchconfiginstancegroupmembership
288	Can delete job launch config instance group membership	67	delete_joblaunchconfiginstancegroupmembership
289	Can view job launch config instance group membership	67	view_joblaunchconfiginstancegroupmembership
290	Can add host metric summary monthly	68	add_hostmetricsummarymonthly
291	Can change host metric summary monthly	68	change_hostmetricsummarymonthly
292	Can delete host metric summary monthly	68	delete_hostmetricsummarymonthly
293	Can view host metric summary monthly	68	view_hostmetricsummarymonthly
294	Can add inventory constructed inventory membership	69	add_inventoryconstructedinventorymembership
295	Can change inventory constructed inventory membership	69	change_inventoryconstructedinventorymembership
296	Can delete inventory constructed inventory membership	69	delete_inventoryconstructedinventorymembership
297	Can view inventory constructed inventory membership	69	view_inventoryconstructedinventorymembership
298	Can add receptor address	70	add_receptoraddress
299	Can change receptor address	70	change_receptoraddress
300	Can delete receptor address	70	delete_receptoraddress
301	Can view receptor address	70	view_receptoraddress
302	Can add event query	77	add_eventquery
303	Can change event query	77	change_eventquery
304	Can delete event query	77	delete_eventquery
305	Can view event query	77	view_eventquery
306	Can add indirect managed node audit	78	add_indirectmanagednodeaudit
307	Can change indirect managed node audit	78	change_indirectmanagednodeaudit
308	Can delete indirect managed node audit	78	delete_indirectmanagednodeaudit
309	Can view indirect managed node audit	78	view_indirectmanagednodeaudit
310	Can add inventory group variables with history	79	add_inventorygroupvariableswithhistory
311	Can change inventory group variables with history	79	change_inventorygroupvariableswithhistory
312	Can delete inventory group variables with history	79	delete_inventorygroupvariableswithhistory
313	Can view inventory group variables with history	79	view_inventorygroupvariableswithhistory
314	Can add service id	80	add_serviceid
315	Can change service id	80	change_serviceid
316	Can delete service id	80	delete_serviceid
317	Can view service id	80	view_serviceid
318	Can add resource type	81	add_resourcetype
319	Can change resource type	81	change_resourcetype
320	Can delete resource type	81	delete_resourcetype
321	Can view resource type	81	view_resourcetype
322	Can add resource	82	add_resource
323	Can change resource	82	change_resource
324	Can delete resource	82	delete_resource
325	Can view resource	82	view_resource
326	Can add permission	85	add_dabpermission
327	Can change permission	85	change_dabpermission
328	Can delete permission	85	delete_dabpermission
329	Can view permission	85	view_dabpermission
330	Can add role definition	83	add_roledefinition
331	Can change role definition	83	change_roledefinition
332	Can delete role definition	83	delete_roledefinition
333	Can view role definition	83	view_roledefinition
334	Can add role team assignment	86	add_roleteamassignment
335	Can change role team assignment	86	change_roleteamassignment
336	Can delete role team assignment	86	delete_roleteamassignment
337	Can view role team assignment	86	view_roleteamassignment
338	Can add role user assignment	87	add_roleuserassignment
339	Can change role user assignment	87	change_roleuserassignment
340	Can delete role user assignment	87	delete_roleuserassignment
341	Can view role user assignment	87	view_roleuserassignment
342	Can add object role	88	add_objectrole
343	Can change object role	88	change_objectrole
344	Can delete object role	88	delete_objectrole
345	Can view object role	88	view_objectrole
346	Can add role evaluation	89	add_roleevaluation
347	Can change role evaluation	89	change_roleevaluation
348	Can delete role evaluation	89	delete_roleevaluation
349	Can view role evaluation	89	view_roleevaluation
350	Can add role evaluation uuid	90	add_roleevaluationuuid
351	Can change role evaluation uuid	90	change_roleevaluationuuid
352	Can delete role evaluation uuid	90	delete_roleevaluationuuid
353	Can view role evaluation uuid	90	view_roleevaluationuuid
354	Can add dab content type	91	add_dabcontenttype
355	Can change dab content type	91	change_dabcontenttype
356	Can delete dab content type	91	delete_dabcontenttype
357	Can view dab content type	91	view_dabcontenttype
358	Can add aap flag	84	add_aapflag
359	Can change aap flag	84	change_aapflag
360	Can delete aap flag	84	delete_aapflag
361	Can view aap flag	84	view_aapflag
362	Can add flag state	92	add_flagstate
363	Can change flag state	92	change_flagstate
364	Can delete flag state	92	delete_flagstate
365	Can view flag state	92	view_flagstate
\.

ALTER TABLE public.auth_permission ENABLE TRIGGER ALL;

--
-- Data for Name: auth_group_permissions; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.auth_group_permissions DISABLE TRIGGER ALL;

COPY public.auth_group_permissions (id, group_id, permission_id) FROM stdin;
\.

ALTER TABLE public.auth_group_permissions ENABLE TRIGGER ALL;

--
-- Data for Name: auth_user_groups; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.auth_user_groups DISABLE TRIGGER ALL;

COPY public.auth_user_groups (id, user_id, group_id) FROM stdin;
\.

ALTER TABLE public.auth_user_groups ENABLE TRIGGER ALL;

--
-- Data for Name: auth_user_user_permissions; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.auth_user_user_permissions DISABLE TRIGGER ALL;

COPY public.auth_user_user_permissions (id, user_id, permission_id) FROM stdin;
\.

ALTER TABLE public.auth_user_user_permissions ENABLE TRIGGER ALL;

--
-- Data for Name: conf_setting; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.conf_setting DISABLE TRIGGER ALL;

COPY public.conf_setting (id, created, modified, key, value, user_id) FROM stdin;
1	2026-07-24 17:33:52.525626+00	2026-07-24 17:33:52.525629+00	INSTALL_UUID	"f31b6164-e273-4e60-b4de-1637bb526722"	\N
\.

ALTER TABLE public.conf_setting ENABLE TRIGGER ALL;

--
-- Data for Name: dab_feature_flags_aapflag; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.dab_feature_flags_aapflag DISABLE TRIGGER ALL;

COPY public.dab_feature_flags_aapflag (id, modified, created, name, ui_name, condition, value, required, support_level, visibility, toggle_type, description, support_url, labels, created_by_id, modified_by_id) FROM stdin;
1	2026-07-24 17:34:56.45108+00	2026-07-24 17:34:56.451095+00	FEATURE_INDIRECT_NODE_COUNTING_ENABLED	Indirect Node Counting	boolean	True	f	TECHNOLOGY_PREVIEW	t	run-time	Indirect Node Counting parses the event stream of all jobs to identify resources and stores these in the platform database. Example: Job automates VMware, the parser will report back the VMs, Hypervisors that were automated. This feature helps customers and partners report on the automations they are doing beyond an API endpoint.	https://access.redhat.com/articles/7109910	["controller"]	\N	\N
2	2026-07-24 17:34:56.461005+00	2026-07-24 17:34:56.461016+00	FEATURE_EDA_ANALYTICS_ENABLED	Event-Driven Ansible Analytics	boolean	False	f	TECHNOLOGY_PREVIEW	f	install-time	Submit Event-Driven Ansible usage analytics to console.redhat.com.	https://access.redhat.com/solutions/7112810	["eda"]	\N	\N
3	2026-07-24 17:34:56.468233+00	2026-07-24 17:34:56.468244+00	FEATURE_OIDC_WORKLOAD_IDENTITY_ENABLED	OIDC Workload Identity	boolean	False	f	TECHNOLOGY_PREVIEW	f	install-time	Enable identity provision of workloads using OIDC		["platform"]	\N	\N
\.

ALTER TABLE public.dab_feature_flags_aapflag ENABLE TRIGGER ALL;

--
-- Data for Name: dab_rbac_dabcontenttype; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.dab_rbac_dabcontenttype DISABLE TRIGGER ALL;

COPY public.dab_rbac_dabcontenttype (id, service, app_label, model, parent_content_type_id, api_slug, pk_field_type) FROM stdin;
39	awx	main	instancegroup	\N	awx.instancegroup	integer
22	shared	main	organization	\N	shared.organization	integer
25	shared	main	team	22	shared.team	integer
6	awx	main	workflowjobtemplate	22	awx.workflowjobtemplate	integer
4	awx	main	jobtemplate	22	awx.jobtemplate	integer
14	awx	main	credential	22	awx.credential	integer
19	awx	main	inventory	22	awx.inventory	integer
30	awx	main	notificationtemplate	22	awx.notificationtemplate	integer
55	awx	main	executionenvironment	22	awx.executionenvironment	integer
5	awx	main	project	22	awx.project	integer
\.

ALTER TABLE public.dab_rbac_dabcontenttype ENABLE TRIGGER ALL;

--
-- Data for Name: dab_rbac_dabpermission; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.dab_rbac_dabpermission DISABLE TRIGGER ALL;

COPY public.dab_rbac_dabpermission (id, name, codename, content_type_id, api_slug) FROM stdin;
1	Can add credential	add_credential	14	awx.add_credential
2	Can change credential	change_credential	14	awx.change_credential
3	Can delete credential	delete_credential	14	awx.delete_credential
5	Can use credential in a job or related resource	use_credential	14	awx.use_credential
4	Can view credential	view_credential	14	awx.view_credential
47	Can add execution environment	add_executionenvironment	55	awx.add_executionenvironment
48	Can change execution environment	change_executionenvironment	55	awx.change_executionenvironment
49	Can delete execution environment	delete_executionenvironment	55	awx.delete_executionenvironment
43	Can change instance group	change_instancegroup	39	awx.change_instancegroup
44	Can delete instance group	delete_instancegroup	39	awx.delete_instancegroup
46	Can use instance group in a preference list of a resource	use_instancegroup	39	awx.use_instancegroup
45	Can view instance group	view_instancegroup	39	awx.view_instancegroup
6	Can add inventory	add_inventory	19	awx.add_inventory
11	Can run ad hoc commands	adhoc_inventory	19	awx.adhoc_inventory
7	Can change inventory	change_inventory	19	awx.change_inventory
8	Can delete inventory	delete_inventory	19	awx.delete_inventory
12	Can update inventory sources in inventory	update_inventory	19	awx.update_inventory
10	Can use inventory in a job template	use_inventory	19	awx.use_inventory
9	Can view inventory	view_inventory	19	awx.view_inventory
23	Can change job template	change_jobtemplate	4	awx.change_jobtemplate
24	Can delete job template	delete_jobtemplate	4	awx.delete_jobtemplate
26	Can run this job template	execute_jobtemplate	4	awx.execute_jobtemplate
25	Can view job template	view_jobtemplate	4	awx.view_jobtemplate
33	Can add notification template	add_notificationtemplate	30	awx.add_notificationtemplate
34	Can change notification template	change_notificationtemplate	30	awx.change_notificationtemplate
35	Can delete notification template	delete_notificationtemplate	30	awx.delete_notificationtemplate
36	Can view notification template	view_notificationtemplate	30	awx.view_notificationtemplate
17	Audit everything inside the organization	audit_organization	22	shared.audit_organization
13	Can change organization	change_organization	22	shared.change_organization
14	Can delete organization	delete_organization	22	shared.delete_organization
16	Basic participation permissions for organization	member_organization	22	shared.member_organization
15	Can view organization	view_organization	22	shared.view_organization
27	Can add project	add_project	5	awx.add_project
28	Can change project	change_project	5	awx.change_project
29	Can delete project	delete_project	5	awx.delete_project
31	Can run a project update	update_project	5	awx.update_project
32	Can use project in a job template	use_project	5	awx.use_project
30	Can view project	view_project	5	awx.view_project
18	Can add team	add_team	25	shared.add_team
19	Can change team	change_team	25	shared.change_team
20	Can delete team	delete_team	25	shared.delete_team
22	Inherit all roles assigned to this team	member_team	25	shared.member_team
21	Can view team	view_team	25	shared.view_team
37	Can add workflow job template	add_workflowjobtemplate	6	awx.add_workflowjobtemplate
42	Can approve steps in this workflow job template	approve_workflowjobtemplate	6	awx.approve_workflowjobtemplate
38	Can change workflow job template	change_workflowjobtemplate	6	awx.change_workflowjobtemplate
39	Can delete workflow job template	delete_workflowjobtemplate	6	awx.delete_workflowjobtemplate
41	Can run this workflow job template	execute_workflowjobtemplate	6	awx.execute_workflowjobtemplate
40	Can view workflow job template	view_workflowjobtemplate	6	awx.view_workflowjobtemplate
\.

ALTER TABLE public.dab_rbac_dabpermission ENABLE TRIGGER ALL;

--
-- Data for Name: dab_rbac_roledefinition; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.dab_rbac_roledefinition DISABLE TRIGGER ALL;

COPY public.dab_rbac_roledefinition (id, name, description, managed, created_by_id, created, modified_by_id, modified, content_type_id) FROM stdin;
1	Team Admin	Has all permissions to a single team	t	\N	2026-07-24 17:34:43.677809+00	\N	2026-07-24 17:34:51.589006+00	25
2	Team Member	Has member permissions to a single team	t	\N	2026-07-24 17:34:43.68243+00	\N	2026-07-24 17:34:51.590063+00	25
3	WorkflowJobTemplate Admin	Has all permissions to a single workflow job template	t	\N	2026-07-24 17:34:43.686625+00	\N	2026-07-24 17:34:51.591062+00	6
4	Organization WorkflowJobTemplate Admin	Has all permissions to workflow job templates within an organization	t	\N	2026-07-24 17:34:43.690115+00	\N	2026-07-24 17:34:51.592076+00	22
5	WorkflowJobTemplate Execute	Has execute permissions to a single workflow job template	t	\N	2026-07-24 17:34:43.693085+00	\N	2026-07-24 17:34:51.593026+00	6
6	WorkflowJobTemplate Approve	Has approve permissions to a single workflow job template	t	\N	2026-07-24 17:34:43.695757+00	\N	2026-07-24 17:34:51.593851+00	6
7	JobTemplate Admin	Has all permissions to a single job template	t	\N	2026-07-24 17:34:43.699138+00	\N	2026-07-24 17:34:51.594717+00	4
8	Organization JobTemplate Admin	Has all permissions to job templates within an organization	t	\N	2026-07-24 17:34:43.702584+00	\N	2026-07-24 17:34:51.595599+00	22
9	JobTemplate Execute	Has execute permissions to a single job template	t	\N	2026-07-24 17:34:43.705658+00	\N	2026-07-24 17:34:51.596435+00	4
10	Credential Admin	Has all permissions to a single credential	t	\N	2026-07-24 17:34:43.708595+00	\N	2026-07-24 17:34:51.597398+00	14
11	Organization Credential Admin	Has all permissions to credentials within an organization	t	\N	2026-07-24 17:34:43.711548+00	\N	2026-07-24 17:34:51.598332+00	22
12	Credential Use	Has use permissions to a single credential	t	\N	2026-07-24 17:34:43.714047+00	\N	2026-07-24 17:34:51.599374+00	14
13	InstanceGroup Admin	Has all permissions to a single instance group	t	\N	2026-07-24 17:34:43.717376+00	\N	2026-07-24 17:34:51.60036+00	39
14	InstanceGroup Use	Has use permissions to a single instance group	t	\N	2026-07-24 17:34:43.719948+00	\N	2026-07-24 17:34:51.601409+00	39
15	Inventory Admin	Has all permissions to a single inventory	t	\N	2026-07-24 17:34:43.723043+00	\N	2026-07-24 17:34:51.602511+00	19
16	Organization Inventory Admin	Has all permissions to inventories within an organization	t	\N	2026-07-24 17:34:43.726633+00	\N	2026-07-24 17:34:51.603649+00	22
17	Inventory Use	Has use permissions to a single inventory	t	\N	2026-07-24 17:34:43.729597+00	\N	2026-07-24 17:34:51.604789+00	19
18	Inventory Adhoc	Has adhoc permissions to a single inventory	t	\N	2026-07-24 17:34:43.732657+00	\N	2026-07-24 17:34:51.605963+00	19
19	Inventory Update	Has update permissions to a single inventory	t	\N	2026-07-24 17:34:43.735624+00	\N	2026-07-24 17:34:51.606968+00	19
20	NotificationTemplate Admin	Has all permissions to a single notification template	t	\N	2026-07-24 17:34:43.739008+00	\N	2026-07-24 17:34:51.608033+00	30
21	Organization NotificationTemplate Admin	Has all permissions to notification templates within an organization	t	\N	2026-07-24 17:34:43.742034+00	\N	2026-07-24 17:34:51.609177+00	22
22	Organization Member	Has member permissions to a single organization	t	\N	2026-07-24 17:34:43.745634+00	\N	2026-07-24 17:34:51.610242+00	22
23	ExecutionEnvironment Admin	Has all permissions to a single execution environment	t	\N	2026-07-24 17:34:43.749036+00	\N	2026-07-24 17:34:51.611203+00	55
24	Organization ExecutionEnvironment Admin	Has all permissions to execution environments within an organization	t	\N	2026-07-24 17:34:43.752097+00	\N	2026-07-24 17:34:51.612396+00	22
25	Project Admin	Has all permissions to a single project	t	\N	2026-07-24 17:34:43.755986+00	\N	2026-07-24 17:34:51.613479+00	5
26	Organization Project Admin	Has all permissions to projects within an organization	t	\N	2026-07-24 17:34:43.7595+00	\N	2026-07-24 17:34:51.614363+00	22
27	Project Use	Has use permissions to a single project	t	\N	2026-07-24 17:34:43.762357+00	\N	2026-07-24 17:34:51.615243+00	5
28	Project Update	Has update permissions to a single project	t	\N	2026-07-24 17:34:43.765497+00	\N	2026-07-24 17:34:51.616105+00	5
29	Organization Admin	Has all permissions to a single organization and all objects inside of it	t	\N	2026-07-24 17:34:43.767738+00	\N	2026-07-24 17:34:51.616878+00	22
30	Organization Audit	Has permission to view all objects inside of a single organization	t	\N	2026-07-24 17:34:43.771206+00	\N	2026-07-24 17:34:51.617595+00	22
31	Organization Execute	Has permission to execute all runnable objects in the organization	t	\N	2026-07-24 17:34:43.773564+00	\N	2026-07-24 17:34:51.618304+00	22
32	Organization Approval	Has permission to approve any workflow steps within a single organization	t	\N	2026-07-24 17:34:43.775827+00	\N	2026-07-24 17:34:51.618991+00	22
\.

ALTER TABLE public.dab_rbac_roledefinition ENABLE TRIGGER ALL;

--
-- Data for Name: dab_rbac_objectrole; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.dab_rbac_objectrole DISABLE TRIGGER ALL;

COPY public.dab_rbac_objectrole (id, object_id, role_definition_id, parent_reference, content_type_id) FROM stdin;
1	1	10		14
\.

ALTER TABLE public.dab_rbac_objectrole ENABLE TRIGGER ALL;

--
-- Data for Name: main_team; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_team DISABLE TRIGGER ALL;

COPY public.main_team (id, created, modified, description, name, created_by_id, modified_by_id, organization_id, admin_role_id, member_role_id, read_role_id) FROM stdin;
\.

ALTER TABLE public.main_team ENABLE TRIGGER ALL;

--
-- Data for Name: dab_rbac_objectrole_provides_teams; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.dab_rbac_objectrole_provides_teams DISABLE TRIGGER ALL;

COPY public.dab_rbac_objectrole_provides_teams (id, objectrole_id, team_id) FROM stdin;
\.

ALTER TABLE public.dab_rbac_objectrole_provides_teams ENABLE TRIGGER ALL;

--
-- Data for Name: dab_rbac_roledefinition_permissions; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.dab_rbac_roledefinition_permissions DISABLE TRIGGER ALL;

COPY public.dab_rbac_roledefinition_permissions (id, roledefinition_id, dabpermission_id) FROM stdin;
1	1	19
2	1	20
3	1	21
4	1	22
5	2	21
6	2	22
7	3	38
8	3	39
9	3	40
10	3	41
11	3	42
12	4	37
13	4	38
14	4	39
15	4	40
16	4	41
17	4	42
18	4	15
19	4	16
20	5	40
21	5	41
22	6	40
23	6	42
24	7	24
25	7	25
26	7	26
27	7	23
28	8	15
29	8	16
30	8	23
31	8	24
32	8	25
33	8	26
34	9	25
35	9	26
36	10	2
37	10	3
38	10	4
39	10	5
40	11	1
41	11	2
42	11	3
43	11	4
44	11	5
45	11	15
46	11	16
47	12	4
48	12	5
49	13	43
50	13	44
51	13	45
52	13	46
53	14	45
54	14	46
55	15	7
56	15	8
57	15	9
58	15	10
59	15	11
60	15	12
61	16	6
62	16	7
63	16	8
64	16	9
65	16	10
66	16	11
67	16	12
68	16	15
69	16	16
70	17	9
71	17	10
72	18	9
73	18	10
74	18	11
75	19	9
76	19	12
77	20	34
78	20	35
79	20	36
80	21	33
81	21	34
82	21	35
83	21	36
84	21	15
85	21	16
86	22	16
87	22	15
88	23	48
89	23	49
91	24	47
92	24	48
93	24	49
95	24	15
96	24	16
97	25	32
98	25	28
99	25	29
100	25	30
101	25	31
102	26	32
103	26	15
104	26	16
105	26	27
106	26	28
107	26	29
108	26	30
109	26	31
110	27	32
111	27	30
112	28	30
113	28	31
114	29	1
115	29	2
116	29	3
117	29	4
118	29	5
119	29	6
120	29	7
121	29	8
122	29	9
123	29	10
124	29	11
125	29	12
126	29	13
127	29	14
128	29	15
129	29	16
130	29	17
131	29	18
132	29	19
133	29	20
134	29	21
135	29	22
136	29	23
137	29	24
138	29	25
139	29	26
140	29	27
141	29	28
142	29	29
143	29	30
144	29	31
145	29	32
146	29	33
147	29	34
148	29	35
149	29	36
150	29	37
151	29	38
152	29	39
153	29	40
154	29	41
155	29	42
156	29	47
157	29	48
158	29	49
160	30	4
161	30	36
162	30	40
163	30	9
164	30	15
165	30	17
167	30	21
168	30	25
169	30	30
170	31	40
171	31	41
172	31	15
173	31	25
174	31	26
175	32	40
176	32	42
177	32	15
\.

ALTER TABLE public.dab_rbac_roledefinition_permissions ENABLE TRIGGER ALL;

--
-- Data for Name: dab_rbac_roleevaluation; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.dab_rbac_roleevaluation DISABLE TRIGGER ALL;

COPY public.dab_rbac_roleevaluation (id, codename, object_id, role_id, content_type_id) FROM stdin;
1	use_credential	1	1	14
2	delete_credential	1	1	14
3	view_credential	1	1	14
4	change_credential	1	1	14
\.

ALTER TABLE public.dab_rbac_roleevaluation ENABLE TRIGGER ALL;

--
-- Data for Name: dab_rbac_roleevaluationuuid; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.dab_rbac_roleevaluationuuid DISABLE TRIGGER ALL;

COPY public.dab_rbac_roleevaluationuuid (id, codename, object_id, role_id, content_type_id) FROM stdin;
\.

ALTER TABLE public.dab_rbac_roleevaluationuuid ENABLE TRIGGER ALL;

--
-- Data for Name: dab_rbac_roleteamassignment; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.dab_rbac_roleteamassignment DISABLE TRIGGER ALL;

COPY public.dab_rbac_roleteamassignment (id, created, object_id, role_definition_id, created_by_id, team_id, object_role_id, content_type_id) FROM stdin;
\.

ALTER TABLE public.dab_rbac_roleteamassignment ENABLE TRIGGER ALL;

--
-- Data for Name: dab_rbac_roleuserassignment; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.dab_rbac_roleuserassignment DISABLE TRIGGER ALL;

COPY public.dab_rbac_roleuserassignment (id, created, object_id, role_definition_id, created_by_id, user_id, object_role_id, content_type_id) FROM stdin;
1	2026-07-24 17:35:03.377574+00	1	10	1	1	1	14
\.

ALTER TABLE public.dab_rbac_roleuserassignment ENABLE TRIGGER ALL;

--
-- Data for Name: dab_resource_registry_resource; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.dab_resource_registry_resource DISABLE TRIGGER ALL;

COPY public.dab_resource_registry_resource (id, object_id, service_id, ansible_id, name, content_type_id, is_partially_migrated) FROM stdin;
1	1	0c26e671-551b-4480-bd5f-198ef9a41880	b35a89a5-8ee8-40fe-8bcd-56a1a09e75af	Team Admin	83	f
2	2	0c26e671-551b-4480-bd5f-198ef9a41880	9915b384-1650-4add-b901-76a92ceb7e19	Team Member	83	f
3	3	0c26e671-551b-4480-bd5f-198ef9a41880	4a0f61a2-c590-4288-bd1f-abbe81f1559e	WorkflowJobTemplate Admin	83	f
4	4	0c26e671-551b-4480-bd5f-198ef9a41880	118f4d95-f6ec-4fdb-a09b-23a00905882b	Organization WorkflowJobTemplate Admin	83	f
5	5	0c26e671-551b-4480-bd5f-198ef9a41880	fff890ee-1a61-4c8d-9ec3-808f8807d544	WorkflowJobTemplate Execute	83	f
6	6	0c26e671-551b-4480-bd5f-198ef9a41880	d910840f-00f8-462c-9868-af21c1306526	WorkflowJobTemplate Approve	83	f
7	7	0c26e671-551b-4480-bd5f-198ef9a41880	f20582e1-3b29-4483-871a-2833f35e900b	JobTemplate Admin	83	f
8	8	0c26e671-551b-4480-bd5f-198ef9a41880	594c7e5c-040f-4831-a797-02072d2ef1ae	Organization JobTemplate Admin	83	f
9	9	0c26e671-551b-4480-bd5f-198ef9a41880	7159f789-64a4-44e8-ab71-4b07dd1a4a84	JobTemplate Execute	83	f
10	10	0c26e671-551b-4480-bd5f-198ef9a41880	671b4782-11be-442f-97c5-26ff259d836b	Credential Admin	83	f
11	11	0c26e671-551b-4480-bd5f-198ef9a41880	444006e9-9f9a-4a71-9055-953bfc54b9af	Organization Credential Admin	83	f
12	12	0c26e671-551b-4480-bd5f-198ef9a41880	9bab012e-7290-4e5f-ac5f-5f648922a521	Credential Use	83	f
13	13	0c26e671-551b-4480-bd5f-198ef9a41880	4f66ca95-73de-40a7-867e-c7137449960e	InstanceGroup Admin	83	f
14	14	0c26e671-551b-4480-bd5f-198ef9a41880	91f47857-be61-4ef9-85a5-731fc94552d5	InstanceGroup Use	83	f
15	15	0c26e671-551b-4480-bd5f-198ef9a41880	4f01ffa0-61a9-479c-bbb6-7fe2aecae104	Inventory Admin	83	f
16	16	0c26e671-551b-4480-bd5f-198ef9a41880	339d6443-30d8-4917-9205-1b4a8400e874	Organization Inventory Admin	83	f
17	17	0c26e671-551b-4480-bd5f-198ef9a41880	c45a1571-97d7-4f25-bb4c-e2d2bdffbe49	Inventory Use	83	f
18	18	0c26e671-551b-4480-bd5f-198ef9a41880	701483a4-05be-4c19-b4b2-a101bbe90544	Inventory Adhoc	83	f
19	19	0c26e671-551b-4480-bd5f-198ef9a41880	cfa10e8a-077e-4119-970b-d79e1dcb4525	Inventory Update	83	f
20	20	0c26e671-551b-4480-bd5f-198ef9a41880	f1be537b-f63d-4663-ad17-1089d7d7af7a	NotificationTemplate Admin	83	f
21	21	0c26e671-551b-4480-bd5f-198ef9a41880	cf144f4b-8aec-456a-bb1c-35d965e537fc	Organization NotificationTemplate Admin	83	f
22	22	0c26e671-551b-4480-bd5f-198ef9a41880	a00f2d68-4df2-4979-8b93-2017a2d4cd1f	Organization Member	83	f
23	23	0c26e671-551b-4480-bd5f-198ef9a41880	14a1c354-f6bc-40c4-9e7d-c282fb713bbb	ExecutionEnvironment Admin	83	f
24	24	0c26e671-551b-4480-bd5f-198ef9a41880	0558b439-6606-47ca-bb45-37ca535f753b	Organization ExecutionEnvironment Admin	83	f
25	25	0c26e671-551b-4480-bd5f-198ef9a41880	d9ad9940-056c-4976-923e-d19504e2a990	Project Admin	83	f
26	26	0c26e671-551b-4480-bd5f-198ef9a41880	1ff1398a-db99-47b1-b4d3-2ff934a2be75	Organization Project Admin	83	f
27	27	0c26e671-551b-4480-bd5f-198ef9a41880	878037d6-b85c-416e-beca-e534b4c73f7e	Project Use	83	f
28	28	0c26e671-551b-4480-bd5f-198ef9a41880	e01f51e9-5aa5-4b45-a597-b50ca2d2bb2a	Project Update	83	f
29	29	0c26e671-551b-4480-bd5f-198ef9a41880	64af6a22-610e-4733-b716-1170f5eea969	Organization Admin	83	f
30	30	0c26e671-551b-4480-bd5f-198ef9a41880	100d032b-7c38-4032-844b-9d98f81d6a63	Organization Audit	83	f
31	31	0c26e671-551b-4480-bd5f-198ef9a41880	fb70883a-44b9-4764-b105-6e4279199e36	Organization Execute	83	f
32	32	0c26e671-551b-4480-bd5f-198ef9a41880	9fb4e9ae-aac5-463f-a723-5e24e7efe19f	Organization Approval	83	f
34	1	0c26e671-551b-4480-bd5f-198ef9a41880	80f2e31f-9f08-4a84-9339-b5240300b18a	FEATURE_INDIRECT_NODE_COUNTING_ENABLED	84	f
35	2	0c26e671-551b-4480-bd5f-198ef9a41880	4617f72f-b343-48c3-a610-d8be9234c7f2	FEATURE_EDA_ANALYTICS_ENABLED	84	f
36	3	0c26e671-551b-4480-bd5f-198ef9a41880	e094ea8e-3c82-4cc5-95d1-3fa0fa226fca	FEATURE_OIDC_WORKLOAD_IDENTITY_ENABLED	84	f
37	1	0c26e671-551b-4480-bd5f-198ef9a41880	b9de30a1-c0e2-424b-a781-b39638305558	admin	2	f
38	1	0c26e671-551b-4480-bd5f-198ef9a41880	d66f583a-9b13-4742-9fdf-3762d0138a94	Default	22	f
\.

ALTER TABLE public.dab_resource_registry_resource ENABLE TRIGGER ALL;

--
-- Data for Name: dab_resource_registry_resourcetype; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.dab_resource_registry_resourcetype DISABLE TRIGGER ALL;

COPY public.dab_resource_registry_resourcetype (id, externally_managed, name, content_type_id) FROM stdin;
1	t	shared.organization	22
2	t	shared.user	2
3	t	shared.team	25
4	t	shared.roledefinition	83
5	t	shared.aapflag	84
\.

ALTER TABLE public.dab_resource_registry_resourcetype ENABLE TRIGGER ALL;

--
-- Data for Name: dab_resource_registry_serviceid; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.dab_resource_registry_serviceid DISABLE TRIGGER ALL;

COPY public.dab_resource_registry_serviceid (id) FROM stdin;
0c26e671-551b-4480-bd5f-198ef9a41880
\.

ALTER TABLE public.dab_resource_registry_serviceid ENABLE TRIGGER ALL;

--
-- Data for Name: django_migrations; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.django_migrations DISABLE TRIGGER ALL;

COPY public.django_migrations (id, app, name, applied) FROM stdin;
1	contenttypes	0001_initial	2026-07-24 17:33:16.136308+00
2	contenttypes	0002_remove_content_type_name	2026-07-24 17:33:16.142211+00
3	auth	0001_initial	2026-07-24 17:33:16.164634+00
4	main	0001_initial	2026-07-24 17:33:19.092882+00
5	main	0002_v300_tower_settings_changes	2026-07-24 17:33:24.104609+00
6	main	0003_v300_notification_changes	2026-07-24 17:33:24.107383+00
7	main	0004_v300_fact_changes	2026-07-24 17:33:24.109786+00
8	main	0005_v300_migrate_facts	2026-07-24 17:33:24.112165+00
9	main	0006_v300_active_flag_cleanup	2026-07-24 17:33:24.114719+00
10	main	0007_v300_active_flag_removal	2026-07-24 17:33:24.117202+00
11	main	0008_v300_rbac_changes	2026-07-24 17:33:24.119601+00
12	main	0009_v300_rbac_migrations	2026-07-24 17:33:24.12201+00
13	main	0010_v300_create_system_job_templates	2026-07-24 17:33:24.124423+00
14	main	0011_v300_credential_domain_field	2026-07-24 17:33:24.126889+00
15	main	0012_v300_create_labels	2026-07-24 17:33:24.129397+00
16	main	0013_v300_label_changes	2026-07-24 17:33:24.131881+00
17	main	0014_v300_invsource_cred	2026-07-24 17:33:24.134402+00
18	main	0015_v300_label_changes	2026-07-24 17:33:24.136919+00
19	main	0016_v300_prompting_changes	2026-07-24 17:33:24.139451+00
20	main	0017_v300_prompting_migrations	2026-07-24 17:33:24.142054+00
21	main	0018_v300_host_ordering	2026-07-24 17:33:24.144677+00
22	main	0019_v300_new_azure_credential	2026-07-24 17:33:24.147311+00
23	main	0020_v300_labels_changes	2026-07-24 17:33:25.020606+00
24	main	0021_v300_activity_stream	2026-07-24 17:33:25.023308+00
25	main	0022_v300_adhoc_extravars	2026-07-24 17:33:25.025889+00
26	main	0023_v300_activity_stream_ordering	2026-07-24 17:33:25.028427+00
27	main	0024_v300_jobtemplate_allow_simul	2026-07-24 17:33:25.030924+00
28	main	0025_v300_update_rbac_parents	2026-07-24 17:33:25.033437+00
29	main	0026_v300_credential_unique	2026-07-24 17:33:25.036036+00
30	main	0027_v300_team_migrations	2026-07-24 17:33:25.038684+00
31	main	0028_v300_org_team_cascade	2026-07-24 17:33:25.041148+00
32	main	0029_v302_add_ask_skip_tags	2026-07-24 17:33:31.340893+00
33	main	0030_v302_job_survey_passwords	2026-07-24 17:33:31.34385+00
34	main	0031_v302_migrate_survey_passwords	2026-07-24 17:33:31.346562+00
35	main	0032_v302_credential_permissions_update	2026-07-24 17:33:31.349532+00
36	main	0033_v303_v245_host_variable_fix	2026-07-24 17:33:31.352395+00
37	main	0034_v310_release	2026-07-24 17:33:31.355081+00
38	conf	0001_initial	2026-07-24 17:33:31.414353+00
39	conf	0002_v310_copy_tower_settings	2026-07-24 17:33:31.461783+00
40	main	0035_v310_remove_tower_settings	2026-07-24 17:33:31.843281+00
41	main	0036_v311_insights	2026-07-24 17:33:31.843869+00
42	main	0037_v313_instance_version	2026-07-24 17:33:31.844357+00
43	main	0006_v320_release	2026-07-24 17:33:36.715479+00
44	main	0007_v320_data_migrations	2026-07-24 17:33:36.723395+00
45	main	0008_v320_drop_v1_credential_fields	2026-07-24 17:33:38.353697+00
46	main	0009_v322_add_setting_field_for_activity_stream	2026-07-24 17:33:38.414212+00
47	main	0010_v322_add_ovirt4_tower_inventory	2026-07-24 17:33:38.515351+00
48	main	0011_v322_encrypt_survey_passwords	2026-07-24 17:33:38.518729+00
49	main	0012_v322_update_cred_types	2026-07-24 17:33:38.521536+00
50	main	0013_v330_multi_credential	2026-07-24 17:33:39.280561+00
51	auth	0002_alter_permission_name_max_length	2026-07-24 17:33:39.33213+00
52	auth	0003_alter_user_email_max_length	2026-07-24 17:33:39.385938+00
53	auth	0004_alter_user_username_opts	2026-07-24 17:33:39.437037+00
54	auth	0005_alter_user_last_login_null	2026-07-24 17:33:39.63522+00
55	auth	0006_require_contenttypes_0002	2026-07-24 17:33:39.647314+00
56	auth	0007_alter_validators_add_error_messages	2026-07-24 17:33:39.697439+00
57	auth	0008_alter_user_username_max_length	2026-07-24 17:33:39.749888+00
58	auth	0009_alter_user_last_name_max_length	2026-07-24 17:33:39.799875+00
59	auth	0010_alter_group_name_max_length	2026-07-24 17:33:39.851651+00
60	auth	0011_update_proxy_permissions	2026-07-24 17:33:39.899623+00
61	auth	0012_alter_user_first_name_max_length	2026-07-24 17:33:39.951726+00
62	conf	0003_v310_JSONField_changes	2026-07-24 17:33:39.983862+00
63	conf	0004_v320_reencrypt	2026-07-24 17:33:39.987513+00
64	conf	0005_v330_rename_two_session_settings	2026-07-24 17:33:40.041338+00
65	conf	0006_v331_ldap_group_type	2026-07-24 17:33:40.044112+00
66	conf	0007_v380_rename_more_settings	2026-07-24 17:33:40.231026+00
67	conf	0008_subscriptions	2026-07-24 17:33:40.337104+00
68	conf	0009_rename_proot_settings	2026-07-24 17:33:40.391101+00
69	conf	0010_change_to_JSONField	2026-07-24 17:33:40.425357+00
70	conf	0011_remove_ldap_auth_conf	2026-07-24 17:33:40.475557+00
71	conf	0012_remove_oidc_auth_conf	2026-07-24 17:33:40.525395+00
72	conf	0013_remove_radius_auth_conf	2026-07-24 17:33:40.575175+00
73	conf	0014_remove_saml_auth_conf	2026-07-24 17:33:40.624499+00
74	conf	0015_remove_social_oauth_conf	2026-07-24 17:33:40.673838+00
75	conf	0016_remove_tacacs_plus_auth_conf	2026-07-24 17:33:40.877809+00
76	dab_feature_flags	0001_initial	2026-07-24 17:33:40.943833+00
77	dab_feature_flags	0002_manual_20251222	2026-07-24 17:33:40.946494+00
78	dab_feature_flags	0003_manual_20260113	2026-07-24 17:33:40.949664+00
79	dab_feature_flags	0004_remove_dispatcherd_feature_flag	2026-07-24 17:33:40.952972+00
80	dab_feature_flags	0005_manual_20260417	2026-07-24 17:33:40.956209+00
81	dab_feature_flags	0006_manual_20260515	2026-07-24 17:33:40.959605+00
82	dab_feature_flags	0007_manual_20260615	2026-07-24 17:33:40.962775+00
83	dab_feature_flags	0008_manual_20260618	2026-07-24 17:33:40.965837+00
84	sessions	0001_initial	2026-07-24 17:33:40.97799+00
85	main	0014_v330_saved_launchtime_configs	2026-07-24 17:33:42.560201+00
86	main	0015_v330_blank_start_args	2026-07-24 17:33:42.563415+00
87	main	0016_v330_non_blank_workflow	2026-07-24 17:33:42.770722+00
88	main	0017_v330_move_deprecated_stdout	2026-07-24 17:33:42.88174+00
89	main	0018_v330_add_additional_stdout_events	2026-07-24 17:33:43.185943+00
90	main	0019_v330_custom_virtualenv	2026-07-24 17:33:43.451828+00
91	main	0020_v330_instancegroup_policies	2026-07-24 17:33:43.549429+00
92	main	0021_v330_declare_new_rbac_roles	2026-07-24 17:33:44.569891+00
93	main	0022_v330_create_new_rbac_roles	2026-07-24 17:33:44.824011+00
94	main	0023_v330_inventory_multicred	2026-07-24 17:33:45.052894+00
95	main	0024_v330_create_user_session_membership	2026-07-24 17:33:45.122153+00
96	main	0025_v330_add_oauth_activity_stream_registrar	2026-07-24 17:33:45.517802+00
97	main	0026_v330_delete_authtoken	2026-07-24 17:33:45.580154+00
98	main	0027_v330_emitted_events	2026-07-24 17:33:45.63235+00
99	main	0028_v330_add_tower_verify	2026-07-24 17:33:45.687136+00
100	main	0030_v330_modify_application	2026-07-24 17:33:45.785569+00
101	main	0031_v330_encrypt_oauth2_secret	2026-07-24 17:33:45.981143+00
102	main	0032_v330_polymorphic_delete	2026-07-24 17:33:46.049619+00
103	main	0033_v330_oauth_help_text	2026-07-24 17:33:46.382028+00
104	main	0034_v330_delete_user_role	2026-07-24 17:33:46.706266+00
105	main	0035_v330_more_oauth2_help_text	2026-07-24 17:33:46.776271+00
106	main	0036_v330_credtype_remove_become_methods	2026-07-24 17:33:46.833354+00
107	main	0037_v330_remove_legacy_fact_cleanup	2026-07-24 17:33:46.902975+00
108	main	0038_v330_add_deleted_activitystream_actor	2026-07-24 17:33:46.960605+00
109	main	0039_v330_custom_venv_help_text	2026-07-24 17:33:47.268582+00
110	main	0040_v330_unifiedjob_controller_node	2026-07-24 17:33:47.329514+00
111	main	0041_v330_update_oauth_refreshtoken	2026-07-24 17:33:47.332709+00
112	main	0042_v330_org_member_role_deparent	2026-07-24 17:33:47.511624+00
113	main	0043_v330_oauth2accesstoken_modified	2026-07-24 17:33:47.549872+00
114	main	0044_v330_add_inventory_update_inventory	2026-07-24 17:33:47.627322+00
115	main	0045_v330_instance_managed_by_policy	2026-07-24 17:33:47.634245+00
116	main	0046_v330_remove_client_credentials_grant	2026-07-24 17:33:47.691476+00
117	main	0047_v330_activitystream_instance	2026-07-24 17:33:47.767701+00
118	main	0048_v330_django_created_modified_by_model_name	2026-07-24 17:33:49.808355+00
119	main	0049_v330_validate_instance_capacity_adjustment	2026-07-24 17:33:49.874775+00
120	main	0050_v340_drop_celery_tables	2026-07-24 17:33:49.884281+00
121	main	0051_v340_job_slicing	2026-07-24 17:33:50.203275+00
122	main	0052_v340_remove_project_scm_delete_on_next_update	2026-07-24 17:33:50.400837+00
123	main	0053_v340_workflow_inventory	2026-07-24 17:33:50.616869+00
124	main	0054_v340_workflow_convergence	2026-07-24 17:33:50.664369+00
125	main	0055_v340_add_grafana_notification	2026-07-24 17:33:50.779796+00
126	main	0056_v350_custom_venv_history	2026-07-24 17:33:51.024653+00
127	main	0057_v350_remove_become_method_type	2026-07-24 17:33:51.089731+00
128	main	0058_v350_remove_limit_limit	2026-07-24 17:33:51.210702+00
129	main	0059_v350_remove_adhoc_limit	2026-07-24 17:33:51.279457+00
130	main	0060_v350_update_schedule_uniqueness_constraint	2026-07-24 17:33:51.391501+00
131	main	0061_v350_track_native_credentialtype_source	2026-07-24 17:33:51.537499+00
132	main	0062_v350_new_playbook_stats	2026-07-24 17:33:51.777017+00
133	main	0063_v350_org_host_limits	2026-07-24 17:33:51.875698+00
134	main	0064_v350_analytics_state	2026-07-24 17:33:51.88141+00
135	main	0065_v350_index_job_status	2026-07-24 17:33:51.946663+00
136	main	0066_v350_inventorysource_custom_virtualenv	2026-07-24 17:33:51.992785+00
137	main	0067_v350_credential_plugins	2026-07-24 17:33:52.226412+00
138	main	0068_v350_index_event_created	2026-07-24 17:33:52.468611+00
139	main	0069_v350_generate_unique_install_uuid	2026-07-24 17:33:52.526912+00
140	main	0070_v350_gce_instance_id	2026-07-24 17:33:52.590456+00
141	main	0071_v350_remove_system_tracking	2026-07-24 17:33:52.679136+00
142	main	0072_v350_deprecate_fields	2026-07-24 17:33:53.778275+00
143	main	0073_v360_create_instance_group_m2m	2026-07-24 17:33:53.973796+00
144	main	0074_v360_migrate_instance_group_relations	2026-07-24 17:33:54.038631+00
145	main	0075_v360_remove_old_instance_group_relations	2026-07-24 17:33:54.364026+00
146	main	0076_v360_add_new_instance_group_relations	2026-07-24 17:33:54.544281+00
147	main	0077_v360_add_default_orderings	2026-07-24 17:33:55.13289+00
148	main	0078_v360_clear_sessions_tokens_jt	2026-07-24 17:33:55.270265+00
149	main	0079_v360_rm_implicit_oauth2_apps	2026-07-24 17:33:55.342352+00
150	main	0080_v360_replace_job_origin	2026-07-24 17:33:55.809372+00
151	main	0081_v360_notify_on_start	2026-07-24 17:33:56.331786+00
152	main	0082_v360_webhook_http_method	2026-07-24 17:33:56.391921+00
153	main	0083_v360_job_branch_override	2026-07-24 17:33:56.911576+00
154	main	0084_v360_token_description	2026-07-24 17:33:56.97394+00
155	main	0085_v360_add_notificationtemplate_messages	2026-07-24 17:33:57.131397+00
156	main	0086_v360_workflow_approval	2026-07-24 17:33:57.999878+00
157	main	0087_v360_update_credential_injector_help_text	2026-07-24 17:33:58.329841+00
158	main	0088_v360_dashboard_optimizations	2026-07-24 17:33:58.542021+00
159	main	0089_v360_new_job_event_types	2026-07-24 17:33:58.612429+00
160	main	0090_v360_WFJT_prompts	2026-07-24 17:33:59.263168+00
161	main	0091_v360_approval_node_notifications	2026-07-24 17:33:59.641829+00
162	main	0092_v360_webhook_mixin	2026-07-24 17:34:00.188087+00
163	main	0093_v360_personal_access_tokens	2026-07-24 17:34:00.320383+00
164	main	0094_v360_webhook_mixin2	2026-07-24 17:34:00.674532+00
165	main	0095_v360_increase_instance_version_length	2026-07-24 17:34:00.890333+00
166	main	0096_v360_container_groups	2026-07-24 17:34:01.138504+00
167	main	0097_v360_workflowapproval_approved_or_denied_by	2026-07-24 17:34:01.20648+00
168	main	0098_v360_rename_cyberark_aim_credential_type	2026-07-24 17:34:01.277993+00
169	main	0099_v361_license_cleanup	2026-07-24 17:34:01.339473+00
170	main	0100_v370_projectupdate_job_tags	2026-07-24 17:34:01.540979+00
171	main	0101_v370_generate_new_uuids_for_iso_nodes	2026-07-24 17:34:01.612342+00
172	main	0102_v370_unifiedjob_canceled	2026-07-24 17:34:01.673315+00
173	main	0103_v370_remove_computed_fields	2026-07-24 17:34:02.298353+00
174	main	0104_v370_cleanup_old_scan_jts	2026-07-24 17:34:02.439493+00
175	main	0105_v370_remove_jobevent_parent_and_hosts	2026-07-24 17:34:02.578011+00
176	main	0106_v370_remove_inventory_groups_with_active_failures	2026-07-24 17:34:02.643146+00
177	main	0107_v370_workflow_convergence_api_toggle	2026-07-24 17:34:02.909185+00
178	main	0108_v370_unifiedjob_dependencies_processed	2026-07-24 17:34:02.9736+00
179	main	0109_v370_job_template_organization_field	2026-07-24 17:34:03.994302+00
180	main	0110_v370_instance_ip_address	2026-07-24 17:34:04.01437+00
181	main	0111_v370_delete_channelgroup	2026-07-24 17:34:04.018175+00
182	main	0112_v370_workflow_node_identifier	2026-07-24 17:34:04.573335+00
183	main	0113_v370_event_bigint	2026-07-24 17:34:04.995146+00
184	main	0114_v370_remove_deprecated_manual_inventory_sources	2026-07-24 17:34:05.292489+00
185	main	0115_v370_schedule_set_null	2026-07-24 17:34:05.600352+00
186	main	0116_v400_remove_hipchat_notifications	2026-07-24 17:34:05.790087+00
187	main	0117_v400_remove_cloudforms_inventory	2026-07-24 17:34:05.983232+00
188	main	0118_add_remote_archive_scm_type	2026-07-24 17:34:06.258632+00
189	main	0119_inventory_plugins	2026-07-24 17:34:07.046875+00
190	main	0120_galaxy_credentials	2026-07-24 17:34:07.546666+00
191	main	0121_delete_toweranalyticsstate	2026-07-24 17:34:07.551392+00
192	main	0122_really_remove_cloudforms_inventory	2026-07-24 17:34:07.615393+00
193	main	0123_drop_hg_support	2026-07-24 17:34:07.799439+00
194	main	0124_execution_environments	2026-07-24 17:34:08.307581+00
195	main	0125_more_ee_modeling_changes	2026-07-24 17:34:08.661831+00
196	main	0126_executionenvironment_container_options	2026-07-24 17:34:08.857621+00
197	main	0127_reset_pod_spec_override	2026-07-24 17:34:08.920082+00
198	main	0128_organiaztion_read_roles_ee_admin	2026-07-24 17:34:08.993244+00
199	main	0129_unifiedjob_installed_collections	2026-07-24 17:34:09.052299+00
200	main	0130_ee_polymorphic_set_null	2026-07-24 17:34:09.314412+00
201	main	0131_undo_org_polymorphic_ee	2026-07-24 17:34:09.541547+00
202	main	0132_instancegroup_is_container_group	2026-07-24 17:34:09.658516+00
203	main	0133_centrify_vault_credtype	2026-07-24 17:34:09.729763+00
204	main	0134_unifiedjob_ansible_version	2026-07-24 17:34:09.788778+00
205	main	0135_schedule_sort_fallback_to_id	2026-07-24 17:34:09.84521+00
206	main	0136_scm_track_submodules	2026-07-24 17:34:09.944934+00
207	main	0137_custom_inventory_scripts_removal_data	2026-07-24 17:34:10.152895+00
208	main	0138_custom_inventory_scripts_removal	2026-07-24 17:34:10.992772+00
209	main	0139_isolated_removal	2026-07-24 17:34:11.214345+00
210	main	0140_rename	2026-07-24 17:34:12.000096+00
211	main	0141_remove_isolated_instances	2026-07-24 17:34:12.219036+00
212	main	0142_update_ee_image_field_description	2026-07-24 17:34:12.291168+00
213	main	0143_hostmetric	2026-07-24 17:34:12.301349+00
214	main	0144_event_partitions	2026-07-24 17:34:13.880496+00
215	main	0145_deregister_managed_ee_objs	2026-07-24 17:34:13.955363+00
216	main	0146_add_insights_inventory	2026-07-24 17:34:14.139418+00
217	main	0147_validate_ee_image_field	2026-07-24 17:34:14.394355+00
218	main	0148_unifiedjob_receptor_unit_id	2026-07-24 17:34:14.462542+00
219	main	0149_remove_inventory_insights_credential	2026-07-24 17:34:14.533359+00
220	main	0150_rename_inv_sources_inv_updates	2026-07-24 17:34:14.798085+00
221	main	0151_rename_managed_by_tower	2026-07-24 17:34:15.00126+00
222	main	0152_instance_node_type	2026-07-24 17:34:15.027124+00
223	main	0153_instance_last_seen	2026-07-24 17:34:15.316792+00
224	main	0154_set_default_uuid	2026-07-24 17:34:15.39087+00
225	main	0155_improved_health_check	2026-07-24 17:34:15.423811+00
226	main	0156_capture_mesh_topology	2026-07-24 17:34:15.629023+00
227	main	0157_inventory_labels	2026-07-24 17:34:15.703599+00
228	main	0158_make_instance_cpu_decimal	2026-07-24 17:34:15.771092+00
229	main	0159_deprecate_inventory_source_UoPU_field	2026-07-24 17:34:15.836024+00
230	main	0160_alter_schedule_rrule	2026-07-24 17:34:15.904211+00
231	main	0161_unifiedjob_host_status_counts	2026-07-24 17:34:16.177208+00
232	main	0162_alter_unifiedjob_dependent_jobs	2026-07-24 17:34:16.255612+00
233	main	0163_convert_job_tags_to_textfield	2026-07-24 17:34:16.393537+00
234	main	0164_remove_inventorysource_update_on_project_update	2026-07-24 17:34:16.564536+00
235	main	0165_task_manager_refactor	2026-07-24 17:34:16.74081+00
236	main	0166_alter_jobevent_host	2026-07-24 17:34:17.103846+00
237	main	0167_project_signature_validation_credential	2026-07-24 17:34:17.320954+00
238	main	0168_inventoryupdate_scm_revision	2026-07-24 17:34:17.374688+00
239	main	0169_jt_prompt_everything_on_launch	2026-07-24 17:34:19.594035+00
240	main	0170_node_and_link_state	2026-07-24 17:34:20.020236+00
241	main	0171_add_health_check_started	2026-07-24 17:34:20.046734+00
242	main	0172_prevent_instance_fallback	2026-07-24 17:34:20.163503+00
243	main	0173_instancegroup_max_limits	2026-07-24 17:34:20.273743+00
244	main	0174_ensure_org_ee_admin_roles	2026-07-24 17:34:20.41931+00
245	main	0175_workflowjob_is_bulk_job	2026-07-24 17:34:20.487346+00
246	main	0176_inventorysource_scm_branch	2026-07-24 17:34:20.82221+00
247	main	0177_instance_group_role_addition	2026-07-24 17:34:21.049085+00
248	main	0178_instance_group_admin_migration	2026-07-24 17:34:21.259842+00
249	main	0179_change_cyberark_plugin_names	2026-07-24 17:34:21.348615+00
250	main	0180_add_hostmetric_fields	2026-07-24 17:34:21.37889+00
251	main	0181_hostmetricsummarymonthly	2026-07-24 17:34:21.384464+00
252	main	0182_constructed_inventory	2026-07-24 17:34:22.349299+00
253	main	0183_pre_django_upgrade	2026-07-24 17:34:22.394308+00
254	main	0184_django_indexes	2026-07-24 17:34:34.916742+00
255	main	0185_move_JSONBlob_to_JSONField	2026-07-24 17:34:37.891855+00
256	main	0186_drop_django_taggit	2026-07-24 17:34:38.298221+00
257	main	0187_hop_nodes	2026-07-24 17:34:38.743017+00
258	main	0188_add_bitbucket_dc_webhook	2026-07-24 17:34:39.370803+00
259	main	0189_inbound_hop_nodes	2026-07-24 17:34:40.532522+00
260	main	0190_alter_inventorysource_source_and_more	2026-07-24 17:34:40.683963+00
261	dab_rbac	0001_initial	2026-07-24 17:34:42.678987+00
262	main	0191_add_django_permissions	2026-07-24 17:34:43.495168+00
263	main	0192_custom_roles	2026-07-24 17:34:43.885291+00
264	main	0193_alter_notification_notification_type_and_more	2026-07-24 17:34:44.346592+00
265	main	0194_alter_inventorysource_source_and_more	2026-07-24 17:34:44.521041+00
266	main	0195_EE_permissions	2026-07-24 17:34:44.676049+00
267	main	0196_indirect_managed_node_audit	2026-07-24 17:34:44.845239+00
268	main	0197_add_opa_query_path	2026-07-24 17:34:45.044337+00
269	main	0198_alter_inventorysource_source_and_more	2026-07-24 17:34:45.479102+00
270	main	0199_inventorygroupvariableswithhistory_and_more	2026-07-24 17:34:45.631271+00
271	main	0200_template_name_constraint	2026-07-24 17:34:46.08905+00
272	main	0201_create_managed_creds	2026-07-24 17:34:46.543104+00
273	main	0202_convert_controller_role_definitions	2026-07-24 17:34:46.639911+00
274	main	0203_remove_team_of_teams	2026-07-24 17:34:46.725191+00
275	dab_rbac	0002_alter_objectrole_provides_teams_and_more	2026-07-24 17:34:48.692131+00
276	dab_rbac	0003_alter_dabpermission_codename_and_more	2026-07-24 17:34:49.982434+00
277	dab_rbac	0004_remote_permissions_additions	2026-07-24 17:34:51.086427+00
278	dab_rbac	0005_remote_permissions_data	2026-07-24 17:34:51.664305+00
279	dab_rbac	0006_remote_data_reverse	2026-07-24 17:34:51.747381+00
280	dab_rbac	0007_remote_permissions_removals	2026-07-24 17:34:52.75476+00
281	dab_rbac	0008_remote_permissions_cleanup	2026-07-24 17:34:53.241355+00
282	dab_resource_registry	0001_initial	2026-07-24 17:34:53.753776+00
283	dab_resource_registry	0002_remove_resource_id	2026-07-24 17:34:53.872449+00
284	dab_resource_registry	0003_alter_resource_object_id	2026-07-24 17:34:53.888304+00
285	dab_resource_registry	0004_remove_resourcetype_migrated	2026-07-24 17:34:53.902331+00
286	dab_resource_registry	0005_resource_is_partially_migrated_and_more	2026-07-24 17:34:53.925865+00
287	dab_resource_registry	0006_alter_resource_service_id	2026-07-24 17:34:53.941198+00
288	dab_resource_registry	0007_alter_resource_ansible_id_and_more	2026-07-24 17:34:54.182585+00
289	dab_resource_registry	0008_resource_covering_index_ansible_id	2026-07-24 17:34:54.22855+00
290	flags	0001_initial	2026-07-24 17:34:54.23758+00
291	flags	0002_auto_20151030_1401	2026-07-24 17:34:54.238043+00
292	flags	0003_flag_hidden	2026-07-24 17:34:54.238437+00
293	flags	0004_remove_flag_hidden	2026-07-24 17:34:54.238865+00
294	flags	0005_flag_enabled_by_default	2026-07-24 17:34:54.239283+00
295	flags	0006_auto_20151217_2003	2026-07-24 17:34:54.239629+00
296	flags	0007_unique_flag_site	2026-07-24 17:34:54.239913+00
297	flags	0008_add_state_conditions	2026-07-24 17:34:54.240145+00
298	flags	0009_migrate_to_conditional_state	2026-07-24 17:34:54.240362+00
299	flags	0010_delete_flag_site_fk	2026-07-24 17:34:54.24057+00
300	flags	0011_migrate_path_data_startswith_to_matches	2026-07-24 17:34:54.240771+00
301	flags	0013_add_required_field	2026-07-24 17:34:54.245685+00
302	main	0204_squashed_deletions	2026-07-24 17:34:55.312945+00
303	main	0205_add_ordering_to_instancegroup_and_workflow_nodes	2026-07-24 17:34:55.726007+00
304	main	0206_jobhostsummary_host_id_idx	2026-07-24 17:34:55.78647+00
305	main	0207_alter_skip_tags_to_textfield	2026-07-24 17:34:55.960663+00
306	sites	0001_initial	2026-07-24 17:34:55.975688+00
307	sites	0002_alter_domain_unique	2026-07-24 17:34:55.982843+00
308	main	0002_squashed_v300_release	2026-07-24 17:34:55.990371+00
309	main	0003_squashed_v300_v303_updates	2026-07-24 17:34:55.992982+00
310	main	0004_squashed_v310_release	2026-07-24 17:34:55.99547+00
311	main	0005_squashed_v310_v313_updates	2026-07-24 17:34:55.997895+00
312	flags	0012_replace_migrations_for_wagtail_independence	2026-07-24 17:34:56.000319+00
\.

ALTER TABLE public.django_migrations ENABLE TRIGGER ALL;

--
-- Data for Name: django_session; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.django_session DISABLE TRIGGER ALL;

COPY public.django_session (session_key, session_data, expire_date) FROM stdin;
\.

ALTER TABLE public.django_session ENABLE TRIGGER ALL;

--
-- Data for Name: django_site; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.django_site DISABLE TRIGGER ALL;

COPY public.django_site (id, domain, name) FROM stdin;
1	example.com	example.com
\.

ALTER TABLE public.django_site ENABLE TRIGGER ALL;

--
-- Data for Name: flags_flagstate; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.flags_flagstate DISABLE TRIGGER ALL;

COPY public.flags_flagstate (id, name, condition, value, required) FROM stdin;
\.

ALTER TABLE public.flags_flagstate ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream DISABLE TRIGGER ALL;

COPY public.main_activitystream (id, operation, "timestamp", changes, object_relationship_type, object1, object2, actor_id, action_node, deleted_actor, setting) FROM stdin;
1	create	2026-07-24 17:35:00.202745+00	{"username": "admin", "first_name": "", "last_name": "", "email": "admin@localhost", "is_superuser": true, "password": "hidden", "id": 1}		user		\N	awx-1	\N	{}
2	create	2026-07-24 17:35:03.282574+00	{"name": "Default", "description": "", "max_hosts": 0, "default_environment": null, "opa_query_path": null, "id": 1}		organization		1	awx-1	{"id": 1, "username": "admin", "last_name": "", "first_name": ""}	{}
3	create	2026-07-24 17:35:03.297398+00	{"name": "Demo Project", "description": "", "local_path": "", "scm_type": "git", "scm_url": "https://github.com/ansible/ansible-tower-samples", "scm_branch": "", "scm_refspec": "", "scm_clean": false, "scm_track_submodules": false, "scm_delete_on_update": false, "credential": null, "timeout": 0, "organization": "Default-1", "scm_update_on_launch": false, "scm_update_cache_timeout": 0, "allow_override": false, "default_environment": null, "signature_validation_credential": null, "id": 5}		project		1	awx-1	{"id": 1, "username": "admin", "last_name": "", "first_name": ""}	{}
4	create	2026-07-24 17:35:03.353053+00	{"name": "Demo Credential", "description": "", "organization": "Default-1", "credential_type": "Machine-7", "inputs": "hidden", "id": 1}		credential		1	awx-1	{"id": 1, "username": "admin", "last_name": "", "first_name": ""}	{}
5	associate	2026-07-24 17:35:03.363676+00	{"object1": "credential", "object1_pk": 1, "object2": "user", "object2_pk": 1, "action": "associate", "relationship": "awx.main.models.rbac.Role_members"}	awx.main.models.credential.Credential.admin_role	credential	user	1	awx-1	{"id": 1, "username": "admin", "last_name": "", "first_name": ""}	{}
6	create	2026-07-24 17:35:03.399703+00	{"name": "Ansible Galaxy", "description": "", "organization": null, "credential_type": "Ansible Galaxy/Automation Hub API Token-2", "inputs": "hidden", "id": 2}		credential		1	awx-1	{"id": 1, "username": "admin", "last_name": "", "first_name": ""}	{}
7	associate	2026-07-24 17:35:03.408188+00	{"object1": "organization", "object1_pk": 1, "object2": "credential", "object2_pk": 2, "action": "associate", "relationship": "awx.main.models.organization.OrganizationGalaxyCredentialMembership"}	awx.main.models.organization.OrganizationGalaxyCredentialMembership	organization	credential	1	awx-1	{"id": 1, "username": "admin", "last_name": "", "first_name": ""}	{}
8	create	2026-07-24 17:35:03.418379+00	{"name": "Demo Inventory", "description": "", "organization": "Default-1", "kind": "", "host_filter": null, "variables": "", "prevent_instance_group_fallback": false, "opa_query_path": null, "id": 1}		inventory		1	awx-1	{"id": 1, "username": "admin", "last_name": "", "first_name": ""}	{}
9	create	2026-07-24 17:35:03.424202+00	{"name": "localhost", "description": "", "inventory": "Demo Inventory-1", "enabled": true, "instance_id": "", "variables": "ansible_connection: local\\nansible_python_interpreter: '{{ ansible_playbook_python }}'", "id": 1}		host		1	awx-1	{"id": 1, "username": "admin", "last_name": "", "first_name": ""}	{}
10	create	2026-07-24 17:35:03.439456+00	{"name": "Demo Job Template", "description": "", "job_type": "run", "inventory": "Demo Inventory-1", "project": "Demo Project-5", "playbook": "hello_world.yml", "scm_branch": "", "forks": 0, "limit": "", "verbosity": 0, "extra_vars": "", "job_tags": "", "force_handlers": false, "skip_tags": "", "start_at_task": "", "timeout": 0, "use_fact_cache": false, "execution_environment": null, "host_config_key": "", "ask_scm_branch_on_launch": false, "ask_diff_mode_on_launch": false, "ask_variables_on_launch": false, "ask_limit_on_launch": false, "ask_tags_on_launch": false, "ask_skip_tags_on_launch": false, "ask_job_type_on_launch": false, "ask_verbosity_on_launch": false, "ask_inventory_on_launch": false, "ask_credential_on_launch": false, "ask_execution_environment_on_launch": false, "ask_labels_on_launch": false, "ask_forks_on_launch": false, "ask_job_slice_count_on_launch": false, "ask_timeout_on_launch": false, "ask_instance_groups_on_launch": false, "survey_enabled": false, "become_enabled": false, "diff_mode": false, "allow_simultaneous": false, "job_slice_count": 1, "webhook_service": "", "webhook_credential": null, "prevent_instance_group_fallback": false, "opa_query_path": null, "survey_spec": "{}", "id": 6}		job_template		1	awx-1	{"id": 1, "username": "admin", "last_name": "", "first_name": ""}	{}
11	associate	2026-07-24 17:35:03.44526+00	{"object1": "job_template", "object1_pk": 6, "object2": "credential", "object2_pk": 1, "action": "associate", "relationship": "awx.main.models.unified_jobs.UnifiedJobTemplate_credentials"}	awx.main.models.unified_jobs.UnifiedJobTemplate_credentials	job_template	credential	1	awx-1	{"id": 1, "username": "admin", "last_name": "", "first_name": ""}	{}
12	create	2026-07-24 17:35:06.421209+00	{"name": "AWX EE (latest)", "description": "", "organization": null, "image": "quay.io/ansible/awx-ee:latest", "credential": null, "pull": "", "id": 1}		execution_environment		\N	awx-1	\N	{}
13	create	2026-07-24 17:35:06.446927+00	{"name": "Control Plane Execution Environment", "description": "", "organization": null, "image": "quay.io/ansible/awx-ee:latest", "credential": null, "pull": "", "id": 2}		execution_environment		\N	awx-1	\N	{}
14	create	2026-07-24 17:35:09.473874+00	{"hostname": "awx-1", "capacity_adjustment": "1", "enabled": true, "managed_by_policy": true, "node_type": "hybrid", "node_state": "installed", "peers": "main.ReceptorAddress.None", "listener_port": null, "peers_from_control_nodes": null, "id": 1}		instance		\N	awx-1	\N	{}
15	create	2026-07-24 17:35:15.450432+00	{"name": "controlplane", "max_concurrent_jobs": 0, "max_forks": 0, "is_container_group": false, "credential": null, "policy_instance_percentage": 0, "policy_instance_minimum": 0, "policy_instance_list": "[]", "pod_spec_override": "", "id": 1}		instance_group		\N	awx-1	\N	{}
16	update	2026-07-24 17:35:15.453547+00	{"policy_instance_percentage": [0, 100]}		instance_group		\N	awx-1	\N	{}
17	create	2026-07-24 17:35:18.396201+00	{"name": "default", "max_concurrent_jobs": 0, "max_forks": 0, "is_container_group": false, "credential": null, "policy_instance_percentage": 0, "policy_instance_minimum": 0, "policy_instance_list": "[]", "pod_spec_override": "", "id": 2}		instance_group		\N	awx-1	\N	{}
18	update	2026-07-24 17:35:18.398813+00	{"policy_instance_percentage": [0, 100]}		instance_group		\N	awx-1	\N	{}
\.

ALTER TABLE public.main_activitystream ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_ad_hoc_command; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_ad_hoc_command DISABLE TRIGGER ALL;

COPY public.main_activitystream_ad_hoc_command (id, activitystream_id, adhoccommand_id) FROM stdin;
\.

ALTER TABLE public.main_activitystream_ad_hoc_command ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_credential; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_credential DISABLE TRIGGER ALL;

COPY public.main_activitystream_credential (id, activitystream_id, credential_id) FROM stdin;
1	4	1
2	5	1
3	6	2
4	7	2
5	11	1
\.

ALTER TABLE public.main_activitystream_credential ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_credential_type; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_credential_type DISABLE TRIGGER ALL;

COPY public.main_activitystream_credential_type (id, activitystream_id, credentialtype_id) FROM stdin;
\.

ALTER TABLE public.main_activitystream_credential_type ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_execution_environment; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_execution_environment DISABLE TRIGGER ALL;

COPY public.main_activitystream_execution_environment (id, activitystream_id, executionenvironment_id) FROM stdin;
1	12	1
2	13	2
\.

ALTER TABLE public.main_activitystream_execution_environment ENABLE TRIGGER ALL;

--
-- Data for Name: main_group; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_group DISABLE TRIGGER ALL;

COPY public.main_group (id, created, modified, description, name, variables, created_by_id, inventory_id, modified_by_id) FROM stdin;
\.

ALTER TABLE public.main_group ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_group; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_group DISABLE TRIGGER ALL;

COPY public.main_activitystream_group (id, activitystream_id, group_id) FROM stdin;
\.

ALTER TABLE public.main_activitystream_group ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_host; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_host DISABLE TRIGGER ALL;

COPY public.main_activitystream_host (id, activitystream_id, host_id) FROM stdin;
1	9	1
\.

ALTER TABLE public.main_activitystream_host ENABLE TRIGGER ALL;

--
-- Data for Name: main_instance; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_instance DISABLE TRIGGER ALL;

COPY public.main_instance (id, uuid, hostname, created, modified, capacity, version, capacity_adjustment, cpu, memory, cpu_capacity, mem_capacity, enabled, managed_by_policy, ip_address, node_type, last_seen, errors, last_health_check, node_state, health_check_started, managed) FROM stdin;
1	145846c1-ac3d-4b71-95cd-521226b6df72	awx-1	2026-07-24 17:35:09.470467+00	2026-07-24 17:35:09.470484+00	619	24.6.2.dev872+gea14ee156	1.00	16.0	67118624768	64	619	t	t		hybrid	2026-07-24 17:37:23.672579+00		2026-07-24 17:37:23.672579+00	ready	\N	t
\.

ALTER TABLE public.main_instance ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_instance; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_instance DISABLE TRIGGER ALL;

COPY public.main_activitystream_instance (id, activitystream_id, instance_id) FROM stdin;
1	14	1
\.

ALTER TABLE public.main_activitystream_instance ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_instance_group; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_instance_group DISABLE TRIGGER ALL;

COPY public.main_activitystream_instance_group (id, activitystream_id, instancegroup_id) FROM stdin;
1	15	1
2	16	1
3	17	2
4	18	2
\.

ALTER TABLE public.main_activitystream_instance_group ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_inventory; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_inventory DISABLE TRIGGER ALL;

COPY public.main_activitystream_inventory (id, activitystream_id, inventory_id) FROM stdin;
1	8	1
\.

ALTER TABLE public.main_activitystream_inventory ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_inventory_source; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_inventory_source DISABLE TRIGGER ALL;

COPY public.main_activitystream_inventory_source (id, activitystream_id, inventorysource_id) FROM stdin;
\.

ALTER TABLE public.main_activitystream_inventory_source ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_inventory_update; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_inventory_update DISABLE TRIGGER ALL;

COPY public.main_activitystream_inventory_update (id, activitystream_id, inventoryupdate_id) FROM stdin;
\.

ALTER TABLE public.main_activitystream_inventory_update ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_job; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_job DISABLE TRIGGER ALL;

COPY public.main_activitystream_job (id, activitystream_id, job_id) FROM stdin;
\.

ALTER TABLE public.main_activitystream_job ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_job_template; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_job_template DISABLE TRIGGER ALL;

COPY public.main_activitystream_job_template (id, activitystream_id, jobtemplate_id) FROM stdin;
1	10	6
2	11	6
\.

ALTER TABLE public.main_activitystream_job_template ENABLE TRIGGER ALL;

--
-- Data for Name: main_label; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_label DISABLE TRIGGER ALL;

COPY public.main_label (id, created, modified, description, name, created_by_id, modified_by_id, organization_id) FROM stdin;
\.

ALTER TABLE public.main_label ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_label; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_label DISABLE TRIGGER ALL;

COPY public.main_activitystream_label (id, activitystream_id, label_id) FROM stdin;
\.

ALTER TABLE public.main_activitystream_label ENABLE TRIGGER ALL;

--
-- Data for Name: main_notificationtemplate; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_notificationtemplate DISABLE TRIGGER ALL;

COPY public.main_notificationtemplate (id, created, modified, description, name, notification_type, notification_configuration, created_by_id, modified_by_id, organization_id, messages) FROM stdin;
\.

ALTER TABLE public.main_notificationtemplate ENABLE TRIGGER ALL;

--
-- Data for Name: main_notification; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_notification DISABLE TRIGGER ALL;

COPY public.main_notification (id, created, modified, status, error, notifications_sent, notification_type, recipients, subject, notification_template_id, body) FROM stdin;
\.

ALTER TABLE public.main_notification ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_notification; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_notification DISABLE TRIGGER ALL;

COPY public.main_activitystream_notification (id, activitystream_id, notification_id) FROM stdin;
\.

ALTER TABLE public.main_activitystream_notification ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_notification_template; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_notification_template DISABLE TRIGGER ALL;

COPY public.main_activitystream_notification_template (id, activitystream_id, notificationtemplate_id) FROM stdin;
\.

ALTER TABLE public.main_activitystream_notification_template ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_organization; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_organization DISABLE TRIGGER ALL;

COPY public.main_activitystream_organization (id, activitystream_id, organization_id) FROM stdin;
1	2	1
2	7	1
\.

ALTER TABLE public.main_activitystream_organization ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_project; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_project DISABLE TRIGGER ALL;

COPY public.main_activitystream_project (id, activitystream_id, project_id) FROM stdin;
1	3	5
\.

ALTER TABLE public.main_activitystream_project ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_project_update; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_project_update DISABLE TRIGGER ALL;

COPY public.main_activitystream_project_update (id, activitystream_id, projectupdate_id) FROM stdin;
\.

ALTER TABLE public.main_activitystream_project_update ENABLE TRIGGER ALL;

--
-- Data for Name: main_receptoraddress; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_receptoraddress DISABLE TRIGGER ALL;

COPY public.main_receptoraddress (id, address, port, websocket_path, protocol, is_internal, canonical, peers_from_control_nodes, instance_id) FROM stdin;
1	awx-1	2222		tcp	f	t	f	1
\.

ALTER TABLE public.main_receptoraddress ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_receptor_address; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_receptor_address DISABLE TRIGGER ALL;

COPY public.main_activitystream_receptor_address (id, activitystream_id, receptoraddress_id) FROM stdin;
\.

ALTER TABLE public.main_activitystream_receptor_address ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_role; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_role DISABLE TRIGGER ALL;

COPY public.main_activitystream_role (id, activitystream_id, role_id) FROM stdin;
1	5	19
\.

ALTER TABLE public.main_activitystream_role ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_schedule; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_schedule DISABLE TRIGGER ALL;

COPY public.main_activitystream_schedule (id, activitystream_id, schedule_id) FROM stdin;
\.

ALTER TABLE public.main_activitystream_schedule ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_team; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_team DISABLE TRIGGER ALL;

COPY public.main_activitystream_team (id, activitystream_id, team_id) FROM stdin;
\.

ALTER TABLE public.main_activitystream_team ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_unified_job; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_unified_job DISABLE TRIGGER ALL;

COPY public.main_activitystream_unified_job (id, activitystream_id, unifiedjob_id) FROM stdin;
\.

ALTER TABLE public.main_activitystream_unified_job ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_unified_job_template; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_unified_job_template DISABLE TRIGGER ALL;

COPY public.main_activitystream_unified_job_template (id, activitystream_id, unifiedjobtemplate_id) FROM stdin;
\.

ALTER TABLE public.main_activitystream_unified_job_template ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_user; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_user DISABLE TRIGGER ALL;

COPY public.main_activitystream_user (id, activitystream_id, user_id) FROM stdin;
1	1	1
2	5	1
\.

ALTER TABLE public.main_activitystream_user ENABLE TRIGGER ALL;

--
-- Data for Name: main_workflowapprovaltemplate; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowapprovaltemplate DISABLE TRIGGER ALL;

COPY public.main_workflowapprovaltemplate (unifiedjobtemplate_ptr_id, timeout) FROM stdin;
\.

ALTER TABLE public.main_workflowapprovaltemplate ENABLE TRIGGER ALL;

--
-- Data for Name: main_workflowapproval; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowapproval DISABLE TRIGGER ALL;

COPY public.main_workflowapproval (unifiedjob_ptr_id, workflow_approval_template_id, timeout, timed_out, approved_or_denied_by_id, expires) FROM stdin;
\.

ALTER TABLE public.main_workflowapproval ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_workflow_approval; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_workflow_approval DISABLE TRIGGER ALL;

COPY public.main_activitystream_workflow_approval (id, activitystream_id, workflowapproval_id) FROM stdin;
\.

ALTER TABLE public.main_activitystream_workflow_approval ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_workflow_approval_template; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_workflow_approval_template DISABLE TRIGGER ALL;

COPY public.main_activitystream_workflow_approval_template (id, activitystream_id, workflowapprovaltemplate_id) FROM stdin;
\.

ALTER TABLE public.main_activitystream_workflow_approval_template ENABLE TRIGGER ALL;

--
-- Data for Name: main_workflowjobtemplate; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobtemplate DISABLE TRIGGER ALL;

COPY public.main_workflowjobtemplate (unifiedjobtemplate_ptr_id, extra_vars, admin_role_id, execute_role_id, read_role_id, survey_enabled, survey_spec, allow_simultaneous, ask_variables_on_launch, ask_inventory_on_launch, inventory_id, approval_role_id, ask_limit_on_launch, ask_scm_branch_on_launch, char_prompts, webhook_credential_id, webhook_key, webhook_service, ask_labels_on_launch, ask_skip_tags_on_launch, ask_tags_on_launch) FROM stdin;
\.

ALTER TABLE public.main_workflowjobtemplate ENABLE TRIGGER ALL;

--
-- Data for Name: main_workflowjob; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjob DISABLE TRIGGER ALL;

COPY public.main_workflowjob (unifiedjob_ptr_id, extra_vars, workflow_job_template_id, allow_simultaneous, is_sliced_job, job_template_id, inventory_id, webhook_credential_id, webhook_guid, webhook_service, is_bulk_job, char_prompts, survey_passwords) FROM stdin;
\.

ALTER TABLE public.main_workflowjob ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_workflow_job; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_workflow_job DISABLE TRIGGER ALL;

COPY public.main_activitystream_workflow_job (id, activitystream_id, workflowjob_id) FROM stdin;
\.

ALTER TABLE public.main_activitystream_workflow_job ENABLE TRIGGER ALL;

--
-- Data for Name: main_workflowjobnode; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobnode DISABLE TRIGGER ALL;

COPY public.main_workflowjobnode (id, created, modified, job_id, unified_job_template_id, workflow_job_id, inventory_id, ancestor_artifacts, extra_data, do_not_run, all_parents_must_converge, identifier, execution_environment_id, char_prompts, survey_passwords) FROM stdin;
\.

ALTER TABLE public.main_workflowjobnode ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_workflow_job_node; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_workflow_job_node DISABLE TRIGGER ALL;

COPY public.main_activitystream_workflow_job_node (id, activitystream_id, workflowjobnode_id) FROM stdin;
\.

ALTER TABLE public.main_activitystream_workflow_job_node ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_workflow_job_template; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_workflow_job_template DISABLE TRIGGER ALL;

COPY public.main_activitystream_workflow_job_template (id, activitystream_id, workflowjobtemplate_id) FROM stdin;
\.

ALTER TABLE public.main_activitystream_workflow_job_template ENABLE TRIGGER ALL;

--
-- Data for Name: main_workflowjobtemplatenode; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobtemplatenode DISABLE TRIGGER ALL;

COPY public.main_workflowjobtemplatenode (id, created, modified, unified_job_template_id, workflow_job_template_id, char_prompts, inventory_id, extra_data, survey_passwords, all_parents_must_converge, identifier, execution_environment_id) FROM stdin;
\.

ALTER TABLE public.main_workflowjobtemplatenode ENABLE TRIGGER ALL;

--
-- Data for Name: main_activitystream_workflow_job_template_node; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_activitystream_workflow_job_template_node DISABLE TRIGGER ALL;

COPY public.main_activitystream_workflow_job_template_node (id, activitystream_id, workflowjobtemplatenode_id) FROM stdin;
\.

ALTER TABLE public.main_activitystream_workflow_job_template_node ENABLE TRIGGER ALL;

--
-- Data for Name: main_credentialinputsource; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_credentialinputsource DISABLE TRIGGER ALL;

COPY public.main_credentialinputsource (id, created, modified, description, input_field_name, metadata, created_by_id, modified_by_id, source_credential_id, target_credential_id) FROM stdin;
\.

ALTER TABLE public.main_credentialinputsource ENABLE TRIGGER ALL;

--
-- Data for Name: main_custominventoryscript; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_custominventoryscript DISABLE TRIGGER ALL;

COPY public.main_custominventoryscript (id, created, modified, description, name, script, created_by_id, modified_by_id) FROM stdin;
\.

ALTER TABLE public.main_custominventoryscript ENABLE TRIGGER ALL;

--
-- Data for Name: main_eventquery; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_eventquery DISABLE TRIGGER ALL;

COPY public.main_eventquery (id, fqcn, collection_version, event_query) FROM stdin;
\.

ALTER TABLE public.main_eventquery ENABLE TRIGGER ALL;

--
-- Data for Name: main_group_hosts; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_group_hosts DISABLE TRIGGER ALL;

COPY public.main_group_hosts (id, group_id, host_id) FROM stdin;
\.

ALTER TABLE public.main_group_hosts ENABLE TRIGGER ALL;

--
-- Data for Name: main_group_inventory_sources; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_group_inventory_sources DISABLE TRIGGER ALL;

COPY public.main_group_inventory_sources (id, group_id, inventorysource_id) FROM stdin;
\.

ALTER TABLE public.main_group_inventory_sources ENABLE TRIGGER ALL;

--
-- Data for Name: main_group_parents; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_group_parents DISABLE TRIGGER ALL;

COPY public.main_group_parents (id, from_group_id, to_group_id) FROM stdin;
\.

ALTER TABLE public.main_group_parents ENABLE TRIGGER ALL;

--
-- Data for Name: main_host_inventory_sources; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_host_inventory_sources DISABLE TRIGGER ALL;

COPY public.main_host_inventory_sources (id, host_id, inventorysource_id) FROM stdin;
\.

ALTER TABLE public.main_host_inventory_sources ENABLE TRIGGER ALL;

--
-- Data for Name: main_hostmetric; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_hostmetric DISABLE TRIGGER ALL;

COPY public.main_hostmetric (hostname, first_automation, last_automation, last_deleted, automated_counter, deleted_counter, deleted, used_in_inventories, id) FROM stdin;
\.

ALTER TABLE public.main_hostmetric ENABLE TRIGGER ALL;

--
-- Data for Name: main_hostmetricsummarymonthly; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_hostmetricsummarymonthly DISABLE TRIGGER ALL;

COPY public.main_hostmetricsummarymonthly (id, date, license_consumed, license_capacity, hosts_added, hosts_deleted, indirectly_managed_hosts) FROM stdin;
\.

ALTER TABLE public.main_hostmetricsummarymonthly ENABLE TRIGGER ALL;

--
-- Data for Name: main_indirectmanagednodeaudit; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_indirectmanagednodeaudit DISABLE TRIGGER ALL;

COPY public.main_indirectmanagednodeaudit (id, created, name, canonical_facts, facts, events, count, host_id, inventory_id, job_id, organization_id) FROM stdin;
\.

ALTER TABLE public.main_indirectmanagednodeaudit ENABLE TRIGGER ALL;

--
-- Data for Name: main_instancegroup_instances; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_instancegroup_instances DISABLE TRIGGER ALL;

COPY public.main_instancegroup_instances (id, instancegroup_id, instance_id) FROM stdin;
1	1	1
2	2	1
\.

ALTER TABLE public.main_instancegroup_instances ENABLE TRIGGER ALL;

--
-- Data for Name: main_instancelink; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_instancelink DISABLE TRIGGER ALL;

COPY public.main_instancelink (id, source_id, link_state, target_id) FROM stdin;
\.

ALTER TABLE public.main_instancelink ENABLE TRIGGER ALL;

--
-- Data for Name: main_inventory_labels; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_inventory_labels DISABLE TRIGGER ALL;

COPY public.main_inventory_labels (id, inventory_id, label_id) FROM stdin;
\.

ALTER TABLE public.main_inventory_labels ENABLE TRIGGER ALL;

--
-- Data for Name: main_inventoryconstructedinventorymembership; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_inventoryconstructedinventorymembership DISABLE TRIGGER ALL;

COPY public.main_inventoryconstructedinventorymembership (id, "position", constructed_inventory_id, input_inventory_id) FROM stdin;
\.

ALTER TABLE public.main_inventoryconstructedinventorymembership ENABLE TRIGGER ALL;

--
-- Data for Name: main_inventorygroupvariableswithhistory; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_inventorygroupvariableswithhistory DISABLE TRIGGER ALL;

COPY public.main_inventorygroupvariableswithhistory (id, variables, group_id, inventory_id) FROM stdin;
\.

ALTER TABLE public.main_inventorygroupvariableswithhistory ENABLE TRIGGER ALL;

--
-- Data for Name: main_inventoryinstancegroupmembership; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_inventoryinstancegroupmembership DISABLE TRIGGER ALL;

COPY public.main_inventoryinstancegroupmembership (id, "position", instancegroup_id, inventory_id) FROM stdin;
\.

ALTER TABLE public.main_inventoryinstancegroupmembership ENABLE TRIGGER ALL;

--
-- Data for Name: main_jobhostsummary; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_jobhostsummary DISABLE TRIGGER ALL;

COPY public.main_jobhostsummary (id, created, modified, host_name, changed, dark, failures, ok, processed, skipped, failed, host_id, job_id, ignored, rescued, constructed_host_id) FROM stdin;
\.

ALTER TABLE public.main_jobhostsummary ENABLE TRIGGER ALL;

--
-- Data for Name: main_joblaunchconfig; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_joblaunchconfig DISABLE TRIGGER ALL;

COPY public.main_joblaunchconfig (id, extra_data, inventory_id, job_id, execution_environment_id, char_prompts, survey_passwords) FROM stdin;
\.

ALTER TABLE public.main_joblaunchconfig ENABLE TRIGGER ALL;

--
-- Data for Name: main_joblaunchconfig_credentials; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_joblaunchconfig_credentials DISABLE TRIGGER ALL;

COPY public.main_joblaunchconfig_credentials (id, joblaunchconfig_id, credential_id) FROM stdin;
\.

ALTER TABLE public.main_joblaunchconfig_credentials ENABLE TRIGGER ALL;

--
-- Data for Name: main_joblaunchconfig_labels; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_joblaunchconfig_labels DISABLE TRIGGER ALL;

COPY public.main_joblaunchconfig_labels (id, joblaunchconfig_id, label_id) FROM stdin;
\.

ALTER TABLE public.main_joblaunchconfig_labels ENABLE TRIGGER ALL;

--
-- Data for Name: main_joblaunchconfiginstancegroupmembership; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_joblaunchconfiginstancegroupmembership DISABLE TRIGGER ALL;

COPY public.main_joblaunchconfiginstancegroupmembership (id, "position", instancegroup_id, joblaunchconfig_id) FROM stdin;
\.

ALTER TABLE public.main_joblaunchconfiginstancegroupmembership ENABLE TRIGGER ALL;

--
-- Data for Name: main_organization_notification_templates_approvals; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_organization_notification_templates_approvals DISABLE TRIGGER ALL;

COPY public.main_organization_notification_templates_approvals (id, organization_id, notificationtemplate_id) FROM stdin;
\.

ALTER TABLE public.main_organization_notification_templates_approvals ENABLE TRIGGER ALL;

--
-- Data for Name: main_organization_notification_templates_error; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_organization_notification_templates_error DISABLE TRIGGER ALL;

COPY public.main_organization_notification_templates_error (id, organization_id, notificationtemplate_id) FROM stdin;
\.

ALTER TABLE public.main_organization_notification_templates_error ENABLE TRIGGER ALL;

--
-- Data for Name: main_organization_notification_templates_started; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_organization_notification_templates_started DISABLE TRIGGER ALL;

COPY public.main_organization_notification_templates_started (id, organization_id, notificationtemplate_id) FROM stdin;
\.

ALTER TABLE public.main_organization_notification_templates_started ENABLE TRIGGER ALL;

--
-- Data for Name: main_organization_notification_templates_success; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_organization_notification_templates_success DISABLE TRIGGER ALL;

COPY public.main_organization_notification_templates_success (id, organization_id, notificationtemplate_id) FROM stdin;
\.

ALTER TABLE public.main_organization_notification_templates_success ENABLE TRIGGER ALL;

--
-- Data for Name: main_organizationgalaxycredentialmembership; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_organizationgalaxycredentialmembership DISABLE TRIGGER ALL;

COPY public.main_organizationgalaxycredentialmembership (id, "position", credential_id, organization_id) FROM stdin;
1	0	2	1
\.

ALTER TABLE public.main_organizationgalaxycredentialmembership ENABLE TRIGGER ALL;

--
-- Data for Name: main_organizationinstancegroupmembership; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_organizationinstancegroupmembership DISABLE TRIGGER ALL;

COPY public.main_organizationinstancegroupmembership (id, "position", instancegroup_id, organization_id) FROM stdin;
\.

ALTER TABLE public.main_organizationinstancegroupmembership ENABLE TRIGGER ALL;

--
-- Data for Name: main_rbac_role_ancestors; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_rbac_role_ancestors DISABLE TRIGGER ALL;

COPY public.main_rbac_role_ancestors (id, role_field, content_type_id, object_id, ancestor_id, descendent_id) FROM stdin;
\.

ALTER TABLE public.main_rbac_role_ancestors ENABLE TRIGGER ALL;

--
-- Data for Name: main_rbac_roles_members; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_rbac_roles_members DISABLE TRIGGER ALL;

COPY public.main_rbac_roles_members (id, role_id, user_id) FROM stdin;
1	19	1
\.

ALTER TABLE public.main_rbac_roles_members ENABLE TRIGGER ALL;

--
-- Data for Name: main_rbac_roles_parents; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_rbac_roles_parents DISABLE TRIGGER ALL;

COPY public.main_rbac_roles_parents (id, from_role_id, to_role_id) FROM stdin;
\.

ALTER TABLE public.main_rbac_roles_parents ENABLE TRIGGER ALL;

--
-- Data for Name: main_schedule_credentials; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_schedule_credentials DISABLE TRIGGER ALL;

COPY public.main_schedule_credentials (id, schedule_id, credential_id) FROM stdin;
\.

ALTER TABLE public.main_schedule_credentials ENABLE TRIGGER ALL;

--
-- Data for Name: main_schedule_labels; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_schedule_labels DISABLE TRIGGER ALL;

COPY public.main_schedule_labels (id, schedule_id, label_id) FROM stdin;
\.

ALTER TABLE public.main_schedule_labels ENABLE TRIGGER ALL;

--
-- Data for Name: main_scheduleinstancegroupmembership; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_scheduleinstancegroupmembership DISABLE TRIGGER ALL;

COPY public.main_scheduleinstancegroupmembership (id, "position", instancegroup_id, schedule_id) FROM stdin;
\.

ALTER TABLE public.main_scheduleinstancegroupmembership ENABLE TRIGGER ALL;

--
-- Data for Name: main_smartinventorymembership; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_smartinventorymembership DISABLE TRIGGER ALL;

COPY public.main_smartinventorymembership (id, host_id, inventory_id) FROM stdin;
\.

ALTER TABLE public.main_smartinventorymembership ENABLE TRIGGER ALL;

--
-- Data for Name: main_towerschedulestate; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_towerschedulestate DISABLE TRIGGER ALL;

COPY public.main_towerschedulestate (id, schedule_last_run) FROM stdin;
1	2026-07-24 17:37:23.667795+00
\.

ALTER TABLE public.main_towerschedulestate ENABLE TRIGGER ALL;

--
-- Data for Name: main_unifiedjob_credentials; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjob_credentials DISABLE TRIGGER ALL;

COPY public.main_unifiedjob_credentials (id, unifiedjob_id, credential_id) FROM stdin;
\.

ALTER TABLE public.main_unifiedjob_credentials ENABLE TRIGGER ALL;

--
-- Data for Name: main_unifiedjob_dependent_jobs; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjob_dependent_jobs DISABLE TRIGGER ALL;

COPY public.main_unifiedjob_dependent_jobs (id, from_unifiedjob_id, to_unifiedjob_id) FROM stdin;
\.

ALTER TABLE public.main_unifiedjob_dependent_jobs ENABLE TRIGGER ALL;

--
-- Data for Name: main_unifiedjob_labels; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjob_labels DISABLE TRIGGER ALL;

COPY public.main_unifiedjob_labels (id, unifiedjob_id, label_id) FROM stdin;
\.

ALTER TABLE public.main_unifiedjob_labels ENABLE TRIGGER ALL;

--
-- Data for Name: main_unifiedjob_notifications; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjob_notifications DISABLE TRIGGER ALL;

COPY public.main_unifiedjob_notifications (id, unifiedjob_id, notification_id) FROM stdin;
\.

ALTER TABLE public.main_unifiedjob_notifications ENABLE TRIGGER ALL;

--
-- Data for Name: main_unifiedjobtemplate_credentials; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjobtemplate_credentials DISABLE TRIGGER ALL;

COPY public.main_unifiedjobtemplate_credentials (id, unifiedjobtemplate_id, credential_id) FROM stdin;
1	6	1
\.

ALTER TABLE public.main_unifiedjobtemplate_credentials ENABLE TRIGGER ALL;

--
-- Data for Name: main_unifiedjobtemplate_labels; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjobtemplate_labels DISABLE TRIGGER ALL;

COPY public.main_unifiedjobtemplate_labels (id, unifiedjobtemplate_id, label_id) FROM stdin;
\.

ALTER TABLE public.main_unifiedjobtemplate_labels ENABLE TRIGGER ALL;

--
-- Data for Name: main_unifiedjobtemplate_notification_templates_error; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjobtemplate_notification_templates_error DISABLE TRIGGER ALL;

COPY public.main_unifiedjobtemplate_notification_templates_error (id, unifiedjobtemplate_id, notificationtemplate_id) FROM stdin;
\.

ALTER TABLE public.main_unifiedjobtemplate_notification_templates_error ENABLE TRIGGER ALL;

--
-- Data for Name: main_unifiedjobtemplate_notification_templates_started; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjobtemplate_notification_templates_started DISABLE TRIGGER ALL;

COPY public.main_unifiedjobtemplate_notification_templates_started (id, unifiedjobtemplate_id, notificationtemplate_id) FROM stdin;
\.

ALTER TABLE public.main_unifiedjobtemplate_notification_templates_started ENABLE TRIGGER ALL;

--
-- Data for Name: main_unifiedjobtemplate_notification_templates_success; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjobtemplate_notification_templates_success DISABLE TRIGGER ALL;

COPY public.main_unifiedjobtemplate_notification_templates_success (id, unifiedjobtemplate_id, notificationtemplate_id) FROM stdin;
\.

ALTER TABLE public.main_unifiedjobtemplate_notification_templates_success ENABLE TRIGGER ALL;

--
-- Data for Name: main_unifiedjobtemplateinstancegroupmembership; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_unifiedjobtemplateinstancegroupmembership DISABLE TRIGGER ALL;

COPY public.main_unifiedjobtemplateinstancegroupmembership (id, "position", instancegroup_id, unifiedjobtemplate_id) FROM stdin;
\.

ALTER TABLE public.main_unifiedjobtemplateinstancegroupmembership ENABLE TRIGGER ALL;

--
-- Data for Name: main_usersessionmembership; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_usersessionmembership DISABLE TRIGGER ALL;

COPY public.main_usersessionmembership (id, created, session_id, user_id) FROM stdin;
\.

ALTER TABLE public.main_usersessionmembership ENABLE TRIGGER ALL;

--
-- Data for Name: main_workflowjobinstancegroupmembership; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobinstancegroupmembership DISABLE TRIGGER ALL;

COPY public.main_workflowjobinstancegroupmembership (id, "position", instancegroup_id, workflowjobnode_id) FROM stdin;
\.

ALTER TABLE public.main_workflowjobinstancegroupmembership ENABLE TRIGGER ALL;

--
-- Data for Name: main_workflowjobnode_always_nodes; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobnode_always_nodes DISABLE TRIGGER ALL;

COPY public.main_workflowjobnode_always_nodes (id, from_workflowjobnode_id, to_workflowjobnode_id) FROM stdin;
\.

ALTER TABLE public.main_workflowjobnode_always_nodes ENABLE TRIGGER ALL;

--
-- Data for Name: main_workflowjobnode_credentials; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobnode_credentials DISABLE TRIGGER ALL;

COPY public.main_workflowjobnode_credentials (id, workflowjobnode_id, credential_id) FROM stdin;
\.

ALTER TABLE public.main_workflowjobnode_credentials ENABLE TRIGGER ALL;

--
-- Data for Name: main_workflowjobnode_failure_nodes; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobnode_failure_nodes DISABLE TRIGGER ALL;

COPY public.main_workflowjobnode_failure_nodes (id, from_workflowjobnode_id, to_workflowjobnode_id) FROM stdin;
\.

ALTER TABLE public.main_workflowjobnode_failure_nodes ENABLE TRIGGER ALL;

--
-- Data for Name: main_workflowjobnode_labels; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobnode_labels DISABLE TRIGGER ALL;

COPY public.main_workflowjobnode_labels (id, workflowjobnode_id, label_id) FROM stdin;
\.

ALTER TABLE public.main_workflowjobnode_labels ENABLE TRIGGER ALL;

--
-- Data for Name: main_workflowjobnode_success_nodes; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobnode_success_nodes DISABLE TRIGGER ALL;

COPY public.main_workflowjobnode_success_nodes (id, from_workflowjobnode_id, to_workflowjobnode_id) FROM stdin;
\.

ALTER TABLE public.main_workflowjobnode_success_nodes ENABLE TRIGGER ALL;

--
-- Data for Name: main_workflowjobnodebaseinstancegroupmembership; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobnodebaseinstancegroupmembership DISABLE TRIGGER ALL;

COPY public.main_workflowjobnodebaseinstancegroupmembership (id, "position", instancegroup_id, workflowjobnode_id) FROM stdin;
\.

ALTER TABLE public.main_workflowjobnodebaseinstancegroupmembership ENABLE TRIGGER ALL;

--
-- Data for Name: main_workflowjobtemplate_notification_templates_approvals; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobtemplate_notification_templates_approvals DISABLE TRIGGER ALL;

COPY public.main_workflowjobtemplate_notification_templates_approvals (id, workflowjobtemplate_id, notificationtemplate_id) FROM stdin;
\.

ALTER TABLE public.main_workflowjobtemplate_notification_templates_approvals ENABLE TRIGGER ALL;

--
-- Data for Name: main_workflowjobtemplatenode_always_nodes; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobtemplatenode_always_nodes DISABLE TRIGGER ALL;

COPY public.main_workflowjobtemplatenode_always_nodes (id, from_workflowjobtemplatenode_id, to_workflowjobtemplatenode_id) FROM stdin;
\.

ALTER TABLE public.main_workflowjobtemplatenode_always_nodes ENABLE TRIGGER ALL;

--
-- Data for Name: main_workflowjobtemplatenode_credentials; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobtemplatenode_credentials DISABLE TRIGGER ALL;

COPY public.main_workflowjobtemplatenode_credentials (id, workflowjobtemplatenode_id, credential_id) FROM stdin;
\.

ALTER TABLE public.main_workflowjobtemplatenode_credentials ENABLE TRIGGER ALL;

--
-- Data for Name: main_workflowjobtemplatenode_failure_nodes; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobtemplatenode_failure_nodes DISABLE TRIGGER ALL;

COPY public.main_workflowjobtemplatenode_failure_nodes (id, from_workflowjobtemplatenode_id, to_workflowjobtemplatenode_id) FROM stdin;
\.

ALTER TABLE public.main_workflowjobtemplatenode_failure_nodes ENABLE TRIGGER ALL;

--
-- Data for Name: main_workflowjobtemplatenode_labels; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobtemplatenode_labels DISABLE TRIGGER ALL;

COPY public.main_workflowjobtemplatenode_labels (id, workflowjobtemplatenode_id, label_id) FROM stdin;
\.

ALTER TABLE public.main_workflowjobtemplatenode_labels ENABLE TRIGGER ALL;

--
-- Data for Name: main_workflowjobtemplatenode_success_nodes; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobtemplatenode_success_nodes DISABLE TRIGGER ALL;

COPY public.main_workflowjobtemplatenode_success_nodes (id, from_workflowjobtemplatenode_id, to_workflowjobtemplatenode_id) FROM stdin;
\.

ALTER TABLE public.main_workflowjobtemplatenode_success_nodes ENABLE TRIGGER ALL;

--
-- Data for Name: main_workflowjobtemplatenodebaseinstancegroupmembership; Type: TABLE DATA; Schema: public; Owner: awx
--

ALTER TABLE public.main_workflowjobtemplatenodebaseinstancegroupmembership DISABLE TRIGGER ALL;

COPY public.main_workflowjobtemplatenodebaseinstancegroupmembership (id, "position", instancegroup_id, workflowjobtemplatenode_id) FROM stdin;
\.

ALTER TABLE public.main_workflowjobtemplatenodebaseinstancegroupmembership ENABLE TRIGGER ALL;

--
-- Name: auth_group_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.auth_group_id_seq', 1, false);

--
-- Name: auth_group_permissions_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.auth_group_permissions_id_seq', 1, false);

--
-- Name: auth_permission_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.auth_permission_id_seq', 365, true);

--
-- Name: auth_user_groups_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.auth_user_groups_id_seq', 1, false);

--
-- Name: auth_user_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.auth_user_id_seq', 1, true);

--
-- Name: auth_user_user_permissions_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.auth_user_user_permissions_id_seq', 1, false);

--
-- Name: conf_setting_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.conf_setting_id_seq', 1, true);

--
-- Name: dab_feature_flags_aapflag_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.dab_feature_flags_aapflag_id_seq', 3, true);

--
-- Name: dab_rbac_dabcontenttype_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.dab_rbac_dabcontenttype_id_seq', 1, false);

--
-- Name: dab_rbac_dabpermission_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.dab_rbac_dabpermission_id_seq', 50, true);

--
-- Name: dab_rbac_objectrole_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.dab_rbac_objectrole_id_seq', 1, true);

--
-- Name: dab_rbac_objectrole_provides_teams_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.dab_rbac_objectrole_provides_teams_id_seq', 1, false);

--
-- Name: dab_rbac_roledefinition_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.dab_rbac_roledefinition_id_seq', 33, true);

--
-- Name: dab_rbac_roledefinition_permissions_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.dab_rbac_roledefinition_permissions_id_seq', 187, true);

--
-- Name: dab_rbac_roleevaluation_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.dab_rbac_roleevaluation_id_seq', 4, true);

--
-- Name: dab_rbac_roleevaluationuuid_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.dab_rbac_roleevaluationuuid_id_seq', 1, false);

--
-- Name: dab_rbac_roleteamassignment_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.dab_rbac_roleteamassignment_id_seq', 1, false);

--
-- Name: dab_rbac_roleuserassignment_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.dab_rbac_roleuserassignment_id_seq', 1, true);

--
-- Name: dab_resource_registry_resource_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.dab_resource_registry_resource_id_seq', 38, true);

--
-- Name: dab_resource_registry_resourcetype_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.dab_resource_registry_resourcetype_id_seq', 5, true);

--
-- Name: django_content_type_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.django_content_type_id_seq', 92, true);

--
-- Name: django_migrations_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.django_migrations_id_seq', 312, true);

--
-- Name: django_site_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.django_site_id_seq', 1, true);

--
-- Name: flags_flagstate_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.flags_flagstate_id_seq', 1, false);

--
-- Name: main_activitystream_ad_hoc_command_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_ad_hoc_command_id_seq', 1, false);

--
-- Name: main_activitystream_credential_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_credential_id_seq', 5, true);

--
-- Name: main_activitystream_credential_type_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_credential_type_id_seq', 1, false);

--
-- Name: main_activitystream_execution_environment_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_execution_environment_id_seq', 2, true);

--
-- Name: main_activitystream_group_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_group_id_seq', 1, false);

--
-- Name: main_activitystream_host_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_host_id_seq', 1, true);

--
-- Name: main_activitystream_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_id_seq', 18, true);

--
-- Name: main_activitystream_instance_group_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_instance_group_id_seq', 4, true);

--
-- Name: main_activitystream_instance_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_instance_id_seq', 1, true);

--
-- Name: main_activitystream_inventory_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_inventory_id_seq', 1, true);

--
-- Name: main_activitystream_inventory_source_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_inventory_source_id_seq', 1, false);

--
-- Name: main_activitystream_inventory_update_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_inventory_update_id_seq', 1, false);

--
-- Name: main_activitystream_job_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_job_id_seq', 1, false);

--
-- Name: main_activitystream_job_template_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_job_template_id_seq', 2, true);

--
-- Name: main_activitystream_label_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_label_id_seq', 1, false);

--
-- Name: main_activitystream_notification_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_notification_id_seq', 1, false);

--
-- Name: main_activitystream_notification_template_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_notification_template_id_seq', 1, false);

--
-- Name: main_activitystream_organization_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_organization_id_seq', 2, true);

--
-- Name: main_activitystream_project_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_project_id_seq', 1, true);

--
-- Name: main_activitystream_project_update_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_project_update_id_seq', 1, false);

--
-- Name: main_activitystream_receptor_address_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_receptor_address_id_seq', 1, false);

--
-- Name: main_activitystream_role_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_role_id_seq', 1, true);

--
-- Name: main_activitystream_schedule_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_schedule_id_seq', 1, false);

--
-- Name: main_activitystream_team_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_team_id_seq', 1, false);

--
-- Name: main_activitystream_unified_job_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_unified_job_id_seq', 1, false);

--
-- Name: main_activitystream_unified_job_template_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_unified_job_template_id_seq', 1, false);

--
-- Name: main_activitystream_user_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_user_id_seq', 2, true);

--
-- Name: main_activitystream_workflow_approval_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_workflow_approval_id_seq', 1, false);

--
-- Name: main_activitystream_workflow_approval_template_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_workflow_approval_template_id_seq', 1, false);

--
-- Name: main_activitystream_workflow_job_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_workflow_job_id_seq', 1, false);

--
-- Name: main_activitystream_workflow_job_node_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_workflow_job_node_id_seq', 1, false);

--
-- Name: main_activitystream_workflow_job_template_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_workflow_job_template_id_seq', 1, false);

--
-- Name: main_activitystream_workflow_job_template_node_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_activitystream_workflow_job_template_node_id_seq', 1, false);

--
-- Name: main_adhoccommandevent_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_adhoccommandevent_id_seq', 1, false);

--
-- Name: main_adhoccommandevent_id_seq1; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_adhoccommandevent_id_seq1', 1, false);

--
-- Name: main_credential_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_credential_id_seq', 2, true);

--
-- Name: main_credentialinputsource_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_credentialinputsource_id_seq', 1, false);

--
-- Name: main_credentialtype_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_credentialtype_id_seq', 8, true);

--
-- Name: main_custominventoryscript_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_custominventoryscript_id_seq', 1, false);

--
-- Name: main_eventquery_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_eventquery_id_seq', 1, false);

--
-- Name: main_executionenvironment_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_executionenvironment_id_seq', 2, true);

--
-- Name: main_group_hosts_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_group_hosts_id_seq', 1, false);

--
-- Name: main_group_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_group_id_seq', 1, false);

--
-- Name: main_group_inventory_sources_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_group_inventory_sources_id_seq', 1, false);

--
-- Name: main_group_parents_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_group_parents_id_seq', 1, false);

--
-- Name: main_host_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_host_id_seq', 1, true);

--
-- Name: main_host_inventory_sources_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_host_inventory_sources_id_seq', 1, false);

--
-- Name: main_hostmetric_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_hostmetric_id_seq', 1, false);

--
-- Name: main_hostmetricsummarymonthly_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_hostmetricsummarymonthly_id_seq', 1, false);

--
-- Name: main_indirectmanagednodeaudit_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_indirectmanagednodeaudit_id_seq', 1, false);

--
-- Name: main_instance_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_instance_id_seq', 1, true);

--
-- Name: main_instancegroup_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_instancegroup_id_seq', 2, true);

--
-- Name: main_instancegroup_instances_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_instancegroup_instances_id_seq', 2, true);

--
-- Name: main_instancelink_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_instancelink_id_seq', 1, false);

--
-- Name: main_inventory_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_inventory_id_seq', 1, true);

--
-- Name: main_inventory_labels_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_inventory_labels_id_seq', 1, false);

--
-- Name: main_inventoryconstructedinventorymembership_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_inventoryconstructedinventorymembership_id_seq', 1, false);

--
-- Name: main_inventorygroupvariableswithhistory_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_inventorygroupvariableswithhistory_id_seq', 1, false);

--
-- Name: main_inventoryinstancegroupmembership_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_inventoryinstancegroupmembership_id_seq', 1, false);

--
-- Name: main_inventoryupdateevent_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_inventoryupdateevent_id_seq', 1, false);

--
-- Name: main_inventoryupdateevent_id_seq1; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_inventoryupdateevent_id_seq1', 1, false);

--
-- Name: main_jobevent_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_jobevent_id_seq', 1, false);

--
-- Name: main_jobevent_id_seq1; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_jobevent_id_seq1', 1, false);

--
-- Name: main_jobhostsummary_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_jobhostsummary_id_seq', 1, false);

--
-- Name: main_joblaunchconfig_credentials_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_joblaunchconfig_credentials_id_seq', 1, false);

--
-- Name: main_joblaunchconfig_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_joblaunchconfig_id_seq', 1, false);

--
-- Name: main_joblaunchconfig_labels_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_joblaunchconfig_labels_id_seq', 1, false);

--
-- Name: main_joblaunchconfiginstancegroupmembership_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_joblaunchconfiginstancegroupmembership_id_seq', 1, false);

--
-- Name: main_label_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_label_id_seq', 1, false);

--
-- Name: main_notification_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_notification_id_seq', 1, false);

--
-- Name: main_notificationtemplate_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_notificationtemplate_id_seq', 1, false);

--
-- Name: main_organization_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_organization_id_seq', 1, true);

--
-- Name: main_organization_notification_templates_approvals_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_organization_notification_templates_approvals_id_seq', 1, false);

--
-- Name: main_organization_notification_templates_error_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_organization_notification_templates_error_id_seq', 1, false);

--
-- Name: main_organization_notification_templates_started_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_organization_notification_templates_started_id_seq', 1, false);

--
-- Name: main_organization_notification_templates_success_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_organization_notification_templates_success_id_seq', 1, false);

--
-- Name: main_organizationgalaxycredentialmembership_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_organizationgalaxycredentialmembership_id_seq', 1, true);

--
-- Name: main_organizationinstancegroupmembership_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_organizationinstancegroupmembership_id_seq', 1, false);

--
-- Name: main_projectupdateevent_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_projectupdateevent_id_seq', 1, false);

--
-- Name: main_projectupdateevent_id_seq1; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_projectupdateevent_id_seq1', 1, false);

--
-- Name: main_rbac_role_ancestors_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_rbac_role_ancestors_id_seq', 1, false);

--
-- Name: main_rbac_roles_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_rbac_roles_id_seq', 38, true);

--
-- Name: main_rbac_roles_members_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_rbac_roles_members_id_seq', 1, true);

--
-- Name: main_rbac_roles_parents_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_rbac_roles_parents_id_seq', 1, false);

--
-- Name: main_receptoraddress_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_receptoraddress_id_seq', 1, true);

--
-- Name: main_schedule_credentials_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_schedule_credentials_id_seq', 1, false);

--
-- Name: main_schedule_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_schedule_id_seq', 4, true);

--
-- Name: main_schedule_labels_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_schedule_labels_id_seq', 1, false);

--
-- Name: main_scheduleinstancegroupmembership_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_scheduleinstancegroupmembership_id_seq', 1, false);

--
-- Name: main_smartinventorymembership_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_smartinventorymembership_id_seq', 1, false);

--
-- Name: main_systemjobevent_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_systemjobevent_id_seq', 1, false);

--
-- Name: main_systemjobevent_id_seq1; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_systemjobevent_id_seq1', 1, false);

--
-- Name: main_team_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_team_id_seq', 1, false);

--
-- Name: main_towerschedulestate_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_towerschedulestate_id_seq', 1, false);

--
-- Name: main_unifiedjob_credentials_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_unifiedjob_credentials_id_seq', 1, false);

--
-- Name: main_unifiedjob_dependent_jobs_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_unifiedjob_dependent_jobs_id_seq', 1, false);

--
-- Name: main_unifiedjob_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_unifiedjob_id_seq', 1, false);

--
-- Name: main_unifiedjob_labels_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_unifiedjob_labels_id_seq', 1, false);

--
-- Name: main_unifiedjob_notifications_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_unifiedjob_notifications_id_seq', 1, false);

--
-- Name: main_unifiedjobtemplate_credentials_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_unifiedjobtemplate_credentials_id_seq', 1, true);

--
-- Name: main_unifiedjobtemplate_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_unifiedjobtemplate_id_seq', 6, true);

--
-- Name: main_unifiedjobtemplate_labels_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_unifiedjobtemplate_labels_id_seq', 1, false);

--
-- Name: main_unifiedjobtemplate_notification_templates_error_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_unifiedjobtemplate_notification_templates_error_id_seq', 1, false);

--
-- Name: main_unifiedjobtemplate_notification_templates_started_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_unifiedjobtemplate_notification_templates_started_id_seq', 1, false);

--
-- Name: main_unifiedjobtemplate_notification_templates_success_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_unifiedjobtemplate_notification_templates_success_id_seq', 1, false);

--
-- Name: main_unifiedjobtemplateinstancegroupmembership_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_unifiedjobtemplateinstancegroupmembership_id_seq', 1, false);

--
-- Name: main_usersessionmembership_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_usersessionmembership_id_seq', 1, false);

--
-- Name: main_workflowjobinstancegroupmembership_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_workflowjobinstancegroupmembership_id_seq', 1, false);

--
-- Name: main_workflowjobnode_always_nodes_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_workflowjobnode_always_nodes_id_seq', 1, false);

--
-- Name: main_workflowjobnode_credentials_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_workflowjobnode_credentials_id_seq', 1, false);

--
-- Name: main_workflowjobnode_failure_nodes_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_workflowjobnode_failure_nodes_id_seq', 1, false);

--
-- Name: main_workflowjobnode_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_workflowjobnode_id_seq', 1, false);

--
-- Name: main_workflowjobnode_labels_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_workflowjobnode_labels_id_seq', 1, false);

--
-- Name: main_workflowjobnode_success_nodes_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_workflowjobnode_success_nodes_id_seq', 1, false);

--
-- Name: main_workflowjobnodebaseinstancegroupmembership_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_workflowjobnodebaseinstancegroupmembership_id_seq', 1, false);

--
-- Name: main_workflowjobtemplate_notification_templates_approval_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_workflowjobtemplate_notification_templates_approval_id_seq', 1, false);

--
-- Name: main_workflowjobtemplatenode_always_nodes_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_workflowjobtemplatenode_always_nodes_id_seq', 1, false);

--
-- Name: main_workflowjobtemplatenode_credentials_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_workflowjobtemplatenode_credentials_id_seq', 1, false);

--
-- Name: main_workflowjobtemplatenode_failure_nodes_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_workflowjobtemplatenode_failure_nodes_id_seq', 1, false);

--
-- Name: main_workflowjobtemplatenode_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_workflowjobtemplatenode_id_seq', 1, false);

--
-- Name: main_workflowjobtemplatenode_labels_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_workflowjobtemplatenode_labels_id_seq', 1, false);

--
-- Name: main_workflowjobtemplatenode_success_nodes_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_workflowjobtemplatenode_success_nodes_id_seq', 1, false);

--
-- Name: main_workflowjobtemplatenodebaseinstancegroupmembership_id_seq; Type: SEQUENCE SET; Schema: public; Owner: awx
--

SELECT pg_catalog.setval('public.main_workflowjobtemplatenodebaseinstancegroupmembership_id_seq', 1, false);

--
-- PostgreSQL database dump complete
--
