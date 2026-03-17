DO $$
DECLARE
  --
  job_content_type_id INTEGER;
  --
  i_text text;
  task_uuid_1 text;
  task_uuid_2 text;
  event_data_1 text;
  event_data_2 text;
  --
  default_organization_id                           INTEGER;
  default_inventory_id                              INTEGER;
  default_instance_uuid UUID;
  default_unified_job_template_id                   INTEGER;
  -- enable for testing purposes for being able to repeatedly insert data
  --random_suffix    TEXT := substring(md5(random()::text), 1, 5);
  random_suffix                   TEXT := '2025-06-13';
  --
  random_ip        TEXT :=
     (floor(random()*256)::int)::text
     ||'.'||(floor(random()*256)::int)::text
     ||'.'||(floor(random()*256)::int)::text
     ||'.'||(floor(random()*256)::int)::text;
  --
  -- hosts
  host_ids          INTEGER[] := ARRAY[]::INTEGER[];
  host_id           INTEGER;
  i                 INTEGER;
  host_name         TEXT;
  -- do not format this, it will break the generator script that uses text replacement
  -- script name: generate_ccsp.py
  -- if you change this, you need to change the generator script
  host_count INTEGER := 2;
  job_count INTEGER := 3;
  --
  -- unified jobs
  unified_jobs_10   INTEGER[] := ARRAY[]::INTEGER[];  -- Jobs for 10:00 hour
  unified_jobs_11   INTEGER[] := ARRAY[]::INTEGER[];  -- Jobs for 11:00 hour
  unified_job_id    INTEGER;
  --
  -- credentials
  machine_credential_type_id INTEGER;
  cloud_credential_type_id INTEGER;
  vault_credential_type_id INTEGER;
  network_credential_type_id INTEGER;
  custom_credential_type_id INTEGER;
  machine_credential_id INTEGER;
  cloud_credential_id INTEGER;
  vault_credential_id INTEGER;
  network_credential_id INTEGER;
  custom_credential_id INTEGER;
  --
  -- execution environments
  ee1_id INTEGER;  -- Python ML Environment (no redhat.rhel_system_roles)
  ee2_id INTEGER;  -- Node Backend Environment (with redhat.rhel_system_roles)
  --
BEGIN
  --
  -- Insert django_content_type entry for 'job' model
  --
  INSERT INTO public.django_content_type (app_label, model)
  VALUES ('main', 'job')
  RETURNING id INTO job_content_type_id;
  
  RAISE NOTICE 'Inserted django_content_type for job model with id = %', job_content_type_id;
  --
  --
  -- ORGANIZATION
  --
  INSERT INTO public.main_organization (
    created,
    modified,
    description,
    name,
    max_hosts
  )
  VALUES (
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',                             -- created
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',                             -- modified
    '',                                -- description
    'default_org_' || random_suffix,   -- name w/ random suffix
    0                                  -- max_hosts
  )
  RETURNING id
    INTO default_organization_id;
  --
  RAISE NOTICE 'Inserted Organization % with id = %',
               'default_org_' || random_suffix,
               default_organization_id;
  --
  -- INVENTORY
  --
  INSERT INTO public.main_inventory (
    created,
    modified,
    description,
    name,
    variables,
    has_active_failures,
    total_hosts,
    hosts_with_active_failures,
    total_groups,
    has_inventory_sources,
    total_inventory_sources,
    inventory_sources_with_failures,
    organization_id,
    kind,
    pending_deletion,
    prevent_instance_group_fallback
  )
  VALUES (
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',                                  -- created
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',                                  -- modified
    '',                                     -- description
    'default_inventory_' || random_suffix,  -- name w/ same suffix
    '{}',                                   -- variables (empty JSON)
    false,                                  -- has_active_failures
    0,                                      -- total_hosts
    0,                                      -- hosts_with_active_failures
    0,                                      -- total_groups
    false,                                  -- has_inventory_sources
    0,                                      -- total_inventory_sources
    0,                                      -- inventory_sources_with_failures
    default_organization_id,                         -- fk to org
    'constructed',                          -- kind (adjust as needed)
    false,                                  -- pending_deletion
    false                                   -- prevent_instance_group_fallback
  )
  RETURNING id
    INTO default_inventory_id;
  --
  RAISE NOTICE 'Inserted Inventory % with id = %',
               'default_inventory_' || random_suffix,
               default_inventory_id;
  --
  -- Get the instance UUID for the default instance (created in main_instance.sql)
  -- This instance is used for linking hosts
  --
  SELECT uuid INTO default_instance_uuid
  FROM public.main_instance
  WHERE hostname = 'default_host_instance_' || random_suffix
  LIMIT 1;
  --
  IF default_instance_uuid IS NULL THEN
    RAISE EXCEPTION 'Default instance with hostname default_host_instance_% not found. Ensure main_instance.sql is loaded before main_jobhostsummary.sql', random_suffix;
  END IF;
  --
  RAISE NOTICE 'Using existing Main Instance with UUID = %', default_instance_uuid;
  --
  -- Fill hosts in loop
  --
  -- LOOP TO INSERT HOSTS
  FOR i IN 1..host_count LOOP
    INSERT INTO public.main_host (
      created,
      modified,
      description,
      name,
      enabled,
      instance_id,
      variables,
      inventory_id,
      ansible_facts
    ) VALUES (
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      '',                                            -- non‐null description
      'default_host_' || i || '_' || random_suffix, -- unique name
      true,
      default_instance_uuid::text,
-- This must not be moved right, otherwise it will break
$yaml$
ansible_host: "default_ansible_host"
ansible_connection: "default_ansible_connection"
ansible_user: "default_ansible_user"
ansible_port: 22
ansible_ssh_private_key_file: "/home/default/.ssh/id_rsa"
max_retries: 3
retry_interval: 5
timeout: 30
deploy_env: "production"
log_level: "INFO"
$yaml$,
      default_inventory_id,
      '{}'::jsonb                                    -- non‐null ansible_facts
    )
    RETURNING id INTO host_id;
    --
    host_ids := array_append(host_ids, host_id);
  END LOOP;
  --
  RAISE NOTICE 'Inserted % hosts with IDs: %', array_length(host_ids,1), host_ids;
  --
  -- UNIFIED JOB TEMPLATE
  --
  INSERT INTO public.main_unifiedjobtemplate (
    created,
    modified,
    description,
    name,
    old_pk,
    last_job_failed,
    status,
    organization_id,
    org_unique
    )
  VALUES (
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00', -- created
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00', -- modified
    '',                                                -- description
    'default_unified_job_template_' || random_suffix,  -- name (w/ random suffix)
    0,                                                 -- old_pk (must be >= 0)
    false,                                             -- last_job_failed
    'never updated',                                   -- status (adjust as needed)
    default_organization_id,                           -- organization_id
    false                                              -- org_unique
  )
  RETURNING id
  INTO default_unified_job_template_id;
  --
  RAISE NOTICE 'Inserted UnifiedJobTemplate % with id = %',
               'default_job_template_' || random_suffix,
               default_unified_job_template_id;
  --
  -- Project
  --
  INSERT INTO public.main_project (
      unifiedjobtemplate_ptr_id,
      local_path,
      scm_type,
      scm_url,
      scm_branch,
      scm_clean,
      scm_delete_on_update,
      scm_update_on_launch,
      scm_update_cache_timeout,
      timeout,
      scm_revision,
      playbook_files,
      inventory_files,
      scm_refspec,
      allow_override,
      scm_track_submodules
  ) VALUES (
      default_unified_job_template_id,
      'LOCAL_PATH',                    
      'git',                      -- scm_type: git for testing
      'SCM_URL',                       
      'SCM_BRANCH',                   
      TRUE,                           
      FALSE,                           
      TRUE,                            
      0,                               
      0,                               
      'SCM_REVISION',                  
      '{}'::jsonb,                   
      '{}'::jsonb,   
      'SCM_REFSPEC',                   
      TRUE,                            
      FALSE                            
  );
  --
  -- Job Template
  --
  INSERT INTO public.main_jobtemplate (
    unifiedjobtemplate_ptr_id,
    job_type,
    playbook,
    forks,
    "limit",
    verbosity,
    extra_vars,
    job_tags,
    force_handlers,
    skip_tags,
    start_at_task,
    become_enabled,
    host_config_key,
    ask_variables_on_launch,
    survey_enabled,
    survey_spec,
    inventory_id,
    ask_limit_on_launch,
    ask_inventory_on_launch,
    ask_credential_on_launch,
    ask_job_type_on_launch,
    ask_tags_on_launch,
    allow_simultaneous,
    ask_skip_tags_on_launch,
    timeout,
    use_fact_cache,
    ask_verbosity_on_launch,
    ask_diff_mode_on_launch,
    diff_mode,
    job_slice_count,
    ask_scm_branch_on_launch,
    scm_branch,
    webhook_key,
    webhook_service,
    ask_execution_environment_on_launch,
    ask_forks_on_launch,
    ask_instance_groups_on_launch,
    ask_job_slice_count_on_launch,
    ask_labels_on_launch,
    ask_timeout_on_launch,
    prevent_instance_group_fallback
  )
  VALUES (
    default_unified_job_template_id,  -- the FK you just created
    'manual',                         -- job_type
    '',                               -- playbook
    0,                                -- forks
    '',                               -- limit
    0,                                -- verbosity
    '{}'::text,                       -- extra_vars
    '{}'::text,                       -- job_tags
    false,                            -- force_handlers
    '',                               -- skip_tags
    '',                               -- start_at_task
    false,                            -- become_enabled
    '',                               -- host_config_key
    false,                            -- ask_variables_on_launch
    false,                            -- survey_enabled
    '{}'::jsonb,                      -- survey_spec
    default_inventory_id,             -- inventory_id
    false,                            -- ask_limit_on_launch
    false,                            -- ask_inventory_on_launch
    false,                            -- ask_credential_on_launch
    false,                            -- ask_job_type_on_launch
    false,                            -- ask_tags_on_launch
    false,                            -- allow_simultaneous
    false,                            -- ask_skip_tags_on_launch
    0,                                -- timeout
    false,                            -- use_fact_cache
    false,                            -- ask_verbosity_on_launch
    false,                            -- ask_diff_mode_on_launch
    false,                            -- diff_mode
    0,                                -- job_slice_count
    false,                            -- ask_scm_branch_on_launch
    '',                               -- scm_branch
    '',                               -- webhook_key
    '',                               -- webhook_service
    false,                            -- ask_execution_environment_on_launch
    false,                            -- ask_forks_on_launch
    false,                            -- ask_instance_groups_on_launch
    false,                            -- ask_job_slice_count_on_launch
    false,                            -- ask_labels_on_launch
    false,                            -- ask_timeout_on_launch
    false                             -- prevent_instance_group_fallback
  );
  --
  RAISE NOTICE 'Inserted Main JobTemplate ptr_id = %',
               default_unified_job_template_id;
  --
  -- Execution Environments
  --
  -- EE 1: Python ML Environment (linked to jobs with no redhat.rhel_system_roles)
  INSERT INTO public.main_executionenvironment (
    created,
    modified,
    description,
    image,
    managed,
    name,
    pull
  ) VALUES (
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
    'Python 3.11 environment with common ML libraries',
    'registry.example.com/envs/python-ml:3.11',
    TRUE,
    'Python ML Environment',
    'always'
  )
  RETURNING id INTO ee1_id;
  --
  -- EE 2: Node Backend Environment (linked to jobs with redhat.rhel_system_roles)
  INSERT INTO public.main_executionenvironment (
    created,
    modified,
    description,
    image,
    managed,
    name,
    pull
  ) VALUES (
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
    'Node.js 20 environment for backend services',
    'registry.example.com/envs/node-backend:20',
    FALSE,
    'Node Backend Environment',
    'missing'
  )
  RETURNING id INTO ee2_id;
  --
  RAISE NOTICE 'Inserted Execution Environments: EE1 (Python ML) id=%, EE2 (Node Backend) id=%',
               ee1_id, ee2_id;
  --
  -- Unified Jobs
  -- Loop to create unified jobs
  FOR i IN 1..job_count LOOP
    INSERT INTO public.main_unifiedjob (
      created,
      started,
      finished,
      modified,
      description,
      name,
      launch_type,
      cancel_flag,
      status,
      failed,
      elapsed,
      job_args,
      job_cwd,
      job_explanation,
      start_args,
      result_traceback,
      celery_task_id,
      unified_job_template_id,
      organization_id,
      execution_node,
      emitted_events,
      controller_node,
      dependencies_processed,
      installed_collections,
      ansible_version,
      task_impact,
      job_env,
      polymorphic_ctype_id,
      execution_environment_id
    )
    VALUES (
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',                                  -- created (same for all jobs)
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00' + (i * INTERVAL '10 seconds'),  -- started (varies by job, 10s apart for waiting time)
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00' + (i * INTERVAL '10 seconds') + 
        CASE i
          WHEN 1 THEN INTERVAL '120 seconds'  -- Job 1: 120 seconds (2 minutes)
          WHEN 2 THEN INTERVAL '180 seconds'  -- Job 2: 180 seconds (3 minutes)
          WHEN 3 THEN INTERVAL '90 seconds'   -- Job 3: 90 seconds (1.5 minutes)
        END,                                  -- finished (varies by job with different durations)
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00' + (i * INTERVAL '10 seconds') + 
        CASE i
          WHEN 1 THEN INTERVAL '120 seconds'
          WHEN 2 THEN INTERVAL '180 seconds'
          WHEN 3 THEN INTERVAL '90 seconds'
        END,                                  -- modified (same as finished)
      ''::text,                               -- description
      'default_unified_job_' || random_suffix, -- name
      CASE (i % 4)
        WHEN 1 THEN 'manual'
        WHEN 2 THEN 'scheduled'
        WHEN 3 THEN 'workflow'
        WHEN 0 THEN 'callback'
      END,                                    -- launch_type (cycles through: manual, scheduled, workflow, callback)
      false,                                  -- cancel_flag
      CASE i
        WHEN 3 THEN 'failed'
        ELSE 'pending'
      END,                                    -- status
      CASE i
        WHEN 3 THEN true
        ELSE false
      END,                                    -- failed
      CASE i
        WHEN 1 THEN 120.000  -- Job 1: 120 seconds
        WHEN 2 THEN 180.000  -- Job 2: 180 seconds
        WHEN 3 THEN 90.000   -- Job 3: 90 seconds
      END,                                    -- elapsed (matches finished - started)
      '{}'::text,                             -- job_args
      '/tmp',                                 -- job_cwd
      ''::text,                               -- job_explanation
      '{}'::text,                             -- start_args
      ''::text,                               -- result_traceback
      gen_random_uuid()::text,                -- celery_task_id
      default_unified_job_template_id,        -- FK to your template
      default_organization_id,
      'auto',                                 -- execution_node
      0,                                      -- emitted_events
      'controller1',                          -- controller_node
      false,                                  -- dependencies_processed
      CASE i
        WHEN 3 THEN '{"ansible.builtin": {"version": "2.9.10"}, "a10.acos_axapi": {"version": "1.0.0"}, "redhat.rhel_system_roles": {"version": "1.23.0"}}'::jsonb
        ELSE '{"ansible.builtin": {"version": "2.9.10"}, "a10.acos_axapi": {"version": "1.0.0"}}'::jsonb
      END,                                    -- installed_collections (job 3 also includes redhat.rhel_system_roles)
      '2.9.10',                               -- ansible_version
      0,                                      -- task_impact
      '{}'::jsonb,                            -- job_env
      job_content_type_id,                    -- polymorphic_ctype_id
      CASE i
        WHEN 3 THEN ee2_id  -- job 3: with redhat.rhel_system_roles → Node Backend EE
        ELSE ee1_id         -- jobs 1,2: no rhel_system_roles → Python ML EE
      END                                     -- execution_environment_id
    )
    RETURNING id
    INTO unified_job_id;

    -- Append to our array
    unified_jobs_10 := array_append(unified_jobs_10, unified_job_id);
    --
    -- Create Main Job and connect it with unified job using its id
    --
    INSERT INTO public.main_job (
      unifiedjob_ptr_id,
      job_type,
      playbook,
      forks,
      "limit",
      verbosity,
      extra_vars,
      job_tags,
      force_handlers,
      skip_tags,
      start_at_task,
      become_enabled,
      inventory_id,
      job_template_id,
      project_id,
      allow_simultaneous,
      artifacts,
      timeout,
      scm_revision,
      use_fact_cache,
      diff_mode,
      job_slice_count,
      job_slice_number,
      scm_branch,
      webhook_guid,
      webhook_service,
      survey_passwords,
      event_queries_processed
    )
    VALUES (
      unified_job_id,                  -- unifiedjob_ptr_id
      'manual',                        -- job_type
      '',                              -- playbook
      CASE (i % 3)
        WHEN 1 THEN 5
        WHEN 2 THEN 10
        WHEN 0 THEN 20
      END,                             -- forks (varied: 5, 10, or 20)
      '',                              -- limit
      0,                               -- verbosity
      '{}'::text,                      -- extra_vars
      '{}'::text,                      -- job_tags
      false,                           -- force_handlers
      '',                              -- skip_tags
      '',                              -- start_at_task
      false,                           -- become_enabled
      default_inventory_id,            -- inventory_id
      default_unified_job_template_id, -- job_template_id
      default_unified_job_template_id, -- project_id
      false,                           -- allow_simultaneous
      '{}'::text,                      -- artifacts
      0,                               -- timeout
      '',                              -- scm_revision
      false,                           -- use_fact_cache
      false,                           -- diff_mode
      0,                               -- job_slice_count
      0,                               -- job_slice_number
      '',                              -- scm_branch
      gen_random_uuid()::text,         -- webhook_guid
      'github',                        -- webhook_service
      '{}'::jsonb,                     -- survey_passwords
      false                            -- event_queries_processed
    );
  END LOOP;
  --
  RAISE NOTICE 'Inserted % unified jobs and jobs with IDs: %',
               array_length(unified_jobs_10,1),
               unified_jobs_10;
  --
  -- Job Host Summaries
  --
  -- For each job in unified_jobs_10 and each host in host_ids,
  -- insert a zeroed-out summary row dated 2025-06-13 00:00:00.
  --
  FOR i IN array_lower(unified_jobs_10,1)..array_upper(unified_jobs_10,1) LOOP
    unified_job_id := unified_jobs_10[i];
    FOREACH host_id IN ARRAY host_ids LOOP
      -- fetch the host's name
      SELECT name
        INTO host_name
      FROM public.main_host
      WHERE id = host_id;
      --
      INSERT INTO public.main_jobhostsummary (
        created,
        modified,
        host_name,
        changed,
        dark,
        failures,
        ok,
        processed,
        skipped,
        failed,
        host_id,
        job_id,
        ignored,
        rescued
      ) VALUES (
        TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
        TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
        host_name,
        0,-- changed
        0,-- dark
        CASE WHEN i = 3 THEN 1 ELSE 0 END,-- failures (job 3 is a failed job)
        CASE WHEN i = 3 THEN 0 ELSE 1 END,-- ok
        0,-- processed
        0,-- skipped
        CASE WHEN i = 3 THEN true ELSE false END,-- failed
        host_id,
        unified_job_id,
        0,-- ignored
        0-- rescued
      );
    END LOOP;
  END LOOP;
  --
  RAISE NOTICE 'Inserted %×% job-host summary rows',
               array_length(unified_jobs_10,1),
               array_length(host_ids,1);

  -- Ensure hourly partition exists for 2025-06-13 10:00
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'main_jobevent_20250613_10'
  ) THEN
    EXECUTE 'CREATE TABLE public.main_jobevent_20250613_10 (LIKE public.main_jobevent INCLUDING DEFAULTS INCLUDING CONSTRAINTS)';
    EXECUTE 'ALTER TABLE public.main_jobevent ATTACH PARTITION public.main_jobevent_20250613_10 FOR VALUES FROM (''2025-06-13 10:00:00+00'') TO (''2025-06-13 11:00:00+00'')';
  END IF;

  -- Job Events (two per job-host), timestamps use fixed literal
  FOR i IN array_lower(unified_jobs_10,1)..array_upper(unified_jobs_10,1) LOOP
    unified_job_id := unified_jobs_10[i];

    FOREACH host_id IN ARRAY host_ids LOOP
      -- get host name
      SELECT name INTO host_name FROM public.main_host WHERE id = host_id;

      -- task_uuid should be i + host_name + 1, second task should be i + host_name + 2
      -- convert i to text
      i_text := i::text;
      task_uuid_1 := i_text || '_' || host_name || '_1';
      task_uuid_2 := i_text || '_' || host_name || '_2';

      event_data_1 := '{"task_action": "ansible.builtin.yum", "task_uuid": "' || task_uuid_1 || '"}';
      event_data_2 := '{"task_action": "a10.acos_axapi.a10_slb_virtual_server", "task_uuid": "' || task_uuid_2 || '"}';

      -- event 1
      INSERT INTO public.main_jobevent (
        created,
        modified,
        event,
        event_data,
        failed,
        changed,
        host_name,
        play,
        role,
        task,
        counter,
        host_id,
        job_id,
        uuid,
        parent_uuid,
        end_line,
        playbook,
        start_line,
        stdout,
        verbosity,
        job_created
      ) VALUES (
        TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
        TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
        'runner_on_ok',
        event_data_1,
        false,
        false,
        host_name,
        'default_play',
        'default_role',
        'default_task',
        1,
        host_id,
        unified_job_id,
        'UUID',
        '',
        1,
        'default_playbook.yml',
        1,
        ''::text,
        0,
        TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00'
      );

      -- event 2
      INSERT INTO public.main_jobevent (
        created,
        modified,
        event,
        event_data,
        failed,
        changed,
        host_name,
        play,
        role,
        task,
        counter,
        host_id,
        job_id,
        uuid,
        parent_uuid,
        end_line,
        playbook,
        start_line,
        stdout,
        verbosity,
        job_created
      ) VALUES (
        TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
        TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
        'runner_on_ok',
        event_data_2,
        false,
        false,
        host_name,
        'default_play',
        'default_role',
        'default_task',
        2,
        host_id,
        unified_job_id,
        'UUID',
        '',
        2,
        'default_playbook.yml',
        2,
        'ok: ' || host_name,
        0,
        TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00'
      );
    END LOOP;
  END LOOP;
  --
  -- Add warning and deprecated events (job-level annotation events)
  -- These don't have task_uuid, host_id, etc. - they're job-level annotations
  -- Note: host_name, play, role, task, playbook are NOT NULL in schema, so we use empty strings
  --
  -- Add warning events (one for job 1, one for job 2) - only if jobs exist
  IF array_length(unified_jobs_10, 1) >= 1 THEN
    INSERT INTO public.main_jobevent (
      created,
      modified,
      event,
      event_data,
      failed,
      changed,
      host_name,
      play,
      role,
      task,
      counter,
      host_id,
      job_id,
      uuid,
      parent_uuid,
      end_line,
      playbook,
      start_line,
      stdout,
      verbosity,
      job_created
    ) VALUES (
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      'warning',
      '{"warning": "This playbook uses deprecated features"}',
      false,
      false,
      '',  -- Empty string for job-level events (host_name is NOT NULL)
      '',  -- Empty string for play (NOT NULL constraint)
      '',  -- Empty string for role (NOT NULL constraint)
      '',  -- Empty string for task (NOT NULL constraint)
      100,
      NULL,  -- host_id is nullable
      unified_jobs_10[1],
      gen_random_uuid()::text,
      '',  -- Empty string for parent_uuid (NOT NULL constraint)
      0,
      '',  -- Empty string for playbook (NOT NULL constraint)
      0,
      'Warning: This playbook uses deprecated features',
      0,
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00'
    );
  END IF;
  --
  IF array_length(unified_jobs_10, 1) >= 2 THEN
    INSERT INTO public.main_jobevent (
      created,
      modified,
      event,
      event_data,
      failed,
      changed,
      host_name,
      play,
      role,
      task,
      counter,
      host_id,
      job_id,
      uuid,
      parent_uuid,
      end_line,
      playbook,
      start_line,
      stdout,
      verbosity,
      job_created
    ) VALUES (
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      'warning',
      '{"warning": "Module XYZ will be removed in future version"}',
      false,
      false,
      '',  -- Empty string for job-level events
      '',
      '',
      '',
      101,
      NULL,  -- host_id is nullable
      unified_jobs_10[2],
      gen_random_uuid()::text,
      '',
      0,
      '',
      0,
      'Warning: Module XYZ will be removed in future version',
      0,
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00'
    );
  END IF;
  --
  -- Add 1 deprecated event (for job 3) - only if job exists
  IF array_length(unified_jobs_10, 1) >= 3 THEN
    INSERT INTO public.main_jobevent (
      created,
      modified,
      event,
      event_data,
      failed,
      changed,
      host_name,
      play,
      role,
      task,
      counter,
      host_id,
      job_id,
      uuid,
      parent_uuid,
      end_line,
      playbook,
      start_line,
      stdout,
      verbosity,
      job_created
    ) VALUES (
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      'deprecated',
      '{"deprecated": "The old_module is deprecated, use new_module instead"}',
      false,
      false,
      '',  -- Empty string for job-level events
      '',
      '',
      '',
      102,
      NULL,  -- host_id is nullable
      unified_jobs_10[3],
      gen_random_uuid()::text,
      '',
      0,
      '',
      0,
      'Deprecated: The old_module is deprecated, use new_module instead',
      0,
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00'
    );
  END IF;
  --
  -- Credential Types
  --
  -- Insert Machine credential type
  INSERT INTO public.main_credentialtype (
      created,
      modified,
      description,
      name,
      kind,
      managed,
      inputs,
      injectors,
      namespace
    ) VALUES (
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      'Machine credential type for SSH connections',
      'Machine',
      'ssh',
      TRUE,
      '{"fields": [{"id": "username", "label": "Username", "type": "string"}, {"id": "password", "label": "Password", "type": "string", "secret": true}]}'::jsonb,
      '{}'::jsonb,
      'credential_type'
    )
    RETURNING id INTO machine_credential_type_id;
  
  -- Insert Cloud credential type
  INSERT INTO public.main_credentialtype (
      created,
      modified,
      description,
      name,
      kind,
      managed,
      inputs,
      injectors,
      namespace
    ) VALUES (
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      'Cloud credential type for AWS',
      'Amazon Web Services',
      'cloud',
      TRUE,
      '{"fields": [{"id": "username", "label": "Access Key", "type": "string"}, {"id": "password", "label": "Secret Key", "type": "string", "secret": true}]}'::jsonb,
      '{}'::jsonb,
      'aws'
    )
    RETURNING id INTO cloud_credential_type_id;
  
  -- Insert Vault credential type
  INSERT INTO public.main_credentialtype (
      created,
      modified,
      description,
      name,
      kind,
      managed,
      inputs,
      injectors,
      namespace
    ) VALUES (
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      'Vault credential type for Ansible Vault',
      'Vault',
      'vault',
      TRUE,
      '{"fields": [{"id": "vault_password", "label": "Vault Password", "type": "string", "secret": true}]}'::jsonb,
      '{}'::jsonb,
      'credential_type'
    )
    RETURNING id INTO vault_credential_type_id;
  
  -- Insert Network credential type
  INSERT INTO public.main_credentialtype (
      created,
      modified,
      description,
      name,
      kind,
      managed,
      inputs,
      injectors,
      namespace
    ) VALUES (
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      'Network credential type for network devices',
      'Network',
      'net',
      TRUE,
      '{"fields": [{"id": "username", "label": "Username", "type": "string"}, {"id": "password", "label": "Password", "type": "string", "secret": true}]}'::jsonb,
      '{}'::jsonb,
      'credential_type'
    )
    RETURNING id INTO network_credential_type_id;
  
  -- Insert Custom credential type (managed=false to test filtering)
  INSERT INTO public.main_credentialtype (
      created,
      modified,
      description,
      name,
      kind,
      managed,
      inputs,
      injectors,
      namespace
    ) VALUES (
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      'Custom credential type for testing filtering',
      'My Custom Credential Type',
      'cloud',
      FALSE,
      '{"fields": [{"id": "api_key", "label": "API Key", "type": "string", "secret": true}]}'::jsonb,
      '{}'::jsonb,
      NULL
    )
    RETURNING id INTO custom_credential_type_id;
  
  RAISE NOTICE 'Inserted credential types: Machine=%, Cloud=%, Vault=%, Network=%, Custom=%',
               machine_credential_type_id,
               cloud_credential_type_id,
               vault_credential_type_id,
               network_credential_type_id,
               custom_credential_type_id;
  --
  -- Credentials
  --
  -- Machine credential
  INSERT INTO public.main_credential (
      created,
      modified,
      description,
      name,
      organization_id,
      credential_type_id,
      managed,
      inputs
    ) VALUES (
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      'Default machine credential for SSH',
      'default_machine_credential_' || random_suffix,
      default_organization_id,
      machine_credential_type_id,
      FALSE,
      '{"username": "ansible", "password": "encrypted_password"}'::jsonb
    )
    RETURNING id INTO machine_credential_id;
  
  -- Cloud credential
  INSERT INTO public.main_credential (
      created,
      modified,
      description,
      name,
      organization_id,
      credential_type_id,
      managed,
      inputs
    ) VALUES (
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      'AWS cloud credential',
      'default_cloud_credential_' || random_suffix,
      default_organization_id,
      cloud_credential_type_id,
      FALSE,
      '{"username": "AKIAIOSFODNN7EXAMPLE", "password": "encrypted_secret"}'::jsonb
    )
    RETURNING id INTO cloud_credential_id;
  
  -- Vault credential
  INSERT INTO public.main_credential (
      created,
      modified,
      description,
      name,
      organization_id,
      credential_type_id,
      managed,
      inputs
    ) VALUES (
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      'Ansible Vault credential',
      'default_vault_credential_' || random_suffix,
      default_organization_id,
      vault_credential_type_id,
      FALSE,
      '{"vault_password": "encrypted_vault_password"}'::jsonb
    )
    RETURNING id INTO vault_credential_id;
  
  -- Network credential
  INSERT INTO public.main_credential (
      created,
      modified,
      description,
      name,
      organization_id,
      credential_type_id,
      managed,
      inputs
    ) VALUES (
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      'Network device credential',
      'default_network_credential_' || random_suffix,
      default_organization_id,
      network_credential_type_id,
      FALSE,
      '{"username": "admin", "password": "encrypted_network_password"}'::jsonb
    )
    RETURNING id INTO network_credential_id;
  
  -- Custom credential (should be filtered out by managed=true filter)
  INSERT INTO public.main_credential (
      created,
      modified,
      description,
      name,
      organization_id,
      credential_type_id,
      managed,
      inputs
    ) VALUES (
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
      'Custom credential for testing filtering',
      'default_custom_credential_' || random_suffix,
      default_organization_id,
      custom_credential_type_id,
      FALSE,
      '{"api_key": "encrypted_custom_api_key"}'::jsonb
    )
    RETURNING id INTO custom_credential_id;
  
  RAISE NOTICE 'Inserted credentials: Machine=%, Cloud=%, Vault=%, Network=%, Custom=%',
               machine_credential_id,
               cloud_credential_id,
               vault_credential_id,
               network_credential_id,
               custom_credential_id;
  --
  -- Link credentials to unified jobs
  -- Assign different combinations of credentials to different jobs
  --
  FOR i IN array_lower(unified_jobs_10,1)..array_upper(unified_jobs_10,1) LOOP
    unified_job_id := unified_jobs_10[i];
    
    -- Every job gets machine credential
    INSERT INTO public.main_unifiedjob_credentials (
      unifiedjob_id,
      credential_id
    ) VALUES (
      unified_job_id,
      machine_credential_id
    );
    
    -- Job 1: Machine + Cloud + Custom (custom should be filtered out)
    IF i = 1 THEN
      INSERT INTO public.main_unifiedjob_credentials (
        unifiedjob_id,
        credential_id
      ) VALUES (
        unified_job_id,
        cloud_credential_id
      );
      -- Add custom credential to job 1 (should be filtered out by managed=true)
      INSERT INTO public.main_unifiedjob_credentials (
        unifiedjob_id,
        credential_id
      ) VALUES (
        unified_job_id,
        custom_credential_id
      );
    END IF;
    
    -- Job 2: Machine + Vault
    IF i = 2 THEN
      INSERT INTO public.main_unifiedjob_credentials (
        unifiedjob_id,
        credential_id
      ) VALUES (
        unified_job_id,
        vault_credential_id
      );
    END IF;
    
    -- Job 3: Machine + Cloud + Network
    IF i = 3 THEN
      INSERT INTO public.main_unifiedjob_credentials (
        unifiedjob_id,
        credential_id
      ) VALUES (
        unified_job_id,
        cloud_credential_id
      );
      INSERT INTO public.main_unifiedjob_credentials (
        unifiedjob_id,
        credential_id
      ) VALUES (
        unified_job_id,
        network_credential_id
      );
    END IF;
  END LOOP;
  
  RAISE NOTICE 'Linked credentials to unified jobs';
  --
  -- ============================================================
  -- Data for 11:00 hour (same day, different hour)
  -- ============================================================
  --
  -- Unified Jobs for 11:00 hour
  -- Loop to create unified jobs at 11:00:00
  FOR i IN 1..job_count LOOP
    INSERT INTO public.main_unifiedjob (
      created,
      started,
      finished,
      modified,
      description,
      name,
      launch_type,
      cancel_flag,
      status,
      failed,
      elapsed,
      job_args,
      job_cwd,
      job_explanation,
      start_args,
      result_traceback,
      celery_task_id,
      unified_job_template_id,
      organization_id,
      execution_node,
      emitted_events,
      controller_node,
      dependencies_processed,
      installed_collections,
      ansible_version,
      task_impact,
      job_env,
      polymorphic_ctype_id,
      execution_environment_id
    )
    VALUES (
      TIMESTAMP WITH TIME ZONE '2025-06-13 11:00:00+00',                                  -- created (same for all jobs)
      TIMESTAMP WITH TIME ZONE '2025-06-13 11:00:00+00' + (i * INTERVAL '10 seconds'),  -- started (varies by job, 10s apart for waiting time)
      TIMESTAMP WITH TIME ZONE '2025-06-13 11:00:00+00' + (i * INTERVAL '10 seconds') + 
        CASE i
          WHEN 1 THEN INTERVAL '100 seconds'  -- Job 1: 100 seconds
          WHEN 2 THEN INTERVAL '150 seconds'  -- Job 2: 150 seconds
          WHEN 3 THEN INTERVAL '80 seconds'    -- Job 3: 80 seconds
        END,                                  -- finished (varies by job with different durations)
      TIMESTAMP WITH TIME ZONE '2025-06-13 11:00:00+00' + (i * INTERVAL '10 seconds') + 
        CASE i
          WHEN 1 THEN INTERVAL '100 seconds'
          WHEN 2 THEN INTERVAL '150 seconds'
          WHEN 3 THEN INTERVAL '80 seconds'
        END,                                  -- modified (same as finished)
      ''::text,                               -- description
      'default_unified_job_11_' || random_suffix, -- name (different from 10:00 jobs)
      CASE (i % 4)
        WHEN 1 THEN 'manual'
        WHEN 2 THEN 'scheduled'
        WHEN 3 THEN 'workflow'
        WHEN 0 THEN 'callback'
      END,                                    -- launch_type (cycles through: manual, scheduled, workflow, callback)
      false,                                  -- cancel_flag
      'pending',                              -- status
      false,                                  -- failed
      CASE i
        WHEN 1 THEN 100.000  -- Job 1: 100 seconds
        WHEN 2 THEN 150.000  -- Job 2: 150 seconds
        WHEN 3 THEN 80.000   -- Job 3: 80 seconds
      END,                                    -- elapsed (matches finished - started)
      '{}'::text,                             -- job_args
      '/tmp',                                 -- job_cwd
      ''::text,                               -- job_explanation
      '{}'::text,                             -- start_args
      ''::text,                               -- result_traceback
      gen_random_uuid()::text,                -- celery_task_id
      default_unified_job_template_id,        -- FK to your template
      default_organization_id,
      'auto',                                 -- execution_node
      0,                                      -- emitted_events
      'controller1',                          -- controller_node
      false,                                  -- dependencies_processed
      CASE i
        WHEN 1 THEN '{"ansible.builtin": {"version": "2.9.10"}, "a10.acos_axapi": {"version": "1.0.0"}, "redhat.rhel_system_roles": {"version": "1.23.0"}}'::jsonb
        WHEN 2 THEN '{"ansible.builtin": {"version": "2.9.10"}, "a10.acos_axapi": {"version": "1.0.0"}, "redhat.rhel_system_roles": {"version": "1.23.0"}}'::jsonb
        ELSE '{"ansible.builtin": {"version": "2.9.10"}, "a10.acos_axapi": {"version": "1.0.0"}}'::jsonb
      END,                                    -- installed_collections (jobs 1 and 2 also include redhat.rhel_system_roles)
      '2.9.10',                               -- ansible_version
      0,                                      -- task_impact
      '{}'::jsonb,                            -- job_env
      job_content_type_id,                    -- polymorphic_ctype_id
      CASE i
        WHEN 3 THEN ee1_id  -- job 3: no rhel_system_roles → Python ML EE
        ELSE ee2_id         -- jobs 1,2: with redhat.rhel_system_roles → Node Backend EE
      END                                     -- execution_environment_id
    )
    RETURNING id
    INTO unified_job_id;

    -- Append to our array
    unified_jobs_11 := array_append(unified_jobs_11, unified_job_id);
    --
    -- Create Main Job and connect it with unified job using its id
    --
    INSERT INTO public.main_job (
      unifiedjob_ptr_id,
      job_type,
      playbook,
      forks,
      "limit",
      verbosity,
      extra_vars,
      job_tags,
      force_handlers,
      skip_tags,
      start_at_task,
      become_enabled,
      inventory_id,
      job_template_id,
      project_id,
      allow_simultaneous,
      artifacts,
      timeout,
      scm_revision,
      use_fact_cache,
      diff_mode,
      job_slice_count,
      job_slice_number,
      scm_branch,
      webhook_guid,
      webhook_service,
      survey_passwords,
      event_queries_processed
    )
    VALUES (
      unified_job_id,                  -- unifiedjob_ptr_id
      'manual',                        -- job_type
      '',                              -- playbook
      CASE (i % 3)
        WHEN 1 THEN 8
        WHEN 2 THEN 15
        WHEN 0 THEN 25
      END,                             -- forks (varied: 8, 15, or 25)
      '',                              -- limit
      0,                               -- verbosity
      '{}'::text,                      -- extra_vars
      '{}'::text,                      -- job_tags
      false,                           -- force_handlers
      '',                              -- skip_tags
      '',                              -- start_at_task
      false,                           -- become_enabled
      default_inventory_id,            -- inventory_id
      default_unified_job_template_id, -- job_template_id
      default_unified_job_template_id, -- project_id
      false,                           -- allow_simultaneous
      '{}'::text,                      -- artifacts
      0,                               -- timeout
      '',                              -- scm_revision
      false,                           -- use_fact_cache
      false,                           -- diff_mode
      0,                               -- job_slice_count
      0,                               -- job_slice_number
      '',                              -- scm_branch
      gen_random_uuid()::text,         -- webhook_guid
      'github',                        -- webhook_service
      '{}'::jsonb,                     -- survey_passwords
      false                            -- event_queries_processed
    );
  END LOOP;
  --
  RAISE NOTICE 'Inserted % unified jobs for 11:00 hour with IDs: %',
               array_length(unified_jobs_11,1),
               unified_jobs_11;
  --
  -- Job Host Summaries for 11:00 hour
  --
  -- For each job in unified_jobs_11 and each host in host_ids,
  -- insert a summary row dated 2025-06-13 11:00:00.
  --
  FOR i IN array_lower(unified_jobs_11,1)..array_upper(unified_jobs_11,1) LOOP
    unified_job_id := unified_jobs_11[i];
    FOREACH host_id IN ARRAY host_ids LOOP
      -- fetch the host's name
      SELECT name
        INTO host_name
      FROM public.main_host
      WHERE id = host_id;
      --
      INSERT INTO public.main_jobhostsummary (
        created,
        modified,
        host_name,
        changed,
        dark,
        failures,
        ok,
        processed,
        skipped,
        failed,
        host_id,
        job_id,
        ignored,
        rescued
      ) VALUES (
        TIMESTAMP WITH TIME ZONE '2025-06-13 11:00:00+00',
        TIMESTAMP WITH TIME ZONE '2025-06-13 11:00:00+00',
        host_name,
        0,-- changed
        0,-- dark
        0,-- failures
        1,-- ok
        0,-- processed
        0,-- skipped
        false,-- failed
        host_id,
        unified_job_id,
        0,-- ignored
        0-- rescued
      );
    END LOOP;
  END LOOP;
  --
  RAISE NOTICE 'Inserted %×% job-host summary rows for 11:00 hour',
               array_length(unified_jobs_11,1),
               array_length(host_ids,1);

  -- Ensure hourly partition exists for 2025-06-13 11:00
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'main_jobevent_20250613_11'
  ) THEN
    EXECUTE 'CREATE TABLE public.main_jobevent_20250613_11 (LIKE public.main_jobevent INCLUDING DEFAULTS INCLUDING CONSTRAINTS)';
    EXECUTE 'ALTER TABLE public.main_jobevent ATTACH PARTITION public.main_jobevent_20250613_11 FOR VALUES FROM (''2025-06-13 11:00:00+00'') TO (''2025-06-13 12:00:00+00'')';
  END IF;

  -- Job Events for 11:00 hour (two per job-host), timestamps use 11:00:00
  FOR i IN array_lower(unified_jobs_11,1)..array_upper(unified_jobs_11,1) LOOP
    unified_job_id := unified_jobs_11[i];

    FOREACH host_id IN ARRAY host_ids LOOP
      -- get host name
      SELECT name INTO host_name FROM public.main_host WHERE id = host_id;

      -- task_uuid should be i + host_name + 1, second task should be i + host_name + 2
      -- convert i to text, add 10 to distinguish from 10:00 hour jobs
      i_text := (i + 10)::text;
      task_uuid_1 := i_text || '_' || host_name || '_1';
      task_uuid_2 := i_text || '_' || host_name || '_2';

      event_data_1 := '{"task_action": "ansible.builtin.yum", "task_uuid": "' || task_uuid_1 || '"}';
      event_data_2 := '{"task_action": "a10.acos_axapi.a10_slb_virtual_server", "task_uuid": "' || task_uuid_2 || '"}';

      -- event 1
      INSERT INTO public.main_jobevent (
        created,
        modified,
        event,
        event_data,
        failed,
        changed,
        host_name,
        play,
        role,
        task,
        counter,
        host_id,
        job_id,
        uuid,
        parent_uuid,
        end_line,
        playbook,
        start_line,
        stdout,
        verbosity,
        job_created
      ) VALUES (
        TIMESTAMP WITH TIME ZONE '2025-06-13 11:00:00+00',
        TIMESTAMP WITH TIME ZONE '2025-06-13 11:00:00+00',
        'runner_on_ok',
        event_data_1,
        false,
        false,
        host_name,
        'default_play',
        'default_role',
        'default_task',
        1,
        host_id,
        unified_job_id,
        'UUID',
        '',
        1,
        'default_playbook.yml',
        1,
        ''::text,
        0,
        TIMESTAMP WITH TIME ZONE '2025-06-13 11:00:00+00'
      );

      -- event 2
      INSERT INTO public.main_jobevent (
        created,
        modified,
        event,
        event_data,
        failed,
        changed,
        host_name,
        play,
        role,
        task,
        counter,
        host_id,
        job_id,
        uuid,
        parent_uuid,
        end_line,
        playbook,
        start_line,
        stdout,
        verbosity,
        job_created
      ) VALUES (
        TIMESTAMP WITH TIME ZONE '2025-06-13 11:00:00+00',
        TIMESTAMP WITH TIME ZONE '2025-06-13 11:00:00+00',
        'runner_on_ok',
        event_data_2,
        false,
        false,
        host_name,
        'default_play',
        'default_role',
        'default_task',
        2,
        host_id,
        unified_job_id,
        'UUID',
        '',
        2,
        'default_playbook.yml',
        2,
        'ok: ' || host_name,
        0,
        TIMESTAMP WITH TIME ZONE '2025-06-13 11:00:00+00'
      );
    END LOOP;
  END LOOP;
  --
  -- Add warning and deprecated events for 11:00 hour (job-level annotation events)
  -- Add warning events (one for job 1, one for job 2) - only if jobs exist
  IF array_length(unified_jobs_11, 1) >= 1 THEN
    INSERT INTO public.main_jobevent (
      created,
      modified,
      event,
      event_data,
      failed,
      changed,
      host_name,
      play,
      role,
      task,
      counter,
      host_id,
      job_id,
      uuid,
      parent_uuid,
      end_line,
      playbook,
      start_line,
      stdout,
      verbosity,
      job_created
    ) VALUES (
      TIMESTAMP WITH TIME ZONE '2025-06-13 11:00:00+00',
      TIMESTAMP WITH TIME ZONE '2025-06-13 11:00:00+00',
      'warning',
      '{"warning": "This playbook uses deprecated features (11:00 hour)"}',
      false,
      false,
      '',  -- Empty string for job-level events (host_name is NOT NULL)
      '',  -- Empty string for play (NOT NULL constraint)
      '',  -- Empty string for role (NOT NULL constraint)
      '',  -- Empty string for task (NOT NULL constraint)
      100,
      NULL,  -- host_id is nullable
      unified_jobs_11[1],
      gen_random_uuid()::text,
      '',  -- Empty string for parent_uuid (NOT NULL constraint)
      0,
      '',  -- Empty string for playbook (NOT NULL constraint)
      0,
      'Warning: This playbook uses deprecated features (11:00 hour)',
      0,
      TIMESTAMP WITH TIME ZONE '2025-06-13 11:00:00+00'
    );
  END IF;
  --
  IF array_length(unified_jobs_11, 1) >= 2 THEN
    INSERT INTO public.main_jobevent (
      created,
      modified,
      event,
      event_data,
      failed,
      changed,
      host_name,
      play,
      role,
      task,
      counter,
      host_id,
      job_id,
      uuid,
      parent_uuid,
      end_line,
      playbook,
      start_line,
      stdout,
      verbosity,
      job_created
    ) VALUES (
      TIMESTAMP WITH TIME ZONE '2025-06-13 11:00:00+00',
      TIMESTAMP WITH TIME ZONE '2025-06-13 11:00:00+00',
      'warning',
      '{"warning": "Module XYZ will be removed in future version (11:00 hour)"}',
      false,
      false,
      '',  -- Empty string for job-level events
      '',
      '',
      '',
      101,
      NULL,  -- host_id is nullable
      unified_jobs_11[2],
      gen_random_uuid()::text,
      '',
      0,
      '',
      0,
      'Warning: Module XYZ will be removed in future version (11:00 hour)',
      0,
      TIMESTAMP WITH TIME ZONE '2025-06-13 11:00:00+00'
    );
  END IF;
  --
  -- Add 1 deprecated event (for job 3) - only if job exists
  IF array_length(unified_jobs_11, 1) >= 3 THEN
    INSERT INTO public.main_jobevent (
      created,
      modified,
      event,
      event_data,
      failed,
      changed,
      host_name,
      play,
      role,
      task,
      counter,
      host_id,
      job_id,
      uuid,
      parent_uuid,
      end_line,
      playbook,
      start_line,
      stdout,
      verbosity,
      job_created
    ) VALUES (
      TIMESTAMP WITH TIME ZONE '2025-06-13 11:00:00+00',
      TIMESTAMP WITH TIME ZONE '2025-06-13 11:00:00+00',
      'deprecated',
      '{"deprecated": "The old_module is deprecated, use new_module instead (11:00 hour)"}',
      false,
      false,
      '',  -- Empty string for job-level events
      '',
      '',
      '',
      102,
      NULL,  -- host_id is nullable
      unified_jobs_11[3],
      gen_random_uuid()::text,
      '',
      0,
      '',
      0,
      'Deprecated: The old_module is deprecated, use new_module instead (11:00 hour)',
      0,
      TIMESTAMP WITH TIME ZONE '2025-06-13 11:00:00+00'
    );
  END IF;
  --
  -- Link credentials to unified jobs for 11:00 hour
  -- Assign different combinations of credentials to different jobs
  --
  FOR i IN array_lower(unified_jobs_11,1)..array_upper(unified_jobs_11,1) LOOP
    unified_job_id := unified_jobs_11[i];
    
    -- Every job gets machine credential
    INSERT INTO public.main_unifiedjob_credentials (
      unifiedjob_id,
      credential_id
    ) VALUES (
      unified_job_id,
      machine_credential_id
    );
    
    -- Job 1: Machine + Cloud + Custom (custom should be filtered out)
    IF i = 1 THEN
      INSERT INTO public.main_unifiedjob_credentials (
        unifiedjob_id,
        credential_id
      ) VALUES (
        unified_job_id,
        cloud_credential_id
      );
      -- Add custom credential to job 1 (should be filtered out by managed=true)
      INSERT INTO public.main_unifiedjob_credentials (
        unifiedjob_id,
        credential_id
      ) VALUES (
        unified_job_id,
        custom_credential_id
      );
    END IF;
    
    -- Job 2: Machine + Vault
    IF i = 2 THEN
      INSERT INTO public.main_unifiedjob_credentials (
        unifiedjob_id,
        credential_id
      ) VALUES (
        unified_job_id,
        vault_credential_id
      );
    END IF;
    
    -- Job 3: Machine + Cloud + Network
    IF i = 3 THEN
      INSERT INTO public.main_unifiedjob_credentials (
        unifiedjob_id,
        credential_id
      ) VALUES (
        unified_job_id,
        cloud_credential_id
      );
      INSERT INTO public.main_unifiedjob_credentials (
        unifiedjob_id,
        credential_id
      ) VALUES (
        unified_job_id,
        network_credential_id
      );
    END IF;
  END LOOP;
  
  RAISE NOTICE 'Linked credentials to unified jobs for 11:00 hour';
END
$$;
