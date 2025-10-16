DO $$
DECLARE
  i_text text;
  task_uuid_1 text;
  task_uuid_2 text;
  event_data_1 text;
  event_data_2 text;
  --
  default_organization_id                           INTEGER;
  default_inventory_id                              INTEGER;
  default_instance_id                               INTEGER;
  default_instance_uuid UUID := gen_random_uuid();
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
  unified_jobs      INTEGER[] := ARRAY[]::INTEGER[];
  unified_job_id    INTEGER;
  --
BEGIN
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
  -- MAIN_INSTANCE
  --
  INSERT INTO public.main_instance (
    uuid,
    hostname,
    created,
    modified,
    capacity,
    version,
    capacity_adjustment,
    cpu,
    memory,
    cpu_capacity,
    mem_capacity,
    enabled,
    managed_by_policy,
    ip_address,
    node_type,
    last_seen,
    errors,
    last_health_check,
    node_state,
    health_check_started,
    managed
  ) VALUES (
    default_instance_uuid,                      -- generate UUID here
    'default_host_instance_' || random_suffix,  -- hostname
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',                                      -- created
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',                                      -- modified
    0,                                          -- capacity
    '1.0',                                      -- version
    1.00,                                       -- capacity_adjustment
    1.0,                                        -- cpu
    1073741824,                                 -- memory (1 GiB)
    100,                                        -- cpu_capacity
    1024,                                       -- mem_capacity (MiB)
    true,                                       -- enabled
    false,                                      -- managed_by_policy
    random_ip,                                  -- ip_address
    'default',                                  -- node_type
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',                                      -- last_seen
    '',                                         -- errors
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',                                      -- last_health_check
    'running',                                  -- node_state
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',                                      -- health_check_started
    true                                        -- managed
  )
  RETURNING id INTO default_instance_id;
  --
  RAISE NOTICE 'Inserted Main Instance % with id = %',
               'default_host_instance_' || random_suffix,
               default_instance_id;
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
    organization_id
    )
  VALUES (
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',                                          -- created
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',                                          -- modified
    '',                                             -- description
    'default_unified_job_template_' || random_suffix,  -- name w/ random suffix
    0,                                              -- old_pk (must be >= 0)
    false,                                          -- last_job_failed
    'never updated',                                -- status (adjust as needed)
    default_organization_id                         -- organization_id
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
      'SCM_TYPE',                      
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
      job_env
    )
    VALUES (
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',                                  -- created
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',                                  -- started
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',                                  -- finished
      TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',                                  -- modified
      ''::text,                               -- description
      'default_unified_job_' || random_suffix, -- name
      'manual',                               -- launch_type
      false,                                  -- cancel_flag
      'pending',                              -- status
      false,                                  -- failed
      0.000,                                  -- elapsed
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
      '{}'::jsonb,                            -- installed_collections
      '2.9.10',                               -- ansible_version
      0,                                      -- task_impact
      '{}'::jsonb                             -- job_env
    )
    RETURNING id
    INTO unified_job_id;

    -- Append to our array
    unified_jobs := array_append(unified_jobs, unified_job_id);
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
      survey_passwords
    )
    VALUES (
      unified_job_id,               -- link to the unified job
      'manual',                     -- job_type
      '',                           -- playbook
      0,                            -- forks
      '',                           -- "limit"
      0,                            -- verbosity
      '{}'::text,                   -- extra_vars
      '{}'::text,                   -- job_tags
      false,                        -- force_handlers
      '',                           -- skip_tags
      '',                           -- start_at_task
      false,                        -- become_enabled
      default_inventory_id,         -- from your DECLARE
      default_unified_job_template_id, -- from your DECLARE
      default_unified_job_template_id, -- from your DECLARE
      false,                        -- allow_simultaneous
      '{}'::text,                   -- artifacts
      0,                            -- timeout
      '',                           -- scm_revision
      false,                        -- use_fact_cache
      false,                        -- diff_mode
      0,                            -- job_slice_count
      0,                            -- job_slice_number
      '',                           -- scm_branch
      gen_random_uuid()::text,      -- webhook_guid
      'github',                     -- webhook_service
      '{}'::jsonb                   -- survey_passwords
    );
  END LOOP;
  --
  RAISE NOTICE 'Inserted % unified jobs and jobs with IDs: %',
               array_length(unified_jobs,1),
               unified_jobs;
  --
  -- Job Host Summaries
  --
  -- For each job in unified_jobs and each host in host_ids,
  -- insert a zeroed-out summary row dated 2025-06-13 00:00:00.
  --
  FOR i IN array_lower(unified_jobs,1)..array_upper(unified_jobs,1) LOOP
    unified_job_id := unified_jobs[i];
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
  RAISE NOTICE 'Inserted %×% job-host summary rows', 
               array_length(unified_jobs,1),
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
  FOR i IN array_lower(unified_jobs,1)..array_upper(unified_jobs,1) LOOP
    unified_job_id := unified_jobs[i];

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
        'runner_on_start',
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
  -- Execution Environments
  --
  INSERT INTO public.main_executionenvironment (
    created,
    modified,
    description,
    image,
    managed,
    name,
    pull
) VALUES 
(
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
    'Python 3.11 environment with common ML libraries',
    'registry.example.com/envs/python-ml:3.11',
    TRUE,
    'Python ML Environment',
    'always'
),
(
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
    'Node.js 20 environment for backend services',
    'registry.example.com/envs/node-backend:20',
    FALSE,
    'Node Backend Environment',
    'missing'
);
END
$$;



