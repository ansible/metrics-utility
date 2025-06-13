DO $$
DECLARE
  default_organization_id                           INTEGER;
  default_inventory_id                              INTEGER;
  default_instance_id                               INTEGER;
  default_instance_uuid UUID := gen_random_uuid();
  default_unified_job_template_id                   INTEGER;
  random_suffix    TEXT := substring(md5(random()::text), 1, 5);
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
    now(),                             -- created
    now(),                             -- modified
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
    now(),                                  -- created
    now(),                                  -- modified
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
    default_instance_uuid,                          -- generate UUID here
    'default_host_instance_' || random_suffix,  -- hostname
    now(),                                      -- created
    now(),                                      -- modified
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
    now(),                                      -- last_seen
    '',                                         -- errors
    now(),                                      -- last_health_check
    'running',                                  -- node_state
    now(),                                      -- health_check_started
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
  -- LOOP TO INSERT 20 HOSTS
  FOR i IN 1..20 LOOP
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
      now(),
      now(),
      '',                                            -- non‐null description
      'default_host_' || i || '_' || random_suffix,  -- unique name
      true,
      default_instance_uuid::text,
      '
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
      ',                                             -- non‐null variables
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
    now(),                                          -- created
    now(),                                          -- modified
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
  FOR i IN 1..5 LOOP
    INSERT INTO public.main_unifiedjob (
      created,
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
      now(),                                  -- created
      now(),                                  -- modified
      ''::text,                               -- description
      'default_job_' || i,                    -- name
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
END
$$;



