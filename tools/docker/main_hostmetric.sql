DO $$
DECLARE
  default_organization_id                           INTEGER;
  default_inventory_id                              INTEGER;
  default_inventory_ids                             INTEGER[] := ARRAY[]::INTEGER[];
  default_instance_id                               INTEGER;
  default_instance_uuid UUID := gen_random_uuid();
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
  host_rec          RECORD;
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
    'default_org_hostmetric_' || random_suffix,   -- name w/ random suffix
    0                                  -- max_hosts
  )
  RETURNING id
    INTO default_organization_id;
  --  
  RAISE NOTICE 'Inserted Organization % with id = %',
               'default_org_hostmetric_' || random_suffix,
               default_organization_id;
  --
  -- INVENTORY
  --
  FOR i IN 1..3 LOOP
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
        'default_inventory_hostmetric_' || i || '_' || random_suffix,  -- name w/ same suffix
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
                'default_inventory_hostmetric_' || random_suffix,
                default_inventory_id;
    default_inventory_ids := array_append(default_inventory_ids, default_inventory_id);
  END LOOP;
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
    'default_host_instance_hostmeric_' || random_suffix,  -- hostname
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
               'default_host_instance_hostmetric_' || random_suffix,
               default_instance_id;
  --
  -- Fill hosts in loop
  --
  -- LOOP TO INSERT HOSTS
  FOREACH default_inventory_id IN ARRAY default_inventory_ids LOOP
    FOR i IN 1..10 LOOP
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
        'default_host_hostmetric_' || i || '_' || random_suffix, -- unique name
        true,
        default_instance_uuid::text,
    -- This must not be moved right, otherwise it will break
    $yaml$ansible_connection: "default_ansible_connection"
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
  END LOOP;
  --
  RAISE NOTICE 'Inserted % hosts with IDs: %', array_length(host_ids,1), host_ids;

  INSERT INTO public.main_hostmetric(
     hostname,
     first_automation,
     last_automation,
     last_deleted,
     automated_counter,
     deleted_counter,
     deleted,
     used_in_inventories
 ) VALUES
 ('default_host_hostmetric_1'|| '_' || random_suffix, '2025-06-01T08:00:00+00', '2025-06-10T14:30:00+00', NULL, 12, 0, false, 3),
 ('default_host_hostmetric_2'|| '_' || random_suffix, '2025-06-28T09:15:00+00', '2025-06-12T16:00:00+00', '2025-06-20T10:00:00+00', 5, 1, true, 1),
 ('default_host_hostmetric_3'|| '_' || random_suffix, '2025-06-03T12:00:00+00', '2025-06-11T13:45:00+00', NULL, 7, 0, false, 2),
 ('default_host_hostmetric_4'|| '_' || random_suffix, '2025-06-02T07:30:00+00', '2025-06-09T15:30:00+00', NULL, 10, 0, false, 5),
 ('default_host_hostmetric_5'|| '_' || random_suffix, '2025-06-30T10:00:00+00', '2025-06-08T11:00:00+00', '2025-06-15T12:00:00+00', 3, 2, true, 0),
 ('default_host_hostmetric_6'|| '_' || random_suffix, '2025-06-01T06:45:00+00', '2025-06-06T13:15:00+00', NULL, 6, 1, true, 1),
 ('default_host_hostmetric_7'|| '_' || random_suffix, '2025-06-04T10:30:00+00', '2025-06-10T12:30:00+00', NULL, 8, 0, false, 4),
 ('default_host_hostmetric_8'|| '_' || random_suffix, '2025-06-29T09:45:00+00', '2025-06-07T14:00:00+00', '2025-06-13T09:30:00+00', 4, 1, true, 2),
 ('default_host_hostmetric_9'|| '_' || random_suffix, '2025-06-05T08:30:00+00', '2025-06-10T16:00:00+00', NULL, 9, 0, false, 3);
END
$$;
