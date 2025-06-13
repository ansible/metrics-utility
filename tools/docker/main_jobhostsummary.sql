DO $$
DECLARE
  default_org_id   INTEGER;
  default_inv_id   INTEGER;
  default_inst_id  INTEGER;
  default_inst_uuid UUID := gen_random_uuid();
  random_suffix    TEXT := substring(md5(random()::text), 1, 5);
  --
  random_ip        TEXT := 
     (floor(random()*256)::int)::text
     ||'.'||(floor(random()*256)::int)::text
     ||'.'||(floor(random()*256)::int)::text
     ||'.'||(floor(random()*256)::int)::text;
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
    INTO default_org_id;
  --  
  RAISE NOTICE 'Inserted Organization % with id = %',
               'default_org_' || random_suffix,
               default_org_id;
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
    default_org_id,                         -- fk to org
    'constructed',                          -- kind (adjust as needed)
    false,                                  -- pending_deletion
    false                                   -- prevent_instance_group_fallback
  )
  RETURNING id
    INTO default_inv_id;
  --
  RAISE NOTICE 'Inserted Inventory % with id = %',
               'default_inventory_' || random_suffix,
               default_inv_id;
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
    default_inst_uuid,                          -- generate UUID here
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
  RETURNING id INTO default_inst_id;
  --
  RAISE NOTICE 'Inserted Main Instance % with id = %',
               'default_host_instance_' || random_suffix,
               default_inst_id;
END
$$;



