INSERT INTO main_instance (
  uuid, hostname,
  created, modified,
  version, enabled, node_type, last_seen, node_state,
  capacity, capacity_adjustment, cpu, memory,
  cpu_capacity, mem_capacity, managed, managed_by_policy, ip_address, errors
) VALUES (
  '00000000-0000-0000-0000-000000000000', 'myaap-controller-task-59777d4bb7-9btjf',
  '2025-11-04 15:08:04.389978+00', '2025-11-04 15:08:04.390002+00',
  '4.7.2', 't', 'control', '2025-11-04 15:15:13.602791+00', 'ready',
  640, 1.0, 8.0, 8::bigint * 1024 * 1024 * 1024,
  123, 456, 't', 't', '10.244.0.32', ''
);
