-- Daily collection test data: 4 records across 2 jobs, host_ids 1, 2, 2, 3.
-- prepare() groups by (organization_name, collection_name) and deduplicates host_names.
-- Expected groups:
--   cisco.ios: cisco-switch-01, cisco-switch-02, cisco-switch-03 = 3 unique hosts
--   azure.azcollection: cisco-switch-02, cisco-switch-03 = 2 unique hosts
-- Total unique hosts across all groups: 3
-- Looks up job_id and organization_id from the job created by main_jobhostsummary.sql.
INSERT INTO public.main_indirectmanagednodeaudit (
    created, name, canonical_facts, facts, events, count, host_id, inventory_id, job_id, organization_id
)
SELECT
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:30:00+00',
    'cisco-switch-01',
    '{"fqdn": "cisco-switch-01.example.com"}'::jsonb,
    '{}'::jsonb,
    '["cisco.ios.ios_command", "cisco.ios.ios_config"]'::jsonb,
    3,
    1,
    NULL,
    j.id,
    j.organization_id
FROM public.main_unifiedjob j
WHERE j.name = 'default_unified_job_2025-06-13'
ORDER BY j.id
LIMIT 1;

INSERT INTO public.main_indirectmanagednodeaudit (
    created, name, canonical_facts, facts, events, count, host_id, inventory_id, job_id, organization_id
)
SELECT
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:30:00+00',
    'cisco-switch-02',
    '{"fqdn": "cisco-switch-02.example.com"}'::jsonb,
    '{}'::jsonb,
    '["azure.azcollection.azure_rm_storageaccount"]'::jsonb,
    5,
    2,
    NULL,
    j.id,
    j.organization_id
FROM public.main_unifiedjob j
WHERE j.name = 'default_unified_job_2025-06-13'
ORDER BY j.id
LIMIT 1;

-- host_id 2 again and host_id 3 (new): deduplication in prepare() keeps 3 unique hosts.
INSERT INTO public.main_indirectmanagednodeaudit (
    created, name, canonical_facts, facts, events, count, host_id, inventory_id, job_id, organization_id
)
SELECT
    TIMESTAMP WITH TIME ZONE '2025-06-13 11:30:00+00',
    'cisco-switch-02',
    '{"fqdn": "cisco-switch-02.example.com"}'::jsonb,
    '{}'::jsonb,
    '["cisco.ios.ios_command"]'::jsonb,
    2,
    2,
    NULL,
    j.id,
    j.organization_id
FROM public.main_unifiedjob j
WHERE j.name = 'default_unified_job_11_2025-06-13'
ORDER BY j.id
LIMIT 1;

INSERT INTO public.main_indirectmanagednodeaudit (
    created, name, canonical_facts, facts, events, count, host_id, inventory_id, job_id, organization_id
)
SELECT
    TIMESTAMP WITH TIME ZONE '2025-06-13 11:30:00+00',
    'cisco-switch-03',
    '{"fqdn": "cisco-switch-03.example.com"}'::jsonb,
    '{}'::jsonb,
    '["azure.azcollection.azure_rm_virtualmachine", "cisco.ios.ios_facts"]'::jsonb,
    1,
    3,
    NULL,
    j.id,
    j.organization_id
FROM public.main_unifiedjob j
WHERE j.name = 'default_unified_job_11_2025-06-13'
ORDER BY j.id
LIMIT 1;
