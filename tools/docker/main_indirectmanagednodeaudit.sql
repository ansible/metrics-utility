-- Snapshot test data: 4 records, host_ids 1, 2, 2, 3.
-- prepare() deduplicates to 3 unique hosts (host_ids 1, 2, 3).
-- Looks up job_id and organization_id from the job created by main_jobhostsummary.sql.
INSERT INTO public.main_indirectmanagednodeaudit (
    created, name, canonical_facts, facts, events, count, host_id, inventory_id, job_id, organization_id
)
SELECT
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:30:00+00',
    'cisco-switch-01',
    '{"fqdn": "cisco-switch-01.example.com"}'::jsonb,
    '{}'::jsonb,
    '[]'::jsonb,
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
    '[]'::jsonb,
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
    '[]'::jsonb,
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
    '[]'::jsonb,
    1,
    3,
    NULL,
    j.id,
    j.organization_id
FROM public.main_unifiedjob j
WHERE j.name = 'default_unified_job_11_2025-06-13'
ORDER BY j.id
LIMIT 1;
