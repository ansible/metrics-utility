-- 10:00-11:00 window: host_ids 1 and 2 (2 distinct indirect hosts).
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

-- 11:00-12:00 window: host_id 2 again (dedup test) and host_id 3 (new).
-- Expected unique count across both windows: 3 (host_ids 1, 2, 3).
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
