-- Feature flag test data for dab_feature_flags_aapflag table

-- Enabled boolean flag
INSERT INTO public.dab_feature_flags_aapflag (
    modified,
    created,
    name,
    ui_name,
    condition,
    value,
    required,
    support_level,
    visibility,
    toggle_type,
    description,
    support_url,
    labels,
    created_by_id,
    modified_by_id
) VALUES
(
    '2025-11-04 15:00:00+00',
    '2025-11-04 15:00:00+00',
    'FEATURE_INDIRECT_NODE_COUNTING_ENABLED',
    'Indirect Node Counting',
    'boolean',
    'True',
    false,
    'supported',
    true,
    'boolean',
    'Enables indirect node counting for managed hosts.',
    '',
    NULL,
    NULL,
    NULL
),
-- Disabled boolean flag
(
    '2025-11-04 15:00:00+00',
    '2025-11-04 15:00:00+00',
    'FEATURE_SOME_DISABLED_FLAG',
    'Some Disabled Feature',
    'boolean',
    'False',
    false,
    'supported',
    true,
    'boolean',
    'A feature that is currently disabled.',
    '',
    NULL,
    NULL,
    NULL
),
-- Another enabled boolean flag
(
    '2025-11-04 15:00:00+00',
    '2025-11-04 15:00:00+00',
    'FEATURE_ANALYTICS_ENABLED',
    'Analytics',
    'boolean',
    'True',
    false,
    'tech-preview',
    true,
    'boolean',
    'Enables analytics data collection.',
    '',
    NULL,
    NULL,
    NULL
);
