-- Seed additional projects with various scm_type values
-- Depends on schema loaded by latest.sql

DO $$
DECLARE
  uj_id INTEGER;
BEGIN
  -- Project with scm_type = 'git'
  INSERT INTO public.main_unifiedjobtemplate (
    created, modified, description, name, old_pk, last_job_failed, status, organization_id
  ) VALUES (
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
    '', 'git_project_1', 0, FALSE, 'never updated', 2
  ) RETURNING id INTO uj_id;

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
    uj_id,
    'LOCAL_PATH_GIT1',
    'git',
    'https://example.com/repo1.git',
    'main',
    TRUE,
    FALSE,
    TRUE,
    0,
    0,
    'rev-git-1',
    '{}'::jsonb,
    '{}'::jsonb,
    '',
    TRUE,
    FALSE
  );

  -- Project with scm_type = 'git' (second row to test counts)
  INSERT INTO public.main_unifiedjobtemplate (
    created, modified, description, name, old_pk, last_job_failed, status, organization_id
  ) VALUES (
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
    '', 'git_project_2', 0, FALSE, 'never updated', 2
  ) RETURNING id INTO uj_id;

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
    uj_id,
    'LOCAL_PATH_GIT2',
    'git',
    'https://example.com/repo2.git',
    'main',
    TRUE,
    FALSE,
    TRUE,
    0,
    0,
    'rev-git-2',
    '{}'::jsonb,
    '{}'::jsonb,
    '',
    TRUE,
    FALSE
  );

  -- Project with scm_type = 'svn'
  INSERT INTO public.main_unifiedjobtemplate (
    created, modified, description, name, old_pk, last_job_failed, status, organization_id
  ) VALUES (
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
    '', 'svn_project', 0, FALSE, 'never updated', 2
  ) RETURNING id INTO uj_id;

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
    uj_id,
    'LOCAL_PATH_SVN',
    'svn',
    'https://example.com/svn/repo',
    'trunk',
    TRUE,
    FALSE,
    TRUE,
    0,
    0,
    'rev-svn-1',
    '{}'::jsonb,
    '{}'::jsonb,
    '',
    TRUE,
    FALSE
  );

  -- Project with scm_type = '' (empty string)
  INSERT INTO public.main_unifiedjobtemplate (
    created, modified, description, name, old_pk, last_job_failed, status, organization_id
  ) VALUES (
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
    TIMESTAMP WITH TIME ZONE '2025-06-13 10:00:00+00',
    '', 'empty_scm_type_project', 0, FALSE, 'never updated', 2
  ) RETURNING id INTO uj_id;

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
    uj_id,
    'LOCAL_PATH_EMPTY',
    '',
    'https://example.com/none',
    '',
    TRUE,
    FALSE,
    TRUE,
    0,
    0,
    'rev-empty-1',
    '{}'::jsonb,
    '{}'::jsonb,
    '',
    TRUE,
    FALSE
  );
END
$$;

-- Seed data for main_project to validate the 'projects' collector
-- This script inserts several projects with varying scm_type values
-- so that grouping and counts can be tested reliably.

DO $$
DECLARE
  org_id INTEGER;
  tmpl_id INTEGER;
  -- helper to create a unified job template and corresponding project
  PROCEDURE insert_project(p_name TEXT, p_scm_type TEXT) LANGUAGE plpgsql AS $$
  BEGIN
    INSERT INTO public.main_unifiedjobtemplate (
      created,
      modified,
      description,
      name,
      old_pk,
      last_job_failed,
      status,
      organization_id
    ) VALUES (
      now(),
      now(),
      '',
      p_name,
      0,
      false,
      'ok',
      org_id
    )
    RETURNING id INTO tmpl_id;

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
      credential_id,
      admin_role_id,
      use_role_id,
      update_role_id,
      read_role_id,
      timeout,
      scm_revision,
      playbook_files,
      inventory_files,
      custom_virtualenv,
      scm_refspec,
      allow_override,
      default_environment_id,
      scm_track_submodules,
      signature_validation_credential_id
    ) VALUES (
      tmpl_id,
      '/var/lib/awx/projects/' || p_name,
      p_scm_type,
      'https://example.com/' || p_name || '.git',
      'main',
      false,
      false,
      false,
      0,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      0,
      'deadbeef',
      '[]'::jsonb,
      '[]'::jsonb,
      NULL,
      '',
      false,
      NULL,
      false,
      NULL
    );
  END;
  $$;
BEGIN
  -- Try to reuse the default org created by main_jobhostsummary.sql if present
  SELECT id INTO org_id
  FROM public.main_organization
  ORDER BY id ASC
  LIMIT 1;

  IF org_id IS NULL THEN
    -- Create a fallback organization if none exists
    INSERT INTO public.main_organization (
      created, modified, description, name, max_hosts
    ) VALUES (
      now(), now(), '', 'default_org_projects_seed', 0
    ) RETURNING id INTO org_id;
  END IF;

  -- Insert projects with scm_type distribution:
  -- git: 3, svn: 1, empty string: 1
  CALL insert_project('proj_git_1', 'git');
  CALL insert_project('proj_git_2', 'git');
  CALL insert_project('proj_git_3', 'git');
  CALL insert_project('proj_svn_1', 'svn');
  CALL insert_project('proj_empty_1', '');
END $$;


