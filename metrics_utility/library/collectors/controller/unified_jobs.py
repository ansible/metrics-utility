from ..util import DataframeOutput, collector


@collector
def unified_jobs(*, db=None, since=None, until=None, output=DataframeOutput()):
    query = f"""
        SELECT
            main_unifiedjob.id,
            main_unifiedjob.polymorphic_ctype_id,
            django_content_type.model,
            main_unifiedjob.organization_id,
            main_organization.name as organization_name,
            main_executionenvironment.image as execution_environment_image,
            main_job.inventory_id,
            main_inventory.name as inventory_name,
            main_unifiedjob.created,
            main_unifiedjob.name,
            main_unifiedjob.unified_job_template_id,
            main_unifiedjob.launch_type,
            main_unifiedjob.schedule_id,
            main_unifiedjob.execution_node,
            main_unifiedjob.controller_node,
            main_unifiedjob.cancel_flag,
            main_unifiedjob.status,
            main_unifiedjob.failed,
            main_unifiedjob.started,
            main_unifiedjob.finished,
            main_unifiedjob.elapsed,
            main_unifiedjob.job_explanation,
            main_unifiedjob.instance_group_id,
            main_unifiedjob.installed_collections,
            main_unifiedjob.ansible_version,
            main_job.forks,
            mut.name AS job_template_name,
            main_project.scm_type
        FROM main_unifiedjob
        LEFT JOIN main_unifiedjobtemplate ON main_unifiedjobtemplate.id = main_unifiedjob.unified_job_template_id
        LEFT JOIN django_content_type ON main_unifiedjob.polymorphic_ctype_id = django_content_type.id
        LEFT JOIN main_job ON main_unifiedjob.id = main_job.unifiedjob_ptr_id
        LEFT JOIN main_inventory ON main_job.inventory_id = main_inventory.id
        LEFT JOIN main_organization ON main_organization.id = main_unifiedjob.organization_id
        LEFT JOIN main_executionenvironment ON main_executionenvironment.id = main_unifiedjob.execution_environment_id
        LEFT JOIN main_project ON main_job.project_id = main_project.unifiedjobtemplate_ptr_id
        LEFT JOIN main_unifiedjobtemplate AS mut ON mut.id = main_unifiedjob.unified_job_template_id
        WHERE
            main_unifiedjob.finished >= '{since.isoformat()}'
            AND main_unifiedjob.finished < '{until.isoformat()}'
        ORDER BY main_unifiedjob.id ASC
    """

    return output.sql(db, query)
