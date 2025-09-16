import csv
import glob
import os
import tarfile

from unittest.mock import patch

import pytest

from metrics_utility.base.collection import Collection
from metrics_utility.test.util import run_gather_ext, run_gather_int

env_vars = {
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
}

jobs_lines = [
    "id,polymorphic_ctype_id,model,organization_id,organization_name,execution_environment_image,inventory_id,inventory_name,created,name,unified_job_template_id,launch_type,schedule_id,execution_node,controller_node,cancel_flag,status,failed,started,finished,elapsed,job_explanation,instance_group_id,installed_collections,ansible_version,forks",
    "1,,2,default_org_2025-06-13,,4,default_inventory_2025-06-13,2025-06-13 10:00:00+00,default_unified_job_2025-06-13,1,manual,,auto,controller1,f,pending,f,,2025-06-13 10:00:00+00,0,,,{},2.9.10,0",
    "2,,2,default_org_2025-06-13,,4,default_inventory_2025-06-13,2025-06-13 10:00:00+00,default_unified_job_2025-06-13,1,manual,,auto,controller1,f,pending,f,,2025-06-13 10:00:00+00,0,,,{},2.9.10,0",
    "3,,2,default_org_2025-06-13,,4,default_inventory_2025-06-13,2025-06-13 10:00:00+00,default_unified_job_2025-06-13,1,manual,,auto,controller1,f,pending,f,,2025-06-13 10:00:00+00,0,,,{},2.9.10,0"
]

# we have to skip columns containing ids because they can change
json_lines_skip_ids_columns = [
    'id',
    'polymorphic_ctype_id',
    'organization_id',
    'inventory_id',
    'unified_job_template_id',
    'schedule_id',
    'instance_group_id',
]








