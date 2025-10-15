import hashlib


def hash(value, salt):
    # has the value and salt, hash should be string
    combined = (salt + ':' + value).encode('utf-8')
    hashed = hashlib.sha512(combined).hexdigest()
    return hashed


def anonymize(data, salt):
    # anonymize jobs job template name
    if 'jobs' in data:
        for job in data['jobs']:
            job['job_template_name'] = hash(job['job_template_name'], salt)

    # anonymize jobhostsummary job template name
    if 'jobhostsummary' in data:
        for jobhostsummary in data['jobhostsummary']:
            jobhostsummary['job_template_name'] = hash(jobhostsummary['job_template_name'], salt)

    # anonymize events modules module name
    if 'events_modules' in data:
        # list of modules to automate
        for module in data['events_modules']['list_of_modules_used_to_automate']:
            if module['collection_source'] == 'Unknown':
                module['module_name'] = hash(module['module_name'], salt)
                module['collection_name'] = hash(module['collection_name'], salt)

        # module_stats
        for module in data['events_modules']['module_stats']:
            if module['collection_source'] == 'Unknown':
                module['module_name'] = hash(module['module_name'], salt)
                module['collection_name'] = hash(module['collection_name'], salt)

        # collection_name_stats
        for collection in data['events_modules']['collection_name_stats']:
            if collection['collection_source'] == 'Unknown':
                collection['module_name'] = hash(module['module_name'], salt)
                collection['collection_name'] = hash(collection['collection_name'], salt)
