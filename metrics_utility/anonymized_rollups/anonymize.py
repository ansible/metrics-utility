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
