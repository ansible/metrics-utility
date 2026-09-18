# vcpu collector docs

## Environment variables:

  - `METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR`: [true/false] Disable the job host summary collector if true (default false), useful in case of multiple cronjobs are running to collect data.
  - `METRICS_UTILITY_COLLECTOR_LOCK_SUFFIX`: [str] Suffix added to the lock name, must be set in case of multiple cronjobs are running and this to avoid one cronjob to prevent the others to run.
  - `METRICS_UTILITY_DISABLE_SAVE_LAST_GATHERED_ENTRIES`: [true/false] Some collectors are not time based and thus are not using 'since/until' as they are taking a sample of the state at regular point of time. In that case we don't need to save the last gathered entries. Default false.

### Environment variables for the `total_workers_vcpu` collector:

  - `METRICS_UTILITY_CLUSTER_NAME`: Contains the cluster name which is part of the collection payload.
  - `METRICS_UTILITY_USAGE_BASED_METERING_ENABLED`: [true/false] In case of true, the payload will contain the actual number of total vcpu accross all workers otherwise the total will be set to 1.
  - `METRICS_UTILITY_PROMETHEUS_URL`: Prometheus base url (defaults to the in-cluster openshift-monitoring endpoint).
  - `METRICS_UTILITY_PROMETHEUS_TOKEN`: Bearer token for Prometheus. Overrides the in-cluster service-account token, mainly for local testing (see `./run-vcpu`).
  - `METRICS_UTILITY_PROMETHEUS_CA_CERT_PATH`: Path to the CA certificate used to verify Prometheus TLS. Overrides the in-cluster service CA; set to an empty string to skip verification (e.g. plain `http` mock).

N.B.: The SaaS solution runs on ROSA HCP so all nodes are workers, if this collector is used for another solution then the filtering must be implemented.
