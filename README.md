# metrics-utility

metrics-utility deals with collecting metrics from [Ansible Automation Platform (AAP)](https://www.ansible.com/products/automation-platform) Controller instances.

It provides two interfaces - a CLI and a python library.

### CLI

* [docs/gather.md](./docs/gather.md) - `gather_automation_controller_billing_data` - collects metrics into tarballs, ships to directory, S3, or console.redhat.com
* [docs/build.md](./docs/build.md) - `build_report` - builds XLSX reports from gathered tarballs

### Library

* [metrics\_utility.library](./metrics_utility/library/) - python library documentation

### Development

* [docs/CONTRIBUTING.md](./docs/CONTRIBUTING.md) - contributor's guide
* [docs/awx.md](./docs/awx.md) - running against awx dev env
* [docs/collectors.md](./docs/collectors.md) - collector reference (tables, time range support)
* [docs/development.md](./docs/development.md) - development environment setup & testing
* [docs/partitions.md](./docs/partitions.md) - partition pruning analysis

### Other

* [CHANGELOG.md](./CHANGELOG.md) - changes between tagged releases
* [LICENSE.md](./LICENSE.md) - Apache-2.0 license
