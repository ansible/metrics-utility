# docker compose environment & mock awx data

This provides the docker compose environment,
and also the database schemas, used both by compose & github CI.

When loading the `.sql` files, `roles.sql` needs to come first, then `latest.sql` (schema),
then the rest of the files can go in any order.
