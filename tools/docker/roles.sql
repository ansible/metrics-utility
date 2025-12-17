CREATE ROLE awx SUPERUSER LOGIN PASSWORD 'awx';

CREATE ROLE metrics_service SUPERUSER LOGIN PASSWORD 'metrics_service';
CREATE DATABASE metrics_service OWNER 'metrics_service';
