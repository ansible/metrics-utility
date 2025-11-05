--
-- PostgreSQL database dump
--

INSERT INTO public.conf_setting (created, modified, key, value) VALUES
  (now(), now(), 'INSTALL_UUID', '"00000000-0000-0000-0000-000000000000"'),
  (now(), now(), 'LICENSE', '{"license_type": "UNLICENSED", "product_name": "AWX", "subscription_name": null, "valid_key": false}'),
  (now(), now(), 'TOWER_URL_BASE', '"https://platformhost"'),
  (now(), now(), 'AUTOMATION_ANALYTICS_LAST_ENTRIES', '"{\"config\": \"2024-01-01T10:00:00Z\", \"jobs\": \"2024-01-02T15:30:00Z\"}"');
