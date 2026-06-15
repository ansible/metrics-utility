# Task Dashboard

Standalone task management UI for local development. Talks directly to the Metrics Service API using Basic auth — no Django dependency.

## Setup

```bash
# Start the dev server (from metrics-service)
cd ../metrics-service
python manage.py runserver

# Serve the dashboard (from metrics-utility root)
python -m http.server 8044
```

Then open http://localhost:8044/tools/service_tasks/dashboard.html and log in with your dev credentials.

## Notes

- Requires `BasicAuthentication` in DRF settings (enabled by default in dev mode, stripped in production)
- The CORS middleware in `apps/settings/development.py` allows cross-origin requests from the dashboard's port — dev mode only
- Auto-refreshes every 5 seconds
- API base URL defaults to `http://localhost:8000/api/v1/` but is configurable on the login screen
