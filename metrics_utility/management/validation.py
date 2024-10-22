import os
from metrics_utility.exceptions import BadShipTarget, MissingRequiredEnvVar, BadRequiredEnvVar


def handle_directory_ship_target(ship_target):
    ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH', None)

    if not ship_path:
        raise MissingRequiredEnvVar(
            "Missing required env variable METRICS_UTILITY_SHIP_PATH, having destination "\
            "for the generated data")

    return {"ship_path": ship_path}



def handle_s3_ship_target(ship_target):
    ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH', None)
    bucket_name = os.getenv('METRICS_UTILITY_BUCKET_NAME', None)
    bucket_endpoint = os.getenv('METRICS_UTILITY_BUCKET_ENDPOINT', None)
    bucket_region = os.getenv('METRICS_UTILITY_BUCKET_REGION', None)
    # Define S3 credentials
    bucket_access_key = os.getenv('METRICS_UTILITY_BUCKET_ACCESS_KEY', None)
    bucket_secret_key = os.getenv('METRICS_UTILITY_BUCKET_SECRET_KEY', None)

    if not ship_path:
        raise MissingRequiredEnvVar(
            "Missing required env variable METRICS_UTILITY_SHIP_PATH, having destination "\
            "for the generated data")

    if not bucket_name or not bucket_endpoint or not bucket_access_key or not bucket_secret_key:
        raise MissingRequiredEnvVar(
            "Missing one of required env variables for S3 configuration, namely: METRICS_UTILITY_BUCKET_NAME,"
            "METRICS_UTILITY_BUCKET_ENDPOINT, METRICS_UTILITY_BUCKET_ACCESS_KEY "\
            "and METRICS_UTILITY_BUCKET_SECRET_KEY.")

    return {
        "ship_path": ship_path,
        "bucket_name": bucket_name,
        "bucket_endpoint": bucket_endpoint,
        "bucket_region": bucket_region,
        "bucket_access_key": bucket_access_key,
        "bucket_secret_key": bucket_secret_key,
        }


def handle_crc_ship_target(ship_target):
    billing_provider = os.getenv('METRICS_UTILITY_BILLING_PROVIDER', None)
    red_hat_org_id = os.getenv('METRICS_UTILITY_RED_HAT_ORG_ID', None)

    billing_provider_params = {"billing_provider": billing_provider}
    if billing_provider == "aws":
        billing_account_id = os.getenv('METRICS_UTILITY_BILLING_ACCOUNT_ID', None)
        if not billing_account_id:
            raise MissingRequiredEnvVar(
                "Env var: METRICS_UTILITY_BILLING_ACCOUNT_ID, containing "\
                " AWS 12 digit customer id needs to be provided.")
        billing_provider_params["billing_account_id"] = billing_account_id
    else:
        raise MissingRequiredEnvVar(
            "Uknown METRICS_UTILITY_BILLING_PROVIDER env var, supported values are"\
            " [aws].")

    if red_hat_org_id:
        billing_provider_params["red_hat_org_id"] = red_hat_org_id

    return billing_provider_params
