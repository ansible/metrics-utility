# call compute_anonymized_rollup

from metrics_utility.anonymized_rollups.compute_anonymized_rollup import compute_anonymized_rollup


def anonymized_rollups_processor(db, salt, since, until, ship_path, save_rollups: bool = True):
    return compute_anonymized_rollup(db, salt, since, until, ship_path, save_rollups)
