# TODO
class DedupCCSP:
    def __init__(self, dataframes, extra_params):
        self.dataframes = dataframes
        self.extra_params = extra_params

    def run_deduplication(self):
        # FIXME - ccsp: jobhost, content, inventory ; ccspv2: jobhost, content, inventory, collection status
        return self.dataframes[0].copy()
