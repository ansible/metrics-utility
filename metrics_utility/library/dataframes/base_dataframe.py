import pandas as pd


# f = path or file-like
def from_csv(f):
    return pd.read_csv(f)


def from_json(f):
    return pd.read_json(f)


def from_parquet(f):
    return pd.read_parquet(f)


# these return the file content if f=None
def to_csv(df, f=None):
    return df.to_csv(f, index=False)


def to_json(df, f=None):
    return df.to_json(f)


def to_parquet(df, f=None):
    return df.to_parquet(f)


# read_sql / to_sql need a sqlalchemy connection handle,
# we already have a read_sql_query in the form of copy_table
# so we might do to_sql manually with Django's connection too

# (raw csv) -> from_csv -> (pd.DataFrame) -> add_raw (= prepare -> group -> add_rollup) -> postprocess
# (parquet rollup) -> ... -> (pd.DataFrame) -> add_rollup -> regroup -> postprocess


class BaseDataframe:
    def __init__(self):
        self.rollup = None

    def add_rollup(self, new_group):
        if self.rollup is None:
            self.rollup = new_group
        else:
            self.rollup = self.merge(self.rollup, new_group)

    # a batch is either a dataframe (straight from `from_csv`), or a tuple of (dataframe(s), config dict)
    def add_raw(self, batch):
        df = self.prepare(batch)
        if df is None:
            return

        group = self.group(df)
        self.add_rollup(group)

    def merge(self, old, new):
        # merges old + new, returns the result
        # both are expected to be pre-grouped dataframes, if applicable
        # default to concat, overridden for complex merges
        return pd.concat([old, new], ignore_index=True)

    def prepare(self, df):
        return df

    def group(self, df):
        return df

    def regroup(self, df):
        return df

    def empty(self):
        # overriden where types are known
        return pd.DataFrame()

    def postprocess(self, df):
        return df.reset_index()

    # build_dataframe, where batches=iter_batches()
    # FIXME: cli only?
    def from_tarballs(self, batches):
        # all-rows dataframe, no aggregation
        self.rollup = None

        for batch in batches:
            self.add_raw(batch)

        if self.rollup is None or self.rollup.empty:
            self.rollup = None
        else:
            self.rollup = self.postprocess(self.rollup)

    def to_csv(self, f=None):
        return to_csv(self.rollup, f)

    def to_json(self, f=None):
        return to_json(self.rollup, f)

    def to_parquet(self, f=None):
        return to_parquet(self.rollup, f)
