#!/usr/bin/env python
import argparse
import datetime
import glob
import io
import json
import logging
import math
import os
import pathlib
import random
import tarfile
import tempfile
import uuid

import numpy as np
import pandas as pd

from metrics_utility.automation_controller_billing.extract.base import Base
from metrics_utility.automation_controller_billing.helpers import parse_json
from metrics_utility.library import CsvFileSplitter


# adds relative time since start; debug with --verbose
logging.basicConfig(format='%(asctime)s(+%(relativeCreated)d): %(message)s', level=logging.WARNING)
logger = logging.getLogger(__name__)


def now():
    return datetime.datetime.now(tz=datetime.timezone.utc)


def parse_date(str):
    return datetime.datetime.fromisoformat(str).astimezone(datetime.timezone.utc)


def random_date(earliest, latest):
    tsmin = earliest.timestamp()
    tsmax = latest.timestamp()
    rand = tsmin + (random.random() * (tsmax - tsmin))
    return datetime.datetime.fromtimestamp(rand, tz=datetime.timezone.utc)


def random_adjective():
    adjectives = 'bold brave bright calm cheerful clever cozy eager exuberant gentle graceful happy honest honorable jolly kind lively lucky merry \
nice noble peaceful playful proud quick quiet shiny strong swift thoughtful vibrant warm wise witty'
    return random.choice(adjectives.split(' '))


def random_noun():
    nouns = 'armadillo axolotl badger beetle bison buffalo capybara cat caribou cassowary chameleon cheetah cobra coyote dolphin eagle elephant \
falcon ferret flamingo fox gazelle giraffe hippo ibex jaguar kangaroo koala lemur leopard lion lynx macaw manul meerkat narwhal octopus orangutan \
otter owl panda panther peacock pelican penguin pigeon puma rabbit raven rhino salmon sparrow tiger toucan turtle whale wolf wombat zebra'
    return random.choice(nouns.split(' '))


def random_hostname():
    adjective = random_adjective()
    noun = random_noun()
    number = format(random.randrange(1000000), '06')

    return f'{adjective}-{noun}-{number}'


def random_machine_id():
    return str(uuid.uuid4())


def random_product_serial():
    noun = random_noun()
    num = random.randint(0, 65536)

    return f'{noun}{num:04x}'


def rule_multiply(df, target_size):
    """repeat each line enough times we reach target_size"""
    return df.loc[np.repeat(df.index, math.ceil(target_size / len(df)))].reset_index(drop=True)


def rule_crop(df, target_size):
    """remove every row after target_size"""
    return df.loc[: (target_size - 1)]


def rule_dates(df, fields, output_from, output_to):
    """change each field to a random date between from, to"""
    for f in fields:
        df[f] = df[f].apply(lambda _old: random_date(output_from, output_to))
    return df


def rule_ids(df, fields):
    """change each field to a sequential number"""
    for f in fields:
        df[f] = range(len(df))
    return df


def rule_hostname(df, fields):
    """change each field to a random hostname-like string"""
    for f in fields:
        df[f] = df[f].apply(lambda _old: random_hostname())
    return df


def rule_hostname_or_null(df, fields):
    """change each field to a random hostname-like string or null"""
    for f in fields:
        df[f] = df[f].apply(lambda _old: random_hostname() if random.choice([True, False]) else None)
    return df


def rule_canonical_facts(df):
    """adjusts canonical_facts - random choice of unchanged, unset, set, for both machine_id & product_serial"""

    def process_row(val):
        facts = parse_json(val) or {}

        choice = random.choice([0, 1, 2])
        if choice == 1:
            facts['ansible_machine_id'] = None
        if choice == 2:
            facts['ansible_machine_id'] = random_machine_id()

        choice = random.choice([0, 1, 2])
        if choice == 1:
            facts['ansible_product_serial'] = None
        if choice == 2:
            facts['ansible_product_serial'] = random_product_serial()

        return json.dumps(facts)

    df['canonical_facts'] = df['canonical_facts'].apply(process_row)
    return df


def job_host_summary_data(df, config, output_from, output_to):
    df = rule_multiply(df, config[1])  # unique
    df = rule_hostname(df, ['host_name'])
    df = rule_hostname_or_null(df, ['ansible_host_variable'])
    df = rule_multiply(df, config[0])  # total
    df = rule_crop(df, config[0])  # total
    df = rule_ids(df, ['id'])
    df = rule_dates(df, ['created', 'modified', 'job_created'], output_from, output_to)
    return df


def main_host_data(df, config, output_from, output_to):
    df = rule_multiply(df, config[1])  # unique
    df = rule_canonical_facts(df)
    df = rule_hostname(df, ['host_name'])
    df = rule_hostname_or_null(df, ['ansible_host_variable'])
    df = rule_multiply(df, config[0])  # total
    df = rule_crop(df, config[0])  # total
    df = rule_canonical_facts(df)
    df = rule_ids(df, ['host_id'])
    df = rule_dates(df, ['last_automation'], output_from, output_to)
    return df


def main_indirectmanagednodeaudit_data(df, config, output_from, output_to):
    df = rule_multiply(df, config[1])  # unique
    df = rule_hostname(df, ['host_name'])
    df = rule_multiply(df, config[0])  # total
    df = rule_crop(df, config[0])  # total
    df = rule_ids(df, ['id'])
    df = rule_dates(df, ['created', 'job_created'], output_from, output_to)
    return df


def main_jobevent_data(df, config, output_from, output_to):
    df = rule_multiply(df, config[1])  # unique
    df = rule_hostname(df, ['host_name'])
    df = rule_multiply(df, config[0])  # total
    df = rule_crop(df, config[0])  # total
    df = rule_ids(df, ['main_jobhostsummary_id'])
    df = rule_dates(df, ['main_jobhostsummary_created', 'created', 'modified', 'job_created'], output_from, output_to)
    return df


def data_collection_status_data(selected, output_from, output_to):
    return pd.DataFrame(
        list(
            map(
                lambda file: {
                    'collection_start_timestamp': now().isoformat(),
                    'since': output_from.isoformat(),
                    'until': output_to.isoformat(),
                    'file_name': f'{file}.csv',
                    'status': 'ok',
                    'elapsed': str(int((output_to - output_from).total_seconds())),
                },
                selected,
            )
        )
    )


def process_tarballs(path, temp_dir, enabled_set):
    logger.info(f'Processing {path}')

    class ProcessTarballs(Base):
        # load config.json
        def load_config(self, file_path):
            with open(file_path) as f:
                return json.loads(f.read())

    # extract csv based on generator SELECTED_DATA
    return ProcessTarballs(extra_params=dict()).process_tarballs(path, temp_dir, enabled_set)


# metrics_utility.automation_controller_billing.collectors daily_slicing, but without the awx imports
def daily_slicing(**kwargs):
    since, until = kwargs.get('since', None), kwargs.get('until', now())
    if since is None:
        return

    start, end = since, None
    start_beginning_of_next_day = start.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)

    # If the date range is over one day, we want first interval to contain the rest of the day
    # then we'll cycle by full days
    if until > start_beginning_of_next_day:
        yield (start, start_beginning_of_next_day)
        start = start_beginning_of_next_day

    while start < until:
        end = min(start + datetime.timedelta(days=1), until)
        yield (start, end)
        start = end


class Main:
    def __init__(self):
        self.parse_env()
        self.parse_args()

        logger.debug(f'config {vars(self)}')

    def parse_env(self):
        year = now().year

        # data_collection_status = ()
        self.job_host_summary = (
            int(os.getenv('MAIN_JOBHOSTSUMMARY_SIZE', '10000')),
            int(os.getenv('MAIN_JOBHOSTSUMMARY_UNIQUE_SIZE', '2000')),
        )
        self.main_host = (
            int(os.getenv('MAIN_HOST_SIZE', '10000')),
            int(os.getenv('MAIN_HOST_UNIQUE_SIZE', '2000')),
            int(os.getenv('MAIN_HOST_FREQUENCY', '1')),  # every N days; or once when 0
        )
        self.main_indirectmanagednodeaudit = (
            int(os.getenv('MAIN_INDIRECT_SIZE', '10000')),
            int(os.getenv('MAIN_INDIRECT_UNIQUE_SIZE', '2000')),
        )
        self.main_jobevent = (
            int(os.getenv('MAIN_JOBEVENT_SIZE', '10000')),
            int(os.getenv('MAIN_JOBEVENT_UNIQUE_SIZE', '2000')),
        )

        # source tarball glob
        self.source_tarballs = os.getenv('SOURCE_DATA_PATH', f'./metrics_utility/test/test_data/data/{year}/**/*.tar.gz')
        self.output_data_path = os.getenv('OUTPUT_DATA_PATH', './metrics_utility/test/test_data/data/')

        # input and output date range
        self.input_from = parse_date(os.getenv('INPUT_DATE_FROM', f'{year - 1}-01-01'))
        self.input_to = parse_date(os.getenv('INPUT_DATE_TO', f'{year}-01-01'))
        self.output_from = parse_date(os.getenv('OUTPUT_DATE_FROM', f'{year}-01-01'))
        self.output_to = parse_date(os.getenv('OUTPUT_DATE_TO', f'{year + 1}-01-01'))

        # csvs to expand
        self.selected = set(
            filter(bool, os.getenv('SELECTED_DATA', 'job_host_summary,main_host,main_indirectmanagednodeaudit,main_jobevent').split(','))
        )

    def parse_args(self):
        parser = argparse.ArgumentParser(
            prog='generator',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Environment vars:
    MAIN_JOBHOSTSUMMARY_SIZE (default: 10000)
    MAIN_JOBHOSTSUMMARY_UNIQUE_SIZE (default: 2000)
    MAIN_HOST_SIZE (default: 10000)
    MAIN_HOST_UNIQUE_SIZE (default: 2000)
    MAIN_HOST_FREQUENCY (default: 1)
    MAIN_INDIRECT_SIZE (default: 10000)
    MAIN_INDIRECT_UNIQUE_SIZE (default: 2000)
    MAIN_JOBEVENT_SIZE (default: 10000)
    MAIN_JOBEVENT_UNIQUE_SIZE (default: 2000)
    SOURCE_DATA_PATH (default: ./metrics_utility/test/test_data/data/{year}/**/*.tar.gz)
    OUTPUT_DATA_PATH (default: ./metrics_utility/test/test_data/data/)
    INPUT_DATE_FROM (default: lastyear-01-01)
    INPUT_DATE_TO (default: year-01-01)
    OUTPUT_DATE_FROM (default: year-01-01)
    OUTPUT_DATE_TO (default: nextyear-01-01)
    SELECTED_DATA (default: job_host_summary,main_host,main_indirectmanagednodeaudit,main_jobevent)
        """,
        )
        parser.add_argument('-v', '--verbose', action='store_true')
        args = parser.parse_args()

        if args.verbose:
            logger.setLevel(logging.DEBUG)

    def concat(self, name, data):
        if name not in self.selected:
            return

        if data.empty:
            return

        if self.loaded[name] is None:
            self.loaded[name] = data
            return

        self.loaded[name] = pd.concat([self.loaded[name], data], ignore_index=True)

    def load(self):
        self.loaded = dict((s, None) for s in self.selected)
        logger.debug(f'loaded {self.loaded}')

        if os.path.isdir(self.source_tarballs):
            tarballs = glob.glob(os.path.join(self.source_tarballs, '**/*.tar.gz'), recursive=True)
        else:
            tarballs = glob.glob(self.source_tarballs, recursive=True)

        logger.debug(f'tarballs {tarballs}')

        for file in tarballs:
            with tempfile.TemporaryDirectory(prefix='metrics-generator-load') as temp_dir:
                data = process_tarballs(file, temp_dir, enabled_set=self.selected)

                self.concat('job_host_summary', data['job_host_summary'])
                self.concat('main_host', data['main_host'])
                self.concat('main_indirectmanagednodeaudit', data['main_indirectmanagednodeaudit'])
                self.concat('main_jobevent', data['main_jobevent'])
                self.config_json = data['config']

        logger.debug(f'loaded {self.loaded}')

    def gen_df(self, table, fn, settings):
        if table not in self.loaded:
            return

        logger.info(f'{table} - loaded')
        self.generated[table] = fn(self.loaded[table], settings, self.output_from, self.output_to)
        logger.info(f'{table} - duplicated')

    def process(self):
        self.generated = dict((s, None) for s in self.selected)

        self.gen_df('job_host_summary', job_host_summary_data, self.job_host_summary)
        self.gen_df('main_host', main_host_data, self.main_host)
        self.gen_df('main_indirectmanagednodeaudit', main_indirectmanagednodeaudit_data, self.main_indirectmanagednodeaudit)
        self.gen_df('main_jobevent', main_jobevent_data, self.main_jobevent)

    def save_csvs(self, table, temp_dir, df):
        logger.info(f'{table} - to_csv')
        splitter = CsvFileSplitter(filespec=f'{temp_dir}/{table}.csv')
        df.to_csv(index=False, path_or_buf=splitter)
        return splitter.file_list(keep_empty=True)

    def tarify(self, table, since, until, file):
        logger.info(f'{table} - add_to_tar {file}')

        target = pathlib.Path(self.output_data_path).joinpath(since.strftime('%Y/%m/%d'))
        os.makedirs(target, exist_ok=True)

        uuid = '00000000-0000-0000-0000-000000000000'
        frm = since.strftime('%Y-%m-%d-%H%M%S%z')
        to = until.strftime('%Y-%m-%d-%H%M%S%z')
        name_base = f'{uuid}-{frm}-{to}'

        index = len(list(target.glob(f'{name_base}-*.*')))
        tarname = f'{name_base}-{index}-{table}.tar.gz'

        filename = target.joinpath(tarname)
        with tarfile.open(filename, 'w:gz') as tar:
            # always
            out = data_collection_status_data([table], since, until)
            self.csv_to_tar('data_collection_status.csv', out, tar, until)
            self.json_to_tar('config.json', self.config_json, tar, until)

            # table
            tar.add(file, arcname=f'./{table}.csv')

        logger.debug(f'created {filename}')

    def save_tarballs(self, table):
        """creates and saves all tarballs for table"""
        if table not in self.generated:
            return

        df = self.generated[table]
        if df.empty:
            return

        if table == 'main_host':
            # main_host - only generate csvs once, not filtered by since/until
            with tempfile.TemporaryDirectory(prefix=f'metrics-generator-save-{table}') as temp_dir:
                file_list = self.save_csvs(table, temp_dir, df)

                # output every N days (MAIN_HOST_FREQUENCY=1), or at the end of the period if 0
                frequency = self.main_host[2]
                idx = 0
                for since, until in daily_slicing(since=self.output_from, until=self.output_to):
                    idx += 1
                    if not frequency:
                        continue
                    if idx % frequency:
                        continue

                    logger.info(f'{table} - {since}-{until}')
                    for file in file_list:
                        self.tarify(table, since, until, file)

                if not frequency:
                    logger.info(f'{table} - {since}-{until}')
                    for file in file_list:
                        self.tarify(table, since, until, file)
        else:
            # generate csvs daily, filtered by since/until
            for since, until in daily_slicing(since=self.output_from, until=self.output_to):
                logger.info(f'{table} - {since}-{until}')
                filtered_df = df[(df['created'] >= since) & (df['created'] <= until)]

                if filtered_df.empty:
                    continue

                with tempfile.TemporaryDirectory(prefix=f'metrics-generator-save-{table}') as temp_dir:
                    file_list = self.save_csvs(table, temp_dir, filtered_df)

                    for file in file_list:
                        self.tarify(table, since, until, file)

    def save(self):
        self.save_tarballs('job_host_summary')
        self.save_tarballs('main_host')
        self.save_tarballs('main_indirectmanagednodeaudit')
        self.save_tarballs('main_jobevent')

    def csv_to_tar(self, filename, content, tar, timestamp):
        self.add_to_tar(filename, content.to_csv(index=False), tar, timestamp)

    def json_to_tar(self, filename, content, tar, timestamp):
        self.add_to_tar(filename, json.dumps(content), tar, timestamp)

    def add_to_tar(self, filename, content, tar, timestamp):
        logger.debug(filename, content)

        buf = content.encode('utf-8')
        info = tarfile.TarInfo(f'./{filename}')
        info.size = len(buf)
        info.mtime = timestamp.timestamp()
        tar.addfile(info, fileobj=io.BytesIO(buf))


if __name__ == '__main__':
    main = Main()
    main.load()
    main.process()
    main.save()
