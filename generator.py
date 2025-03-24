#!/usr/bin/env python
import argparse
import datetime
import glob
import io
import json
import math
import numpy as np
import os
import pandas as pd
import pathlib
import random
import tarfile
import tempfile
from metrics_utility.automation_controller_billing.extract.base import Base


def parse_date(str):
    return datetime.datetime.fromisoformat(str).astimezone(datetime.timezone.utc)


def random_date(earliest, latest):
    tsmin = earliest.timestamp()
    tsmax = latest.timestamp()
    rand = tsmin + (random.random() * (tsmax - tsmin))
    return datetime.datetime.fromtimestamp(rand, tz=datetime.timezone.utc)


def random_hostname():
    nouns = 'armadillo axolotl badger beetle bison buffalo capybara caribou cassowary chameleon cheetah cobra coyote dolphin eagle elephant falcon \
ferret flamingo fox gazelle giraffe hippo ibex jaguar kangaroo koala lemur leopard lynx macaw meerkat narwhal octopus orangutan otter owl panda \
panther peacock pelican penguin pigeon puma rabbit raven rhino sparrow tiger toucan turtle whale wolf wombat zebra'
    adjectives = 'bold brave bright calm cheerful clever eager gentle graceful happy honest jolly kind lively lucky merry nice noble peaceful \
playful proud quick quiet shiny strong swift thoughtful vibrant warm witty'

    adjective = random.choice(adjectives.split(' '))
    noun = random.choice(nouns.split(' '))
    number = random.randint(100, 999)

    return f'{adjective}-{noun}-{number}'


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


# ? host_remote_id ? ansible_host_variable ?
def job_host_summary_data(df, config, output_from, output_to):
    df = rule_multiply(df, config[1])  # unique
    df = rule_hostname(df, ['host_name'])
    df = rule_multiply(df, config[0])  # total
    df = rule_crop(df, config[0])  # total
    df = rule_ids(df, ['id'])
    df = rule_dates(df, ['created', 'modified', 'job_created'], output_from, output_to)
    return df


# ? ansible_host_variable ? canonical_facts ? facts ?
def main_host_data(df, config, output_from, output_to):
    df = rule_multiply(df, config[1])  # unique
    df = rule_hostname(df, ['host_name'])
    df = rule_multiply(df, config[0])  # total
    df = rule_crop(df, config[0])  # total
    df = rule_ids(df, ['host_id'])
    df = rule_dates(df, ['last_automation'], output_from, output_to)
    return df


# ? host_remote_id ? canonical_facts ? facts ?
def main_indirectmanagednodeaudit_data(df, config, output_from, output_to):
    df = rule_multiply(df, config[1])  # unique
    df = rule_hostname(df, ['host_name'])
    df = rule_multiply(df, config[0])  # total
    df = rule_crop(df, config[0])  # total
    df = rule_ids(df, ['id'])
    df = rule_dates(df, ['created', 'job_created'], output_from, output_to)
    return df


# ? main_jobhostsummary_id ?
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
                    'collection_start_timestamp': datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
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
    class ProcessTarballs(Base):
        # load config.json
        def load_config(self, file_path):
            with open(file_path) as f:
                return json.loads(f.read())

        # extract csv based on generator SELECTED_DATA
        def csv_enabled(self, name):
            return name in enabled_set

    return ProcessTarballs().process_tarballs(path, temp_dir)


class Main:
    def __init__(self):
        self.parse_env()
        self.parse_args()

        if self.verbose:
            print('config', vars(self))

    def parse_env(self):
        year = datetime.datetime.now(tz=datetime.timezone.utc).year

        # data_collection_status = ()
        self.job_host_summary = (
            int(os.getenv('MAIN_JOBHOSTSUMMARY_SIZE', '10000')),
            int(os.getenv('MAIN_JOBHOSTSUMMARY_UNIQUE_SIZE', '2000')),
        )
        self.main_host = (
            int(os.getenv('MAIN_HOST_SIZE', '10000')),
            int(os.getenv('MAIN_HOST_UNIQUE_SIZE', '2000')),
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

        self.verbose = args.verbose

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
        if self.verbose:
            print('loaded', self.loaded)

        if os.path.isdir(self.source_tarballs):
            tarballs = glob.glob(os.path.join(self.source_tarballs, '**/*.tar.gz'), recursive=True)
        else:
            tarballs = glob.glob(self.source_tarballs, recursive=True)

        if self.verbose:
            print('tarballs', tarballs)

        for file in tarballs:
            with tempfile.TemporaryDirectory(prefix='metrics-generator') as temp_dir:
                data = process_tarballs(file, temp_dir, enabled_set=self.selected)

                self.concat('job_host_summary', data['job_host_summary'])
                self.concat('main_host', data['main_host'])
                self.concat('main_indirectmanagednodeaudit', data['indirect_nodes'])
                self.concat('main_jobevent', data['main_jobevent'])
                self.config_json = data['config']

        if self.verbose:
            print('loaded', self.loaded)

    def save(self):
        target = pathlib.Path(self.output_data_path).joinpath(self.output_to.strftime('%Y/%m/%d'))
        os.makedirs(target, exist_ok=True)

        uuid = '00000000-0000-0000-0000-000000000000'
        name_base = f'{uuid}-{self.output_from.strftime("%Y-%m-%d-%H%M%S%z")}-{self.output_to.strftime("%Y-%m-%d-%H%M%S%z")}'
        index = len(list(target.glob(f'{name_base}-*.*')))
        tarname = f'{name_base}-{index}.tar.gz'

        filename = target.joinpath(tarname)
        with tarfile.open(filename, 'w:gz') as tar:
            if 'job_host_summary' in self.loaded:
                out = job_host_summary_data(self.loaded['job_host_summary'], self.job_host_summary, self.output_from, self.output_to)
                self.csv_to_tar('job_host_summary.csv', out, tar, self.output_to)
            if 'main_host' in self.loaded:
                out = main_host_data(self.loaded['main_host'], self.main_host, self.output_from, self.output_to)
                self.csv_to_tar('main_host.csv', out, tar, self.output_to)
            if 'main_indirectmanagednodeaudit' in self.loaded:
                out = main_indirectmanagednodeaudit_data(
                    self.loaded['main_indirectmanagednodeaudit'], self.main_indirectmanagednodeaudit, self.output_from, self.output_to
                )
                self.csv_to_tar('main_indirectmanagednodeaudit.csv', out, tar, self.output_to)
            if 'main_jobevent' in self.loaded:
                out = main_jobevent_data(self.loaded['main_jobevent'], self.main_jobevent, self.output_from, self.output_to)
                self.csv_to_tar('main_jobevent.csv', out, tar, self.output_to)
            # always
            out = data_collection_status_data(self.selected, self.output_from, self.output_to)
            self.csv_to_tar('data_collection_status.csv', out, tar, self.output_to)
            self.json_to_tar('config.json', self.config_json, tar, self.output_to)

        if self.verbose:
            print(f'created {filename}')

    def csv_to_tar(self, filename, content, tar, timestamp):
        return self.add_to_tar(filename, content.to_csv(index=False), tar, timestamp)

    def json_to_tar(self, filename, content, tar, timestamp):
        return self.add_to_tar(filename, json.dumps(content), tar, timestamp)

    def add_to_tar(self, filename, content, tar, timestamp):
        if self.verbose:
            print(filename, content)

        buf = content.encode('utf-8')
        info = tarfile.TarInfo(f'./{filename}')
        info.size = len(buf)
        info.mtime = timestamp.timestamp()
        tar.addfile(info, fileobj=io.BytesIO(buf))


if __name__ == '__main__':
    main = Main()
    main.load()
    main.save()
