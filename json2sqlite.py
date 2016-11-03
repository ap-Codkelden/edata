#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2016 Renat Nasridinov
# This software may be freely distributed under the MIT license.
# https://opensource.org/licenses/MIT The MIT License (MIT)
# or see LICENSE file


import argparse
import errno
import json
import os
import re
import sqlite3
import sys
from datetime import datetime
from os import scandir


class Error(Exception):
    pass


class ValueIsNotADateError(Error):
    def __init__(self, message):
        self.message = message


class NotValidEDataJSONError(Error):
    def __init__(self, filename):
        sys.stderr.write(
            'Файл `{}` не є файлом, вивантаженим '
            'з порталу\n'.format(filename)
            )


class NoTransactionsFoundError(Error):
    def __init__(self, filename):
        sys.stderr.write('У файлі `{}` відсутні транзакції\n'.format(filename))


class NoFilesProvidedError(Error):
    def __init__(self):
        sys.stderr.write('Файли для обробки відсутні\n')


class ErrorsInJSONFileError(Error):
    def __init__(self, filename):
        sys.stderr.write('У файлі `{}` присутні помилки\n'.format(filename))


arg_parser = argparse.ArgumentParser(
    prog=None,
    usage=None,
    description="Імпортує дані із JSON-файлів, вивантажених з порталу "
                "Є-Data, у базу даних SQLite",
    epilog=None
    )

arg_parser.add_argument('-f', '--file', default=[], help='JSON-файл(и)',
                        type=str,  nargs='+')
arg_parser.add_argument('-d', '--database', dest='database',
                        default='edata',
                        help="ім'я файла бази даних (БЕЗ розширення), "
                        "за замовчуванням -- `edata`"
                        )


class EDataSQLDatabase(object):
    def __init__(self, database=None):
        self._database_name = database+'.sqlite' if database \
            else 'edata.sqlite'
        self._database = sqlite3.connect(self._database_name)
        self.date8601 = True
        self.values = {'amount': None, 'payer_bank': None, 'region_id': None,
                       'trans_date': None, 'recipt_name': None, 'id': None,
                       'payment_details': None, 'recipt_mfo': None,
                       'payer_edrpou': None, 'recipt_bank': None,
                       'recipt_edrpou': None, 'payer_mfo': None,
                       'payer_name': None}
        # регулярний вираз для перетворення iso 8601 datetime
        # на iso8601 date
        self._dt_8601 = re.compile(
            "(\d{4}\-\d{2}\-\d{2}T\d{2}\:\d{2}\:\d{2})((?:\+|\-)\d{2}\:\d{2})"
            )
        if not self._check_table():
            self._create_table()

    def _check_table(self):
        c = self._database.cursor()
        chk_qry = """SELECT name FROM sqlite_master WHERE type='table'
            AND name='{}';""".format(self._database_name)
        c.execute(chk_qry)
        return c.fetchone()

    def _create_table(self):
        try:
            c = self._database.cursor()
            qry = """CREATE TABLE IF NOT EXISTS edata (amount real,
                payer_bank text, region_id integer, trans_date text,
                recipt_name text, id integer PRIMARY KEY ON CONFLICT REPLACE,
                payment_details text, recipt_mfo integer NULL,
                payer_edrpou text, recipt_bank text NULL, recipt_edrpou text,
                payer_mfo integer NULL,
                payer_name text NULL);"""
            columns = ', '.join(self.values.keys())
            placeholders = ':'+', :'.join(self.values.keys())
            c.execute(qry)
            return 0
        except:
            raise

    def _iso8601_replace(self, edata):
        transactions = self._date_generator(edata)
        for t in transactions:
            t['trans_date'] = self._iso8601_to_date(t['trans_date'])
        return edata

    def _date_generator(self, edata_transactions):
        transactions = [t for t in edata_transactions]
        for t in transactions:
            yield t

    def _iso8601_to_date(self, s):
        m = self._dt_8601.match(s)
        if m:
            datetime_part, timezone_part = m.groups()
        else:
            return s

        d = datetime.strptime(
            "{}{}".format(datetime_part, re.sub('\:', '', timezone_part)),
            '%Y-%m-%dT%H:%M:%S%z'
            )
        return d.strftime('%Y-%m-%d')

    def _insert_json(self, edata):
        c = self._database.cursor()
        # convert dates
        self._iso8601_replace(edata)
        qry = """INSERT INTO edata (amount, payer_bank, region_id, trans_date,
            recipt_name, id, payment_details, recipt_mfo, payer_edrpou,
            recipt_bank, recipt_edrpou, payer_mfo, payer_name) VALUES (:amount,
            :payer_bank, :region_id, :trans_date, :recipt_name, :id,
            :payment_details, :recipt_mfo, :payer_edrpou, :recipt_bank,
            :recipt_edrpou, :payer_mfo, :payer_name);"""
        try:
            c.executemany(
                qry,
                ({k: d.get(k, self.values[k]) for k in self.values}
                    for d in edata)
                )
        except:
            raise
        self._database.commit()

    def _check_structure(self, f, j):
        if 'response' not in j:
            raise NotValidEDataJSONError(f)
        if 'transactions' not in j['response']:
            raise NotValidEDataJSONError(f)
        if not j['response']['transactions']:
            raise NoTransactionsFoundError(f)
        if j["response"]["errors"]:
            raise ErrorsInJSONFileError(f)

    def import_file(self, json_file):
        try:
            with open(json_file) as f:
                json_data = json.load(f)
            self._check_structure(json_file, json_data)
        except json.decoder.JSONDecodeError as e:
            sys.stderr.write(
                'Файл `{}` не є файлом JSON або містить '
                'наступні помилки: {}\n'.format(json_file, e.msg)
                )
        except (NotValidEDataJSONError, NoTransactionsFoundError):
            pass
        except:
            raise
        else:
            self._insert_json(json_data['response']['transactions'])


def check_file(json_file):
    try:
        if not os.path.isfile(json_file):
            raise FileNotFoundError(
                errno.ENOENT,
                os.strerror(errno.ENOENT),
                json_file
                )
        return 1
    except FileNotFoundError as e:
        if e.errno == errno.ENOENT:
            print('Файл `{}` не існує, пропуск'.format(e.filename))
        return


def main():
    results = arg_parser.parse_args()
    try:
        json_filenames = results.file if results.file else \
        [f.path for f in scandir() if f.is_file() and
         os.path.splitext(f.path)[1].lower() == '.json']
        if not json_filenames:
            raise NoFilesProvidedError
    except NoFilesProvidedError:
        sys.exit(2)
    else:
        edb = EDataSQLDatabase(database=results.database)
        for f in [f for f in json_filenames if check_file(f)]:
            edb.import_file(f)


if __name__ == '__main__':
    main()
