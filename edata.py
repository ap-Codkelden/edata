#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2016 Renat Nasridinov
# This software may be freely distributed under the MIT license.
# https://opensource.org/licenses/MIT The MIT License (MIT)
# or see LICENSE file

# This software needs Requests -- Non-GMO HTTP library for Python
# Requests -- Python HTTP for Humans
# <https://pypi.python.org/pypi/requests/>

import requests
import json
import csv
import sqlite3
import argparse
import sys
import re
from datetime import datetime
from requests.exceptions import ConnectionError


SQLITE_MAX_VARIABLE_NUMBER = 999
DATE_DASH = re.compile('(\d{2})\-(\d{2})\-(\d{4})')
DATE_DOTS = re.compile('(\d{2})\.(\d{2})\.(\d{4})')
TREASURY = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
            19, 20, 21, 22, 23, 24, 25, 26, 27, 99]
EDATA_API_URL = "http://api.e-data.gov.ua:8080/api/rest/1.0"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:47.0) "
    "Gecko/20100101 Firefox/47.0",
    "Accept": "application/json,text/html,application/xhtml+xml,"
    "application/xml;q=0.9,*/*;q=0.8",
    'Content-Type': 'application/json',
    }


class Error(Exception):
    pass


class NoOutputFormatSpecifiedError(Error):
    def __init__(self):
        sys.stderr.write(
            'Не вказано ані відправників (параметр -p/--payers),'
            ' ані отримувачів (параметр -r/--receipts). Повинен бути вказаний'
            ' хоч один з них.\n'
            )


class OnlyOneOutputFormatIsAllowedError(Error):
    def __init__(self):
        sys.stderr.write(
            'Забагато вихідних форматів, має бути вказано лише один '
            'формат для зберігання.\n'
            )


class OnlyLastLoadParameterIsAllowedError(Error):
    def __init__(self):
        sys.stderr.write(
            'Параметр `lastload` не призначений для використання разом з '
            'іншими параметрами.\n'
            )


class NoDataReturnError(Error):
    def __init__(self):
        sys.stderr.write('Системою Є-Data на запит не повернуто даних.\n')


class EDataSystemError(Error):
    def __init__(self, message):
        self.message = "Отримано помилку API порталу Є-Data:\n" \
            "{}\n".format(message)


class ValueIsNotADateError(Error):
    def __init__(self, message):
        self.message = message


class DateOrderViolation(Error):
    def __init__(self):
        sys.stderr.write(
            'Початкова дата більша за кінцеву, дати буде поміняно '
            'місцями.\n'
            )


class WrongTreasuryInList(Error):
    def __init__(self):
        sys.stderr.write('Казначейства з даним кодом не існує.\n')


arg_parser = argparse.ArgumentParser(
    prog=None,
    usage=None,
    description="Отримує дані з порталу державних коштів Є-Data та зберігає "
    "їх в різноманітні формати файлів",
    epilog=None
    )


arg_parser.add_argument("-j", "--json", action='store_true',
                        help="Зберегти у файл JSON")
arg_parser.add_argument('-c', '--csv', action='store_true', help='зберегти '
                        'у файл CSV')
arg_parser.add_argument('-sql', '--sqlite', action='store_true',
                        help='записати у базу даних SQLite')
arg_parser.add_argument('-p', '--payers', dest='payers', default=[],
                        help='відправники платежу', type=str,  nargs='+')
arg_parser.add_argument('-r', '--receipts', dest='receipts', default=[],
                        help='отримувачі платежу', type=str, nargs='+')
arg_parser.add_argument('-s', '--startdate', action='store', type=str,
                        help='початкова дата пошуку транзакцій',
                        dest="startdate")
arg_parser.add_argument('-e', '--enddate', action='store', type=str,
                        help='кінцева дата пошуку транзакцій', dest="enddate")
arg_parser.add_argument('-t', '--treasury', nargs='+', default=[], type=int,
                        help='перелік регіональних управлінь ДКС',
                        dest="treasury")
arg_parser.add_argument('-a', '--ascii', action='store_true', help='вивести '
                        'ASCII-сумісний JSON-файл')
arg_parser.add_argument('-i', '--indent', dest='indent', type=int,
                        help='кількість пробілів для відступу у JSON-файлі',
                        default=0)
arg_parser.add_argument('-iso', '--iso8601', action='store_false',
                        help='залишити дату транзакції у форматі '
                        'datetime ISO 8601')
arg_parser.add_argument('-l', '--lastload', action='store_true',
                        help='показати дату повного завантаження усіх '
                        'платежів')
arg_parser.add_argument('-k', '--keep-json', action='store_true',
                        help='зберегти файл JSON при зберіганні до бази '
                        'даних SQLite')
arg_parser.add_argument('-v', '--verbose', action='store_true',
                        help='виводити додаткову інформацію')


def show_db_stats(processed_records, present_records):
    verbose_msg = "Кількість оброблених записів: {:>10}\n" \
        .format(processed_records)
    sys.stdout.write(verbose_msg)
    sys.stdout.write("Кількість доданих записів:" +
                     ' ' * 4 + "{:>10}\n"
                     .format(processed_records - present_records))
    return


def chunks(list_, n):
    """Yield successive n-sized chunks from list_."""
    for i in range(0, len(list_), n):
        yield list_[i:i + n]


def iso8601_to_date(s, lastload=False):
    dt_regex = re.compile(
        "(\d{4}\-\d{2}\-\d{2}T\d{2}\:\d{2}\:\d{2})((?:\+|\-)\d{2}\:\d{2})"
        )
    m = dt_regex.match(s)
    if m:
        datetime_part, timezone_part = m.groups()
    else:
        return s
    d = datetime.strptime(
        "{}{}".format(datetime_part, re.sub('\:', '', timezone_part)),
        '%Y-%m-%dT%H:%M:%S%z')
    if lastload:
        return d.strftime('%a, %b %d %Y %H:%M:%S %Z')
    return d.strftime('%Y-%m-%d')


def _date_generator(edata_transactions):
    transactions = [t for t in edata_transactions]
    for t in transactions:
        yield t


def iso8601_replace(edata):
    transactions = _date_generator(edata['response']['transactions'])
    new_transactions = []
    edata['response']['transactions'] = None
    for t in transactions:
        t['trans_date'] = iso8601_to_date(t['trans_date'])
        new_transactions.append(t)
    edata['response']['transactions'] = new_transactions
    return edata


def make_csv(edata, verbose=None):
    fieldnames = ["amount", "payer_bank", "region_id", "trans_date",
                  "recipt_name", "id", "payment_details", "recipt_mfo",
                  "payer_edrpou", "recipt_bank", "recipt_edrpou", "payer_mfo",
                  "payer_name", ]
    try:
        with open('edata.csv', 'w') as csvfile:
            writer = csv.DictWriter(
                csvfile, fieldnames=fieldnames,
                delimiter=';', quoting=csv.QUOTE_NONNUMERIC
                )
            writer.writeheader()
            for row in edata:
                writer.writerow(row)
        if verbose:
            sys.stdout.write("{} рядків записано\n".format(len(edata)))
    except:
        raise


def make_sqlite(edata, verbose=False):
    db = sqlite3.connect('edata.sqlite')
    c = db.cursor()
    qry = """CREATE TABLE IF NOT EXISTS edata (amount real, payer_bank text,
        region_id integer, trans_date text, recipt_name text,
        id integer PRIMARY KEY ON CONFLICT REPLACE,
        payment_details text, recipt_mfo integer NULL, payer_edrpou text,
        recipt_bank text NULL, recipt_edrpou text, payer_mfo integer NULL,
        payer_name text NULL);"""
    values = {'amount': None, 'payer_bank': None, 'region_id': None,
              'trans_date': None, 'recipt_name': None, 'id': None,
              'payment_details': None, 'recipt_mfo': None,
              'payer_edrpou': None, 'recipt_bank': None,
              'recipt_edrpou': None, 'payer_mfo': None, 'payer_name': None}
    c.execute(qry)

    qry = """INSERT INTO edata (amount, payer_bank, region_id, trans_date,
        recipt_name, id, payment_details, recipt_mfo, payer_edrpou,
        recipt_bank, recipt_edrpou, payer_mfo, payer_name) VALUES (:amount,
        :payer_bank, :region_id, :trans_date, :recipt_name, :id,
        :payment_details, :recipt_mfo, :payer_edrpou, :recipt_bank,
        :recipt_edrpou, :payer_mfo, :payer_name);"""
    try:
        if verbose:
            present_records = 0
            for chunk in chunks(edata, SQLITE_MAX_VARIABLE_NUMBER):
                id2insert = [x['id'] for x in chunk]
                placeholders = ', '.join(['?']*len(id2insert))
                query = 'SELECT COUNT(*) FROM edata WHERE id IN (%s)' \
                    % placeholders
                c.execute(query, id2insert)
                chunk_count = c.fetchone()[0]
                present_records += chunk_count

        c.executemany(qry, ({k: d.get(k, values[k]) for k in values}
                      for d in edata))
        if verbose:
            processed_records = c.rowcount
            show_db_stats(processed_records, present_records)
    except:
        raise
    db.commit()


def fetch(qry_dict, output_format=None, ascii=False, indent=False,
          iso8601=False, keep_json=None, verbose=False):
    try:
        r = requests.post(
            EDATA_API_URL + '/transactions',
            headers=HEADERS,
            data=json.dumps(qry_dict)
            )
        edata_json = r.json()
        if 'transactions' in edata_json['response']:
            if not edata_json['response']['transactions']:
                if not edata_json['response']['errors']:
                    raise NoDataReturnError
                else:
                    raise EDataSystemError(
                        edata_json['response']['errors'][0]['error']
                        )
    except ConnectionError as e:
        print("Помилка з'єднання: `{}`".format(e.args[0].args[0]))
        sys.exit(1)
    except NoDataReturnError:
        sys.exit(0)
    except EDataSystemError as e:
        print(e.message)
        sys.exit(1)
    except:
        raise
    else:
        if iso8601:
            edata_json = iso8601_replace(edata_json)
        if output_format == '0x2':    # json
            make_json(edata_json, ensure_ascii=ascii, indent=indent,
                      verbose=verbose)
        elif output_format == '0x4':  # csv
            make_csv(edata_json['response']['transactions'],
                     verbose=verbose)
        elif output_format == '0x8':  # sqlite
            if keep_json:
                make_json(edata_json, ensure_ascii=ascii, indent=indent,
                          verbose=False)
            make_sqlite(edata_json['response']['transactions'],
                        verbose=verbose)


def make_json(edata_json, ensure_ascii=None, indent=None, verbose=None):
    try:
        with open('edata.json', 'w', encoding='utf-8') as f:
            json.dump(edata_json, f, ensure_ascii=ascii, indent=indent)
    except:
        raise
    else:
        if verbose:
            sys.stdout.write("{} значень збережено\n".format(
                len(edata_json['response']['transactions']))
                )


def checkdate(date_string):
    do, da = DATE_DOTS.match(date_string), DATE_DASH.match(date_string)
    try:
        if not (da or do):
            raise ValueIsNotADateError(
                'Передане значення `{}` не відповідає допустимому формату '
                'дати'.format(date_string)
                )
    except ValueIsNotADateError as e:
        print(e.message)
        sys.exit(1)
    try:
        if da:
            date_string = '{}-{}-{}'.format(
                da.group(1), da.group(2), da.group(3)
                )
            datetime.strptime(date_string, "%d-%m-%Y")
        elif do:
            datetime.strptime(date_string, "%d.%m.%Y")
    except ValueError:
        print('Значення `{}` вигядає як дата, але є '
              'некоректним.'.format(date_string))
        sys.exit(1)
    else:
        return date_string if re.search('\-', date_string) \
            else re.sub('\.', '-', date_string)


def get_date(d):
    return datetime.strptime(d, "%d-%m-%Y" if re.search('\-', d)
                             else "%d.%m.%Y")


def check_date_order(startdate, enddate):
    if not get_date(startdate) < get_date(enddate):
        raise DateOrderViolation
    return


def show_lastload():
    try:
        r = requests.get(
            EDATA_API_URL + '/lastload',
            headers=HEADERS,
            )
        lastload_json = r.json()
    except:
        raise
    else:
        d = iso8601_to_date(lastload_json['response']['lastload'],
                            lastload=True)
        print(d)
        sys.exit(0)


def compose_data_dict(
        payers_edrpous,
        recipt_edrpous,
        startdate=None,
        enddate=None,
        regions=None
        ):
    d = {}
    if startdate:
        d['startdate'] = startdate
    if enddate:
        d['enddate'] = enddate
    if payers_edrpous:
        d['payers_edrpous'] = payers_edrpous
    if recipt_edrpous:
        d['recipt_edrpous'] = recipt_edrpous
    if regions:
        d['regions'] = regions
    return d


def get_date_value(date_):
    return checkdate(date_) if date_ else None


def main():
    results = arg_parser.parse_args()

    if len(sys.argv) == 1:
        arg_parser.print_help()
        sys.exit(2)

    try:
        if results.lastload and not (results.payers or results.receipts):
            show_lastload()
        elif results.lastload and (results.payers or results.receipts):
            raise OnlyLastLoadParameterIsAllowedError
    except OnlyLastLoadParameterIsAllowedError:
        sys.exit(2)

    try:
        output_formats = [results.json, results.csv, results.sqlite]
        if sum(output_formats) > 1:
            raise OnlyOneOutputFormatIsAllowedError
    except OnlyOneOutputFormatIsAllowedError:
        sys.exit(2)

    # format constants:
    # 0x2: json
    # 0x4: csv
    # 0x8: sqlite
    if results.json:
        format_ = '0x2'
    elif results.csv:
        format_ = '0x4'
    elif results.sqlite:
        format_ = '0x8'

    if (not results.json) and results.indent:
        results.indent = False
        print('Параметр `--indent` проігноровано.\n')

    try:
        if not (results.payers or results.receipts):
            raise NoOutputFormatSpecifiedError
    except NoOutputFormatSpecifiedError:
        sys.exit(2)

    startdate, enddate = get_date_value(results.startdate), \
        get_date_value(results.enddate)

    if startdate and enddate:
        try:
            if startdate != enddate:
                check_date_order(startdate, enddate)
        except DateOrderViolation:
            startdate, enddate = enddate, startdate
    if results.treasury:
        try:
            if not set(results.treasury).issubset(TREASURY):
                raise WrongTreasuryInList
        except WrongTreasuryInList:
            sys.exit(2)
        finally:
            treasury = results.treasury
    else:
        treasury = results.treasury

    qry = compose_data_dict(startdate=startdate,
                            recipt_edrpous=results.receipts,
                            payers_edrpous=results.payers,
                            enddate=enddate,
                            regions=treasury,
                            )
    fetch(qry, output_format=format_, ascii=results.ascii,
          indent=results.indent, iso8601=results.iso8601,
          keep_json=results.keep_json, verbose=results.verbose)


if __name__ == '__main__':
    main()
