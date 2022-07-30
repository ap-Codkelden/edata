#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2016-2020 Renat Nasridinov
# This software may be freely distributed under the MIT license.
# https://opensource.org/licenses/MIT The MIT License (MIT)
# or see LICENSE file

# This software needs Requests -- Non-GMO HTTP library for Python
# Requests -- Python HTTP for Humans
# <https://pypi.python.org/pypi/requests/>

# TODO:
# procedure for CSV download
# URL parts as constants

import requests
import json
import sqlite3
import argparse
import sys
import re
from datetime import datetime
from requests.exceptions import ConnectionError
from requests.packages.urllib3.exceptions import ProtocolError
from regions import REGIONS


SQLITE_MAX_VARIABLE_NUMBER = 999
ISO_DATE_TEMPLATE = re.compile(r'(\d{4})-(\d{2})-(\d{2})')
TREASURY = [x['regionCode'] for x in REGIONS]
ZIPPED_STAT_NAME = '_stat'
EDATA_API_URL = "http://api.spending.gov.ua/api"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:47.0) "
    "Gecko/20100101 Firefox/47.0",
    "Accept": "application/json",
    'Content-Type': 'application/json'
    }


class Error(Exception):
    pass


class NoEDRPOUError(Error):
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


class Top100WithEDRPOUError(Error):
    def __init__(self):
        sys.stderr.write(
            'Параметр --top100 не може використовуватися разом з кодами '
            'ЄДРПОУ отримувачів коштів та платників, ігноруємо…\n'
            )


class CannotFetchStatFileError(Error):
    def __init__(self):
        sys.stderr.write(
            'Не вдалося отримати файл статистики.\n'
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
        self.message = "Помилка API порталу Є-Data:\n" \
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


class DatesWithoutPayersError(Error):
    def __init__(self):
        sys.stderr.write(
            'Початкова та/або кінцева дата зазначені без кодів платників '
            'або отримувачів, параметри проігноровано.\n'
            )


class StatisticProcNeedsParameterError(Error):
    def __init__(self):
        sys.stderr.write(
            'Ця процедура потребує наявності одного з параметрів `--doc` або '
            '`--org`. Вкажіть потрібний і запустіть скрипт знову.\n'
            )


class WrongTreasuryInList(Error):
    def __init__(self):
        sys.stderr.write('Казначейства з даним кодом не існує.\n')


arg_parser = argparse.ArgumentParser(
    prog=None,
    usage=None,
    description="Отримує дані з порталу державних коштів Є-Data та зберігає "
    "їх в різноманітні формати файлів",
    epilog=None,
    )

subparsers = arg_parser.add_subparsers(dest='subparser_name')

# Дані по транзакціях
trans_parser = subparsers.add_parser(
    'transactions',
    help='Отримання інформації щодо трансакцій і збереження її у різних '
    'форматах'
    )
# Статистика документів на порталі
stat_parser = subparsers.add_parser(
    'statistic',
    help='Статистика документів на порталі (лише JSON)'
    )
doc_org_group = stat_parser.add_mutually_exclusive_group()
# Довідник регіонів
region_parser = subparsers.add_parser('regions', help='Довідник регіонів')
# Статистика по документах органиізацій
cabinets_parser = subparsers.add_parser(
    'cabinets',
    help='Статистика по документах органиізацій (zipped CSV)'
    )
trans_parser.add_argument(
    '-v', '--verbose', action='store_true',
    help='виводити додаткову інформацію'
    )
trans_parser.add_argument("-j", "--json", action='store_true',
                          help="Зберегти у файл JSON")
trans_parser.add_argument('-c', '--csv', action='store_true', help='зберегти '
                          'у файл CSV (за замовчуванням)')
trans_parser.add_argument('-sql', '--sqlite', action='store_true',
                          help='записати у базу даних SQLite')
trans_parser.add_argument('-p', '--payers', dest='payers', default=[],
                          help='відправники платежу', type=str,  nargs='+')
trans_parser.add_argument('-r', '--receipts', dest='receipts', default=[],
                          help='отримувачі платежу', type=str, nargs='+')
trans_parser.add_argument('-s', '--startdate', action='store', type=str,
                          help='початкова дата пошуку транзакцій',
                          dest="startdate")
trans_parser.add_argument('-e', '--enddate', action='store', type=str,
                          help='кінцева дата пошуку транзакцій',
                          dest="enddate")
trans_parser.add_argument('-t', '--treasury', nargs='+', default=[], type=int,
                          help='перелік регіональних управлінь ДКС',
                          dest="treasury")
trans_parser.add_argument('-a', '--ascii', action='store_true', help='вивести '
                          'ASCII-сумісний JSON-файл')
trans_parser.add_argument('-i', '--indent', dest='indent', type=int,
                          help='кількість пробілів для відступу у JSON-файлі',
                          default=0)
trans_parser.add_argument('-l', '--lastload', action='store_true',
                          help='показати дату повного завантаження усіх '
                          'платежів')
trans_parser.add_argument('-k', '--keep-json', action='store_true',
                          help='зберегти файл JSON при зберіганні до бази '
                          'даних SQLite')
trans_parser.add_argument('--ping', action='store_true',
                          help='перевірити доступність API')
trans_parser.add_argument('--top', action='store_true', dest='top100',
                          help='Повертає Топ 100 транзакцій по регіону')

region_parser.add_argument('-p', '--ping', action='store_true',
                           help='Перевірка доступності API')
region_parser.add_argument('-v', '--verbose', action='store_true',
                           help='виводити додаткову інформацію')
region_parser.add_argument('-a', '--ascii', action='store_true', help='вивести'
                           ' ASCII-сумісний JSON-файл')

stat_parser.add_argument('-v', '--verbose', action='store_true',
                         help='виводити додаткову інформацію')
stat_parser.add_argument('-a', '--ascii', action='store_true', help='вивести '
                         'ASCII-сумісний JSON-файл')
doc_org_group.add_argument('--org', action='store_true', help='Зберегти '
                           'статистику документів організацій на порталі')
doc_org_group.add_argument('--doc', action='store_true', help='Зберегти '
                           'агреговану ститистику документів на порталі '
                           '(загальні кількість/кількість оприлюднених)')
trans_parser.add_argument('--zipname', type=str, default='_transactions',
                          help='імя ZIP-файлу з транзакціями',)


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


def _date_generator(edata_transactions):
    transactions = [t for t in edata_transactions]
    for t in transactions:
        yield t


def save_file(binary_iter_content, file_name, verbose=None):
    if re.match(r'^.+?\.zip$', file_name):
        # re.sub(pattern, repl, string, count=0, flags=0)
        file_name = re.sub('zip$', '', file_name)
    with open(file_name+'.zip', 'wb') as f:
        for chunk in binary_iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
    return


def make_sqlite(edata, verbose=False):
    db = sqlite3.connect('edata.sqlite')
    c = db.cursor()
    qry = """CREATE TABLE IF NOT EXISTS edata (amount real, payer_bank text,
        region_id integer, trans_date text, recipt_name text,
        id integer PRIMARY KEY ON CONFLICT REPLACE,
        payment_details text, recipt_mfo integer NULL, payer_edrpou text,
        recipt_bank text NULL, recipt_edrpou text, payer_mfo integer NULL,
        payer_name text NULL, doc_number text NULL, doc_date text,
        doc_v_date text, payer_account text, recipt_account text,
        doc_add_attr text NULL);"""
    values = {'amount': None, 'payer_bank': None, 'region_id': None,
              'trans_date': None, 'recipt_name': None, 'id': None,
              'payment_details': None, 'recipt_mfo': None,
              'payer_edrpou': None, 'recipt_bank': None,
              'recipt_edrpou': None, 'payer_mfo': None, 'payer_name': None,
              'doc_number': None, 'doc_date': None, 'doc_v_date': None,
              'payer_account': None, 'recipt_account': None,
              'doc_add_attr': None}
    c.execute(qry)

    qry = """INSERT INTO edata (amount, payer_bank, region_id, trans_date,
        recipt_name, id, payment_details, recipt_mfo, payer_edrpou,
        recipt_bank, recipt_edrpou, payer_mfo, payer_name, doc_number,
        doc_date, doc_v_date, payer_account, recipt_account, doc_add_attr)
        VALUES (:amount, :payer_bank, :region_id, :trans_date, :recipt_name,
        :id, :payment_details, :recipt_mfo, :payer_edrpou, :recipt_bank,
        :recipt_edrpou, :payer_mfo, :payer_name, :doc_number, :doc_date,
        :doc_v_date, :payer_account, :recipt_account, :doc_add_attr);"""
    try:
        if verbose:
            present_records = 0
            for chunk in chunks(edata, SQLITE_MAX_VARIABLE_NUMBER):
                id2insert = [x['id'] for x in chunk]
                placeholders = ', '.join(['?']*len(id2insert))
                query = "SELECT COUNT(*) FROM edata WHERE id IN (%s)" \
                        % placeholders
                c.execute(query, id2insert)
                chunk_count = c.fetchone()[0]
                present_records += chunk_count

        c.executemany(qry, ({k: d.get(k, values[k]) for k in values}
                      for d in edata))
        if verbose:
            processed_records = c.rowcount
            show_db_stats(processed_records, present_records)
    except Error:
        raise
    db.commit()


def fetch(qry_dict, output_format=None, ascii=False, indent=False,
          keep_json=None, top100=None, verbose=None, zipname=None):
    transactions_api_part = '/v2/api/transactions/top100' if top100 \
        and not qry_dict else '/v2/api/transactions/'
    if output_format == '0x4':
        HEADERS['Accept'] = 'application/octet-stream'
    try:
        r = requests.get(EDATA_API_URL + transactions_api_part,
                         headers=HEADERS,
                         params=qry_dict
                         )
        if output_format == '0x4':
            if r.status_code == 200:
                try:
                    save_file(r.iter_content, zipname, verbose)
                except Error:
                    raise
                else:
                    return 0
            elif r.status_code in (403, 403, 404):
                r.raise_for_status()
        edata_json = r.json()
        if 'error' in edata_json:
            raise EDataSystemError(
                edata_json['error']
                )
    except requests.exceptions.HTTPError as e:
        print(e.args[0])
        # raise
        sys.exit(1)
    except ConnectionError as e:
        print("Помилка з'єднання: `{}`".format(e.args[0].args[0]))
        sys.exit(1)
    except NoDataReturnError:
        sys.exit(0)
    except EDataSystemError as e:
        print(e.message)
        sys.exit(1)
    except Error:
        # print(r.text)
        raise
        # sys.exit(1)
    else:
        if output_format == '0x2':    # json
            make_json(edata_json, ensure_ascii=ascii, indent=indent,
                      verbose=verbose)
        elif output_format == '0x8':  # sqlite
            if keep_json:
                make_json(edata_json, ensure_ascii=ascii, indent=indent,
                          verbose=False)
            make_sqlite(edata_json,
                        verbose=verbose)


def make_json(edata_json, ensure_ascii=None, indent=None, verbose=None):
    try:
        with open('edata.json', 'w', encoding='utf-8') as f:
            json.dump(edata_json, f, ensure_ascii=ensure_ascii, indent=indent)
    except Error:
        raise
    else:
        if verbose:
            sys.stdout.write("{} значень збережено\n".format(
                len(edata_json['response']['transactions']))
                )


def checkdate(date_string):
    iso_date = ISO_DATE_TEMPLATE.match(date_string)
    try:
        if not iso_date:
            raise ValueIsNotADateError(
                'Передане значення `{}` не відповідає '
                'допустимому формату дати. Допустимий формат ISO8601: '
                '`YYYY-MM-DD`'.format(date_string)
                )
    except ValueIsNotADateError as e:
        print(e.message)
        sys.exit(1)
    try:
        datetime.strptime(date_string, "%Y-%m-%d")
    except ValueError as e:
        print(e.args[0])
        sys.exit(1)
    else:
        return date_string


def get_date(d):
    return datetime.strptime(d, "%Y-%m-%d")


def check_date_order(startdate, enddate):
    if not get_date(startdate) < get_date(enddate):
        raise DateOrderViolation
    return


def ping(regions=None):
    ping_url_part = '/v2/regions/ping' if regions else \
        '/v2/api/transactions/ping'
    try:
        r = requests.get(
            EDATA_API_URL + ping_url_part,
            headers=HEADERS,
            )
        if r.status_code == 200:
            print('{}API is alive!'.format('Regions ' if regions else ''))
        elif r.status_code in (403, 403, 404):
            r.raise_for_status()
    except (ConnectionError, ProtocolError) as e:
        print("Помилка з'єднання: `{}`".format(e.args[0].args[0]))
        sys.exit(1)
    else:
        sys.exit(0)


def show_lastload(verbose=None):
    try:
        r = requests.get(
            EDATA_API_URL + '/v2/api/transactions/lastload',
            headers=HEADERS,
            )
        lastload_json = r.json()
        if verbose:
            if r.status_code == 200:
                print('Response 200, OK…')
        if r.status_code in (403, 403, 404):
            r.raise_for_status()
    except (ConnectionError, ProtocolError) as e:
        print("Помилка з'єднання: `{}`".format(e.args[0].args[0]))
        sys.exit(1)
    else:
        d = lastload_json['lastLoad']
        d1 = datetime.strptime(d, '%Y-%m-%d')
        print(d1.strftime('%a, %b %d %Y'))
        sys.exit(0)


def compose_data_dict(
        payers_edrpous,
        recipt_edrpous,
        startdate=None,
        enddate=None,
        regions=None
        ):
    d = {}
    try:
        if (startdate or enddate) and not (payers_edrpous or recipt_edrpous):
            if startdate != enddate:
                raise DatesWithoutPayersError
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
    except DatesWithoutPayersError:
        pass
    finally:
        return d


def get_date_value(date_):
    return checkdate(date_) if date_ else None


def transactions(results):
    try:
        if results.lastload and not (results.payers or results.receipts):
            show_lastload(verbose=results.verbose)
        elif results.ping and not (results.payers or results.receipts):
            ping(regions=False)
        elif results.lastload and (results.payers or results.receipts):
            raise OnlyLastLoadParameterIsAllowedError
        elif results.top100 and (results.payers or results.receipts):
            raise Top100WithEDRPOUError
    except OnlyLastLoadParameterIsAllowedError:
        sys.exit(2)
    except Top100WithEDRPOUError:
        pass

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
    else:
        format_ = '0x4'

    if (not results.json) and results.indent:
        results.indent = False
        sys.stdout.write('Параметр `--indent` проігноровано.\n')

    if (not results.sqlite) and results.keep_json:
        sys.stdout.write('Параметр `--keep-json` проігноровано, оскільки '
                         'збереження проводиться не в базу даних SQLite.\n')

    startdate, enddate = get_date_value(results.startdate), \
        get_date_value(results.enddate)

    try:
        if not results.top100:
            if startdate != enddate and not (results.payers or results.receipts):
                raise NoEDRPOUError
    except NoEDRPOUError:
        sys.exit(2)

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
          top100=results.top100, indent=results.indent,
          keep_json=results.keep_json, verbose=results.verbose,
          zipname=results.zipname)


def _stat_get_org(verbose=None):
    stat_part = '/v2/stat/organizations/csv'
    HEADERS['Accept'] = 'application/octet-stream'
    try:
        r = requests.get(EDATA_API_URL + stat_part,
                         headers=HEADERS,
                         )
        if r.status_code in (403, 403, 404):
            r.raise_for_status()
        if r.status_code != 200:
            raise CannotFetchStatFileError
    except CannotFetchStatFileError:
        sys.exit(1)
    except Error:
        raise
    else:
        save_file(r.iter_content, ZIPPED_STAT_NAME, verbose)


def _stat_get_doc(url, ascii=None, verbose=None):
    try:
        _download_arbitrary_json(url, ascii=ascii, verbose=verbose,
                                 json_filename='_stat_documents.json',
                                 )
    except Error:
        raise
    else:
        sys.exit(0)


def statistic(org, doc, ascii=None, verbose=None):
    try:
        if not (org or doc):
            raise StatisticProcNeedsParameterError
    except StatisticProcNeedsParameterError:
        sys.exit(1)

    try:
        if org:
            _stat_get_org(verbose)
        elif doc:
            _stat_get_doc('/v2/stat/documents', ascii, verbose)
    except Error:
        raise
    else:
        sys.exit(0)


def _download_arbitrary_json(url_part, ascii, json_filename, verbose):
    """Downloads JSON data through API URL and saves it to
    file with specified name"""
    try:
        r = requests.get(EDATA_API_URL + url_part,
                         headers=HEADERS,
                         )
        if r.status_code in (403, 403, 404):
            r.raise_for_status()
    except Error:
        raise
    else:
        if r.status_code == 200:
            with open(json_filename, 'w') as json_file:
                json.dump(r.json(), json_file, ensure_ascii=ascii)
        return


def regions(ping_region=None, ascii=None, verbose=None):
    if ping_region:
        ping(regions=True)
    region_list_part = '/v2/regions'
    try:
        _download_arbitrary_json(
            '/v2/regions', ascii=ascii, json_filename='_regions.json',
            verbose=verbose)
    except Error:
        raise
    else:
        sys.exit(0)


if __name__ == '__main__':
    if len(sys.argv) == 1:
        arg_parser.print_help()
        sys.exit(2)
    results = arg_parser.parse_args()
    print(results)
    command = results.subparser_name
    if command == 'transactions':
        transactions(results)
    elif command == 'statistic':
        statistic(results.org, results.doc, results.ascii, results.verbose,)
    elif command == 'regions':
        regions(results.ping, results.ascii)
    elif command == 'cabinets':
        cabinets(results)
