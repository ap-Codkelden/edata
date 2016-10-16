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


DATE_DASH = re.compile('(\d{2})\-(\d{2})\-(\d{4})')
DATE_DOTS = re.compile('(\d{2})\.(\d{2})\.(\d{4})')
TREASURY = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
            19, 20, 21, 22, 23, 24, 25, 26, 27, 99, -1]


class Error(Exception):
    pass


class NoOutputFormatSpecifiedError(Error):
    def __init__(self):
        sys.stderr.write(
            'Не вказано ані відправників (параметр -r/--payers),'
            ' ані отримувачів (параметр -r/--receipts). Повинен бути вказаний'
            ' хоч один з них.\n'
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
            'Початкова дата більша за кінцеву, дати буде '
            'поміняно місцями.\n'
            )


class WrongTreasuryInList(Error):
    def __init__(self):
        sys.stderr.write('Казначейства з даним кодом не існує.\n')


class HelpDefaultArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('ПОМИЛКА: {}\n\n'.format(message))
        self.print_help()
        sys.exit(2)


arg_parser = HelpDefaultArgumentParser(
    prog=None,
    usage=None,
    description="Отримує дані з порталу державних коштів Є-Data та зберігає "
    "їх в різноманітні формати файлів",
    epilog=None
    )

format_group = arg_parser.add_mutually_exclusive_group(required=True)
format_group.add_argument("-j", "--json", action='store_true',
                          help="Зберегти у файл JSON")
format_group.add_argument('-c', '--csv', action='store_true', help='зберегти '
                          'у файл CSV')
format_group.add_argument('-sql', '--sqlite', action='store_true',
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


def write_csv(edata):
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
    except:
        raise


def make_sqlite(edata):
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
    columns = ', '.join(values.keys())
    placeholders = ':'+', :'.join(values.keys())
    c.execute(qry)

    qry = """INSERT INTO edata (amount, payer_bank, region_id, trans_date,
        recipt_name, id, payment_details, recipt_mfo, payer_edrpou,
        recipt_bank, recipt_edrpou, payer_mfo, payer_name) VALUES (:amount,
        :payer_bank, :region_id, :trans_date, :recipt_name, :id,
        :payment_details, :recipt_mfo, :payer_edrpou, :recipt_bank,
        :recipt_edrpou, :payer_mfo, :payer_name);"""
    try:
        c.executemany(qry, ({k: d.get(k, values[k]) for k in values}
                      for d in edata))
    except:
        raise
    db.commit()


def fetch(qry_dict, output_format=None, ascii=False, indent=False):
    EDATA_URL = "http://api.e-data.gov.ua:8080/api/rest/1.0/transactions"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:47.0) "
        "Gecko/20100101 Firefox/47.0",
        "Accept": "application/json,text/html,application/xhtml+xml,"
        "application/xml;q=0.9,*/*;q=0.8",
        'Content-Type': 'application/json',
        }

    try:
        r = requests.post(
            EDATA_URL,
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
    except NoDataReturnError:
        sys.exit(0)
    except EDataSystemError as e:
        print(e.message)
        sys.exit(1)
    except:
        raise
    else:
        if output_format == '0x2':    # json
            with open('edata.json', 'w', encoding='utf-8') as f:
                json.dump(edata_json, f, ensure_ascii=ascii, indent=indent)
        elif output_format == '0x4':  # csv
            write_csv(edata_json['response']['transactions'])
        elif output_format == '0x8':  # csv
            make_sqlite(edata_json['response']['transactions'])


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


def main():
    results = arg_parser.parse_args()
    # format constants:
    # json   - 0x2
    # csv    - 0x4
    # sqlite - 0x8
    if results.json:
        format_ = '0x2'
    elif results.csv:
        format_ = '0x4'
    elif results.sqlite:
        format_ = '0x8'

    if (not results.json) and results.indent:
        results.indent = False
        print('Параметр -i/--indent проігноровано.\n')

    if not (results.payers or results.receipts):
        raise NoOutputFormatSpecifiedError
        sys.exit(2)
    startdate = checkdate(results.startdate) if results.startdate else None
    enddate = checkdate(results.enddate) if results.enddate else None
    if startdate and enddate:
        try:
            check_date_order(startdate, enddate)
        except DateOrderViolation:
            startdate, enddate = enddate, startdate
    if results.treasury:
        try:
            if not set(results.treasury).issubset(TREASURY):
                raise WrongTreasuryInList
        except WrongTreasuryInList:
            sys.exit(1)
        finally:
            treasury = results.treasury
    else:
        treasury = results.treasury

    qry = compose_data_dict(startdate=startdate,
                            recipt_edrpous=results.receipts,
                            payers_edrpous=results.payers, enddate=enddate,
                            regions=treasury)
    fetch(qry, output_format=format_, ascii=results.ascii,
          indent=results.indent)


if __name__ == '__main__':
    main()
