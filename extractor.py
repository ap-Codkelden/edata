#!/usr/bin/env python
# -*- coding: utf-8 -*-


import edata
import sys
import argparse
import time
import os.path
from collections import namedtuple
from datetime import timedelta, date, datetime
from pathlib import Path


Namespace = namedtuple('Namespace', "csv,indent,json,keep_json,lastload,"
    "payers,ping,receipts,sqlite,startdate,enddate,subparser_name,top100,"
    "treasury,verbose,zipname,ascii")

Path("data").mkdir(parents=True, exist_ok=True)
start_date = end_date = None

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        k = start_date + timedelta(n)
        isodate = k.isoformat()
        if k.day == 1:
            print(isodate)
        yield k.weekday(), isodate

def extract(start_date, end_date, verbose=None):
    for single_date in daterange(start_date, end_date):
        weekday, tr_date = single_date
        # if weekday >= 5:
        #     continue
        if verbose:
            print(tr_date)
        results = Namespace(
            ascii=False, csv=False,
            startdate=tr_date,
            enddate=tr_date,
            indent=0, json=False, keep_json=False, lastload=False,
            payers=[], ping=False, receipts=[], sqlite=False,
            subparser_name='transactions', top100=False, treasury=[],
            verbose=False, zipname=os.path.join("data", tr_date))
        edata.transactions(results)
        time.sleep(1.5)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('start_date', type=str,
                            help='початкова дата завантаження')
    arg_parser.add_argument('-ed', type=str,
                            help='кінцева дата завантаження. Якщо не вказана, '
                            'береться сьогодні')
    arg_parser.add_argument('-v', '--verbose', action="store_true",
                            help='вивід дат')
    args = arg_parser.parse_args()
    # print(args)
    try:
        k1 = datetime.strptime(args.start_date, "%Y-%m-%d")
    except ValueError as e:
        print(e.args[0],
              "Невірний формат дати, має бути ISO 8601")
        sys.exit(1)
    else:
        start_date = k1.date()
        end_date = date.today() if args.ed is None else datetime.strptime(
            args.ed, "%Y-%m-%d").date() + timedelta(days=1)
        extract(start_date, end_date, verbose=args.verbose)
