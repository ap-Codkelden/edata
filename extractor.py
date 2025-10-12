#!/usr/bin/env python
# -*- coding: utf-8 -*-


from .edata import transactions
import sys
import argparse
import time
import os.path
import tempfile
from collections import namedtuple
from calendar import monthrange
from datetime import timedelta, date, datetime
from pathlib import Path
from typing import Optional


Namespace = namedtuple('Namespace', "csv,indent,json,keep_json,lastload,"
    "payers,ping,receipts,sqlite,startdate,enddate,subparser_name,top100,"
    "treasury,verbose,zipname,ascii")

save_dir_name: str = "data"
start_date = end_date = None

try:
    save_dir: Path = Path(tempfile.gettempdir()) / save_dir_name
    save_dir.mkdir(parents=True, exist_ok=True)
except Exception as e:
    save_dir = Path(save_dir_name)
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    print("Не вдається створити директорію для збереження даних у \n"
          "тимчасовій директорії. Створено у поточній.\n")


def daterange(start_date: date, end_date: date):
    for n in range((end_date - start_date).days + 1):
        d = start_date + timedelta(days=n)
        yield d.weekday(), d.isoformat()


def extract(start_date: date, end_date: date, verbose: Optional[bool]=None):
    for single_date in daterange(start_date, end_date):
        weekday, tr_date = single_date
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
        transactions(results)
        time.sleep(1.5)


def last_day_date(d: date):
    y, m = d.year, d.month
    last_day = monthrange(y, m)[1]
    return date(y, m, last_day) + timedelta(days=1)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('start_date', type=str,
                            help='початкова дата завантаження')
    arg_parser.add_argument('-ed', type=str,
                            help='кінцева дата завантаження. Якщо не вказана, '
                            'береться останній день місяця `start_date`')
    arg_parser.add_argument('-v', '--verbose', action="store_true",
                            help='вивід дат')
    args = arg_parser.parse_args()
    # print(args)
    try:
        start_date= datetime.strptime(args.start_date, r"%Y-%m-%d").date()
        if args.ed is None:
            end_date = last_day_date(start_date) 
        else:
            end_date = datetime.strptime(args.ed, r"%Y-%m-%d").date()
    except ValueError as e:
        print(e.args[0], "Невірний формат дати, має бути ISO 8601")
        sys.exit(1)
    else:
        extract(start_date, end_date, verbose=args.verbose)
