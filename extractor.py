#!/usr/bin/env python
# -*- coding: utf-8 -*-

import edata
from collections import namedtuple
import argparse
from datetime import timedelta, date, datetime
import time
from pathlib import Path
import os.path


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

def extract(start_date, end_date):
    for single_date in daterange(start_date, end_date):
        weekday, tr_date = single_date
        if weekday >= 5:
            continue
        # print(tr_date)
        results = Namespace(ascii=False, csv=False,
                        startdate=tr_date, 
                        enddate=tr_date,
                        indent=0, json=False, keep_json=False, lastload=False,
                        payers=[], ping=False, receipts=[], sqlite=False,
                        subparser_name='transactions', top100=False, treasury=[],
                        verbose=False, zipname=os.path.join("data", tr_date))
        edata.transactions(results)
        time.sleep(1.5)


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('start_date', type=str,
                        help='початкова дата завантаження')
args = arg_parser.parse_args()

try:
    k = datetime.strptime(args.start_date, "%Y-%m-%d")
except ValueError as e:
    print(e.args[0],
          "Невірний формат дати, треба ISO 8601")
    sys.exit(1)
else:
    start_date = k.date()
    end_date = date.today()
    extract(start_date, end_date)


