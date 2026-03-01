#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np
pd.options.display.max_columns = 32
import time
from os import scandir


cur_cat = pd.CategoricalDtype(
    [
    'UAH', 'USD', 'EUR', 'GBP', 'JPY', 'CNY', 'RUB', 'AED', 'AFN', 'ALL', 'AMD', 'ANG', 'ARS', 
    'AUD', 'AWG', 'AZN', 'BAM', 'BBD', 'BDT', 'BGN', 'BHD', 'BIF', 'BMD', 'BND', 'BOB', 'BOV', 
    'BRL', 'BSD', 'BTN', 'BWP', 'BYR', 'BZD', 'CAD', 'CDF', 'CHE', 'CHF', 'CHW', 'WIR', 'CLF', 
    'CLP', 'COP', 'COU', 'CRC', 'CUC', 'CUP', 'CVE', 'CZK', 'DJF', 'DKK', 'DOP', 'DZD', 'EGP', 
    'ERN', 'ETB', 'FJD', 'FKP', 'GEL', 'GHS', 'GIP', 'GMD', 'GNF', 'GTQ', 'GYD', 'HKD', 'HNL', 
    'HRK', 'HTG', 'HUF', 'IDR', 'ILS', 'INR', 'IQD', 'IRR', 'ISK', 'JMD', 'JOD', 'KES', 'KGS', 
    'KHR', 'KMF', 'KPW', 'KRW', 'KWD', 'KYD', 'KZT', 'LAK', 'LBP', 'LKR', 'LRD', 'LSL', 'LYD', 
    'MKD', 'MMK', 'MNT', 'MOP', 'MRO', 'MUR', 'MVR', 'MWK', 'MXN', 'MXV', 'MYR', 'MZN', 'NAD', 
    'NGN', 'NIO', 'NOK', 'NPR', 'NZD', 'OMR', 'PAB', 'PEN', 'PGK', 'PHP', 'PKR', 'PLN', 'PYG', 
    'QAR', 'RON', 'RSD', 'RWF', 'SAR', 'SBD', 'SCR', 'SDG', 'SEK', 'SGD', 'SHP', 'SLL', 'SOS', 
    'SRD', 'SSP', 'STD', 'SVC', 'SYP', 'SZL', 'THB', 'TJS', 'TMT', 'TND', 'TOP', 'TRY', 'TTD', 
    'TWD', 'TZS', 'UGX', 'UYU', 'UZS', 'VEF', 'VND', 'VUV', 'WST', 'XAF', 'MAD', 'MDL', 'MGA', 
    'CFA', 'BEA', 'XCD', 'XOF', 'BCE', 'XPF', 'YER', 'ZAR', 'ZMW', 'ZWL'
    ], ordered=False)

reg_cat = pd.CategoricalDtype(
    [x for x in range(1, 28)] + [99], ordered=False)

str_dict = {
    x: 'string' for x in [
        'doc_vob', 'doc_vob_name', 'doc_number', 'payer_edrpou', 'payer_name', 
        'payer_account', 'payer_mfo', 'payer_bank', 'recipt_edrpou', 'recipt_name', 
        'recipt_account', 'recipt_bank', 'recipt_mfo', 'payment_details', 
        'doc_add_attr', 'payment_type', 'payment_data', 'source_name',
        'kekv', 'kpk', 'contractId', 'contractNumber', 'budgetCode'
]}

dtype= str_dict | {
    'currency': cur_cat, 'region_id': reg_cat,
    'source_id': np.int32
}


def read_edata(csv):
    s = time.time()
    _ = pd.read_csv(csv, encoding="cp1251", sep=";", skiprows=1,
                    dtype=dtype,
                    low_memory=False
                   )
    print(time.time() - s)
    if 'doc_date' not in _.columns:
        print(_.columns)
        return 1
    return _


with scandir("data") as it:
    for i in it:
        print(i.path)
        if not i.path.endswith('csv.zip'):
            continue
        nm = i.path.split('/')[1].split('.')[0]
        _ = read_edata(i.path)
        if isinstance(_, int) and _ == 1:
            break
        if _.shape[1] != 32:
            print("!")
            continue
        for col in ['doc_date','doc_v_date','trans_date']:
            try:
                _[col] = pd.to_datetime(_[col])
            except:
                print(col)
                col1 = _[col].to_list()
        _.to_parquet(f"{nm}.parquet")

