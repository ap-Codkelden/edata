#!/usr/bin/env python

# Copyright (c) 2016-2025 Renat Nasridinov
# This software may be freely distributed under the MIT license.
# https://opensource.org/licenses/MIT The MIT License (MIT)
# or see LICENSE file

import sys


class EdataError(Exception):
    def __init__(self, message, error_code=None):
        super().__init__(message)
        self.error_code = error_code


class NoEDRPOUError(EdataError):
    def __init__(self):
        sys.stderr.write(
            'Не вказано ані відправників (параметр -p/--payers),'
            ' ані отримувачів (параметр -r/--receipts). Повинен бути вказаний'
            ' хоч один з них.\n'
            )


class OnlyOneOutputFormatIsAllowedError(EdataError):
    def __init__(self):
        sys.stderr.write(
            'Забагато вихідних форматів, має бути вказано лише один '
            'формат для зберігання.\n'
            )


class Top100WithEDRPOUError(EdataError):
    def __init__(self):
        sys.stderr.write(
            'Параметр --top100 не може використовуватися разом з кодами '
            'ЄДРПОУ отримувачів коштів та платників, ігноруємо…\n'
            )


class CannotFetchStatFileError(EdataError):
    def __init__(self):
        sys.stderr.write(
            'Не вдалося отримати файл статистики.\n'
            )


class OnlyLastLoadParameterIsAllowedError(EdataError):
    def __init__(self):
        sys.stderr.write(
            'Параметр `lastload` не призначений для використання разом з '
            'іншими параметрами.\n'
            )


class NoDataReturnError(EdataError):
    def __init__(self):
        sys.stderr.write('Системою Є-Data на запит не повернуто даних.\n')


class EDataSystemError(EdataError):
    def __init__(self, message):
        self.message = "Помилка API порталу Є-Data:\n" \
            "{}\n".format(message)


class ValueIsNotADateError(EdataError):
    def __init__(self, message):
        self.message = message


class DateOrderError(EdataError):
    def __init__(self):
        sys.stderr.write(
            'Початкова дата більша за кінцеву, дати буде поміняно '
            'місцями.\n'
            )


class DatesWithoutPayersError(EdataError):
    def __init__(self):
        sys.stderr.write(
            'Початкова та/або кінцева дата зазначені без кодів платників '
            'або отримувачів, параметри проігноровано.\n'
            )


class StatisticProcNeedsParameterError(EdataError):
    def __init__(self):
        sys.stderr.write(
            'Ця процедура потребує наявності одного з параметрів `--doc` або '
            '`--org`. Вкажіть потрібний і запустіть скрипт знову.\n'
            )


class WrongTreasuryInList(EdataError):
    def __init__(self):
        sys.stderr.write('Казначейства з даним кодом не існує.\n')
