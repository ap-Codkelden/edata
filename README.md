# Скрипти для обробки даних з порталу E-Data #
Набір із двох скриптів: `edata.py` та `json2sqlite.py`. Дозволяють отримувати та зберігати дані з Єдиного веб-порталу використання публічних коштів [Є-Data](http://spending.gov.ua/).

Можуть бути використані незалежно один від одного.

## edata.py ##
Скрипт дозволяє отримувати дані за запитами до API з Єдиного веб-порталу 
використання публічних коштів та зберігати їх у наступних форматах:

* CSV
* JSON
* XML

Оскільки скрипт написано на Python, він працюватиме під усіма [операційними системами](https://www.python.org/downloads/operating-systems/), для яких реалізовано інтерпретатор, з деякими обмеженнями (див. [Вимоги](#Вимоги)).

Щодо особливостей запуску під окремими операційними системами зверніться до 
відповідної документації мови Python для цих систем:

* [Windows](https://docs.python.org/3.5/using/windows.html)
* [Mas OS X](https://docs.python.org/3.5/using/mac.html)

### Вимоги ###
Скрипт використовує бібліотеку Requests ([сторінка](https://github.com/kennethreitz/requests) на GitHub, [сторінка](https://pypi.python.org/pypi/requests) у каталозі PyPi, [документація](http://docs.python-requests.org/en/master/)).

Більш досвідченим користувачам можливо буде зручніше використовувати 
[віртуальне оточення](http://docs.python-guide.org/en/latest/dev/virtualenvs/).
#### До уваги користувачів Windows
Для роботи скриптів `edata.py` та `json2sqlite.py` без додаткових налаштувань потрібен Python версії не нижче 3.5. У  випадку використання Windows, [останньою версією](https://docs.python.org/3.5/using/windows.html#supported-versions), під якою працюватиме Python 3.5, є Windows 7 SP1.
### Отримання та встановлення ###

Отримати скрипт можна кількома шляхами:

* завантажити останню версію за [прямим посиланням](https://raw.githubusercontent.com/ap-Codkelden/edata/master/edata.py)
* завантажити [ZIP-архів](https://github.com/ap-Codkelden/edata/archive/master.zip) репозиторія 
* [клонувати](https://git-scm.com/book/it/v2/Git-Basics-Getting-a-Git-Repository#Cloning-an-Existing-Repository) даний репозиторій

Встановлення скрипти не потребують, головне — запам'ятати директорію, у яку буде 
збережено файли `edata.py` та `json2sqlite.py`.

### Запуск ###

Особливості запуску залежать від використовуваної операційної системи, тому у 
випадку, якщо ви не знаєте, як краще це зробити, радимо звернутися до 
відповідної документації вище.

Якщо файл скрипту не є виконуваним, запуск відбувається стандартним для мови 
Python шляхом:

```python
python edata.py [параметри]
```

#### Параметри командного рядка ####

Отримати коротку довідку можна, запустивши скрипт без параметрів, або з 
параметром `-h` (або  `--help`):

```python
python edata.py [-h] | [--help]
```
*Хоча скрипт використовує як короткі опції командного рядка, так і відповідні 
їм довгі, далі у прикладах будуть використовуватися короткі.*

##### Обов'язкові #####

Обов'язковими параметрами запуску є наявність формату виводу та коди 
платників і отримувачів:

* `-p`, `--payers` - перелік кодів ЄДРПОУ **відправників** платежів, вказаних через пробіл
* `-r`, `--receipts` - перелік кодів ЄДРПОУ **отримувачів** платежів, вказаних через пробіл
* `-j`, `--json` для JSON з ім'ям `edata.json`
* `-c`, `--csv` для CSV з ім'ям `edata.csv`
* `-sql`, `--sqlite` для зберігання даних у [базу даних SQLite](https://en.wikipedia.org/wiki/SQLite) `edata.sqlite`, що дозволятиме згодом виконувати запити SQL

При зберіганні файлів JSON та CSV файли з такими назвами, що існують, буде 
перезаписано, а при зберіганні у базу даних SQLite буде або створено нову 
базу даних (якщо її не існує), або буде додано у існуючу.

Коди отримувачів *або* платників (параметри командного рядка `-r` або `-p`) 
вказуються через пробіл:

```python
python edata.py -j -p 00130850 13648033
```
або

```python
python edata.py -r 23359034 20077720
```

##### Опціональні #####

###### Дати транзакцій ######

Відповідно до [документації](http://www.minfin.gov.ua/uploads/redactor/files/e-data-API.pdf) 
API, можливо вказувати початкову та кінцеву дату пошуку (як разом, так і 
окремо) транзакцій (параметри `-s`, `--startdate` та `-e`, `--enddate` 
відповідно). 

Дати можливо вказувати у двох форматах:

* `dd.mm.yyyy`
* `dd-mm-yyyy`

```python
python edata.py -p 00130850 13648033 -j -s 01.02.2015 -e 03-10-2015
python edata.py -p 00130850 13648033 -s 01.02.2015
```

У випадку, якщо діапазон дат не вказано, система самостійно визначає діапазон 
дат. Так, при запуску скрипта 16 жовтня 2016 року транзакції повернуто за 
період з 15.09.2015 по 16.10.2016.

Якщо користувачем буде сказано як початкову, так і кінцеву дату і при цьому 
кінцева дата виявиться меншою за початкову, в такому випадку дати буде
поміняно місцями.

###### Регіональні казначейства ######

Параметр `-t`, `--treasury` дозволяє вказати перелік регіональних 
казначейств, для яких буде виконуватись пошук транзакцій. Коди обласних 
казначейств відповідають [кодам областей України](https://docs.google.com/spreadsheets/d/1tRlvK6Kjuds1y3WzZSXzAUXfxSw6VRAuMhLpbi83Y-8/edit?usp=sharing) 
і співпадають з кодами головних управлінь ДФС в цих  областях 
(не плутати з **K_DFM11**). 

```python
python edata.py -p 00130850 13648033 -j -t 2 9 
```

###### Форматування JSON-файлу ######

Параметр `-i`, `--indent` дозволяє вказати кількість пробілів, що буде 
використана для відступів у JSON-файлі. 

За замочуванням дорівнює `0`.

###### Перетворення дати у зручний формат ######

Дати транзакцій вивантажуються із Є-Data у форматі [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) 
datetime. 

При зберіганні дата конвертується у формат ISO 8601 date `YYYY-MM-DD`.

За допомогою параметру `-iso`, `--iso8601` можливо залишити дату транзакції у 
форматі ISO 8601 datetime.

###### Екранізація не-ASCII символів (у JSON-файлі) ######

Параметр `-a`, `--ascii` дозволяє вивести JSON у ASCII-сумісний файл, в цьому 
випадку усі не-ASCII символи буде замінено на послідовності, визначені у 
розділі 7 «Strings» [RFC 7159](https://tools.ietf.org/html/rfc7159#section-7).

Використовуйте цей параметр лише тоді, коли точно знаєте, що робите!

## json2sqlite.py ##

Скрипт виконує єдину функцію — імпортує дані з JSON-файлів, що були вивантажені з 
порталу Є-Data, до бази даних SQLite. Якщо ім'я файлу бази даних не вказано, 
за замовчуванням буде використано  ім'я `edata.sqlite`.

Якщо такої бази в поточній директорії не існує, вона буде створена. Якщо ж 
база даних існує, дані буде додано в кінець таблиці. 

При додаванні перевіряється унікальність записів по полю `id` транзакції, у 
випадку конфліктів запис буде перезаписано.

### Опції ###
Скрипт не має обов'язкових опцій.

Якщо просто виконати його, наприклад:

```python
$ python json2sqlite.py
```
то скрипт спробує обробити всі файли із розширенням `*.json`, що знаходяться 
в одній з ним директорії.

#### JSON-файли
За допомогою опції `-f`, `--file` можна вказувати через пробіл імена 
JSON-файлів, які потрібно обробити.
#### Файл бази даних SQLite
Опція `-d`, `--database` дозволяє вказувати інше, ніж `edata.sqlite` ім'я 
бази даних, у яку буде додано записи. 

Ім'я потрібно вказувати *без* розширення `.sqlite`. Якщо ж розширення все 
одно буде вказано, його буде видалено.
#### Приклад виклику
```python
$ python json2sqlite.py -d mysqlite -f file1.json file2.json
```
Імпортує дані із файлів `file1.json` та `file2.json` у базу даних SQLite 
`mysqlite.sqlite`.
### TODO ###
#### edata.py ####
- [x] конвертувати ISO 8601 datetime у ISO 8601 date
- [ ] додати завантаження переліку ЄДРПОУ із файлів
- [ ] з'ясувати механізм роботи та додати обробку параметра `lastload`, який повертає дату повного завантаження транзакцій (які іноді додаються частинами протягом дня)
- [ ] реалізувати можливість зберігання файлів JSON та CSV під довільними іменами
- [ ] реалізувати можливість стиснення файлів у `bzip`, `gzip` або інший архівний формат

### Помилки ###

У разі виявлення помилок або бажання нових фіч вітається [відкриття](https://github.com/ap-Codkelden/edata/issues/new) issues.

