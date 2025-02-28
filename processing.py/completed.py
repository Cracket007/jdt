import pandas as pd
import json
import os
import logging

async def load_mappings():
    """Загружает маппинги из JSON файла."""
    try:
        with open('config/mappings.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Ошибка при загрузке маппингов: {e}")
        # Возвращаем пустые словари в случае ошибки
        return {
            "debit_mapping": {},
            "credit_mapping": {},
            "special_accounts": {
                "reseller_fee": "207001",
                "net_fee_wire_transfer": "420001",
                "net_fee_cc_apm": "420002",
                "additional_fee": "420003"
            }
        }

async def format_date(date_str):
    """Преобразует дату в формат yyyymmdd."""
    try:
        # Для формата dd/mm/yyyy HH:MM:SS
        return pd.to_datetime(date_str, format='%d/%m/%Y %H:%M:%S').strftime('%Y%m%d')
    except:
        try:
            # Для других форматов с явным указанием dayfirst=True
            return pd.to_datetime(date_str, dayfirst=True).strftime('%Y%m%d')
        except:
            return date_str

async def get_column_value(row, possible_names, default=0):
    """Получает значение колонки из нескольких возможных имен."""
    for name in possible_names:
        if name in row and pd.notna(row[name]):
            return row[name]
    return default

async def process_jdt(input_file, output_file):
    """Обрабатывает COMPLETED файл и создает JDT отчет."""
    try:
        logging.info(f"Начинаем обработку COMPLETED JDT отчета из файла {input_file}")

        # Загружаем маппинги
        mappings = load_mappings()
        DEBIT_MAPPING = mappings["debit_mapping"]
        SPECIAL_ACCOUNTS = mappings["special_accounts"]

        # Читаем входной файл
        df = pd.read_csv(input_file)
        logging.info(f"Прочитан файл с {len(df)} строками")

        # Очищаем названия колонок от лишних пробелов
        df.columns = df.columns.str.strip()

        # Проверяем существование шаблона
        template_path = 'templates/jdt_completed.csv'
        if not os.path.exists(template_path):
            raise FileNotFoundError(f"Шаблон не найден: {template_path}")

        # Читаем шаблон
        template_df = pd.read_csv(template_path, nrows=1)

        # Создаем списки для строк
        debit_rows = []
        reseller_rows = []
        net_fee_rows = []
        additional_fee_debit_rows = []  # Дебет Additional Fee
        additional_fee_credit_rows = []  # Кредит Additional Fee

        # Определяем имена колонок (с учетом возможных вариантов)
        reseller_fee_columns = ['Reseller Fee EUR', 'Reseller\nFee EUR']
        net_fee_columns = ['Net Fee EUR', 'Net\nFee EUR']
        additional_fee_columns = ['Additional Fee', 'Additionall Fee']

        # Обрабатываем каждую транзакцию
        for index, row in df.iterrows():
            # Форматируем дату
            formatted_date = await format_date(row['Completed'])

            # Получаем значения из колонок с учетом возможных вариантов имен
            reseller_fee = get_column_value(row, reseller_fee_columns)
            net_fee = get_column_value(row, net_fee_columns)

            # Дебетовая запись - используем Payment Provider
            debit_row = {
                'ParentKey': index + 1,
                'JdtNum': index + 1,
                'LineNum': '',
                'Debit': row['Total Fee EUR'],
                'Credit': '',
                'DueDate': formatted_date,
                'ShortName': DEBIT_MAPPING.get(row['Payment Provider'], row['Payment Provider']),
                'Reference1': row['Name'],
                'Reference2': row['Order'],
                'TaxDate': formatted_date
            }
            debit_rows.append(debit_row)

            # Reseller Fee - фиксированное значение из маппингов
            reseller_row = {
                'ParentKey': index + 1,
                'JdtNum': index + 1,
                'LineNum': '1',
                'Debit': '',
                'Credit': reseller_fee,
                'DueDate': formatted_date,
                'ShortName': SPECIAL_ACCOUNTS["reseller_fee"],
                'Reference1': row['Name'],
                'Reference2': row['Order'],
                'TaxDate': formatted_date
            }
            reseller_rows.append(reseller_row)

            # Net Fee - определяем ShortName в зависимости от провайдера
            short_name = SPECIAL_ACCOUNTS["net_fee_wire_transfer"] if row['Payment Provider'] == 'wire_transfer' else SPECIAL_ACCOUNTS["net_fee_cc_apm"]

            net_fee_row = {
                'ParentKey': index + 1,
                'JdtNum': index + 1,
                'LineNum': '2',
                'Debit': '',
                'Credit': net_fee,
                'DueDate': formatted_date,
                'ShortName': short_name,  # Используем определенное значение
                'Reference1': row['Name'],
                'Reference2': row['Order'],
                'TaxDate': formatted_date
            }
            net_fee_rows.append(net_fee_row)

        # Находим максимальный ParentKey из основных записей
        max_key = len(df)  # Просто количество транзакций

        # Additional Fee
        for index, row in df.iterrows():
            additional_fee = get_column_value(row, additional_fee_columns)

            if additional_fee and float(additional_fee) != 0:
                formatted_date = await format_date(row['Completed'])
                max_key += 1
                # Additional Fee дебет - используем DEBIT_MAPPING по значению из Payment Provider
                add_fee_debit = {
                    'ParentKey': max_key,
                    'JdtNum': max_key,
                    'LineNum': '',
                    'Debit': additional_fee,
                    'Credit': '',
                    'DueDate': formatted_date,
                    'ShortName': DEBIT_MAPPING.get(row['Payment Provider'], row['Payment Provider']),
                    'Reference1': row['Name'],
                    'Reference2': row['Order'],
                    'TaxDate': formatted_date
                }
                additional_fee_debit_rows.append(add_fee_debit)

                # Additional Fee кредит - фиксированное значение из маппингов
                add_fee_credit = {
                    'ParentKey': max_key,
                    'JdtNum': max_key,
                    'LineNum': '1',
                    'Debit': '',
                    'Credit': additional_fee,
                    'DueDate': formatted_date,
                    'ShortName': SPECIAL_ACCOUNTS["additional_fee"],
                    'Reference1': row['Name'],
                    'Reference2': row['Order'],
                    'TaxDate': formatted_date
                }
                additional_fee_credit_rows.append(add_fee_credit)

        # Объединяем все строки в нужном порядке
        result_rows = (debit_rows + reseller_rows + net_fee_rows +
                      additional_fee_debit_rows + additional_fee_credit_rows)

        # Создаем DataFrame из результата
        result_df = pd.DataFrame(result_rows)

        # Создаем пустой DataFrame с колонками из шаблона
        final_df = pd.DataFrame(columns=template_df.columns)

        # Копируем данные
        for col in result_df.columns:
            if col in final_df.columns:
                final_df[col] = result_df[col]

        # Объединяем заголовки и данные
        final_df = pd.concat([template_df, final_df], ignore_index=True)

        # Сохраняем результат
        final_df.to_csv(output_file, index=False)
        logging.info(f"COMPLETED JDT отчет успешно сохранен в {output_file}")

        return output_file
    except Exception as e:
        logging.error(f"Ошибка при обработке COMPLETED JDT отчета: {e}", exc_info=True)
        raise

async def process_ojdt(input_file, output_file):
    """
    Обрабатывает COMPLETED файл и создает OJDT отчет.
    """
    try:
        logging.info(f"Начинаем обработку COMPLETED OJDT отчета из файла {input_file}")

        # Читаем исходный файл - используем переданный параметр input_file
        df = pd.read_csv(input_file)
        logging.info(f"Прочитан файл с {len(df)} строками")

        # Очищаем названия колонок от лишних пробелов
        df.columns = df.columns.str.strip()

        # Проверяем существование шаблона
        template_path = 'templates/ojdt_completed.csv'
        if not os.path.exists(template_path):
            raise FileNotFoundError(f"Шаблон не найден: {template_path}")

        # Читаем шаблон completed
        template_df = pd.read_csv(template_path, nrows=1)

        # Создаем список для строк результата
        result_rows = []

        # Проходим по всем строкам исходного файла
        for index, row in df.iterrows():
            formatted_date = await format_date(row['Completed'])

            # Основная запись
            ojdt_row = {
                'JdtNum': index + 1,
                'ReferenceDate': formatted_date,
                'Reference': row['Name'],
                'Reference2': row['Order'],
                'TaxDate': formatted_date,
                'DueDate': formatted_date
            }
            result_rows.append(ojdt_row)

        # Создаем DataFrame с данными
        data_df = pd.DataFrame(result_rows, columns=template_df.columns)

        # Объединяем заголовки и данные
        final_df = pd.concat([template_df, data_df], ignore_index=True)

        # Сохраняем результат
        final_df.to_csv(output_file, index=False)
        logging.info(f"COMPLETED OJDT отчет успешно сохранен в {output_file}")

        return output_file
    except Exception as e:
        logging.error(f"Ошибка при обработке COMPLETED OJDT отчета: {e}", exc_info=True)
        raise