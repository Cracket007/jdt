import pandas as pd
import os
import logging
from config.mappings import DEBIT_MAPPING_PAYD, CREDIT_MAPPING_PEYD


async def format_date(date_str):
    """Преобразует дату в формат yyyymmdd.
    Поддерживает несколько форматов ввода."""
    try:
        # Для формата dd/mm/yyyy
        return pd.to_datetime(date_str, format='%d/%m/%Y').strftime('%Y%m%d')
    except:
        try:
            # Для других форматов с явным указанием dayfirst=True
            return pd.to_datetime(date_str, dayfirst=True).strftime('%Y%m%d')
        except:
            return date_str

async def process_jdt(input_file, output_file):
    """Обрабатывает PAYD файл и создает JDT отчет."""
    try:
        logging.info(f"Начинаем обработку JDT отчета из файла {input_file}")


        # Читаем исходный файл
        df = pd.read_csv(input_file, encoding='utf-8-sig')

        # Очищаем названия колонок от лишних пробелов
        df.columns = df.columns.str.strip()

        # Проверяем существование шаблона
        template_path = 'templates/jdt_template.csv'
        if not os.path.exists(template_path):
            raise FileNotFoundError(f"Шаблон не найден: {template_path}")

        # Читаем шаблон и сохраняем только первую строку
        template_df = pd.read_csv(template_path, nrows=1)

        # Создаем пустой список для строк результата
        debit_rows = []
        credit_rows = []

        # Создаем debit строки с их нумерацией
        for index, row in df.iterrows():
            # Форматируем дату
            formatted_date = await format_date(row['Paid'])

            # Дебетовая запись
            debit_row = {
                'ParentKey': index + 1,
                'JdtNum': index + 1,
                'LineNum': '',
                'Debit': row['Fval EUR'],
                'Credit': '',
                'DueDate': formatted_date,
                'ShortName': DEBIT_MAPPING_PAYD.get(row['Payment Provider'], row['Payment Provider']),
                'ReferenceDate1': formatted_date,
                'Reference1': row['Name'],
                'Reference2': row['Order'],
                'TaxDate': formatted_date
            }
            debit_rows.append(debit_row)

            # Кредитовая запись
            credit_row = {
                'ParentKey': index + 1,
                'JdtNum': index + 1,
                'LineNum': '1',
                'Debit': '',
                'Credit': row['Fval EUR'],
                'DueDate': formatted_date,
                'ShortName': CREDIT_MAPPING_PEYD.get(row['Payment Provider'], row['Payment Provider']),
                'ReferenceDate1': formatted_date,
                'Reference1': row['Name'],
                'Reference2': row['Order'],
                'TaxDate': formatted_date
            }
            credit_rows.append(credit_row)

        # Объединяем списки
        result_rows = debit_rows + credit_rows

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
        logging.info(f"JDT отчет успешно сохранен в {output_file}")

        return output_file
    except Exception as e:
        logging.error(f"Ошибка при обработке JDT отчета: {e}", exc_info=True)
        raise

async def process_ojdt(input_file, output_file):
    """
    Обрабатывает PAYD файл и создает OJDT отчет.
    """
    try:
        logging.info(f"Начинаем обработку OJDT отчета из файла {input_file}")

        # Читаем исходный файл
        df = pd.read_csv(input_file)
        logging.info(f"Прочитан файл с {len(df)} строками")

        # Очищаем названия колонок от лишних пробелов
        df.columns = df.columns.str.strip()

        # Проверяем существование шаблона
        template_path = 'templates/ojdt_template.csv'
        if not os.path.exists(template_path):
            raise FileNotFoundError(f"Шаблон не найден: {template_path}")

        # Читаем шаблон и сохраняем только первую строку
        template_df = pd.read_csv(template_path, nrows=1)

        # Создаем список для строк результата
        result_rows = []

        # Проходим по всем строкам исходного файла
        for index, row in df.iterrows():
            formatted_date = await format_date(row['Paid'])

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
        logging.info(f"OJDT отчет успешно сохранен в {output_file}")

        return output_file
    except Exception as e:
        logging.error(f"Ошибка при обработке OJDT отчета: {e}", exc_info=True)
        raise