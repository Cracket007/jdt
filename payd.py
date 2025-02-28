import pandas as pd

# Словари для маппинга провайдеров на их номера
DEBIT_MAPPING = {
    'astropay': '141009',
    'Eupago': '141011',
    'Fibo PNC': '141002',
    'Intergiro': '141012',
    'Paysafe': '141007',
    'PNC': '141002',
    'PNC VISA': '141002',
    'RPD': '141013',
    'TRU': '141006',
    'Truevo PayOn': '141006',
    'volt': '141005',
    'voltx': '141005',
    'wire_transfer': '141001'
}

CREDIT_MAPPING = {
    'astropay': '210009',
    'Eupago': '210011',
    'Fibo PNC': '210002',
    'Intergiro': '210012',
    'Paysafe': '210007',
    'PNC': '210002',
    'PNC VISA': '210002',
    'RPD': '210013',
    'TRU': '210006',
    'Truevo PayOn': '210006',
    'volt': '210005',
    'voltx': '210005',
    'wire_transfer': '210001'
}

async def format_date(date_str):
    """
    Преобразует дату в формат yyyymmdd
    """
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
    """
    Обрабатывает payd файл и создает JDT отчет
    """
    # Читаем исходный файл
    df = pd.read_csv(input_file)

    # Очищаем названия колонок от лишних пробелов
    df.columns = df.columns.str.strip()

    # Читаем шаблон и сохраняем только первую строку
    template_df = pd.read_csv('templates/jdt_template.csv', nrows=1)

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
            'Debit': row['Total Fee EUR'],
            'Credit': '',
            'DueDate': formatted_date,
            'ShortName': DEBIT_MAPPING.get(row['Payment Method'], row['Payment Method']),
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
            'Credit': row['Total Fee EUR'],
            'DueDate': formatted_date,
            'ShortName': CREDIT_MAPPING.get(row['Payment Method'], row['Payment Method']),
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

async def process_ojdt(input_file, output_file):
    """
    Обработка OJDT для payd отчета
    """
    # Читаем исходный файл
    df = pd.read_csv(input_file)

    # Очищаем названия колонок от лишних пробелов
    df.columns = df.columns.str.strip()

    # Читаем шаблон и сохраняем только первую строку
    template_df = pd.read_csv('templates/ojdt_template.csv', nrows=1)

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