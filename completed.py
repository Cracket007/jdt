import pandas as pd

# Словари для маппинга провайдеров на их номера (могут отличаться от payd)
DEBIT_MAPPING = {
    'astropay': '210009',
    'Eupago': '210011',
    'Fibo PNC': '210002',
    'Intergiro': '210012',
    'Paysafe': '210007',
    'PNC': '210002',
    'PNC VISA': '210002',
    'RPD': '210013',
    'TRU': '210006',
    'Truevo PayOn': '210008',
    'volt': '210005',
    'voltx': '210005',
    'wire_transfer': '210001'
}

async def format_date(date_str):
    """
    Преобразует дату в формат yyyymmdd
    """
    try:
        # Для формата dd/mm/yyyy HH:MM:SS
        return pd.to_datetime(date_str, format='%d/%m/%Y %H:%M:%S').strftime('%Y%m%d')
    except:
        try:
            # Для других форматов с явным указанием dayfirst=True
            return pd.to_datetime(date_str, dayfirst=True).strftime('%Y%m%d')
        except:
            return date_str

async def process_jdt(output_file):
    """
    Обрабатывает completed файл и создает JDT отчет с группами строк:
    1. Все дебетовые записи
    2. Все кредитовые записи Reseller Fee
    3. Все кредитовые записи Net Fee
    4. Дебетовые записи Additional Fee
    5. Кредитовые записи Additional Fee
    """
    # Читаем входной файл
    df = pd.read_csv('temp/исходник (Completed).csv')

    # Читаем шаблон
    template_df = pd.read_csv('templates/jdt_completed.csv', nrows=1)

    # Создаем списки для строк
    debit_rows = []
    reseller_rows = []
    net_fee_rows = []
    additional_fee_debit_rows = []  # Дебет Additional Fee
    additional_fee_credit_rows = []  # Кредит Additional Fee

    # Обрабатываем каждую транзакцию
    for index, row in df.iterrows():
        # Форматируем дату
        formatted_date = await format_date(row['Completed'])

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

        # Reseller Fee - фиксированное значение
        reseller_row = {
            'ParentKey': index + 1,
            'JdtNum': index + 1,
            'LineNum': '1',
            'Debit': '',
            'Credit': row['Reseller\nFee EUR'],
            'DueDate': formatted_date,
            'ShortName': '207001',
            'Reference1': row['Name'],
            'Reference2': row['Order'],
            'TaxDate': formatted_date
        }
        reseller_rows.append(reseller_row)

        # Net Fee - определяем ShortName в зависимости от провайдера
        short_name = '420001' if row['Payment Provider'] == 'wire_transfer' else '420002'

        net_fee_row = {
            'ParentKey': index + 1,
            'JdtNum': index + 1,
            'LineNum': '2',
            'Debit': '',
            'Credit': row['Net\nFee EUR'],
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
        if pd.notna(row.get('Additionall Fee', 0)) and float(row['Additionall Fee']) != 0:
            formatted_date = await format_date(row['Completed'])
            max_key += 1
            # Additional Fee дебет - используем DEBIT_MAPPING по значению из Payment Provider
            add_fee_debit = {
                'ParentKey': max_key,
                'JdtNum': max_key,
                'LineNum': '',
                'Debit': row['Additionall Fee'],
                'Credit': '',
                'DueDate': formatted_date,
                'ShortName': DEBIT_MAPPING.get(row['Payment Provider'], row['Payment Provider']),
                'Reference1': row['Name'],
                'Reference2': row['Order'],
                'TaxDate': formatted_date
            }
            additional_fee_debit_rows.append(add_fee_debit)

            # Additional Fee кредит - фиксированное значение 420003
            add_fee_credit = {
                'ParentKey': max_key,
                'JdtNum': max_key,
                'LineNum': '1',
                'Debit': '',
                'Credit': row['Additionall Fee'],
                'DueDate': formatted_date,
                'ShortName': '420003',  # Фиксированное значение для Additional Fee кредит
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

async def process_ojdt(output_file):
    """
    Обработка OJDT для completed отчета
    """
    # Читаем исходный файл
    df = pd.read_csv('temp/исходник (Completed).csv')

    # Читаем шаблон completed
    template_df = pd.read_csv('templates/ojdt_completed.csv', nrows=1)

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