import pandas as pd

# Словари для маппинга провайдеров на их номера (могут отличаться от payd)
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

def process_jdt(output_file):
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
        # Основные строки без изменений
        debit_row = {
            'ParentKey': index + 1,
            'JdtNum': index + 1,
            'LineNum': '',
            'Debit': row['Total Fee EUR'],
            'Credit': '',
            'DueDate': row['Completed'],
            'ShortName': row['Payment Provider'],
            'Reference1': row['Name'],
            'Reference2': row['Order'],
            'TaxDate': row['Completed']
        }
        debit_rows.append(debit_row)
        
        reseller_row = {
            'ParentKey': index + 1,
            'JdtNum': index + 1,
            'LineNum': '1',
            'Debit': '',
            'Credit': row['Reseller\nFee EUR'],
            'DueDate': row['Completed'],
            'ShortName': CREDIT_MAPPING.get(row['Payment Method'], row['Payment Method']),
            'Reference1': row['Name'],
            'Reference2': row['Order'],
            'TaxDate': row['Completed']
        }
        reseller_rows.append(reseller_row)
        
        net_fee_row = {
            'ParentKey': index + 1,
            'JdtNum': index + 1,
            'LineNum': '2',
            'Debit': '',
            'Credit': row['Net\nFee EUR'],
            'DueDate': row['Completed'],
            'ShortName': CREDIT_MAPPING.get(row['Payment Method'], row['Payment Method']),
            'Reference1': row['Name'],
            'Reference2': row['Order'],
            'TaxDate': row['Completed']
        }
        net_fee_rows.append(net_fee_row)
    
    # Находим максимальный ParentKey из основных записей
    max_key = len(df)  # Просто количество транзакций
    
    # Добавляем Additional Fee с продолжением нумерации
    for index, row in df.iterrows():
        if pd.notna(row.get('Additionall Fee', 0)) and float(row['Additionall Fee']) != 0:
            max_key += 1
            # Дебет Additional Fee
            add_fee_debit = {
                'ParentKey': max_key,
                'JdtNum': max_key,
                'LineNum': '',  # Пустой для дебета
                'Debit': row['Additionall Fee'],
                'Credit': '',
                'DueDate': row['Completed'],
                'ShortName': row['Payment Provider'],
                'Reference1': row['Name'],
                'Reference2': row['Order'],
                'TaxDate': row['Completed']
            }
            additional_fee_debit_rows.append(add_fee_debit)
            
            # Кредит Additional Fee
            add_fee_credit = {
                'ParentKey': max_key,
                'JdtNum': max_key,
                'LineNum': '1',  # 1 для кредита
                'Debit': '',
                'Credit': row['Additionall Fee'],  # То же значение в кредит
                'DueDate': row['Completed'],
                'ShortName': row['Payment Provider'],
                'Reference1': row['Name'],
                'Reference2': row['Order'],
                'TaxDate': row['Completed']
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

def process_ojdt(output_file):
    """
    Обработка OJDT для completed отчета
    """
    # Читаем исходный файл
    df = pd.read_csv('temp/исходник (Completed).csv')
    
    # Читаем шаблон completed
    template_df = pd.read_csv('templates/ojdt_completed.csv', nrows=1)
    
    print(f"Количество колонок в completed ojdt: {len(template_df.columns)}")
    
    # Создаем список для строк результата
    result_rows = []
    
    # Проходим по всем строкам исходного файла
    for index, row in df.iterrows():
        # Основная запись
        ojdt_row = {
            'JdtNum': index + 1,
            'ReferenceDate': row['Completed'],
            'Reference': row['Name'],
            'Reference2': row['Order'],
            'TaxDate': row['Completed'],
            'DueDate': row['Completed']
        }
        result_rows.append(ojdt_row)
        
        # Если есть Additional Fee, создаем дополнительную запись
        if pd.notna(row.get('Additionall Fee', 0)) and float(row['Additionall Fee']) != 0:
            additional_row = {
                'JdtNum': index + 2,
                'ReferenceDate': row['Completed'],
                'Reference': row['Name'],
                'Reference2': row['Order'],
                'TaxDate': row['Completed'],
                'DueDate': row['Completed']
            }
            result_rows.append(additional_row)
    
    # Создаем DataFrame с данными
    data_df = pd.DataFrame(result_rows, columns=template_df.columns)
    
    # Объединяем заголовки и данные
    final_df = pd.concat([template_df, data_df], ignore_index=True)
    
    # Сохраняем результат
    final_df.to_csv(output_file, index=False) 