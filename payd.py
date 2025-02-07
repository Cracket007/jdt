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

def process_jdt(output_file):
    # Читаем исходный файл
    df = pd.read_csv('temp/исходник (Payd).csv')
    
    # Читаем шаблон и сохраняем только первую строку
    template_df = pd.read_csv('templates/jdt_template.csv', nrows=1)
    
    print(f"Количество колонок в jdt: {len(template_df.columns)}")
    
    # Создаем пустой список для строк результата
    debit_rows = []  # Отдельный список для debit строк
    credit_rows = [] # Отдельный список для credit строк
    
    # Создаем debit строки с их нумерацией
    for index, row in df.iterrows():
        provider_number = DEBIT_MAPPING.get(row['Payment Provider'], row['Payment Provider'])
        
        debit_row = {
            'ParentKey': index + 1,
            'JdtNum': index + 1,
            'LineNum': '',
            'Debit': row['Fval EUR'],
            'Credit': '',
            'DueDate': row['Paid'],
            'ShortName': provider_number,
            'Reference1': row['Name'],
            'Reference2': row['Order '],
            'TaxDate': row['Paid'],
            'ReferenceDate1': row['Paid']
        }
        debit_rows.append(debit_row)
    
    # Создаем credit строки
    for index, row in df.iterrows():
        provider_number = CREDIT_MAPPING.get(row['Payment Provider'], row['Payment Provider'])
        
        credit_row = {
            'ParentKey': index + 1,
            'JdtNum': index + 1,
            'LineNum': '1',
            'Debit': '',
            'Credit': row['Fval EUR'],
            'DueDate': row['Paid'],
            'ShortName': provider_number,
            'Reference1': row['Name'],
            'Reference2': row['Order '],
            'TaxDate': row['Paid'],
            'ReferenceDate1': row['Paid']
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

def process_ojdt(output_file):
    # Читаем исходный файл
    df = pd.read_csv('temp/исходник (Payd).csv')
    
    # Читаем шаблон и сохраняем только первую строку
    template_df = pd.read_csv('templates/ojdt_template.csv', nrows=1)
    
    print(f"Количество колонок в ojdt: {len(template_df.columns)}")
    
    # Создаем список для строк результата
    result_rows = []
    
    # Проходим по всем строкам исходного файла
    for index, row in df.iterrows():
        ojdt_row = {
            'JdtNum': index + 1,
            'ReferenceDate': row['Paid'],
            'Reference': row['Name'],
            'Reference2': row['Order '],
            'TaxDate': row['Paid'],
            'DueDate': row['Paid']
        }
        result_rows.append(ojdt_row)
    
    # Создаем DataFrame с данными
    data_df = pd.DataFrame(result_rows, columns=template_df.columns)
    
    # Объединяем заголовки и данные
    final_df = pd.concat([template_df, data_df], ignore_index=True)
    
    # Сохраняем результат
    final_df.to_csv(output_file, index=False) 