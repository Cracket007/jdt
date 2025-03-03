import os
import pandas as pd
from abc import ABC, abstractmethod

class BaseProcessor(ABC):
    """ Базовый класс для обработки отчетов.
    Определяет интерфейс и общие методы для всех типов отчетов."""

    def __init__(self, input_file, output_dir="temp"):
        """Инициализация базового процессора.
        :param input_file: Путь к входному файлу.
        :param output_dir: Директория для сохранения выходных файлов."""
        self.input_file = input_file
        self.output_dir = output_dir
        self.data = None

        # Убедимся, что выходная директория существует
        os.makedirs(self.output_dir, exist_ok=True)

    async def load_data(self):
        """Загружает данные из входного файла в DataFrame."""
        try:
            self.data = pd.read_csv(self.input_file)
            self.data.columns = self.data.columns.str.strip()  # Убираем лишние пробелы в названиях колонок
        except Exception as e:
            raise ValueError(f"Ошибка при загрузке данных из файла {self.input_file}: {e}")

    @abstractmethod
    async def process_jdt(self, input_file, output_file):
        """Абстрактный метод для обработки JDT отчета.
        Должен быть реализован в подклассах."""
        pass

    @abstractmethod
    async def process_ojdt(self, input_file, output_file):
        """Абстрактный метод для обработки OJDT отчета.
        Должен быть реализован в подклассах."""
        pass

    async def save_to_csv(self, data, output_file):
        """Сохраняет DataFrame в CSV файл.
        :param data: DataFrame для сохранения.
        :param output_file: Путь к выходному файлу."""
        try:
            output_path = os.path.join(self.output_dir, output_file)
            data.to_csv(output_path, index=False)
        except Exception as e:
            raise ValueError(f"Ошибка при сохранении файла {output_file}: {e}")

    async def format_date(self, date_str, formats=None):
        """Преобразует дату в формат yyyymmdd.
        :param date_str: Строка с датой.
        :param formats: Список форматов для попытки преобразования.
        :return: Дата в формате yyyymmdd или исходная строка, если преобразование не удалось. """
        if formats is None:
            formats = ['%d/%m/%Y', '%d/%m/%Y %H:%M:%S']

        for fmt in formats:
            try:
                return pd.to_datetime(date_str, format=fmt).strftime('%Y%m%d')
            except:
                continue

        # Если преобразование не удалось, возвращаем исходную строку
        return date_str

    async def clean_temp_files(self, files):
        """ Удаляет временные файлы.:param files: Список путей к файлам для удаления. """
        for file in files:
            if os.path.exists(file):
                try:
                    os.remove(file)
                except Exception as e:
                    print(f"Ошибка при удалении файла {file}: {e}")