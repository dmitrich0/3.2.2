import csv

from line_profiler import LineProfiler
from Helper import Helper
from multiprocessing import Pool

profiler = LineProfiler()


class Vacancy:
    """
    Класс для представления вакансии

    Attributes:
        name (str): Название
        salary_from (int): Нижняя граница вилки оклада
        salary_to (int): Верхняя граница вилки оклада
        salary_currency (str): Валюта оклада
        salary_average (float): Средняя зарплата
        area_name (str): Место, где есть вакансия
        year (int): Год создания вакансии
    """
    currency_to_rub = {
        "AZN": 35.68,
        "BYR": 23.91,
        "EUR": 59.90,
        "GEL": 21.74,
        "KGS": 0.76,
        "KZT": 0.13,
        "RUR": 1,
        "UAH": 1.64,
        "USD": 60.66,
        "UZS": 0.0055,
    }

    def __init__(self, vacancy: dict[str, str]) -> None:
        """
        Инициализирует объект Vacancy, выполняет конвертацию для некоторых полей, считает среднюю зарплату

        :param vacancy: Словарь вакансии вида [str, str]
        :returns: None
        """
        self.name = vacancy['name']
        self.salary_from = int(float(vacancy['salary_from']))
        self.salary_to = int(float(vacancy['salary_to']))
        self.salary_currency = vacancy['salary_currency']
        self.salary_average = self.currency_to_rub[self.salary_currency] * (self.salary_from + self.salary_to) / 2
        self.area_name = vacancy['area_name']
        self.year = Helper.parse_year_from_date_slice(vacancy['published_at'])


class DataSet:
    """
    Класс для представления данных

    Attributes:
        file_name (str): Название файла
        vacancy_name (str): Название вакансии
    """

    def __init__(self, file: str, vacancy: str) -> None:
        """
        Инициализирует объект Dataset

        :param file: Название файла
        :param vacancy: Название вакансии

        >>> type(DataSet("123.csv", "Программист")).__name__
        'DataSet'
        >>> type(DataSet("123.csv", "Программист").file_name).__name__
        'str'
        >>> type(DataSet("123.csv", "Программист").vacancy_name).__name__
        'str'
        """
        self.file_name = file
        self.vacancy_name = vacancy

    @staticmethod
    def increment(subject: dict, key, value) -> None:
        """
        Если в subject есть значение с ключом key: увеличивает его на value, иначе: присваивает ему значение value

        :param subject: Словарь объектов
        :param key: Ключ для поиска элемента
        :param value: Значение для инкремента или присваивания
        :return: None
        """
        if key in subject:
            subject[key] += value
        else:
            subject[key] = value

    @staticmethod
    def get_average_dict(data: dict) -> dict:
        """
        Создаёт новый словарь из данного, где элементы - среднее значение

        :param data: Словарь с данными
        :return: Массив средних

        >>> DataSet.get_average_dict({1: [2, 5], 2: [3, 6]})
        {1: 3, 2: 4}
        """
        result = {}
        for key, data in data.items():
            result[key] = int(sum(data) / len(data))
        return result

    def csv_reader(self) -> dict:
        """
        Открывает файл и лениво возвращает словари с данными вакансии
        """
        with open(self.file_name, mode='r', encoding='utf-8-sig') as file:
            reader = csv.reader(file)
            titles = next(reader)
            titles_count = len(titles)
            for row in reader:
                if '' not in row and len(row) == titles_count:
                    yield dict(zip(titles, row))

    def get_statistics(self) -> (dict, dict, dict, dict):
        """
        Формирует статистику по вакансиям и возвращает кортеж с данными

        :returns (stat_salary, vacancies_number, stat_salary_by_vac, vacs_per_name): Статистика по зп,
         статистика по числу вакансий, статистика вакансий по ЗП
         статистика вакансий по названию
        """
        salary = {}
        salary_of_vacancy_name = {}
        salary_city = {}
        vacancies_count = 0
        for vacancy in self.csv_reader():
            vacancy = Vacancy(vacancy)
            self.increment(salary, vacancy.year, [vacancy.salary_average])
            if vacancy.name.find(self.vacancy_name) != -1:
                self.increment(salary_of_vacancy_name, vacancy.year, [vacancy.salary_average])
            self.increment(salary_city, vacancy.area_name, [vacancy.salary_average])
            vacancies_count += 1
        vacancies_number = dict([(key, len(value)) for key, value in salary.items()])
        vacs_per_name = dict([(key, len(value)) for key, value in salary_of_vacancy_name.items()])
        if not salary_of_vacancy_name:
            salary_of_vacancy_name = dict([(key, [0]) for key, value in salary.items()])
            vacs_per_name = dict([(key, 0) for key, value in vacancies_number.items()])
        stat_salary = self.get_average_dict(salary)
        stat_salary_by_vac = self.get_average_dict(salary_of_vacancy_name)
        stat_salary_by_city = self.get_average_dict(salary_city)
        stat_salary_by_year = {}
        for year, salaries in salary_city.items():
            stat_salary_by_year[year] = round(len(salaries) / vacancies_count, 4)
        stat_salary_by_year = list(filter(lambda a: a[-1] >= 0.01,
                                          [(key, value) for key, value in stat_salary_by_year.items()]))
        stat_salary_by_year.sort(key=lambda a: a[-1], reverse=True)
        stat_salary_by_year = dict(stat_salary_by_year)
        stat_salary_by_city = list(filter(lambda a: a[0] in list(stat_salary_by_year.keys()),
                                          [(key, value) for key, value in stat_salary_by_city.items()]))
        stat_salary_by_city.sort(key=lambda a: a[-1], reverse=True)
        return stat_salary, vacancies_number, stat_salary_by_vac, vacs_per_name

    @staticmethod
    def print_statistic(salary_by_year: dict, vacs_per_year: dict, salary_by_vac: dict, count_by_vac: dict) -> None:
        """
        Печатает статистику

        :param salary_by_year: Статистика зарплат по годам
        :param vacs_per_year: Статистика количества вакансий по годам
        :param salary_by_vac: Статистика зарплаты по годам для выбранной профессии
        :param count_by_vac: Статистика количества вакансий по годам для выбранной профессии
        :return: None
        """
        print(f'Динамика уровня зарплат по годам: {salary_by_year}')
        print(f'Динамика количества вакансий по годам: {vacs_per_year}')
        print(f'Динамика уровня зарплат по годам для выбранной профессии: {salary_by_vac}')
        print(f'Динамика количества вакансий по годам для выбранной профессии: {count_by_vac}')


class InputConnect:
    """
    Работает с вводом пользователя, составляет датасет

    Attributes:
        files_folder (str): Название папки с файлами
        vacancy_name (str): Название профессии
    """

    def __init__(self):
        self.files_folder = input("Введите название папки с чанками: ")
        self.vacancy_name = input('Введите название профессии: ')

    def process_input(self) -> None:
        """
        Обрабатывает входные данные, создает процессы и печатает результат
        :return: None
        """
        files = Helper.get_filenames_from_dir(self.files_folder)
        pools = []
        for file in files:
            dataset = DataSet(file, self.vacancy_name)
            p = Pool(5)
            result = p.apply_async(dataset.get_statistics)
            pools.append(result)
        multi = Multiprocessing(pools)
        result = multi.get_united_dict()
        dataset.print_statistic(result[0], result[1], result[2], result[3])


class Multiprocessing:
    """
    Работает с мультипроцессингом

    Attributes:
        pools (str): Созданные пуллы
    """
    def __init__(self, pools) -> None:
        self.pools = pools

    def get_united_dict(self) -> set:
        """
        Возвращает результаты выполнения всех процессов
        :return: Множество результатов
        """
        answer = self.pools[0].get()
        for i in range(1, len(self.pools)):
            for k in range(0, 4):
                answer[k].update(self.pools[i].get()[k])
        return self.pools[0].get()


if __name__ == '__main__':
    InputConnect().process_input()
