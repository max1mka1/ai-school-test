import os
import random
import sys
import logging
from os import environ as env
from dotenv import load_dotenv
from mysql.connector import connect, Error, MySQLConnection
import pandas as pd
from collections import OrderedDict
from typing import List, Dict, Tuple

from config.db_config import Config, AppConfig
from database.database import (Base, Users, Products, create_engine)


logging.basicConfig(level=logging.INFO,
                    filename='database.log',
                    filemode='w',
                    format='Дата-Время: %(asctime)s : Номер строки.: %(lineno)d - %(message)s')

DATA_PATH = os.path.join(os.getcwd(), 'data')
if not os.path.exists(DATA_PATH):
    os.makedirs(DATA_PATH)


class DBManager:
    """
    Класс для создания и обновления MySQL БД
    """

    config: AppConfig = None
    connection: MySQLConnection = None

    def __init__(self, config: AppConfig, columns: List[str],
                 csv_db_path: str, db_dump_name: str = None):
        self.config = config
        self.columns = columns
        self.csv_db_path = csv_db_path
        self.db_dump_name = db_dump_name
        self.connect_db()
        self.engine = create_engine(
            f'mysql://{self.config.USERNAME}:{self.config.PASSWORD}@{self.config.HOSTNAME}/{self.config.DATABASE}',
            echo=True)
        # Создание таблиц
        Base.metadata.create_all(self.engine)
        self.connection = self.engine.connect()


    def connect_db(self) -> None:
        """
        Метод для установления соединения к БД через MySQL Connector
        :return:    None
        """
        try:
            with connect(
                    host=self.config.HOSTNAME,
                    port=self.config.PORT,
                    user=self.config.USERNAME,
                    password=self.config.PASSWORD,
                    database=self.config.DATABASE,
                    auth_plugin='mysql_native_password'
            ) as connection:
                logging.info(f'Связь с БД установлена {connection}')
                self.connection = connection
                self.create_db()
        except Error as e:
            logging.error(f'ERROR: {e.msg}')
            if 'Unknown database' in e.msg:
                self.create_db()


    def create_db(self) -> None:
        """
        Метод проверяет, была ли создана БД ранее, и создает ее в случае отсутствия
        :return:        None
        """
        try:
            engine = create_engine(f'mysql://{self.config.USERNAME}:{self.config.PASSWORD}@{self.config.HOSTNAME}',
                                     echo=True)
            with engine.connect() as connection:
                connection.execute("commit")
                connection.execute(f"CREATE DATABASE IF NOT EXISTS {self.config.DATABASE}")
                if not self.connection:
                    self.connection = connection
                logging.info(f'База данных {self.config.DATABASE} была успешно создана')
        except Error as e:
            logging.error(e)


    def generate_csv(self) -> None:
        """
        Метод для генерации csv файла с продуктами
        id - первичный ключ
        name - название товара
        code - символьный код товара
        price - цена в рублях
        preview_text - превью описание
        detail_text - детальное описание
        user_id - вторичный ключ для связи с таблицей пользователей
        """
        import random
        data = OrderedDict.fromkeys(self.columns, [])
        df = pd.DataFrame(data=data)
        for i in range(10):
            index = i
            if i % 5 == 0:
                index = random.randint(0, i)
            df = df.append({'prod_id': index,
                            'name': f'name_{index}',
                            'code': f'code_{i}',
                            'preview_text': f'preview_text_{i}',
                            'detail_text': f'detail_text_{i}',
                            'user_id': f'{index}',
                            }, ignore_index=True)

        csv_db_path = os.path.join(DATA_PATH, 'database.csv')
        df.to_csv(csv_db_path, index=False)


    def read_db(self, table_name: str) -> pd.DataFrame:
        """
        Метод для ивлечения днных из уже имеющейся БД
        """
        # Using pandas to get data directly
        df = pd.read_sql(f"select * from {table_name}", con=self.engine)

        return df


    def update_db(self) -> None:
        """
        Метод для обновления БД согласно требованиям задания
        """
        # PRODUCTS
        table_name = 'products'
        # Сперва проверим наличие данных в таблице БД
        sql_db = self.read_db(table_name=table_name)
        # Проверяем существует ли база csv
        if not os.path.isfile(self.csv_db_path):
            # Генерируем синтетческий датафрейм (генерим сами, хотя есть соответствующие либы)
            df = self.generate_csv()
        else:
            df = pd.read_csv(self.csv_db_path)
        df.to_sql(table_name, self.engine, if_exists='replace', index=False)

        # Выберем уникальные значения пользователей из БД
        unique_users = df['user_id'].unique().tolist()

        # USERS
        table_name = 'users'
        # Сперва проверим наличие данных в таблице БД
        sql_db = self.read_db(table_name=table_name)
        df = pd.DataFrame(data={'user_id': unique_users})
        df.to_sql(table_name, self.engine, if_exists='replace', index=False)

        # Дамп базы
        # self.dumb_db()


    # def dumb_db(self):
    #     """
    #     Метод для создания дампа базы
    #     """
    #     con = self.engine.raw_connection()
    #     if self.db_dump_name:
    #         dump_path = os.path.join(DATA_PATH, f'{self.db_dump_name}')
    #         with open(dump_path, 'w') as f:
    #             for line in con.iterdump():
    #                 f.write('%s\n' % line)


if __name__ == '__main__':
    db_dump_name = 'dump.sql'
    csv_db_path = os.path.join(DATA_PATH, 'database.csv')
    columns = ['id', 'prod_id', 'name', 'code', 'price', 'preview_text', 'detail_text', 'user_id']

    manager = DBManager(config=Config, columns=columns,
                        csv_db_path=csv_db_path, db_dump_name=db_dump_name)
    manager.update_db()
