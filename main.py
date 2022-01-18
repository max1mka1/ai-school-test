import os
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


class DBManager:
    """
    Класс для создания и обновления MySQL БД
    """

    config: AppConfig = None
    connection: MySQLConnection = None

    def __init__(self, config, columns: List[str]):
        self.config = config
        self.columns = columns
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
        data = OrderedDict.fromkeys(columns, [])
        for i in range(10):
            pass
            # data[]
            # if i % 5 == 0:

        user_id = 0
        df = pd.DataFrame(data=data)
        # df.to_csv('./data/new_data.csv')

    def read_db(self) -> pd.DataFrame:
        """
        Метод для ивлечения днных из уже имеющейся БД
        """
        # Using pandas to get data directly
        df = pd.read_sql("select * from products", con=self.engine)

        return df


    def csv_to_sql(self, dataframe: pd.DataFrame, tablename: str) -> None:
        """

        """

        books = pd.DataFrame(
            {"bname": ['gone with wind', 'good by', 'game of throne', 'king of ring'], "price": [128, 22, 67, 190],
             'student_id': [1, 1, 3, 2]})
        #
        books.to_sql(tablename, self.engine, if_exists='replace', index=False)
        dataframe = self.generate_csv()
        tablename = 'products'
        self.csv_to_sql(dataframe=, tablename=tablename)


if __name__ == '__main__':
    columns = ['id', 'name', 'code', 'price', 'preview_text', 'detail_text', 'user_id']
    manager = DBManager(config=Config, columns=columns)
    manager.update_db()
