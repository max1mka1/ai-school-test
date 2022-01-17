import os
import sys
import logging
from os import environ as env
from dotenv import load_dotenv
from mysql.connector import connect, Error, MySQLConnection
import pandas as pd
from collections import OrderedDict

from config.db_config import Config, AppConfig
from database.database import engine, Base, Users, Products

# Создание таблицы
Base.metadata.create_all(engine)


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

    def __init__(self, config):
        self.config = config
        self.connect_db()

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
            ) as connection:
                logging.info(f'Связь с БД установлена {connection}')
                self.connection = connection
                self.create_db()
        except Error as e:
            logging.error(e)

    def create_db(self) -> None:
        """
        Метод проверяет, была ли создана БД ранее, и создает ее в случае отсутствия
        :return:        None
        """
        try:
            create_db_query = f"CREATE DATABASE IF NOT EXISTS {self.config.DATABASE}"
            if self.connection:
                with self.connection.cursor() as cursor:
                    cursor.execute(create_db_query)
                    logging.info(f'База данных {self.config.DATABASE} была успешно создана')
        except Error as e:
            logging.error(e)


def generate_csv() -> None:
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
    columns = ['id', 'name', 'code', 'price', 'preview_text', 'detail_text', 'user_id']


    data = OrderedDict.fromkeys(columns, [])
    for i in range(10):
        pass
    user_id = 0
    df = pd.DataFrame(data=data)
    df.to_csv('./data/new_data.csv')


if __name__ == '__main__':
    manager = DBManager(config=Config)
