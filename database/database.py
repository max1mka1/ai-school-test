from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (create_engine, Table, Column,
                        Integer, String, MetaData, ForeignKey)

engine = create_engine('mysql://test:AISchool2022@localhost/products', echo=True)


class Base(declarative_base()):
    """
    Доопределение базового класса
    """

    def __repr__(self):
        for field in self.__annotations__:
            return f'<Поле {self.field}>'


class Products(Base):
    """
    Sqlalchemy модель
    id - первичный ключ
    name - название товара
    code - символьный код товара
    price - цена в рублях
    preview_text - превью описание
    detail_text - детальное описание
    user_id - вторичный ключ для связи с таблицей пользователей
    """
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, autoincrement=False)
    name = Column('name', String(64), nullable=False)
    code = Column('code', String(32), nullable=False)
    price = Column('price', Integer, nullable=False)
    preview_text = Column('preview_text', String(128), nullable=False)
    detail_text = Column('detail_text', String(256), nullable=False)
    user_id = Column(Integer, ForeignKey('users.id'))


class Users(Base):
    """
    user_id - первичный ключ для связи с таблицей товаров
    """
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=False)



