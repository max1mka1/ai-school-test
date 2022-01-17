import os
from typing import get_type_hints, Union
from dotenv import load_dotenv

load_dotenv(dotenv_path='./config/.env')


class AppConfigError(Exception):
    pass


def _parse_bool(val: Union[str, bool]) -> bool:  # pylint: disable=E1136
    return val if type(val) == bool else val.lower() in ['true', 'yes', '1']


class AppConfig:
    """
    Класс сопоставляет переменные среды с полями класса согласно правилам:
      - Поле не будет проанализировано, если у него нет аннотации типа
      - Поле будет пропущено, если оно написано не капсом
      - Поле класса и имя переменной среды совпадают
    """
    DEBUG: bool = False
    ENV: str = 'production'
    HOSTNAME: str
    PORT: int
    USERNAME: str
    PASSWORD: str
    DATABASE: str



    def __init__(self, env):
        for field in self.__annotations__:
            if not field.isupper():
                continue

            # AppConfigError, если обязательное поле не указано
            default_value = getattr(self, field, None)
            if default_value is None and env.get(field) is None:
                raise AppConfigError(f'Поле {field} обязательно')

            # Приведем значение переменной env к ожидаемому типу
            try:
                var_type = get_type_hints(AppConfig)[field]
                if var_type == bool:
                    value = _parse_bool(env.get(field, default_value))
                else:
                    value = var_type(env.get(field, default_value))

                self.__setattr__(field, value)
            # вызов AppConfigError в случае ошибки
            except ValueError:
                raise AppConfigError(f'''Невозможно привести значение "{env[field]}" /
                                        к типу "{var_type}" для "{field}" поля''')

    def __repr__(self):
        return str(self.__dict__)

# Создадим объект конфигурации для импорта в приложение
Config = AppConfig(os.environ)
