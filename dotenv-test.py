from os import environ as env
from dotenv import load_dotenv

load_dotenv()

print('HOSTNAME: {}'.format(env['HOSTNAME']))
print('USERNAME:  {}'.format(env['USERNAME']))
print('PASSWORD:     {}'.format(env['PASSWORD']))