import os
basedir = os.path.abspath(os.path.dirname(__file__))
# You need to replace the next values with the appropriate values for your configuration

env = 'local'
if env == 'local':
    print('Using release configuration...')
    SQLALCHEMY_DATABASE_URI = os.environ.get('SQLALCHEMY_DATABASE_URI_AI')
    BASE_URL = 'http://0.0.0.0:5000/'
elif env == 'dev':
    print('Using release configuration...')
    SQLALCHEMY_DATABASE_URI = os.environ.get('SQLALCHEMY_DATABASE_URI_AI')
    BASE_URL = 'https://qairamapnapi-dev.qairadrones.com/'
elif env == 'prod':
    print('Using release configuration...')
    SQLALCHEMY_DATABASE_URI = os.environ.get('SQLALCHEMY_DATABASE_URI_AI')
    BASE_URL = 'https://qairamapnapi.qairadrones.com/'