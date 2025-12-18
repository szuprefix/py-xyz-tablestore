import os

SERVER = os.getenv('MONGO_SERVER', 'localhost:27017')
if not SERVER.startswith('mongodb://'):
    SERVER = f'mongodb://{SERVER}'
CONN = SERVER.replace('mongodb://', '')
DB = os.getenv('MONGO_DB') or ('/' in CONN and CONN.split('/')[1]) or os.path.basename(os.getcwd())
TIMEOUT = 3000
