import pymongo
from .config import SERVER, DB, TIMEOUT
import datetime, json

from six import text_type
from bson.objectid import ObjectId

def json_schema(d, prefix=''):
    import bson
    tm = {
        int: 'integer',
        bson.int64.Int64: 'integer',
        ObjectId: 'oid',
        float: 'number',
        bool: 'boolean',
        list: 'array',
        text_type: 'string',
        type(None): 'null',
        dict: 'object',
        datetime.datetime: 'datetime'
    }
    r = {}
    for k, v in d.items():
        t = tm[type(v)]
        fn = '%s%s' % (prefix, k)
        r[fn] = t
        if t == 'object':
            r.update(json_schema(v, prefix='%s.' % fn))
    return r



def loadMongoDB(server=SERVER, db=DB, timeout=TIMEOUT):
    client = pymongo.MongoClient(server, serverSelectionTimeoutMS=timeout)
    return getattr(client, db)

def filed_type_func(f):
    return {
        'integer': int,
        'number': float,
        'datetime': datetime.datetime.fromisoformat,
        'date': datetime.datetime.fromisoformat,
        'object': json.loads,
        'oid': ObjectId
    }.get(f, text_type)

def all_fields_type_func(fs):
    return dict([(fn, filed_type_func(ft)) for fn, ft in fs.items()])
