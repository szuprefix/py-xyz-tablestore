import datetime, json, os, logging
from tablestore import *

def get_client(
        endpoint=os.getenv('OTS_ENDPOINT', ''),
        instance_name=os.getenv('OTS_DB', 'test'),
        access_key_id=os.getenv('OTS_KEY_ID'),
        access_key_secret=os.getenv('OTS_KEY_SECRET')
):
    return OTSClient(endpoint, access_key_id, access_key_secret, instance_name)

def timestamp_ms(d=None):
    if not d:
        d = datetime.now()
    return math.floor(d.timestamp()*1000)


def encode(v):
    if isinstance(v, (list, tuple, dict)):
        return json.dumps(v)
    return v

def map_encode(d):
    return dict([(k, encode(v)) for k, v in d.items()])

def decode(v):
    if isinstance(v, str):
        if (v.startswith('[') and v.endswith(']')) or (v.startswith('{') and v.endswith('}')):
            try:
                v = json.loads(v)
            except Exception as e:
                logging.warning(f'json error:{e}')
            return v
    return v

def dict2row(d, pks):
    d = dict(**d)
    pfs = []
    fs = []
    for k in pks:
        pfs.append((k, d.pop(k, None)))

    for k, v in d.items():
        if v is None:
            continue
        fs.append((k, encode(v)))
    return Row(pfs, fs)

def row2dict(row):
    d = dict()
    r = row if isinstance(row, tuple) else [row.primary_key, row.attribute_columns]
    for s in r:
        for f in s:
            d[f[0]] = decode(f[1])
    return d

