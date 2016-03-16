from tempfile import TemporaryDirectory
from functools import partial

import plyvel

def _get_or(reverse, db, key, default=None,
            include_key=True, include_value=True,
            verify_checksums=False, fill_cache=True):
    if reverse:
        startstop = {'stop': key + b'\x00'}
    else:
        startstop = {'start': key}
    i = db.iterator(include_key=include_key, include_value=include_value,
                    reverse=reverse, verify_checksums=verify_checksums,
                    fill_cache=fill_cache, **startstop)
    try:
        return next(i)
    except StopIteration:
        return default

get_or_prev = partial(_get_or, True)
get_or_next = partial(_get_or, False)

with TemporaryDirectory() as tmp:
    db = plyvel.DB(tmp, create_if_missing=True)
    for j in b'abcdefqrstuy':
        k = bytes([j])
        db.put(k,k)
    testcases = [
        (plyvel.DB.get, b'q', b'q'),
        (get_or_prev, b'a', b'a'),
        (get_or_next, b'q', b'q'),

        (get_or_prev, b'0', None),
        (get_or_prev, b'j', b'f'),

        (get_or_next, b'v', b'y'),
        (get_or_next, b'z', None),
    ]
    for f, x, expected in testcases:
        observed = f(db, x)
        assert observed == expected, (f, x, observed)
