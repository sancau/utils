from functools import partial
from multiprocessing import cpu_count, Pool
from typing import List, Union

import pandas as pd

from django.db.models import QuerySet


DEFAULT_WORKERS_COUNT = cpu_count()


def _wrapper(frame, job, axis):
    return frame.apply(job, axis=axis)


def multiprocess_apply(df, job, axis=1, workers=DEFAULT_WORKERS_COUNT):
    """pd.DataFrame.apply wrapper with multiprocessing support.
    A helpers for cases where a simple "map-reduce" stuff is appropriate.
    """
    chunk_size = df.shape[0] // workers
    chunks = [chunk for _, chunk in df.groupby(np.arange(len(df)) // chunk_size)]
    if len(chunks) == workers + 1:
        last = chunks.pop()
        chunks[-1] = pd.concat([chunks[-1], last])
    pool = Pool(workers)
    return pd.concat(pool.map(partial(_wrapper, job=job, axis=axis), chunks))


def qs_to_df(
    *,
    qs: QuerySet,
    columns: Union[List[str], dict],
    use_iterator=True,
) -> pd.DataFrame:
    """A wrapper to get a pandas DataFrame from a queryset.
    If a dict is passed as columns parameter then keys of that dict represent
    the fields to query and the values of the dict specify the names to rename
    the queried fields to. If a value in dict is None then the corresponding
    column will not be renamed.
    ["*"] can be passed as columns to query all columns of the model
    Note that it's possible to use standard Django syntax to access nested
    relations ex: ['user__address__street', 'user__status__is_paid'], etc.
    """
    if qs is None:
        raise ValueError("None object was passed instead of a queryset.")

    dict_passed = isinstance(columns, dict)

    if dict_passed:
        fields = columns.keys()
    else:
        fields = columns
        if fields == ["*"]:
            fields = [f.name for f in qs.model._meta.fields]

    if use_iterator:
        data = qs.values_list(*fields).iterator()
    else:
        data = list(qs.values_list(*fields))

    df = pd.DataFrame(data, columns=fields)

    if dict_passed:
        return df.rename(columns={
            k: v for k, v in columns.items() if v
        })
    return df
