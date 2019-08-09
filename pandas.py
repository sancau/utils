from typing import List, Union

import pandas as pd

from django.db.models import QuerySet


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
