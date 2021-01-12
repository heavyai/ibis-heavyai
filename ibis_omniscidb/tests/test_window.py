from typing import List, Union

import ibis
import pandas as pd
import pytest


def _ntile(
    data: Union[pd.Series, pd.core.groupby.generic.SeriesGroupBy], bucket: int
):
    """
    NTILE divides given data set into a number of buckets.

    It divides an ordered and grouped data set into a number of buckets
    and assigns the appropriate bucket number to each row.
    Return an integer ranging from 0 to `bucket - 1`, dividing the
    partition as equally as possible.
    Adapted from:
    https://gist.github.com/xmnlab/2c1f93df1a6c6bde4e32c8579117e9cc

    Parameters
    ----------
    data : pandas.core.groupby.generic.SeriesGroupBy or pandas.Series
    bucket: int

    Returns
    -------
    pandas.Series

    Notes
    -----
    This function would be used to test the result from the OmniSci backend.
    """
    if isinstance(data, pd.core.groupby.generic.SeriesGroupBy):
        return pd.concat([_ntile(group, bucket) for name, group in data])

    n = data.shape[0]
    sub_n = n // bucket
    diff = n - (sub_n * bucket)

    result = []
    for i in range(bucket):
        sub_result = [i] * (sub_n + (1 if diff else 0))
        result.extend(sub_result)
        if diff > 0:
            diff -= 1
    return pd.Series(result, index=data.index)


@pytest.mark.parametrize(
    'column_name,group_by,order_by,buckets',
    [
        ('string_col', ['string_col'], 'id', 7),
    ],
)
def test_ntile(
    con: ibis.omniscidb.OmniSciDBClient,
    alltypes: ibis.expr.types.TableExpr,
    df_alltypes: pd.DataFrame,
    column_name: str,
    group_by: List[str],
    order_by: List[str],
    buckets: int,
):
    result_pd = df_alltypes.copy()
    result_pd_grouped = result_pd.sort_values(order_by).groupby(group_by)
    result_pd['val'] = _ntile(result_pd_grouped[column_name], buckets)

    expr = alltypes.mutate(
        val=(
            alltypes[column_name]
            .ntile(buckets=buckets)
            .over(
                ibis.window(
                    following=0,
                    group_by=group_by,
                    order_by=order_by,
                )
            )
        )
    )

    result_pd = result_pd.sort_values(order_by).reset_index(drop=True)
    result_expr = expr.execute().sort_values(order_by).reset_index(drop=True)

    pd.testing.assert_series_equal(result_pd.val, result_expr.val)
