def test_centroid(geo_table):
    result = geo_table.geo_polygon.centroid().execute()
    assert 'POINT (25.4545454545455 26.969696969697)' == result.values[0].wkt


def test_equals(geo_table):
    expr = geo_table.geo_point == geo_table.geo_point
    raise Exception(expr.compile())

    assert expr.execute().all()
