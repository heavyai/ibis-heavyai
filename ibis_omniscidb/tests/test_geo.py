def test_centroid(geo_table):
    assert (
        'POINT (25.4545454545455 26.969696969697)'
        == geo_table.geo_polygon.centroid().execute().values[0]
    )
