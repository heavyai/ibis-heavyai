import ibis_omniscidb


def test_versioning():
    assert ibis_omniscidb.__version__ not in (None, "", "0.0.0")
