import ibis_heavyai


def test_versioning():
    assert ibis_heavyai.__version__ not in (None, "", "0.0.0")
