"""HeavyDB test configuration module."""
import os
import typing
from pathlib import Path

import ibis
import ibis.expr.operations as ops
import ibis.util as util
import pandas
import pytest
from ibis.backends.base import BaseBackend
from ibis.backends.tests.base import BackendTest, RoundAwayFromZero

HEAVYDB_HOST = os.environ.get('IBIS_TEST_HEAVYDB_HOST', 'localhost')
HEAVYDB_PORT = int(os.environ.get('IBIS_TEST_HEAVYDB_PORT', 6274))
HEAVYDB_USER = os.environ.get('IBIS_TEST_HEAVYDB_USER', 'admin')
HEAVYDB_PASS = os.environ.get('IBIS_TEST_HEAVYDB_PASSWORD', 'HyperInteractive')
HEAVYDB_PROTOCOL = os.environ.get('IBIS_TEST_HEAVYDB_PROTOCOL', 'binary')
HEAVYDB_DB = os.environ.get('IBIS_TEST_DATA_DB', 'ibis_testing')

URI_USER = f'{HEAVYDB_USER}:{HEAVYDB_PASS}'
URI_HOST = f'{HEAVYDB_HOST}:{HEAVYDB_PORT}'
URI = f'heavydb://{URI_USER}@{URI_HOST}/{HEAVYDB_DB}'


class TestConf(BackendTest, RoundAwayFromZero):
    """Backend-specific class with information for testing."""

    check_dtype = False
    check_names = False
    supports_window_operations = True
    supports_divide_by_zero = False
    supports_floating_modulus = False
    supports_arrays = False
    supports_arrays_outside_of_select = False
    returned_timestamp_unit = 's'
    # Exception: Non-empty LogicalValues not supported yet
    additional_skipped_operations = frozenset(
        {
            ops.Abs,
            ops.Ceil,
            ops.Floor,
            ops.Exp,
            ops.Sign,
            ops.Sqrt,
            ops.Ln,
            ops.Log10,
            ops.Modulus,
        }
    )

    def name(self):
        """Name of the backend.

        In the parent class, this is automatically obtained from the name of
        the module, which is not the case for third-party backends.
        """
        return 'heavyai'

    @staticmethod
    def connect(data_directory: Path) -> BaseBackend:
        """Connect to the test database."""
        user = os.environ.get('IBIS_TEST_HEAVYDB_USER', 'admin')
        password = os.environ.get(
            'IBIS_TEST_HEAVYDB_PASSWORD', 'HyperInteractive'
        )
        host = os.environ.get('IBIS_TEST_HEAVYDB_HOST', 'localhost')
        port = os.environ.get('IBIS_TEST_HEAVYDB_PORT', '6274')
        database = os.environ.get('IBIS_TEST_HEAVYDB_DATABASE', 'ibis_testing')
        return ibis.heavyai.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
        )


@pytest.fixture(scope='module')
def con():
    """Define a connection fixture.

    Returns
    -------
    ibis.heavyai.HeavyDBClient
    """
    return ibis.heavyai.connect(
        protocol=HEAVYDB_PROTOCOL,
        host=HEAVYDB_HOST,
        port=HEAVYDB_PORT,
        user=HEAVYDB_USER,
        password=HEAVYDB_PASS,
        database=HEAVYDB_DB,
    )


@pytest.fixture(scope='function')
def test_table(con):
    """
    Define fixture for test table.

    Yields
    ------
    ibis.expr.types.TableExpr
    """
    table_name = _random_identifier('table')
    con.drop_table(table_name, force=True)

    schema = ibis.schema(
        [('a', 'polygon'), ('b', 'point'), ('c', 'int8'), ('d', 'double')]
    )
    con.create_table(table_name, schema=schema)

    yield con.table(table_name)

    con.drop_table(table_name)


@pytest.fixture(scope='module')
def session_con():
    """Define a session connection fixture."""
    # TODO: fix return issue
    return ibis.heavyai.connect(
        protocol=HEAVYDB_PROTOCOL,
        host=HEAVYDB_HOST,
        port=HEAVYDB_PORT,
        user=HEAVYDB_USER,
        password=HEAVYDB_PASS,
        database=HEAVYDB_DB,
    )
    return session_con


@pytest.fixture(scope='module')
def alltypes(con) -> ibis.expr.types.TableExpr:
    """Define a functional_alltypes table fixture.

    Parameters
    ----------
    con : ibis.heavyai.HeavyDBClient

    Returns
    -------
    ibis.expr.types.TableExpr
    """
    return con.table('functional_alltypes')


@pytest.fixture(scope='module')
def awards_players(con) -> ibis.expr.types.TableExpr:
    """Define a awards_players table fixture.

    Parameters
    ----------
    con : ibis.heavyai.HeavyDBClient

    Returns
    -------
    ibis.expr.types.TableExpr
    """
    return con.table('awards_players')


@pytest.fixture(scope='module')
def batting(con) -> ibis.expr.types.TableExpr:
    """Define a awards_players table fixture.

    Parameters
    ----------
    con : ibis.heavyai.HeavyDBClient

    Returns
    -------
    ibis.expr.types.TableExpr
    """
    return con.table('batting')


@pytest.fixture(scope='module')
def geo_table(con) -> ibis.expr.types.TableExpr:
    """Define a geo table fixture.

    Parameters
    ----------
    con : ibis.heavyai.HeavyDBClient

    Returns
    -------
    ibis.expr.types.TableExpr
    """
    return con.table('geo')


@pytest.fixture(scope='module')
def df_alltypes(alltypes: ibis.expr.types.TableExpr) -> pandas.DataFrame:
    """Return all the data for functional_alltypes table.

    Parameters
    ----------
    alltypes : ibis.expr.types.TableExpr
        [description]

    Returns
    -------
    pandas.DataFrame
    """
    return alltypes.execute()


@pytest.fixture
def translate() -> typing.Callable:
    """Create a translator function.

    Returns
    -------
    function
    """
    from ibis_heavyai import HeavyDBCompiler

    context = HeavyDBCompiler.make_context()
    return lambda expr: (
        HeavyDBCompiler.translator(expr, context).get_result()
    )


def _random_identifier(suffix):
    return '__ibis_test_{}_{}'.format(suffix, util.guid())


@pytest.fixture
def temp_table(con) -> typing.Generator[str, None, None]:
    """Return a temporary table name.

    Parameters
    ----------
    con : ibis.heavyai.HeavyDBClient

    Yields
    ------
    name : string
        Random table name for a temporary usage.
    """
    name = _random_identifier('table')
    try:
        yield name
    finally:
        assert name in con.list_tables()
        con.drop_table(name)


@pytest.fixture(scope='session')
def test_data_db() -> str:
    """Return the database name."""
    return HEAVYDB_DB


@pytest.fixture
def temp_database(con, test_data_db: str) -> typing.Generator[str, None, None]:
    """Create a temporary database.

    Parameters
    ----------
    con : ibis.heavyai.HeavyDBClient
    test_data_db : str

    Yields
    ------
    str
    """
    name = _random_identifier('database')
    con.create_database(name)
    try:
        yield name
    finally:
        con.set_database(test_data_db)
        con.drop_database(name, force=True)
