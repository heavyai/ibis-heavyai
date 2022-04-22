"""OmniSciDB test configuration module."""
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

OMNISCIDB_HOST = os.environ.get('IBIS_TEST_OMNISCIDB_HOST', 'localhost')
OMNISCIDB_PORT = int(os.environ.get('IBIS_TEST_OMNISCIDB_PORT', 6274))
OMNISCIDB_USER = os.environ.get('IBIS_TEST_OMNISCIDB_USER', 'admin')
OMNISCIDB_PASS = os.environ.get(
    'IBIS_TEST_OMNISCIDB_PASSWORD', 'HyperInteractive'
)
OMNISCIDB_PROTOCOL = os.environ.get('IBIS_TEST_OMNISCIDB_PROTOCOL', 'binary')
OMNISCIDB_DB = os.environ.get('IBIS_TEST_DATA_DB', 'ibis_testing')

URI_USER = f'{OMNISCIDB_USER}:{OMNISCIDB_PASS}'
URI_HOST = f'{OMNISCIDB_HOST}:{OMNISCIDB_PORT}'
URI = f'omniscidb://{URI_USER}@{URI_HOST}/{OMNISCIDB_DB}'


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
        return 'omniscidb'

    @staticmethod
    def connect(data_directory: Path) -> BaseBackend:
        """Connect to the test database."""
        user = os.environ.get('IBIS_TEST_OMNISCIDB_USER', 'admin')
        password = os.environ.get(
            'IBIS_TEST_OMNISCIDB_PASSWORD', 'HyperInteractive'
        )
        host = os.environ.get('IBIS_TEST_OMNISCIDB_HOST', 'localhost')
        port = os.environ.get('IBIS_TEST_OMNISCIDB_PORT', '6274')
        database = os.environ.get(
            'IBIS_TEST_OMNISCIDB_DATABASE', 'ibis_testing'
        )
        return ibis.omniscidb.connect(
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
    ibis.omniscidb.OmniSciDBClient
    """
    return ibis.omniscidb.connect(
        protocol=OMNISCIDB_PROTOCOL,
        host=OMNISCIDB_HOST,
        port=OMNISCIDB_PORT,
        user=OMNISCIDB_USER,
        password=OMNISCIDB_PASS,
        database=OMNISCIDB_DB,
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
    return ibis.omniscidb.connect(
        protocol=OMNISCIDB_PROTOCOL,
        host=OMNISCIDB_HOST,
        port=OMNISCIDB_PORT,
        user=OMNISCIDB_USER,
        password=OMNISCIDB_PASS,
        database=OMNISCIDB_DB,
    )
    return session_con


@pytest.fixture(scope='module')
def alltypes(con) -> ibis.expr.types.TableExpr:
    """Define a functional_alltypes table fixture.

    Parameters
    ----------
    con : ibis.omniscidb.OmniSciDBClient

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
    con : ibis.omniscidb.OmniSciDBClient

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
    con : ibis.omniscidb.OmniSciDBClient

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
    con : ibis.omniscidb.OmniSciDBClient

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
    from ..compiler import OmniSciDBCompiler

    context = OmniSciDBCompiler.make_context()
    return lambda expr: (
        OmniSciDBCompiler.translator(expr, context).get_result()
    )


def _random_identifier(suffix):
    return '__ibis_test_{}_{}'.format(suffix, util.guid())


@pytest.fixture
def temp_table(con) -> typing.Generator[str, None, None]:
    """Return a temporary table name.

    Parameters
    ----------
    con : ibis.omniscidb.OmniSciDBClient

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
    return OMNISCIDB_DB


@pytest.fixture
def temp_database(con, test_data_db: str) -> typing.Generator[str, None, None]:
    """Create a temporary database.

    Parameters
    ----------
    con : ibis.omniscidb.OmniSciDBClient
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
