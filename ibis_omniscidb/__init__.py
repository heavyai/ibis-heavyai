"""OmniSciDB backend."""
from __future__ import annotations

import warnings
from importlib.metadata import PackageNotFoundError, version
from typing import Optional, Union

import ibis.common.exceptions as com
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import ibis.expr.types as ir
import pandas as pd
import pyarrow
import pyomnisci
import regex as re
from ibis.backends.base import Database
from ibis.backends.base.sql import BaseSQLBackend
from ibis.backends.base.sql.compiler import DDL, DML
from omnisci._parsers import _extract_column_details
from omnisci.dtypes import TDatumType as pyomnisci_dtype

from . import ddl
from . import dtypes as omniscidb_dtypes
from .client import OmniSciDBDataType, OmniSciDBTable, get_cursor_class
from .compiler import OmniSciDBCompiler
from .udf import OmniSciDBUDF

try:
    __version__ = version("ibis_omniscidb")
except PackageNotFoundError:
    __version__ = ""


try:
    from cudf import DataFrame as GPUDataFrame
except (ImportError, OSError):
    GPUDataFrame = None

__all__ = ('Backend', "__version__")


class Backend(BaseSQLBackend):
    """When the backend is loaded, this class becomes `ibis.omniscidb`."""

    name = 'omniscidb'
    database_class = Database
    table_expr_class = OmniSciDBTable
    compiler = OmniSciDBCompiler
    db_name: str | None
    con: pyomnisci.Connection

    def __del__(self):
        """Close the connection when instance is deleted."""
        if hasattr(self, 'con') and self.con:
            self.close()

    def __enter__(self, **kwargs):
        """Update internal attributes when using `with` statement."""
        self.__dict__.update(**kwargs)
        return self

    def __exit__(self, *args):
        """Close the connection when exits the `with` statement."""
        self.close()

    def connect(
        self,
        uri: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = 6274,
        database: Optional[str] = None,
        protocol: str = 'binary',
        session_id: Optional[str] = None,
        ipc: Optional[bool] = None,
        gpu_device: Optional[int] = None,
    ):
        """Create a client for OmniSciDB backend.

        Parameters
        ----------
        uri : str, optional
        user : str, optional
        password : str, optional
        host : str, optional
        port : int, default 6274
        database : str, optional
        protocol : {'binary', 'http', 'https'}, default 'binary'
        session_id: str, optional
        ipc : bool, optional
          Enable Inter Process Communication (IPC) execution type.
          `ipc` default value is False when `gpu_device` is None, otherwise
          its default value is True.
        gpu_device : int, optional
          GPU Device ID.

        Returns
        -------
        OmniSciDBClient
        """
        new_backend = self.__class__()
        new_backend.uri = uri
        new_backend.user = user
        new_backend.password = password
        new_backend.host = host
        new_backend.port = port
        new_backend.db_name = database
        new_backend.protocol = protocol
        new_backend.session_id = session_id

        self._check_execution_type(ipc=ipc, gpu_device=gpu_device)

        new_backend.ipc = ipc
        new_backend.gpu_device = gpu_device

        if session_id:
            kwargs = {'sessionid': session_id}
        elif uri is not None:
            kwargs = {}
        elif (
            user is not None and password is not None and database is not None
        ):
            kwargs = {'user': user, 'password': password, 'dbname': database}
        else:
            raise ValueError(
                'If `session_id` is not provided, then the connection `uri` '
                'or all `user`, `password` and `database` must be provided.'
            )

        new_backend.con = pyomnisci.connect(
            uri=uri, host=host, port=port, protocol=protocol, **kwargs
        )

        # used for UDF
        new_backend.udf = OmniSciDBUDF(
            host=new_backend.con._host,
            port=new_backend.con._port,
            database=new_backend.con._dbname,
            user=new_backend.con._user,
            password=new_backend.con._password,
        )
        return new_backend

    def close(self):
        """Close OmniSciDB connection and drop any temporary objects."""
        self.con.close()

    def _adapt_types(self, descr):
        names = []
        adapted_types = []

        for col in descr:
            names.append(col.name)
            col_type = OmniSciDBDataType._omniscidb_to_ibis_dtypes[col.type]
            col_type.nullable = col.nullable
            adapted_types.append(col_type)
        return names, adapted_types

    def _check_execution_type(
        self, ipc: Optional[bool], gpu_device: Optional[int]
    ):
        """
        Check if the execution type (ipc and gpu_device) is valid.

        Parameters
        ----------
        ipc : bool, optional
        gpu_device : int, optional

        Raises
        ------
        com.IbisInputError
            if "gpu_device" is not None and "ipc" is False
        """
        if gpu_device is not None and ipc is False:
            raise com.IbisInputError(
                'If GPU device is provided, IPC parameter should '
                'be True or None (default).'
            )

    def _fully_qualified_name(self, name, database):
        # OmniSciDB raises error sometimes with qualified names
        return name

    def _get_list(self, cur):
        tuples = cur.cursor.fetchall()
        return [v[0] for v in tuples]

    def _get_schema_using_query(self, query):
        result = self.raw_sql(query)
        # resets the state of the cursor and closes operation
        result.cursor.fetchall()
        names, ibis_types = self._adapt_types(
            _extract_column_details(result.cursor._result.row_set.row_desc)
        )

        return sch.Schema(names, ibis_types)

    def _get_schema_using_validator(self, query):
        result = self.con._client.sql_validate(self.con._session, query)
        return sch.Schema.from_tuples(
            (
                r.col_name,
                OmniSciDBDataType._omniscidb_to_ibis_dtypes[
                    pyomnisci_dtype._VALUES_TO_NAMES[r.col_type.type]
                ],
            )
            for r in result
        )

    def _get_table_schema(self, table_name, database=None):
        """Get table schema.

        Parameters
        ----------
        table_name : str
        database : str

        Returns
        -------
        schema : ibis Schema
        """
        table_name_ = table_name.split('.')
        if len(table_name_) == 2:
            database, table_name = table_name_
        return self.get_schema(table_name, database)

    def raw_sql(
        self,
        query: str,
        ipc: Optional[bool] = None,
        gpu_device: Optional[int] = None,
        **kwargs,
    ):
        """
        Compile and execute Ibis expression.

        Return result in-memory in the appropriate object type.

        Parameters
        ----------
        query : string
          DML or DDL statement
        results : boolean, default False
          Pass True if the query as a result set
        ipc : bool, optional, default None
          Enable Inter Process Communication (IPC) execution type.
          `ipc` default value (None) when `gpu_device` is None is interpreted
           as False, otherwise it is interpreted as True.
        gpu_device : int, optional, default None
          GPU device ID.

        Returns
        -------
        output : execution type dependent
          If IPC is set as True and no GPU device is set:
            ``pandas.DataFrame``
          If IPC is set as True and GPU device is set: ``cudf.DataFrame``
          If IPC is set as False and no GPU device is set:
            pandas.DataFrame or
            geopandas.GeoDataFrame (if it uses geospatial data)

        Raises
        ------
        Exception
            if execution method fails.
        """
        # time context is not implemented for omniscidb yet
        kwargs.pop('timecontext', None)
        # raise an Exception if kwargs is not empty:
        if kwargs:
            raise com.IbisInputError(
                '"OmniSciDB.execute" method just support the follow parameter:'
                ' "query", "results", "ipc" and "gpu_device". The follow extra'
                ' parameters was given: "{}".'.format(', '.join(kwargs.keys()))
            )

        if isinstance(query, (DDL, DML)):
            query = query.compile()

        if ipc is None and gpu_device is None:
            ipc = self.ipc
            gpu_device = self.gpu_device

        self._check_execution_type(ipc, gpu_device)

        params = {}

        if gpu_device is None:
            execute = self.con.select_ipc if ipc else self.con.cursor().execute
        else:
            params['device_id'] = gpu_device
            execute = self.con.select_ipc_gpu

        cursor = get_cursor_class(use_gpu=gpu_device is not None)

        try:
            result = cursor(execute(query, **params))
        except Exception as e:
            e.args = (f'{e.args[0]}\n\n{query}',)
            raise e

        return result

    def ast_schema(self, query_ast, ipc=None, gpu_device=None):
        """Allow ipc and gpu_device params, used in OmniSciDB `execute`."""
        return super().ast_schema(query_ast)

    def fetch_from_cursor(self, cursor, schema):
        """Fetch OmniSciDB cursor and return a dataframe."""
        result = cursor.to_df()
        # TODO: try to use `apply_to` for cudf.DataFrame using cudf 0.9
        if GPUDataFrame is None or not isinstance(result, GPUDataFrame):
            return schema.apply_to(result)
        else:
            return result

    def create_database(self, name, owner=None):
        """
        Create a new OmniSciDB database.

        Parameters
        ----------
        name : string
          Database name
        """
        statement = ddl.CreateDatabase(name, owner=owner)
        self.raw_sql(statement)

    def describe_formatted(self, name: str) -> pd.DataFrame:
        """Describe a given table name.

        Parameters
        ----------
        name : string

        Returns
        -------
        pandas.DataFrame
        """
        return pd.DataFrame(
            [
                (
                    col.name,
                    OmniSciDBDataType.parse(col.type),
                    col.nullable,
                    col.precision,
                    col.scale,
                    col.comp_param,
                    col.encoding,
                )
                for col in self.con.get_table_details(name)
            ],
            columns=[
                'column_name',
                'nullable',
                'type',
                'precision',
                'scale',
                'comp_param',
                'encoding',
            ],
        )

    def drop_database(self, name, force=False):
        """
        Drop an OmniSciDB database.

        Parameters
        ----------
        name : string
          Database name
        force : boolean, default False
          If False and there are any tables in this database, raises an
          IntegrityError

        Raises
        ------
        ibis.common.exceptions.IntegrityError
            if given database has tables and force is not define as True
        """
        tables = []

        if not force or self.database(name):
            tables = self.list_tables(database=name)

        if not force and len(tables):
            raise com.IntegrityError(
                'Database {0} must be empty before being dropped, or set '
                'force=True'.format(name)
            )
        statement = ddl.DropDatabase(name)
        self.raw_sql(statement)

    def create_user(self, name, password, is_super=False):
        """
        Create a new OmniSciDB user.

        Parameters
        ----------
        name : string
          User name
        password : string
          Password
        is_super : bool
          if user is a superuser
        """
        statement = ddl.CreateUser(
            name=name, password=password, is_super=is_super
        )
        self.raw_sql(statement)

    def alter_user(
        self, name, password=None, is_super=None, insert_access=None
    ):
        """
        Alter OmniSciDB user parameters.

        Parameters
        ----------
        name : string
          User name
        password : string
          Password
        is_super : bool
          If user is a superuser
        insert_access : string
          If users need to insert records to a database they do not own,
          use insert_access property to give them the required privileges.
        """
        statement = ddl.AlterUser(
            name=name,
            password=password,
            is_super=is_super,
            insert_access=insert_access,
        )
        self.raw_sql(statement)

    def drop_user(self, name):
        """
        Drop a given user.

        Parameters
        ----------
        name : string
          User name
        """
        statement = ddl.DropUser(name)
        self.raw_sql(statement)

    def create_view(self, name, expr, database=None):
        """
        Create a view with a given name from a table expression.

        Parameters
        ----------
        name : string
        expr : ibis TableExpr
        database : string, optional
        """
        ast = self.compiler.to_ast(expr)
        select = ast.queries[0]
        statement = ddl.CreateView(name, select, database=database)
        self.raw_sql(statement)

    def drop_view(self, name, database=None, force: bool = False):
        """
        Drop a given view.

        Parameters
        ----------
        name : string
        database : string, default None
        force : boolean, default False
          Database may throw exception if table does not exist
        """
        statement = ddl.DropView(name, database=database, must_exist=not force)
        self.raw_sql(statement)

    def create_table(
        self,
        table_name: str,
        obj: Optional[Union[ir.TableExpr, pd.DataFrame]] = None,
        schema: Optional[sch.Schema] = None,
        database: Optional[str] = None,
        max_rows: Optional[int] = None,
        fragment_size: Optional[int] = None,
        is_temporary: bool = False,
        **kwargs,
    ):
        """
        Create a new table from an Ibis table expression.

        Parameters
        ----------
        table_name : string
        obj : ibis.expr.types.TableExpr or pandas.DataFrame, optional
          If passed, creates table from select statement results
        schema : ibis.Schema, optional
        table_name : str
        obj : TableExpr or pandas.DataFrame, optional, default None
          If passed, creates table from select statement results.
        schema : ibis.Schema, optional, default None
          Mutually exclusive with expr, creates an empty table with a
          particular schema
        database : str, optional, default None
        max_rows : int, optional, default None
          Set the maximum number of rows allowed in a table to create a capped
          collection. When this limit is reached, the oldest fragment is
          removed.
        fragment_size: int, optional,
          default 32000000 if gpu_device is enabled otherwise 5000000
          Number of rows per fragment that is a unit of the table for query
          processing, which is not expected to be changed.
        is_temporary : bool, default False
            If True it the table will be created as temporary.

        Examples
        --------
        >>> con.create_table('new_table_name', table_expr)  # doctest: +SKIP
        """
        _database = self.db_name
        self.set_database(database)

        if fragment_size is None:
            fragment_size = 32000000 if self.gpu_device else 5000000

        if obj is not None:
            if isinstance(obj, pd.DataFrame):
                raise NotImplementedError(
                    'Pandas Data Frame input not implemented.'
                )
            else:
                to_insert = obj
            ast = self.compiler.to_ast(to_insert)
            select = ast.queries[0]

            statement = ddl.CTAS(table_name, select, database=database)
        elif schema is not None:
            statement = ddl.CreateTableWithSchema(
                table_name,
                schema,
                database=database,
                max_rows=max_rows,
                fragment_size=fragment_size,
                is_temporary=is_temporary,
            )
        else:
            raise com.IbisError('Must pass expr or schema')

        self.raw_sql(statement)
        self.set_database(_database)

    def drop_table(self, table_name, database=None, force=False):
        """
        Drop a given table.

        Parameters
        ----------
        table_name : string
        database : string, default None (optional)
        force : boolean, default False
          Database may throw exception if table does not exist

        Examples
        --------
        >>> table = 'my_table'
        >>> db = 'operations'
        >>> con.drop_table(table, database=db, force=True)  # doctest: +SKIP
        """
        _database = self.db_name
        self.set_database(database)

        statement = ddl.DropTable(
            table_name, database=database, must_exist=not force
        )
        self.raw_sql(statement)
        self.set_database(_database)

    def truncate_table(self, table_name, database=None):
        """
        Delete all rows from, but do not drop, an existing table.

        Parameters
        ----------
        table_name : string
        database : string, optional
        """
        statement = ddl.TruncateTable(table_name, database=database)
        self.execute(statement)

    def drop_table_or_view(
        self, name: str, database: str = None, force: bool = False
    ):
        """Attempt to drop a relation that may be a view or table.

        Parameters
        ----------
        name : str
        database : str, optional
        force : bool, optional

        Raises
        ------
        Exception
            if the drop operation fails.
        """
        try:
            self.drop_table(name, database=database)
        except Exception as e:
            try:
                self.drop_view(name, database=database)
            except Exception:
                raise e

    def load_data(
        self,
        table_name: str,
        obj: Union[pd.DataFrame, pyarrow.Table],
        database: Optional[str] = None,
        method: str = 'rows',
    ):
        """
        Load data into a given table.

        Wraps the LOAD DATA DDL statement. Loads data into an OmniSciDB table
        by physically moving data files.

        Parameters
        ----------
        table_name : string
        obj: pandas.DataFrame or pyarrow.Table
        database : string, optional
        method : string, {‘infer’, ‘columnar’, ‘rows’, ‘arrow’}, default 'rows'
            The Arrow loader is typically the fastest, followed by the columnar
            loader, followed by the row-wise loader.
        """
        _database = self.db_name
        self.set_database(database)
        self.con.load_table(table_name, obj, method=method)
        self.set_database(_database)

    @property
    def current_database(self):
        """Get the current database name."""
        return self.con._client.get_session_info(self.con._session).database

    def set_database(self, name: Optional[str]):
        """Set a given database for the current connect.

        Parameters
        ----------
        name : string
        """
        if self.db_name != name and name is not None:
            self.con.close()
            self.con = pyomnisci.connect(
                uri=self.uri,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                dbname=name,
                protocol=self.protocol,
                sessionid=self.session_id,
            )
            self.db_name = name

    @com.mark_as_unsupported
    def exists_database(self, name: str):
        """Check if the given database exists.

        Parameters
        ----------
        name : str

        Raises
        ------
        NotImplementedError
            Method not supported yet.
        """

    def list_databases(self, like: str = None) -> list:
        """List all databases.

        Parameters
        ----------
        like : str, optional

        Returns
        -------
        list
        """
        dbs = [
            d.db_name
            for d in self.con._client.get_databases(self.con._session)
        ]
        if like is None:
            return dbs
        pattern = re.compile(like)
        return list(filter(lambda t: pattern.findall(t), dbs))

    def list_tables(self, like: str = None, database: str = None) -> list:
        """List all tables inside given or current database.

        Parameters
        ----------
        like : str, optional
        database : str, optional

        Returns
        -------
        list
        """
        _database = None

        if not self.db_name == database:
            _database = self.db_name
            self.set_database(database)

        tables = self.con.get_tables()

        if _database:
            self.set_database(_database)

        if like is None:
            return tables
        pattern = re.compile(like)
        return list(filter(lambda t: pattern.findall(t), tables))

    def get_schema(self, table_name, database=None):
        """
        Return a Schema object for the given table and database.

        Parameters
        ----------
        table_name : string
          May be fully qualified
        database : string, default None

        Returns
        -------
        schema : ibis Schema
        """
        cols = {
            col.name: omniscidb_dtypes.sql_to_ibis_dtypes[col.type](
                nullable=col.nullable
            )
            for col in self.con.get_table_details(table_name)
        }

        return sch.schema([(name, tp) for name, tp in cols.items()])

    def sql(self, query: str):
        """
        Convert a SQL query to an Ibis table expression.

        Parameters
        ----------
        query : string

        Returns
        -------
        table : TableExpr
        """
        # Remove `;` + `--` (comment)
        query = re.sub(r'\s*;\s*--', '\n--', query.strip())
        # Remove trailing ;
        query = re.sub(r'\s*;\s*$', '', query.strip())
        schema = self._get_schema_using_validator(query)
        return ops.SQLQueryResult(query, schema, self).to_expr()

    @property
    def version(self):
        """Return the backend library version.

        Returns
        -------
        string
            Version of the backend library.
        """
        return pyomnisci.__version__


def connect(
    uri: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = 6274,
    database: Optional[str] = None,
    protocol: str = 'binary',
    session_id: Optional[str] = None,
    ipc: Optional[bool] = None,
    gpu_device: Optional[int] = None,
):
    warnings.warn(
        '`ibis_omniscidb.connect(...)` is deprecated and will be removed in '
        'a future version. Use `ibis.omniscidb.connect(...)` instead.',
        FutureWarning,
    )
    return Backend().connect(
        uri,
        user,
        password,
        host,
        port,
        database,
        protocol,
        session_id,
        ipc,
        gpu_device,
    )
