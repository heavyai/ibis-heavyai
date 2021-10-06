"""Ibis OmniSciDB Client."""
from pathlib import Path
from typing import Optional, Union

import ibis.common.exceptions as com
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
import pandas as pd
import pyarrow
import regex as re
from omnisci.cursor import Cursor

from . import ddl
from . import dtypes as omniscidb_dtypes

fully_qualified_re = re.compile(r"(.*)\.(?:`(.*)`|(.*))")


def _validate_compatible(from_schema, to_schema):
    if set(from_schema.names) != set(to_schema.names):
        raise com.IbisInputError('Schemas have different names')

    for name in from_schema:
        lt = from_schema[name]
        rt = to_schema[name]
        if not lt.castable(rt):
            raise com.IbisInputError(
                'Cannot safely cast {0!r} to {1!r}'.format(lt, rt)
            )
    return


class OmniSciDBDataType:
    """OmniSciDB Backend Data Type."""

    __slots__ = 'typename', 'nullable'

    # using impala.client._HS2_TTypeId_to_dtype as reference
    dtypes = omniscidb_dtypes.sql_to_ibis_dtypes
    ibis_dtypes = {v: k for k, v in dtypes.items()}
    _omniscidb_to_ibis_dtypes = omniscidb_dtypes.sql_to_ibis_dtypes_str

    def __init__(self, typename, nullable=True):
        if typename not in self.dtypes:
            raise com.UnsupportedBackendType(typename)
        self.typename = typename
        self.nullable = nullable

    def __str__(self):
        """Return the data type name."""
        if self.nullable:
            return 'Nullable({})'.format(self.typename)
        else:
            return self.typename

    def __repr__(self):
        """Return the backend name and the datatype name."""
        return '<OmniSciDB {}>'.format(str(self))

    @classmethod
    def parse(cls, spec: str):
        """Return a OmniSciDBDataType related to the given data type name.

        Parameters
        ----------
        spec : string

        Returns
        -------
        OmniSciDBDataType
        """
        if spec.startswith('Nullable'):
            return cls(spec[9:-1], nullable=True)
        else:
            return cls(spec)

    def to_ibis(self):
        """
        Return the Ibis data type correspondent to the current OmniSciDB type.

        Returns
        -------
        ibis.expr.datatypes.DataType
        """
        return self.dtypes[self.typename](nullable=self.nullable)

    @classmethod
    def from_ibis(cls, dtype, nullable=None):
        """
        Return a OmniSciDBDataType correspondent to the given Ibis data type.

        Parameters
        ----------
        dtype : ibis.expr.datatypes.DataType
        nullable : bool

        Returns
        -------
        OmniSciDBDataType

        Raises
        ------
        NotImplementedError
            if the given data type was not implemented.
        """
        dtype_ = type(dtype)
        if dtype_ in cls.ibis_dtypes:
            typename = cls.ibis_dtypes[dtype_]
        elif dtype in cls.ibis_dtypes:
            typename = cls.ibis_dtypes[dtype]
        else:
            raise NotImplementedError('{} dtype not implemented'.format(dtype))

        if nullable is None:
            nullable = dtype.nullable
        return cls(typename, nullable=nullable)


class OmniSciDBDefaultCursor:
    """Default cursor that exports a result to Pandas Data Frame."""

    def __init__(self, cursor):
        self.cursor = cursor

    def to_df(self):
        """Convert the cursor to a data frame.

        Returns
        -------
        dataframe : pandas.DataFrame
        """
        if isinstance(self.cursor, Cursor):
            col_names = [c.name for c in self.cursor.description]
            result = pd.DataFrame(self.cursor.fetchall(), columns=col_names)
        elif self.cursor is None:
            result = pd.DataFrame([])
        else:
            result = self.cursor

        return result

    def __enter__(self):
        """For compatibility when constructed from Query.execute()."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit when using `with` statement."""
        pass

    def release(self):
        """Releasing the cursor once it's not needed."""
        pass


class OmniSciDBGPUCursor(OmniSciDBDefaultCursor):
    """Cursor that exports result to GPU Dataframe."""

    def to_df(self):
        """
        Return the result as a data frame.

        Returns
        -------
        dataframe : cudf.DataFrame
        """
        return self.cursor


def get_cursor_class(use_gpu: bool):
    """Return a cursor of the appropriate type.

    If `use_gpu` is True, the cursor will be a GPU compatible
    cursor. Otherwise, if geo dependencies are installed a
    geo compatible cursor, or if not, a default cursor.
    """
    if use_gpu:
        return OmniSciDBGPUCursor

    try:
        from .geo import OmniSciDBGeoCursor
    except ImportError:
        return OmniSciDBDefaultCursor
    else:
        return OmniSciDBGeoCursor


class OmniSciDBTable(ir.TableExpr):
    """References a physical table in the OmniSciDB metastore."""

    @property
    def _qualified_name(self):
        return self.op().args[0]

    @property
    def _unqualified_name(self):
        return self._match_name()[1]

    @property
    def _client(self):
        return self.op().args[2]

    def _match_name(self):
        m = ddl.fully_qualified_re.match(self._qualified_name)
        if not m:
            raise com.IbisError(
                'Cannot determine database name from {0}'.format(
                    self._qualified_name
                )
            )
        db, quoted, unquoted = m.groups()
        return db, quoted or unquoted

    @property
    def _database(self):
        return self._match_name()[0]

    @com.mark_as_unsupported
    def invalidate_metadata(self):
        """Invalidate table metadata.

        Raises
        ------
        common.exceptions.UnsupportedOperationError
        """

    @com.mark_as_unsupported
    def refresh(self):
        """Refresh table metadata.

        Raises
        ------
        common.exceptions.UnsupportedOperationError
        """

    def metadata(self):
        """
        Return parsed results of DESCRIBE FORMATTED statement.

        Returns
        -------
        metadata : pandas.DataFrame
        """
        return pd.DataFrame(
            [
                (
                    col.name,
                    OmniSciDBDataType.parse(col.type),
                    col.precision,
                    col.scale,
                    col.comp_param,
                    col.encoding,
                )
                for col in self._client.con.get_table_details(
                    self._qualified_name
                )
            ],
            columns=[
                'column_name',
                'type',
                'precision',
                'scale',
                'comp_param',
                'encoding',
            ],
        )

    describe_formatted = metadata

    def truncate(self):
        """Delete all rows from, but do not drop, an existing table."""
        self._client.truncate_table(self._qualified_name)

    def load_data(self, df: Union[pd.DataFrame, pyarrow.Table]):
        """
        Load a data frame into database.

        Wraps the LOAD DATA DDL statement. Loads data into an OmniSciDB table
        from pandas.DataFrame or pyarrow.Table

        Parameters
        ----------
        df: pandas.DataFrame or pyarrow.Table

        Returns
        -------
        query : OmniSciDBQuery
        """
        stmt = ddl.LoadData(self._qualified_name, df)
        return self._client.raw_sql(stmt)

    def read_csv(
        self,
        path: Union[str, Path],
        header: Optional[bool] = True,
        quotechar: Optional[str] = '"',
        delimiter: Optional[str] = ',',
        threads: Optional[int] = None,
    ):
        """
        Load data into an Omniscidb table from CSV file.

        Wraps the COPY FROM DML statement.

        Parameters
        ----------
        path: str or pathlib.Path
          Path to the input data file
        header: bool, optional, default True
          Indicating whether the input file has a header line
        quotechar: str, optional, default '"'
          The character used to denote the start and end of a quoted item.
        delimiter: str, optional, default ','
        threads: int, optional, default number of CPU cores on the system
          Number of threads for performing the data import.

        Returns
        -------
        query : OmniSciDBQuery

        Examples
        --------
        # assumptions:
        #   - dataset can be found on ./datasets/functional_alltypes.csv
        #       https://github.com/ibis-project/testing-data/blob/master/functional_alltypes.csv
        #   - omnisci server is launched on localhost and using port: 6274

        import ibis

        conn = ibis.omniscidb.connect(
            host="localhost",
            port="6274",
            user="admin",
            password="HyperInteractive",
        )

        t_name = "functional_alltypes"
        db_name = "ibis_testing"
        filename = "./datasets/functional_alltypes.csv"

        schema = ibis.schema(
            [
                ('index', 'int64'),
                ('Unnamed__0', 'int64'),
                ('id', 'int32'),
                ('bool_col', 'bool'),
                ('tinyint_col', 'int16'),
                ('smallint_col', 'int16'),
                ('int_col', 'int32'),
                ('bigint_col', 'int64'),
                ('float_col', 'float32'),
                ('double_col', 'double'),
                ('date_string_col', 'string'),
                ('string_col', 'string'),
                ('timestamp_col', 'timestamp'),
                ('year_', 'int32'),
                ('month_', 'int32'),
            ]
        )
        conn.create_table(t_name, schema=schema)

        db = conn.database(db_name)
        table = db.table(t_name)
        table.read_csv(filename, header=False, quotechar='"', delimiter=",")
        """
        kwargs = {
            'header': header,
            # 'quote' field couldn't be empty string for omnisci backend
            'quote': quotechar if quotechar else '"',
            'quoted': bool(quotechar),
            'delimiter': delimiter,
            'threads': threads,
        }
        stmt = ddl.LoadData(self._qualified_name, path, **kwargs)
        return self._client.raw_sql(stmt)

    @property
    def name(self) -> str:
        """Return the operation name.

        Returns
        -------
        str
        """
        return self.op().name

    def rename(self, new_name, database=None):
        """
        Rename table to a given name.

        Parameters
        ----------
        new_name : string
        database : string

        Returns
        -------
        renamed : OmniSciDBTable
        """
        statement = ddl.RenameTable(self._qualified_name, new_name)

        self._client.raw_sql(statement)

        op = self.op().change_name(statement.new_qualified_name)
        return type(self)(op)

    def alter(self, tbl_properties=None):
        """
        Change setting and parameters of the table.

        Raises
        ------
        NotImplementedError
            Method is not implemented yet.
        """
        raise NotImplementedError('This method is not implemented yet!')

    def _alter_table_helper(self, f, **alterations):
        results = []
        for k, v in alterations.items():
            if v is None:
                continue
            result = f(**{k: v})
            results.append(result)
        return results

    def add_columns(
        self,
        cols_with_types: dict,
        nullables: Optional[list] = None,
        defaults: Optional[list] = None,
        encodings: Optional[list] = None,
    ):
        """
        Add a given column(s).

        Parameters
        ----------
        cols_with_types : dict
            Set dict of column(s) with type(s) to add into table,
            where key is column name and value is column type
        nullables : list, optional
            Set list of boolean values for new columns
            that could be nullable/not nullable, by default None (i.e
            all the new columns added are nullables)
        defaults : list, optional
            Set list of default values for the new columns, by default None
        encodings : list, optional
            Set list of strings of encoding attributes
            for the new columns, by default None

        Examples
        --------
        >>> table_name = 'my_table'
        >>> my_table = con.table(table_name)  # doctest: +SKIP
        >>> cols_with_types = {'col1': 'int32', 'col2': 'string',
        ... 'col3': 'float', 'col4': 'point'}
        >>> nullables = [True, True, False, True]
        >>> defaults = [1, None, None, 'point(0 0)']
        >>> encodings = ['', 'DICT', '', '']
        >>> my_table.add_columns(cols_with_types,
        ... nullables=nullables, defaults=defaults,
        ... encodings=encodings)  # doctest: +SKIP
        """
        statement = ddl.AddColumns(
            self._qualified_name,
            cols_with_types,
            nullables=nullables,
            defaults=defaults,
            encodings=encodings,
        )
        self._client.raw_sql(statement)

    def drop_columns(self, column_names: list):
        """
        Drop a given column(s).

        Parameters
        ----------
        column_names : list
            Set list of column's names to drop from table

        Examples
        --------
        >>> table_name = 'my_table'
        >>> my_table = con.table(table_name)  # doctest: +SKIP
        >>> column_names = ['col1', 'col2']
        >>> my_table.drop_columns(column_names)  # doctest: +SKIP
        """
        statement = ddl.DropColumns(self._qualified_name, column_names)
        self._client.raw_sql(statement)


@dt.dtype.register(OmniSciDBDataType)
def omniscidb_to_ibis_dtype(omniscidb_dtype):
    """
    Register OmniSciDB Data Types.

    Parameters
    ----------
    omniscidb_dtype : OmniSciDBDataType

    Returns
    -------
    ibis.expr.datatypes.DataType
    """
    return omniscidb_dtype.to_ibis()
