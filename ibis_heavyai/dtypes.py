"""HeavyDB data types and SQL dtypes compatibility."""
import ibis.expr.datatypes as dt

sql_to_ibis_dtypes = {
    'BIGINT': dt.int64,
    'BOOL': dt.Boolean,
    'DATE': dt.date,
    'DECIMAL': dt.Decimal(18, 9),
    'DOUBLE': dt.double,
    'FLOAT': dt.float32,
    'INT': dt.int32,
    'LINESTRING': dt.linestring,
    'MULTIPOLYGON': dt.multipolygon,
    'NULL': dt.Null,
    'NUMERIC': dt.Decimal(18, 9),
    'POINT': dt.point,
    'POLYGON': dt.polygon,
    'SMALLINT': dt.int16,
    'STR': dt.string,
    'TIME': dt.time,
    'TIMESTAMP': dt.timestamp,
    'TINYINT': dt.int8,
}


sql_to_ibis_dtypes_str = {
    'BIGINT': 'int64',
    'BOOLEAN': 'Boolean',
    'BOOL': 'Boolean',
    'CHAR': 'string',
    'DATE': 'date',
    'DECIMAL': 'decimal',
    'DOUBLE': 'double',
    'INT': 'int32',
    'INTEGER': 'int32',
    'FLOAT': 'float32',
    'NUMERIC': 'float64',
    'REAL': 'float32',
    'SMALLINT': 'int16',
    'STR': 'string',
    'TEXT': 'string',
    'TIME': 'time',
    'TIMESTAMP': 'timestamp',
    'TINYINT': 'int8',
    'VARCHAR': 'string',
    'POINT': 'point',
    'LINESTRING': 'linestring',
    'POLYGON': 'polygon',
    'MULTIPOLYGON': 'multipolygon',
}


ibis_dtypes_str_to_sql = {
    'boolean': 'boolean',
    'date': 'date',
    'decimal': 'decimal',
    'double': 'double',
    'float32': 'float',
    'float64': 'double',
    'int8': 'tinyint',
    'int16': 'smallint',
    'int32': 'int',
    'int64': 'bigint',
    'linestring': 'linestring',
    'multipolygon': 'multipolygon',
    'point': 'point',
    'polygon': 'polygon',
    'string': 'text',
    'time': 'time',
    'timestamp': 'timestamp',
}


# example: {dt.int64: 'int64'}
ibis_dtypes_to_str = {}
for sql_type, ibis_type in sql_to_ibis_dtypes.items():
    ibis_dtypes_to_str[ibis_type] = str(ibis_type)
