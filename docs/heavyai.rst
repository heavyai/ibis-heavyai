.. _heavyai:

*******
HeavyDB
*******

To get started with the HeavyDB client, check the :ref:`HeavyDB quick start <install.heavyai>`.

For the API documentation, visit the :ref:`HeavyDB API <api.heavyai>`.

.. _install.heavyai:

`heavyai <https://heavy.ai>`_ Quickstart
==================================================

Install dependencies for Ibis's HeavyDB dialect:

::

  pip install ibis-heavyai

Create a client by passing in database connection parameters such as ``host``,
``port``, ``database``,  ``user`` and ``password`` to
:func:`ibis.heavyai.connect`:

.. code-block:: python

   con = ibis.heavyai.connect(
       host='localhost',
       database='ibis_testing',
       user='admin',
       password='HyperInteractive',
   )


.. note::

   HeavyDB backend support HeavyDB version greater than or equals to `5.3.0`.

.. _api.heavyai:

API
===
.. currentmodule:: ibis.backends.heavyai

The HeavyDB client is accessible through the ``ibis.heavyai`` namespace.

Use ``ibis.heavyai.connect`` to create a client.

.. autosummary::
   :toctree: ../generated/

   compile
   connect
   verify
   HeavyDBClient.alter_user
   HeavyDBClient.close
   HeavyDBClient.create_database
   HeavyDBClient.create_table
   HeavyDBClient.create_user
   HeavyDBClient.create_view
   HeavyDBClient.database
   HeavyDBClient.describe_formatted
   HeavyDBClient.drop_database
   HeavyDBClient.drop_table
   HeavyDBClient.drop_table_or_view
   HeavyDBClient.drop_user
   HeavyDBClient.drop_view
   HeavyDBClient.exists_table
   HeavyDBClient.get_schema
   HeavyDBClient.list_tables
   HeavyDBClient.load_data
   HeavyDBClient.log
   HeavyDBClient.set_database
   HeavyDBClient.sql
   HeavyDBClient.table
   HeavyDBClient.truncate_table
   HeavyDBClient.version

Backend internals
=================

In this document it would be explained the main aspects of `ibis` and
`heavyai` backend implementation.

Modules
-------

The `heavyai` backend has 7 modules, the main three are:

- `api`
- `client`
- `compiler`

The `identifiers` and `operations` modules complement the `compiler` module.

api
---

`api` module is the central key to access the backend. Ensure to include
the follow code into `ibis.__init__`:

.. code-block:: python

    with suppress(ImportError):
        # pip install ibis-heavyai
        from ibis.backends import heavyai

There are three functions in the `api` module:

- :func:`~ibis.heavyai.api.compile`
- :func:`~ibis.heavyai.api.connect`
- :func:`~ibis.heavyai.api.verify`

:func:`~ibis.heavyai.api.compile` method compiles an `ibis` expression into `SQL`:

.. code-block:: python

    t = heavydb_cli.table('flights_2008_10k')
    proj = t['arrtime', 'arrdelay']
    print(ibis.heavyai.compile(proj))

:func:`~ibis.heavyai.api.connect` method instantiates a :class:`~ibis.heavyai.api.HeavyDBClient` object that connect to the specified
`heavyai` database:

.. code-block:: python

    heavydb_cli = ibis.heavyai.connect(
        host='localhost', user='admin', password='HyperInteractive',
        port=6274, database='heavyai'
    )

:func:`~ibis.heavyai.api.verify` method checks if the `ibis` expression can be compiled:

.. code-block:: python

    t = heavydb_cli.table('flights_2008_10k')
    proj = t['arrtime', 'arrdelay']
    assert ibis.heavyai.verify(proj) == True

client
------

`client` module contains the main classes to handle the connection to an `heavyai`
database.

The main classes are:

- :class:`~ibis.heavyai.api.HeavyDBClient`
- `HeavyDBQuery`
- `HeavyDBDataType`
- `HeavyDBDefaultCursor`

`HeavyDBDataType` class is used to translate data type from `ibis` and to `ibis`.
Its main methods are:

- `parse`
- `to_ibis`
- `from_ibis`

:class:`~ibis.heavyai.api.HeavyDBClient` class is used to connect to an `heavyai` database and manipulate data
expressions. Its main methods are:

- `__init__`
- `_build_ast`
- `_execute`
- `_fully_qualified_name`
- `_get_table_schema`
- `_table_expr_klass`
- :func:`~ibis.heavyai.api.HeavyDBClient.log`
- :func:`~ibis.heavyai.api.HeavyDBClient.close`
- :func:`~ibis.heavyai.api.HeavyDBClient.database`
- `current_database`
- :func:`~ibis.heavyai.api.HeavyDBClient.set_database`
- `exists_database`
- `list_databases`
- :func:`~ibis.heavyai.api.HeavyDBClient.exists_table`
- :func:`~ibis.heavyai.api.HeavyDBClient.list_tables`
- :func:`~ibis.heavyai.api.HeavyDBClient.get_schema`
- :func:`~ibis.heavyai.api.HeavyDBClient.version`

`_build_ast` method is required.

`HeavyDBQuery` class should define at least the `_fetch` method. If `Query`
class is used when the `HeavyDBClient.execute` method is called, an exception
is raised.

    (...) once the data arrives from the database we need to convert that data
    to a pandas DataFrame.

    The Query class, with its _fetch() method, provides a way for ibis
    SQLClient objects to do any additional processing necessary after
    the database returns results to the client.
    (http://docs.ibis-project.org/design.html#execution)

Under the hood the `execute` method, uses a cursor class that will fetch the
result from the database and load it to a dataframe format (e.g. pandas, GeoPandas, cuDF).

compiler
--------

The main classes inside `compiler` module are:

- `HeavyDBDialect`
- `HeavyDBExprTranslator`
- `HeavyDBQueryBuilder`
- `HeavyDBSelect`
- `HeavyDBSelectBuilder`
- `HeavyDBTableSetFormatter`

operations
----------

    `Node` subclasses make up the core set of operations of ibis.
    Each node corresponds to a particular operation.
    Most nodes are defined in the `operations` module.
    (http://docs.ibis-project.org/design.html#the-node-class).


Creating new expression: To create new expressions it is necessary to do these
steps:

1. create a new class
2. create a new function and assign it to a DataType
3. create a compiler function to this new function and assign it to the compiler translator

A new Class database function would be like this (`my_backend_operations.py`):

.. code-block:: python

    class MyNewFunction(ops.Unary):
        """My new class function"""
        output_type = rlz.shape_like('arg', 'float')

After creating the new class database function, the follow step is to create a
function and assign it to the DataTypes allowed to use it:

.. code-block:: python

    def my_new_function(numeric_value):
        return MyNewFunction(numeric_value).to_expr()


    NumericValue.sin = sin

Also, it is necessary to register the new function:

.. code-block:: python

    # if it necessary define the fixed_arity function
    def fixed_arity(func_name, arity):
        def formatter(translator, expr):
            op = expr.op()
            arg_count = len(op.args)
            if arity != arg_count:
                msg = 'Incorrect number of args {0} instead of {1}'
                raise com.UnsupportedOperationError(
                    msg.format(arg_count, arity)
                )
            return _call(translator, func_name, *op.args)
        return formatter

    _operation_registry.update({
        MyNewFunction: fixed_arity('my_new_function', 1)
    })

Now, it just needs a compiler function to translate the function to a SQL code
(my_backend/compiler.py):

.. code-block:: python

    compiles = MyBackendExprTranslator.compiles

    @compiles(MyNewFunction)
    def compile_my_new_function(translator, expr):
        # pull out the arguments to the expression
        arg, = expr.op().args

        # compile the argument
        compiled_arg = translator.translate(arg)
        return 'my_new_function(%s)' % compiled_arg


identifiers
-----------

`identifiers` module keep a set of identifiers (`_identifiers`) to be used
inside `quote_identifier` function (inside the same module). `_identifiers` is
a set of reserved words from HeavyDB language.

`quote_identifiers` is used to put quotes around the string sent if the string
match to specific criteria.

Timestamp/Date operations
-------------------------

**Interval:**

HeavyDB Interval statement allows just the following date/time attribute: YEAR, DAY,
MONTH, HOUR, MINUTE, SECOND

To use the interval statement, it is necessary to use a `integer literal/constant`
and use the `to_interval` method:

.. code-block:: python

    >>> t['arr_timestamp'] + ibis.literal(1).to_interval('Y')

.. code-block:: sql

    SELECT TIMESTAMPADD(YEAR, 1, "arr_timestamp") AS tmp
    FROM heavyai.flights_2008_10k

Another way to use intervals is using `ibis.interval(years=1)`

**Extract date/time**

To extract a date part information from a timestamp, `extract` would be used:

.. code-block:: python

    >>> t['arr_timestamp'].extract('YEAR')

The `extract` method is just available on `ibis.heavyai` backend.

The operators allowed are: YEAR, QUARTER, MONTH, DAY, HOUR, MINUTE, SECOND,
DOW, ISODOW, DOY, EPOCH, QUARTERDAY, WEEK

**Direct functions to extract date/time**

There are some direct functions to extract date/time, the following shows how
to use them:

.. code-block:: python

    >>> t['arr_timestamp'].year()
    >>> t['arr_timestamp'].month()
    >>> t['arr_timestamp'].day()
    >>> t['arr_timestamp'].hour()
    >>> t['arr_timestamp'].minute()
    >>> t['arr_timestamp'].second()

The result will be:

.. code-block:: sql

    SELECT EXTRACT(YEAR FROM "arr_timestamp") AS tmp
    FROM heavyai.flights_2008_10k

    SELECT EXTRACT(MONTH FROM "arr_timestamp") AS tmp
    FROM heavyai.flights_2008_10k

    SELECT EXTRACT(DAY FROM "arr_timestamp") AS tmp
    FROM heavyai.flights_2008_10k

    SELECT EXTRACT(HOUR FROM "arr_timestamp") AS tmp
    FROM heavyai.flights_2008_10k

    SELECT EXTRACT(MINUTE FROM "arr_timestamp") AS tmp
    FROM heavyai.flights_2008_10k

    SELECT EXTRACT(SECOND FROM "arr_timestamp") AS tmp
    FROM heavyai.flights_2008_10k

**Timestap/Date Truncate**

A truncate timestamp/data value function is available as `truncate`:

.. code-block:: python

    >>> t['arr_timestamp'].truncate(date_part)

The date part operators allowed are: YEAR or Y, QUARTER or Q, MONTH or M,
DAY or D, HOUR or h, MINUTE or m, SECOND or s, WEEK, MILLENNIUM, CENTURY,
DECADE, QUARTERDAY


String operations
-----------------

- `byte_length` is not part of `ibis` `string` operations, it will work just for `heavyai` backend.

`Not` operation can be done using `~` operator:

.. code-block:: python

    >>> ~t['dest_name'].like('L%')

`regexp` and `regexp_like` operations can be done using `re_search` operation:

.. code-block:: python

    >>> t['dest_name'].re_search('L%')


Aggregate operations
====================

The aggregation operations available are: max, min, mean, count, distinct and count, nunique, approx_nunique.

The following examples show how to use count operations:

- get the row count of the table: `t['taxiin'].count()`
- get the distinct count of a field: `t['taxiin'].distinct().count()` or `t['taxiin'].nunique().name('v')`
- get the approximate distinct count of a field: `t['taxiin'].approx_nunique(10)`


Best practices
--------------

- Use `Numpy` standard for docstrings: https://numpydoc.readthedocs.io/en/latest/format.html#docstring-standard
- Use `format` string function to format a string instead of `%` statement.


History
-------

New implementations on `ibis` core:

- Trigonometric operations (https://github.com/ibis-project/ibis/issues/893 );
- Radians and Degrees operations (https://github.com/ibis-project/ibis/issues/1431 );
- PI constant (https://github.com/ibis-project/ibis/issues/1418 );
- Correlation and Covariation operations added (https://github.com/ibis-project/ibis/issues/1432 );
- ILIKE operation (https://github.com/ibis-project/ibis/issues/1433 );
- Distance operation (https://github.com/ibis-project/ibis/issues/1434 );

Issues appointed:

- `Ibis` `CASE` statement wasn't allowing input and output with different types (https://github.com/ibis-project/ibis/issues/93 )
- At this time, not all heavyai `date parts` are available on `ibis` (https://github.com/ibis-project/ibis/issues/1430 )


Pull Requests:

- https://github.com/ibis-project/ibis/pull/1419

References
----------

- ibis API: http://ibis-project.org/docs/api.html
