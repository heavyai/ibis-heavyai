"""OmniSciDB User Defined Function (UDF) Implementation."""
import functools
import types
from typing import Callable, List, Optional

import ibis
import ibis.expr.types as ir
from ibis.expr import datatypes as dt
from ibis.udf import vectorized
from rbc.omniscidb import RemoteOmnisci

from .dtypes import ibis_dtypes_to_str


def _udf_decorator(node_type, input_type, output_type, infer_literal=False):
    # NOTE: it is used instead of the one in the core because it uses
    #       `infer_literal` parameter.
    def wrapper(func):
        return UserDefinedFunction(
            func, node_type, input_type, output_type, infer_literal
        )

    return wrapper


def elementwise(input_type, output_type, infer_literal=False):
    """
    Define a UDF that operates element wise on a Pandas Series.

    Parameters
    ----------
    input_type : List[ibis.expr.datatypes.DataType]
        A list of the types found in :mod:`~ibis.expr.datatypes`. The
        length of this list must match the number of arguments to the
        function. Variadic arguments are not yet supported.
    output_type : ibis.expr.datatypes.DataType
        The return type of the function.
    infer_literal : bool, default False
        Define if literal scalar values should be infered or if this method
        should accept explicitly just ibis literal.

    Examples
    --------
    >>> import ibis
    >>> import ibis.expr.datatypes as dt
    >>> from ibis.udf.vectorized import elementwise
    >>> @elementwise(input_type=[dt.string], output_type=dt.int64)
    ... def my_string_length(series):
    ...     return series.str.len() * 2
    >>> @elementwise(input_type=[dt.string], output_type=dt.int64)
    ... def my_string_length(series, *, times):
    ...     return series.str.len() * times
    """
    # NOTE: it is used instead of the one in the core because it uses
    #       `infer_literal` parameter.
    return _udf_decorator(
        vectorized.ElementWiseVectorizedUDF,
        input_type,
        output_type,
        infer_literal,
    )


class UserDefinedFunction(vectorized.UserDefinedFunction):
    """The UDF is used to create functions inside the backend."""

    def __init__(
        self, func, func_type, input_type, output_type, infer_literal=False
    ):
        super().__init__(func, func_type, input_type, output_type)
        # Note: implement a way to use literal with UDF.
        #       It could be pushed to the core in the future
        self.infer_literal = infer_literal

    def __call__(self, *args, **kwargs):
        """Return a UDF expression."""
        # kwargs cannot be part of the node object because it can contain
        # unhashable object, e.g., list.
        # Here, we keep the node hashable by creating a closure that contains
        # kwargs.

        @functools.wraps(self.func)
        def func(*args):
            return self.func(*args, **kwargs)

        # Note: implement a way to use literal with UDF.
        #       It could be pushed to the core in the future
        if self.infer_literal:
            new_args = []
            for i, arg in enumerate(args):
                if not isinstance(arg, ir.Expr):
                    params = {}
                    if arg is not None:
                        params.update({'type': self.input_type[i]})
                    arg = ibis.literal(arg, **params)

                new_args.append(arg)
            args = new_args

        op = self.func_type(
            func=func,
            args=args,
            input_type=self.input_type,
            output_type=self.output_type,
        )

        return op.to_expr()


def _create_function(name, nargs):
    """
    Create a function dinamically for the given name and number of arguments.

    Parameters
    ----------
    name : str
        name of the function
    nargs : int
        number of the arguments the function accept.

    Returns
    -------
    Callable
    """
    # this function is used just as a template
    def y():
        pass

    args_name = tuple('arg{}'.format(i) for i in range(nargs))

    co_args = (
        nargs,  # argcount
        0,  # kwonlyargcount
        0,  # nlocals
        1,  # stacksize
        0,  # flags
        bytes(),  # codestring
        (),  # constants
        args_name,  # names
        args_name,  # varnames
        y.__code__.co_filename,  # filename
        name,  # name
        0,  # firstlineno
        bytes(),  # lnotab
    )
    new_code = types.CodeType(*co_args)
    return types.FunctionType(new_code, {}, name)


class OmniSciDBUDF:
    """
    OmniSciDB UDF class.

    OmniSciDB uses RBC (Remote Backend Client) to support UDF.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = 6274,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.remote_backend_compiler = RemoteOmnisci(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
        )

    def _udf(
        self,
        udf_operation: Callable,
        input_type: List[dt.DataType],
        output_type: Optional[dt.DataType] = None,
        name: Optional[str] = None,
        infer_literal: bool = True,
    ) -> Callable:
        """
        Elementwise operation for UDF.

        Parameters
        ----------
        udf_operation : Callable
            Used for creating UDF operation
        input_type : List[dt.DataType]
        output_type: dt.DataType
        name : str
            Used for reusing an existent UDF
        infer_literal : bool, default True
            Define if literal scalar values should be infered or if this method
            should accept explicitly just ibis literal.

        Returns
        -------
        Callable
        """
        if name:
            f = _create_function(name, len(input_type))
            return udf_operation(
                input_type, output_type, infer_literal=infer_literal
            )(f)

        def omnisci_wrapper(f, input_type=input_type, output_type=output_type):
            signature = '{}({})'.format(
                ibis_dtypes_to_str[output_type],
                ', '.join([ibis_dtypes_to_str[v] for v in input_type]),
            )

            if isinstance(f, vectorized.UserDefinedFunction):
                f = f.func

            self.remote_backend_compiler(signature)(f)
            self.remote_backend_compiler.register()
            return udf_operation(
                input_type, output_type, infer_literal=infer_literal
            )(f)

        return omnisci_wrapper

    def elementwise(
        self,
        input_type: List[dt.DataType],
        output_type: Optional[dt.DataType] = None,
        name: Optional[str] = None,
        infer_literal: bool = True,
    ) -> Callable:
        """
        Create an elementwise UDF operation.

        Parameters
        ----------
        input_type : List[dt.DataType]
        output_type: dt.DataType
        name : str
            Used for reusing an existent UDF
        infer_literal : bool, default True
            Define if literal scalar values should be infered or if this method
            should accept explicitly just ibis literal.

        Returns
        -------
        Callable
        """
        # NOTE: elementwise here is a modify version of vectorized.elementwise
        return self._udf(
            udf_operation=elementwise,
            input_type=input_type,
            output_type=output_type,
            name=name,
            infer_literal=infer_literal,
        )

    def reduction(
        self,
        input_type: List[dt.DataType],
        output_type: Optional[dt.DataType] = None,
        name: Optional[str] = None,
    ) -> Callable:
        """
        Create a reduction UDF operation.

        Parameters
        ----------
        input_type : List[dt.DataType]
        output_type: dt.DataType
        name : str
            Used for reusing an existent UDF

        Returns
        -------
        Callable
        """
        raise NotImplementedError('UDF Reduction is not implemented yet.')

    def analytic(
        self,
        input_type: List[dt.DataType],
        output_type: Optional[dt.DataType] = None,
        name: Optional[str] = None,
    ) -> Callable:
        """
        Create an analytic UDF operation.

        Parameters
        ----------
        input_type : List[dt.DataType]
        output_type: dt.DataType
        name : str
            Used for reusing an existent UDF

        Returns
        -------
        Callable
        """
        raise NotImplementedError('UDF Analytic is not implemented yet.')
