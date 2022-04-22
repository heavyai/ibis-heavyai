"""HeavyDB Compiler module."""
import typing
from io import StringIO

import ibis
import ibis.common.exceptions as com
import ibis.expr.operations as ops
import ibis.expr.types as ir
import ibis.util as util
from ibis.backends.base.sql import compiler
from ibis.expr.api import _add_methods, _binop_expr, _unary_op

from . import operations as heavydb_ops
from .identifiers import quote_identifier  # noqa: F401
from .operations import _type_to_sql_string  # noqa: F401


class HeavyDBSelectBuilder(compiler.SelectBuilder):
    """HeavyDB Select Builder class."""

    def _convert_group_by(self, exprs):
        return exprs


class HeavyDBQueryContext(compiler.QueryContext):
    """HeavyDB Query Context class."""

    always_alias = False


class HeavyDBSelect(compiler.Select):
    """HeavyDB Select class."""

    def format_group_by(self) -> typing.Optional[str]:
        """Format the group by clause.

        Returns
        -------
        string
        """
        if not self.group_by:
            # There is no aggregation, nothing to see here
            return None

        lines = []
        if self.group_by:
            columns = ['{}'.format(expr.get_name()) for expr in self.group_by]
            clause = 'GROUP BY {}'.format(', '.join(columns))
            lines.append(clause)

        if self.having:
            trans_exprs = []
            for expr in self.having:
                translated = self._translate(expr)
                trans_exprs.append(translated)
            lines.append('HAVING {}'.format(' AND '.join(trans_exprs)))

        return '\n'.join(lines)

    def format_limit(self):
        """Format the limit clause.

        Returns
        -------
        string
        """
        if not self.limit:
            return None

        buf = StringIO()

        n, offset = self.limit['n'], self.limit['offset']
        buf.write('LIMIT {}'.format(n))
        if offset is not None and offset != 0:
            buf.write(', {}'.format(offset))

        return buf.getvalue()


class HeavyDBTableSetFormatter(compiler.TableSetFormatter):
    """HeavyDB Table Set Formatter class."""

    _join_names = {
        ops.InnerJoin: 'JOIN',
        ops.LeftJoin: 'LEFT JOIN',
        ops.LeftSemiJoin: 'JOIN',  # needed by topk as filter
        ops.CrossJoin: 'JOIN',
    }

    def get_result(self):
        """Get a formatted string for the expression.

        Got to unravel the join stack; the nesting order could be
        arbitrary, so we do a depth first search and push the join tokens
        and predicates onto a flat list, then format them

        Returns
        -------
        string
        """
        op = self.expr.op()

        if isinstance(op, ops.Join):
            self._walk_join_tree(op)
        else:
            self.join_tables.append(self._format_table(self.expr))

        buf = StringIO()
        buf.write(self.join_tables[0])
        for jtype, table, preds in zip(
            self.join_types, self.join_tables[1:], self.join_predicates
        ):
            buf.write('\n')
            buf.write(util.indent('{} {}'.format(jtype, table), self.indent))

            fmt_preds = []
            npreds = len(preds)
            for pred in preds:
                new_pred = self._translate(pred)
                if npreds > 1:
                    new_pred = '({})'.format(new_pred)
                fmt_preds.append(new_pred)

            if len(fmt_preds):
                buf.write('\n')

                conj = ' AND\n{}'.format(' ' * 3)
                fmt_preds = util.indent(
                    'ON ' + conj.join(fmt_preds), self.indent * 2
                )
                buf.write(fmt_preds)
            else:
                buf.write(util.indent('ON TRUE', self.indent * 2))

        return buf.getvalue()

    _non_equijoin_supported = True

    def _validate_join_predicates(self, predicates):
        for pred in predicates:
            op = pred.op()

            if (
                not isinstance(op, ops.Equals)
                and not self._non_equijoin_supported
            ):
                raise com.TranslationError(
                    'Non-equality join predicates, '
                    'i.e. non-equijoins, are not '
                    'supported'
                )

    def _format_predicate(self, predicate):
        column = predicate.op().args[0]
        return column.get_name()

    def _quote_identifier(self, name):
        return name


class HeavyDBExprTranslator(compiler.ExprTranslator):
    """HeavyDB Expr Translator class."""

    _registry = heavydb_ops._operation_registry
    _rewrites = compiler.ExprTranslator._rewrites.copy()

    def name(self, translated: str, name: str, force=True):
        """Define name for the expression.

        Parameters
        ----------
        translated : str
            translated expresion
        name : str
        force : bool, optional
            if True force the new name, by default True

        Returns
        -------
        str
        """
        return heavydb_ops._name_expr(translated, name)


rewrites = HeavyDBExprTranslator.rewrites

heavydb_reg = heavydb_ops._operation_registry


@rewrites(ops.FloorDivide)
def _floor_divide(expr):
    left, right = expr.op().args
    return left.div(right).floor()


@rewrites(ops.All)
def heavydb_rewrite_all(expr: ibis.Expr) -> ibis.Expr:
    """Rewrite All operation.

    Parameters
    ----------
    expr : ibis.Expr

    Returns
    -------
    [type]
    """
    return heavydb_ops._all(expr)


@rewrites(ops.Any)
def heavydb_rewrite_any(expr: ibis.Expr) -> ibis.Expr:
    """Rewrite Any operation.

    Parameters
    ----------
    expr : ibis.Expr

    Returns
    -------
    ibis.Expr
    """
    return heavydb_ops._any(expr)


@rewrites(ops.NotAll)
def heavydb_rewrite_not_all(expr: ibis.Expr) -> ibis.Expr:
    """Rewrite Not All operation.

    Parameters
    ----------
    expr : ibis.Expr

    Returns
    -------
    ibis.Expr
    """
    return heavydb_ops._not_all(expr)


@rewrites(ops.NotAny)
def heavydb_rewrite_not_any(expr: ibis.Expr) -> ibis.Expr:
    """Rewrite Not Any operation.

    Parameters
    ----------
    expr : ibis.Expr

    Returns
    -------
    ibis.Expr
    """
    return heavydb_ops._not_any(expr)


_add_methods(
    ir.NumericValue,
    {
        'conv_4326_900913_x': _unary_op(
            'conv_4326_900913_x', heavydb_ops.Conv_4326_900913_X
        ),
        'conv_4326_900913_y': _unary_op(
            'conv_4326_900913_y', heavydb_ops.Conv_4326_900913_Y
        ),
        'truncate': _binop_expr('truncate', heavydb_ops.NumericTruncate),
    },
)

_add_methods(
    ir.StringValue,
    {'byte_length': _unary_op('length', heavydb_ops.ByteLength)},
)


class HeavyDBCompiler(compiler.Compiler):
    """HeavyDB Query Builder class."""

    translator_class = HeavyDBExprTranslator
    select_builder_class = HeavyDBSelectBuilder
    context_class = HeavyDBQueryContext
    table_set_formatter_class = HeavyDBTableSetFormatter
    select_class = HeavyDBSelect
    union_class = None

    @staticmethod
    def _make_union(union_class, expr, context):
        raise com.UnsupportedOperationError(
            "HeavyDB backend doesn't support Union operation"
        )
