__author__ = 'cnelson'

from cStringIO import StringIO

class Expression(object):
   def __init__(self, left, op, right):
      self.left = left
      self.op = op
      self.right = right

   def gen(self, engine, **kw):
      s = StringIO()
      self.gen_simple_expression(engine, self.left, s, **kw)
      s.write(self.op)
      self.gen_simple_expression(engine, self.right, s, **kw)
      return s.getvalue()

   @classmethod
   def gen_simple_expression(cls, engine, value, stream, **kw):
      if isinstance(value, Expression):
         stream.write(value.gen(engine, **kw))
      else:
         stream.write(convert_to_sql(value, engine))

   def __or__(self, other):
      return BooleanExpression(self, "OR", other)

   def __and__(self, other):
      return BooleanExpression(self, "AND", other)


class BooleanExpression(Expression):
   def __init__(self, left, op, right):
      Expression.__init__(self, left, op, right)

   def gen(self, engine, **kw):
      s = StringIO()
      s.write("(")
      self.gen_simple_expression(engine, self.left, s, **kw)
      s.write(") ")
      s.write(self.op)
      s.write(" (")
      self.gen_simple_expression(engine, self.right, s, **kw)
      s.write(")")
      return s.getvalue()


class FieldNameExpression(Expression):
   def __init__(self, field):
      self.field = field

   def gen(self, engine, **kw):
      is_field = isinstance(self.field, Field)
      # If the field attached to this expression is an Alias we will use the alias value
      # from that object, otherwise we will use our given alias.
      alias = kw.get("alias", None) if is_field else self.field.alias
      name = self.field.name if is_field else self.field.field.name
      if alias is None:
         return name
      else:
         return alias+"."+name

class InExpression(Expression):
   def __init__(self, left, query_or_collection):
      if not isinstance(query_or_collection, SelectQuery) and\
         type(query_or_collection) not in (types.ListType, types.TupleType, type(set())):
         raise TypeError("To qualify for an in() operator, the parameter must be a SelectQuery or a simple Python collection like list or tuple.")

      self.left = left
      self.right = query_or_collection

   def gen(self, engine, **kw):
      o = StringIO()
      o.write(self.left.gen(engine, **kw))
      o.write(" IN (")
      if isinstance(self.right, SelectQuery):
         o.write(self.right.gen(engine, **kw))
      else:
         o.write(",".join([convert_to_sql(x,engine) for x in self.right]))
      o.write(")")
      return o.getvalue()

class BetweenExpression(Expression):
   def __init__(self, left, min_value, max_value):
      self.left = left
      self.min_value = min_value
      self.max_value = max_value

   def gen(self, engine, **kw):
      s = StringIO()
      s.write(self.left.gen(engine, **kw))
      s.write(" BETWEEN ")
      self.gen_simple_expression(engine, self.min_value, s, **kw)
      s.write(" AND ")
      self.gen_simple_expression(engine, self.max_value, s, **kw)
      return s.getvalue()