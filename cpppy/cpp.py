import clang.cindex
import os

arg_map = {
  clang.cindex.TypeKind.BOOL : "b",
  clang.cindex.TypeKind.USHORT : "H",
  clang.cindex.TypeKind.UINT : "I",
  clang.cindex.TypeKind.ULONG : "k",
  clang.cindex.TypeKind.ULONGLONG : "K",
  clang.cindex.TypeKind.SHORT : "h",
  clang.cindex.TypeKind.INT : "i",
  clang.cindex.TypeKind.LONG : "l",
  clang.cindex.TypeKind.LONGLONG : "L",
  clang.cindex.TypeKind.FLOAT : "f",
  clang.cindex.TypeKind.DOUBLE : "d",
  clang.cindex.TypeKind.POINTER : "w",
}

class CppIndex:

  def __init__(self): 
    self.tu = None
    self.marks = []

  def parse(self, filename):
    if not os.path.exists(filename):
      return
      
    index = clang.cindex.Index.create()
    self.tu = index.parse(filename, args=("-std=c++11",))    
    
  def _mark_class_decl(self, node, class_name):
    if node.spelling == class_name:
      self.marks.append(node)
      return       
      
    for c in node.get_children():
      self._mark_class_decl(c, class_name)
      
  def find_methods(self, node):
    methods = []
    for c in node.get_children():
      if c.kind == clang.cindex.CursorKind.CXX_METHOD:
        methods.append(c)
        
    return methods
         
  def mark(self, class_name):
    self._mark_class_decl(self.tu.cursor, class_name)
    
  def generate_args(self, c, method):
    tuple_args = []
    tuple_fmt = []
    for arg in method.get_arguments():
      tuple_args.append((arg.type.get_canonical().spelling, arg.spelling))
      t = arg.type.get_canonical()
      tuple_fmt.append(arg_map[t.kind])
      
    tuple_str = ",".join(["&"+arg[1] for arg in tuple_args])
    for arg in tuple_args:
      c.write('\t%s %s;\n' % arg)
    
    c.write('\tif (!PyArg_ParseTuple(args, "%s", %s) {\n' % ("".join(tuple_fmt), tuple_str,))
    c.write('\t\treturn nullptr;\n')
    c.write('\t}\n')
    
    
  def generate(self):
    with open("module.cpp", "w") as c:
      c.write("#include <Python.h>\n\n")
      for m in self.marks:
        methods = self.find_methods(m)
        
        # Write out method thunking bodies
        for method in methods:
          c.write("static PyObject * %s_%s(PyObject* self, PyObject* args) {\n" % (m.spelling, method.spelling))
          self.generate_args(c, method)
          c.write("}\n\n")
          
        # Write out method lists.
        c.write("static PyMethodDef %s_methods[] = {\n" % m.spelling)
        for method in methods:
          c.write('\t{"%s", %s_%s, METH_VARARGS, nullptr},\n' % (method.spelling, m.spelling, method.spelling))
        c.write("\t{nullptr, nullptr, 0, nullptr}\n};\n\n")
           
        
        c.write('PyMODINIT_FUNC init%s(void) {\n' % (m.spelling,))      
        c.write('\tPyObject *m = Py_InitModule("%s", %s_methods);\n' % (m.spelling,m.spelling,))
        c.write('\tif (m==nullptr) { return; }\n')
        c.write('}\n')
  

if __name__ == "__main__":
  i = CppIndex()
  i.parse("../native/src/cpp/compression/wkdm.h")
  #print dir(i.tu)
  #print dir(clang.cindex.CursorKind)
  i.mark("wkdm")
  i.generate()
