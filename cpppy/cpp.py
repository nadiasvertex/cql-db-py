import os
from clang import cindex

custom_type = """
static PyTypeObject {typename}_type = {{
    PyObject_HEAD_INIT(NULL)
    0,                            /*ob_size*/
    "{modname}.{typename}",       /*tp_name*/
    sizeof({typename}_object),    /*tp_basicsize*/
    0,                            /*tp_itemsize*/
    (destructor){typename}_dealloc,           /*tp_dealloc*/
    0,                            /*tp_print*/
    0,                            /*tp_getattr*/
    0,                            /*tp_setattr*/
    0,                            /*tp_compare*/
    0,                            /*tp_repr*/
    0,                            /*tp_as_number*/
    0,                            /*tp_as_sequence*/
    0,                            /*tp_as_mapping*/
    0,                            /*tp_hash */
    0,                            /*tp_call*/
    0,                            /*tp_str*/
    0,                            /*tp_getattro*/
    0,                            /*tp_setattro*/
    0,                            /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,           /*tp_flags*/
    "{typename} objects",         /* tp_doc */
    0,                            /* tp_traverse */
    0,                            /* tp_clear */
    0,                            /* tp_richcompare */
    0,                            /* tp_weaklistoffset */
    0,                            /* tp_iter */
    0,                            /* tp_iternext */
    {typename}_methods,           /* tp_methods */
    0,                            /* tp_members */
    0,                            /* tp_getset */
    0,                            /* tp_base */
    0,                            /* tp_dict */
    0,                            /* tp_descr_get */
    0,                            /* tp_descr_set */
    0,                            /* tp_dictoffset */
    (initproc){typename}_init,    /* tp_init */
    {typename}_alloc,             /* tp_alloc */
    0                             /* tp_new */
}};
"""

module_init = """
PyMODINIT_FUNC init{modname}(void) {{
   {type_initializers}
   
   PyObject *m = Py_InitModule("{modname}", nullptr);
   if (m==nullptr) {{
      return; 
   }}
   
   {type_appenders}
}}
"""

type_init = """
   {typename}_type.tp_new = PyType_GenericNew;
   if (PyType_Ready(&{typename}_type) < 0) {{
      return;
   }}
"""

type_append = """
   Py_INCREF(&{typename}_type);
   PyModule_AddObject(m, "{typename}", (PyObject*)&{typename}_type);
"""

type_init_func = """
static int 
{typename}_init({typename}_object* self, PyObject* args, PyObject* kwds) {{
   self->native_object = std::make_shared<{native_typename}>();
   return 0;
}}
"""

type_alloc_func = """
static PyObject* 
{typename}_alloc(PyTypeObject* type, Py_ssize_t nitems) {{
   {typename}_object* o = new {typename}_object{{}};
   o->ob_refcnt = 1;
   o->ob_type = type;
   return (PyObject*)o;
}}
"""

type_dealloc_func = """
static void 
{typename}_dealloc({typename}_object* self) {{
   delete self;
}}
"""

arg_map = {
  cindex.TypeKind.BOOL : "b",
  cindex.TypeKind.USHORT : "H",
  cindex.TypeKind.UINT : "I",
  cindex.TypeKind.ULONG : "k",
  cindex.TypeKind.ULONGLONG : "K",
  cindex.TypeKind.SHORT : "h",
  cindex.TypeKind.INT : "i",
  cindex.TypeKind.LONG : "l",
  cindex.TypeKind.LONGLONG : "L",
  cindex.TypeKind.FLOAT : "f",
  cindex.TypeKind.DOUBLE : "d",
  cindex.TypeKind.POINTER : "w",
}

class CppIndex:
   """
   Maintains an index of the C++ file(s) being processed by this system. Also
   provides a way to generate a C++ Python module from selected C++ objects.
   """
   def __init__(self): 
      self.tu = None
      self.marks = []
      self.compiler = "clang++"
      self.include_dirs = []
      self.library_dirs = []
      self.libraries = []
   
   def parse(self, filename):
      if not os.path.exists(filename):
         return
        
      index = cindex.Index.create()
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
         if c.kind == cindex.CursorKind.CXX_METHOD:
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
        
      tuple_str = ",".join(["&" + arg[1] for arg in tuple_args])
      for arg in tuple_args:
         c.write('\t%s %s;\n' % arg)
      
      c.write('\tif (!PyArg_ParseTuple(args, "%s", %s)) {\n' % ("".join(tuple_fmt), tuple_str,))
      c.write('\t\treturn nullptr;\n')
      c.write('\t}\n')
     
   def generate(self, module_name):
      with open("%s.cpp" % module_name, "w") as c:
         c.write("#include <memory>\n")
         c.write("#include <Python.h>\n")
         c.write('#include "%s"\n\n' % self.tu.spelling)
         
         type_initializers = []
         type_appenders = []
         for m in self.marks:
            methods = [md for md in self.find_methods(m) if md.access == 1]
            
            type_name = m.spelling.capitalize()
            native_type_name = m.spelling
                        
            # Write out special type
            c.write("typedef struct {\n\tPyObject_HEAD\n")
            c.write("\tstd::shared_ptr<%s> native_object;\n" % native_type_name)
            c.write("} %s_object;\n\n" % type_name)
            
            # Write out method thunking bodies
            for method in methods:
               c.write("static PyObject* " +\
                       "%s_%s(PyObject* self, PyObject* args) {\n" \
                       % (type_name, method.spelling))
               self.generate_args(c, method)
               c.write("}\n\n")
              
            # Write out method lists.
            c.write("static PyMethodDef %s_methods[] = {\n" % type_name)
            for method in methods:
               c.write(('\t{{"{method}", {typename}_{method}, ' +\
                       'METH_VARARGS, nullptr}},\n').format(
                           method = method.spelling,
                           typename = type_name
                        )
               )
            c.write("\t{nullptr, nullptr, 0, nullptr}\n};\n\n")
            
            # Write out the custom type management functions.
            c.write(type_init_func.format(typename=type_name,
                                          native_typename=native_type_name))
            c.write(type_alloc_func.format(typename=type_name,
                                          native_typename=native_type_name))
            c.write(type_dealloc_func.format(typename=type_name,
                                          native_typename=native_type_name))
            
            # Write out Python mapping for custom type.
            c.write(custom_type.format(modname=module_name, 
                                       typename=type_name))
               
            type_initializers.append(type_init.format(typename=type_name))
            type_appenders.append(type_append.format(typename=type_name))
            
         c.write(module_init.format(modname=module_name, 
                                    type_initializers="\n\t".join(type_initializers),
                                    type_appenders="\n\t".join(type_appenders),
                                    ))
      # Compile the module   
      self.compile(module_name)
         
   def compile(self, module_name):
      cmd = self.compiler + " -shared -std=c++11 -fPIC " +\
         " ".join(["-L"+x for x in self.library_dirs]) + " " +\
         " ".join(["-I"+x for x in self.include_dirs]) + " " +\
         " ".join(["-l"+x for x in self.libraries]) + " " +\
         " -o" + module_name + ".so " +\
         module_name + ".cpp"
      print cmd
      os.system(cmd)


if __name__ == "__main__":
   cindex.Config.set_library_path("/usr/lib/llvm-3.4/lib")
   i = CppIndex()
   i.include_dirs.append("/usr/include/python2.7")
   i.libraries.append("python2.7")
   i.parse("../native/src/cpp/compression/wkdm.h")
   # print dir(i.tu)
   # print dir(cindex.CursorKind)
   i.mark("wkdm")
   i.generate("compression")
  
  
