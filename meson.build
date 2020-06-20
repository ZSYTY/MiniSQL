project('MiniSQL', 'cpp', default_options : ['cpp_std=c++1z', 'b_lto=true'], version : '1.0')

src = [
    'Interpreter/Interpreter.cc',
    'Interpreter/Executor.cc',
    'Interpreter/ExecutorCreateIndex.cc',
    'Interpreter/ExecutorCreateTable.cc',
    'Interpreter/ExecutorDelete.cc',
    'Interpreter/ExecutorDropIndex.cc',
    'Interpreter/ExecutorDropTable.cc',
    'Interpreter/ExecutorHelper.cc',
    'Interpreter/ExecutorInsert.cc',
    'Interpreter/ExecutorSelect.cc',
    'IndexManager/IndexManager.cpp',
    'BufferManager/BufferManager.cpp',
    'CatalogManager/CatalogManager.cpp',
    'RecordManager/RecordManager.cpp',
    'main.cpp'
]

libsqltoast_proj = subproject('libsqltoast')
libsqltoast_dep = libsqltoast_proj.get_variable('libsqltoast_dep')

executable('MiniSQL', src, dependencies : libsqltoast_dep, install : true)
