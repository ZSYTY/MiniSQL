#include "Interpreter.h"

#include <iostream>
#include "API/API.h"

void Interpreter::execute_drop_table(const sqltoast::drop_table_statement_t *stmt)
{
    std::string table_name(stmt->table_name.start, stmt->table_name.end);
    API::dropTable(table_name);
    std::cout << "Dropping table `" << table_name << '`' << std::endl;
}
