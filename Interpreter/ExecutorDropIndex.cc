#include "Interpreter.h"

#include <iostream>
#include "API/API.h"

void Interpreter::execute_drop_index(const sqltoast::drop_index_statement_t *stmt)
{
    std::string index_name(stmt->index_name.start, stmt->index_name.end);
    API::dropIndex(index_name);
    std::cout << "Dropping index `" << index_name << '`' << std::endl;
}
