#include "Interpreter.h"

#include <iostream>
#include "API/API.h"

void Interpreter::execute_create_index(const sqltoast::create_index_statement_t *stmt)
{
    std::string index_name(stmt->index_name.start, stmt->index_name.end);
    std::string table_name(stmt->table_name.start, stmt->table_name.end);
    if(stmt->col_list.size() != 1) {
        std::cout << "Error: indices on multiple columns are not supported" << std::endl;
        return;
    }
    std::string column(stmt->col_list[0].start, stmt->col_list[0].end);

#ifdef _INTERP_DEBUG
    std::cout << "Creating index `" << index_name << "` on `" << table_name
        << "` (" << column << << ')' << std::endl;
#endif /* _INTERP_DEBUG */

    API::createIndex(table_name, column, index_name);
}
