#include "Interpreter.h"

#include <iostream>
#include "API/API.h"

void Interpreter::execute_create_index(const sqltoast::create_index_statement_t *stmt)
{
    std::string index_name(stmt->index_name.start, stmt->index_name.end);
    std::string table_name(stmt->table_name.start, stmt->table_name.end);
    std::cout << "Creating index `" << index_name << "` on `" << table_name << "` (";
    for(const auto &col : stmt->col_list) {
        std::cout << std::string(col.start, col.end) << ", ";
    }
    std::cout << ')' << std::endl;
}
