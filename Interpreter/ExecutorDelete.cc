#include "Interpreter.h"

#include <iostream>
#include "API/API.h"

void Interpreter::execute_delete(const sqltoast::delete_statement_t *stmt)
{
    std::string table_name(stmt->table_name.start, stmt->table_name.end);
    std::vector<SqlCondition> where_cond;
    if(stmt->where_condition) {
        if(stmt->where_condition->terms.size() > 1) {
            std::cout << "Error: OR in WHERE conditions are not supported yet" << std::endl;
            return;
        }
        if(stmt->where_condition->terms.size() > 0) {
            const auto &bool_term = stmt->where_condition->terms[0];
            if(!parse_where_conds(where_cond, bool_term))
                return;
        }
    }

    API::deleteTuple(table_name, where_cond);
    std::cout << "Deleting from " << table_name;
    if(!where_cond.empty()) {
        std::cout << " where ";
        for(const auto &c : where_cond) {
            // FIXME non-int types are not printed
            std::cout << c.columnName << ' ' << (int) c.op << ' ' << c.val.int_val << ", ";
        }
    }
    std::cout << std::endl;
}
