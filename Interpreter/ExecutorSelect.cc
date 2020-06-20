#include "Interpreter.h"

#include <iostream>
#include "API/API.h"

void Interpreter::execute_select(const sqltoast::select_statement_t *stmt)
{
    auto &tables = stmt->query->table_expression->referenced_tables;
    if(tables.size() != 1 || tables[0]->joined /*->join_type != sqltoast::JOIN_TYPE_NONE*/) {
        std::cout << "Error: union queries are not supported yet" << std::endl;
        return;
    }
    if(tables[0]->type != sqltoast::TABLE_REFERENCE_TYPE_TABLE) {
        std::cout << "Error: cascade queries are not supported yet" << std::endl;
        return;
    }
    if(!stmt->query->table_expression->group_by_columns.empty() ||
        stmt->query->table_expression->having_condition /*->terms.empty()*/) {
        std::cout << "Error: aggregates are not supported yet" << std::endl;
        return;
    }

    auto table = static_cast<const sqltoast::table_t *>(tables[0].get());
    if(table->has_alias() || !table->correlation_spec->columns.empty() ||
        stmt->query->selected_columns.size() != 1 ||
        stmt->query->selected_columns[0].value /* the asterisk case */) {
        std::cout << "Error: aliases or partial selections are not supported yet" << std::endl;
        return;
    }
    if(stmt->query->distinct) {
        std::cout << "Error: distinct selections are not supported yet" << std::endl;
        return;
    }

    std::string table_name(table->table_name.start, table->table_name.end);
    std::vector<SqlCondition> where_cond;
    if(stmt->query->table_expression->where_condition) {
        if(stmt->query->table_expression->where_condition->terms.size() > 1) {
            std::cout << "Error: OR in WHERE conditions are not supported yet" << std::endl;
            return;
        }
        if(stmt->query->table_expression->where_condition->terms.size() > 0) {
            const auto &bool_term = stmt->query->table_expression->where_condition->terms[0];
            if(!parse_where_conds(where_cond, bool_term))
                return;
        }
    }

    // TODO stub
    API::select(table_name, where_cond);
    std::cout << "Selecting * from " << table_name;
    if(!where_cond.empty()) {
        std::cout << " where ";
        for(const auto &c : where_cond) {
            // FIXME non-int types are not printed
            std::cout << c.columnName << ' ' << (int) c.op << ' ' << c.val.int_val << ", ";
        }
    }
    std::cout << std::endl;
}
