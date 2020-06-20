#include "Interpreter.h"

#include <iostream>
#include "API/API.h"

void Interpreter::execute_insert(const sqltoast::insert_statement_t *stmt)
{
    std::string table(stmt->table_name.start, stmt->table_name.end);
    if(!stmt->insert_columns.empty()) {
        std::cout << "Error: partial insertion is not supported" << std::endl;
        return;
    }
    if(stmt->query->query_expression_type != sqltoast::QUERY_EXPRESSION_TYPE_NON_JOIN_QUERY_EXPRESSION) {
        std::cout << "Error: JOIN operations are not supported" << std::endl;
        return;
    }
    auto query = static_cast<const sqltoast::non_join_query_expression_t *>(stmt->query.get());
    if(query->term->primary->primary_type != sqltoast::NON_JOIN_QUERY_PRIMARY_TYPE_TABLE_VALUE_CONSTRUCTOR) {
        std::cout << "Error: subqueries in INSERT statements are not supported" << std::endl;
        return;
    }
    auto query2 = static_cast<const sqltoast::table_value_constructor_non_join_query_primary_t *>
        (query->term->primary.get());
    for(const auto &row: query2->table_value->values) {
        if(row->rvc_type != sqltoast::RVC_TYPE_LIST) {
            std::cout << "Error: only tuples are allowed in VALUES statements" << std::endl;
            continue;
        }
        auto row2 = static_cast<const sqltoast::row_value_constructor_list_t *>(row.get());
        std::vector<SqlValue> values;
        for(const auto &val : row2->elements) {
            if(val->rvc_type != sqltoast::RVC_TYPE_ELEMENT) {
                std::cout << "Error: aggregate types or subqueries are not supported" << std::endl;
                goto continue_outer;
            }
            auto val2 = static_cast<const sqltoast::row_value_constructor_element_t *>(val.get());
            if(val2->rvc_element_type != sqltoast::RVC_ELEMENT_TYPE_VALUE_EXPRESSION) {
                std::cout << "Error: NULL or DEFAULT are not supported" << std::endl;
                goto continue_outer;
            }
            auto val3 = static_cast<const sqltoast::row_value_expression_t *>(val2);
            auto val_final = sql_val_from_literal(val3);
            if(!val_final)
                goto continue_outer;
            values.push_back(*val_final);
        }

        API::insertTuple(table, values);
        std::cout << "Inserting into `" << table << "` (";
        for(const auto &val : values) {
            // FIXME non-int types are not printed
            std::cout << val.int_val << ", ";
        }
        std::cout << ')' << std::endl;

        continue_outer:
        void(0);    // labels at end of blocks are not allowed, so we add an empty statement
    }
}
