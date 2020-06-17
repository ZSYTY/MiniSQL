#include "Interpreter.h"

#include <iostream>

void Interpreter::execute_sql(sqltoast::parse_input_t subject)
{
    sqltoast::parse_result_t res = sqltoast::parse(subject);

    if(res.code == sqltoast::PARSE_OK) {
        for(const auto &stmt : res.statements) {
            switch(stmt->type) {
            case sqltoast::STATEMENT_TYPE_SELECT: {
                auto sel = static_cast<const sqltoast::select_statement_t *>(stmt.get());
                execute_select(sel);
                break;
            }
            case sqltoast::STATEMENT_TYPE_INSERT: {
                auto ins = static_cast<const sqltoast::insert_statement_t *>(stmt.get());
                execute_insert(ins);
                break;
            }
            case sqltoast::STATEMENT_TYPE_DELETE: {
                auto del = static_cast<const sqltoast::delete_statement_t *>(stmt.get());
                execute_delete(del);
                break;
            }
            case sqltoast::STATEMENT_TYPE_CREATE_TABLE: {
                auto cre = static_cast<const sqltoast::create_table_statement_t *>(stmt.get());
                execute_create_table(cre);
                break;
            }
            case sqltoast::STATEMENT_TYPE_DROP_TABLE: {
                auto drp = static_cast<const sqltoast::drop_table_statement_t *>(stmt.get());
                execute_drop_table(drp);
                break;
            }
            default: {
                std::cout << "Error: unsupported statement type " << stmt->type << std::endl;
                break;
            }
            }
        }
    } else if (res.code == sqltoast::PARSE_SYNTAX_ERROR) {
        std::cout << "SQL syntax error:" << std::endl;
        std::cout << res.error << std::endl;
    } else {
        std::cout << "Unknown parser error " << res.code << ':' << std::endl;
        std::cout << res.error << std::endl;
    }
}
