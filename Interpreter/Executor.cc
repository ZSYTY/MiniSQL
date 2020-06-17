#include "Interpreter.h"

#include <iostream>

void Interpreter::execute_sql(const std::string &statement)
{
    sqltoast::parse_input_t subject(statement.cbegin(), statement.cend());
    sqltoast::parse_result_t res = sqltoast::parse(subject);

    if(res.code == sqltoast::PARSE_OK) {
        for(const auto &stmt : res.statements) {
            switch(stmt->type) {
            case sqltoast::STATEMENT_TYPE_SELECT: {
                auto sel = static_cast<const sqltoast::select_statement_t *>(stmt.get());
                execute_select(sel);
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
