#include "Interpreter.h"

#include <cassert>
#include <cstdlib>
#include <iostream>

SqlValue Interpreter::sql_val_from_string(const std::string &str, int8_t sign)
{
    if(str.empty()) {
        abort();
    } else if(str[0] == '\'') {
        // is a string
        assert(str.back() == '\'');
        auto substr = str.substr(1, str.size() - 2);    // FIXME escape characters are ignored
        return SqlValue(SqlValueBaseType::MiniSQL_char, substr);
    } else if(str.find('.') != std::string::npos || str.find('.') != std::string::npos
        || str.find('.') != std::string::npos) {
        // is a float
        return SqlValue(SqlValueBaseType::MiniSQL_float, std::stof(str));
    } else {
        // treat it as an int
        return SqlValue(SqlValueBaseType::MiniSQL_int, std::stoi(str));
    }
}

std::optional<SqlValue> Interpreter::sql_val_from_literal(const sqltoast::row_value_expression_t *lit)
{
    if(lit->value->type != sqltoast::VALUE_EXPRESSION_TYPE_NUMERIC_EXPRESSION) {
        std::cout << "Error: unsupported literal type" << std::endl;
        return {};
    }
    auto lit1 = static_cast<const sqltoast::numeric_expression_t *>(lit->value.get());
    if(lit1->op != sqltoast::NUMERIC_OP_NONE || lit1->left->op != sqltoast::NUMERIC_OP_NONE) {
        std::cout << "Error: arithmetics in SQL expressions are supported yet" << std::endl;
        return {};
    }
    auto val_sign = lit1->left->left->sign;
    const auto &lit2 = lit1->left->left->primary;
    if(lit2->type != sqltoast::NUMERIC_PRIMARY_TYPE_VALUE) {
        std::cout << "Error: SQL functions are not supported yet" << std::endl;
        return {};
    }
    auto lit3 = static_cast<const sqltoast::numeric_value_t *>(lit2.get());
    if(lit3->primary->vep_type != sqltoast::VEP_TYPE_UNSIGNED_VALUE_SPECIFICATION) {
        std::cout << "Error: expected a literal, found an aggregate type" << std::endl;
        return {};
    }
    
    std::string val_str(lit3->primary->lexeme.start, lit3->primary->lexeme.end);
    return sql_val_from_string(val_str, val_sign);
}

std::optional<std::string> Interpreter::col_ref_from_literal(const sqltoast::row_value_expression_t *lit)
{
    if(lit->value->type != sqltoast::VALUE_EXPRESSION_TYPE_NUMERIC_EXPRESSION) {
        std::cout << "Error: unsupported literal type" << std::endl;
        return {};
    }
    auto lit1 = static_cast<const sqltoast::numeric_expression_t *>(lit->value.get());
    if(lit1->op != sqltoast::NUMERIC_OP_NONE || lit1->left->op != sqltoast::NUMERIC_OP_NONE) {
        std::cout << "Error: arithmetics in SQL expressions are supported yet" << std::endl;
        return {};
    }
    if(lit1->left->left->sign != 0) {
        std::cout << "Error: arithmetics in SQL expressions are supported yet" << std::endl;
        return {};
    }
    const auto &lit2 = lit1->left->left->primary;
    if(lit2->type != sqltoast::NUMERIC_PRIMARY_TYPE_VALUE) {
        std::cout << "Error: SQL functions are not supported yet" << std::endl;
        return {};
    }
    auto lit3 = static_cast<const sqltoast::numeric_value_t *>(lit2.get());
    if(lit3->primary->vep_type != sqltoast::VEP_TYPE_COLUMN_REFERENCE) {
        std::cout << "Error: expected a column reference, found other types" << std::endl;
        return {};
    }
    std::string col_ref(lit3->primary->lexeme.start, lit3->primary->lexeme.end);
    return col_ref;
}
