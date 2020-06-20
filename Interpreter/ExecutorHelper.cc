#include "Interpreter.h"

#include <cassert>
#include <cstdlib>
#include <iostream>

SqlValue Interpreter::sql_val_from_string(const std::string &str, int8_t sign)
{
    if(str.empty()) {
        abort();
    } else if(str[0] == '\'' || str[0] == '"') {
        // is a string
        assert(str.back() == str[0]);
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

bool Interpreter::parse_where_conds(std::vector<SqlCondition> &vec,
    const std::unique_ptr<sqltoast::boolean_term> &term)
{
    if(!term)
        return true;
    
    auto pred = term->factor->primary->predicate.get();
    if(pred->predicate_type != sqltoast::PREDICATE_TYPE_COMPARISON) {
        std::cout << "Error: only comparisons in WHERE statement is supported" << std::endl;
        return false;
    }
    auto comp_pred = static_cast<const sqltoast::comp_predicate_t *>(pred);
    // parse operand
    if(comp_pred->left->rvc_type != sqltoast::RVC_TYPE_ELEMENT ||
        comp_pred->right->rvc_type != sqltoast::RVC_TYPE_ELEMENT) {
        std::cout << "Error: aggregate types in WHERE statements are not supported" << std::endl;
        return false;
    }
    auto comp_l = static_cast<const sqltoast::row_value_constructor_element_t *>(comp_pred->left.get()),
         comp_r = static_cast<const sqltoast::row_value_constructor_element_t *>(comp_pred->right.get());
    if(comp_l->rvc_element_type != sqltoast::RVC_ELEMENT_TYPE_VALUE_EXPRESSION ||
        comp_r->rvc_element_type != sqltoast::RVC_ELEMENT_TYPE_VALUE_EXPRESSION) {
        std::cout << "Error: NULL or DEFAULT in WHERE statements are not supported" << std::endl;
        return false;
    }
    auto comp_l_1 = static_cast<const sqltoast::row_value_expression_t *>(comp_l),
         comp_r_1 = static_cast<const sqltoast::row_value_expression_t *>(comp_r);
    auto col_ref = col_ref_from_literal(comp_l_1);
    auto val = sql_val_from_literal(comp_r_1);
    if(!col_ref || !val)
        return false;
    // parse operator
    bool inverse = term->factor->reverse_op;
    Operator operator_;
    switch(comp_pred->op) {
    case sqltoast::COMP_OP_EQUAL:
        operator_ = !inverse ? Operator::EQ : Operator::NEQ;
        break;
    case sqltoast::COMP_OP_NOT_EQUAL:
        operator_ = !inverse ? Operator::NEQ : Operator::EQ;
        break;
    case sqltoast::COMP_OP_LESS:
        operator_ = !inverse ? Operator::LT : Operator::GEQ;
        break;
    case sqltoast::COMP_OP_GREATER:
        operator_ = !inverse ? Operator::GT : Operator::LEQ;
        break;
    case sqltoast::COMP_OP_LESS_EQUAL:
        operator_ = !inverse ? Operator::LEQ : Operator::GT;
        break;
    case sqltoast::COMP_OP_GREATER_EQUAL:
        operator_ = !inverse ? Operator::GEQ : Operator::LT;
        break;
    default:
        std::cout << "Error: unsupported comparison type in WHERE statement" << std::endl;
        return false;
    }

    vec.push_back(SqlCondition(*col_ref, operator_, *val));
    
    return parse_where_conds(vec, term->and_operand);
}
