#include "Interpreter.h"

#include <iostream>
#include "API/API.h"

static bool process_where_conds(std::vector<SqlCondition> &vec,
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
    if(comp_l_1->value->type != sqltoast::VALUE_EXPRESSION_TYPE_NUMERIC_EXPRESSION ||
        comp_r_1->value->type != sqltoast::VALUE_EXPRESSION_TYPE_NUMERIC_EXPRESSION) {
        std::cout << "Error: only numeric types in WHERE statements are supported" << std::endl;
        return false;
    }
    auto comp_l_2 = static_cast<const sqltoast::numeric_expression_t *>(comp_l_1->value.get()),
         comp_r_2 = static_cast<const sqltoast::numeric_expression_t *>(comp_r_1->value.get());
    if(comp_l_2->op != sqltoast::NUMERIC_OP_NONE || comp_r_2->op != sqltoast::NUMERIC_OP_NONE
        || comp_l_2->left->op != sqltoast::NUMERIC_OP_NONE || comp_r_2->left->op != sqltoast::NUMERIC_OP_NONE) {
        std::cout << "Error: arithmetics in WHERE statements are supported yet" << std::endl;
        return false;
    }
    if(comp_l_2->left->left->sign != 0) {
        std::cout << "Error: arithmetics in WHERE statements are supported yet" << std::endl;
        return false;
    }
    auto val_sign = comp_r_2->left->left->sign;
    const auto &comp_l_3 = comp_l_2->left->left->primary,
               &comp_r_3 = comp_r_2->left->left->primary;
    if(comp_l_3->type != sqltoast::NUMERIC_PRIMARY_TYPE_VALUE ||
        comp_r_3->type != sqltoast::NUMERIC_PRIMARY_TYPE_VALUE) {
        std::cout << "Error: functions in WHERE statements are supported yet" << std::endl;
        return false;
    }
    auto comp_l_4 = static_cast<const sqltoast::numeric_value_t *>(comp_l_3.get()),
         comp_r_4 = static_cast<const sqltoast::numeric_value_t *>(comp_r_3.get());
    if(comp_l_4->primary->vep_type != sqltoast::VEP_TYPE_COLUMN_REFERENCE ||
        comp_r_4->primary->vep_type != sqltoast::VEP_TYPE_UNSIGNED_VALUE_SPECIFICATION) {
        std::cout << "Error: in a comparison, the lhs must be a column reference, "
            "and the rhs must be a number" << std::endl;
        return false;
    }
    std::string col_ref(comp_l_4->primary->lexeme.start, comp_l_4->primary->lexeme.end);
    std::string val_str(comp_r_4->primary->lexeme.start, comp_r_4->primary->lexeme.end);
    int val = std::stoi(val_str);
    if(val_sign > 0)
        val = std::abs(val);
    else if(val_sign < 0)
        val = -val;
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

    vec.push_back(SqlCondition(col_ref, operator_, SqlValue(SqlValueBaseType::MiniSQL_int, val)));
    
    return process_where_conds(vec, term->and_operand);
}

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
            if(!process_where_conds(where_cond, bool_term))
                return;
        }
    }

    // TODO stub
    // API::select(table_name, where_cond);
    std::cout << "Selecting * from " << table_name;
    if(!where_cond.empty()) {
        std::cout << " where ";
        for(const auto &c : where_cond) {
            std::cout << c.columnName << ' ' << (int) c.op << ' ' << c.val.int_val << ", ";
        }
    }
    std::cout << std::endl;
}
