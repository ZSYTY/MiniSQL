/*
 * @Author: Tianyu You 
 * @Date: 2020-05-24 16:28:51 
 * @Last Modified by: Chen Yu
 * @Last Modified time: 2020-06-14 17:07
 */

#ifndef MINISQL_INTERPRETER_H
#define MINISQL_INTERPRETER_H

#include <optional>
#include <string>
#include <sqltoast/sqltoast.h>
#include "Common/Common.h"

using namespace MiniSQL;

class Interpreter
{
public:
    Interpreter();
    ~Interpreter();
    
    /* run an SQL interpreter */
    void run();

private:
    /* execute an SQL statement */
    void execute_sql(sqltoast::parse_input_t subject);
    
    /* execute sqltoast statements */
    void execute_select(const sqltoast::select_statement_t *stmt);
    void execute_insert(const sqltoast::insert_statement_t *stmt);
    void execute_delete(const sqltoast::delete_statement_t *stmt);
    void execute_create_table(const sqltoast::create_table_statement_t *stmt);
    void execute_drop_table(const sqltoast::drop_table_statement_t *stmt);
    void execute_create_index(const sqltoast::create_index_statement_t *stmt);
    void execute_drop_index(const sqltoast::drop_index_statement_t *stmt);

    /* helper functions for parsing SQL statements */
    bool parse_where_conds(std::vector<SqlCondition> &vec,
        const std::unique_ptr<sqltoast::boolean_term> &term);
    SqlValue sql_val_from_string(const std::string &str, int8_t sign = 0);
    std::optional<SqlValue> sql_val_from_literal(const sqltoast::row_value_expression_t *lit);
    std::optional<std::string> col_ref_from_literal(const sqltoast::row_value_expression_t *lit);
};

#endif
