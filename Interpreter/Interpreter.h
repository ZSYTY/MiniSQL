/*
 * @Author: Tianyu You 
 * @Date: 2020-05-24 16:28:51 
 * @Last Modified by: Chen Yu
 * @Last Modified time: 2020-06-14 17:07
 */

#ifndef MINISQL_INTERPRETER_H
#define MINISQL_INTERPRETER_H

#include <string>

#include <sqltoast/sqltoast.h>

class Interpreter
{
public:
    Interpreter();
    ~Interpreter();
    
    /* run an SQL interpreter */
    void run();

private:
    /* execute an SQL statement */
    void execute_sql(const std::string &statement);
    
    /* execute sqltoast statements */
    void execute_select(const sqltoast::select_statement_t *stmt);
};

#endif
