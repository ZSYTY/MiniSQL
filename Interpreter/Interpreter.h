/*
 * @Author: Tianyu You 
 * @Date: 2020-05-24 16:28:51 
 * @Last Modified by: Chen Yu
 * @Last Modified time: 2020-06-14 17:07
 */

#ifndef MINISQL_INTERPRETER_H
#define MINISQL_INTERPRETER_H

#include <string>

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
};

#endif
