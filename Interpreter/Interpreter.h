/*
 * @Author: Tianyu You 
 * @Date: 2020-05-24 16:28:51 
 * @Last Modified by: Tianyu You
 * @Last Modified time: 2020-05-24 16:40:57
 */

#ifndef MINISQL_INTERPRETER_H
#define MINISQL_INTERPRETER_H

class Interpreter
{
private:
    /* data */
public:
    Interpreter(/* args */);
    ~Interpreter();
    
    /* run a SQL interpreter */
    void run();
};

#endif