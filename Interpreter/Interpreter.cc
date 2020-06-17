#include "Interpreter.h"

#include <algorithm>
#include <iostream>

static constexpr auto GREETINGS = "MiniSQL Interpreter v0.1\n\n";

Interpreter::Interpreter() {}

Interpreter::~Interpreter() {}

void Interpreter::run()
{
    std::cout << GREETINGS;
    std::string sql;    // buffer for SQL statements

    while(!std::cin.eof()) {
        if(sql.empty())
            std::cout << "MiniSQL > ";
        else
            std::cout << "        > ";

        std::string input;
        std::getline(std::cin, input);

        {
            // check if the input is an interpreter directive
            // e.g. ".exit", ".break"
            auto it = std::find_if(input.begin(), input.end(), [](int ch) {
                return !std::isspace(ch);
            });
            if(it == input.end())   // the input is empty
                continue;
            if(*it == '.') {
                auto end = std::find_if(it + 1, input.end(), [](int ch) {
                    return std::isspace(ch);
                });
                std::string directive(it, end);
                if(directive == ".b" || directive == ".break") {
                    sql.clear();
                    continue;
                } else if(directive == ".e" || directive == ".exit" || directive == ".q"
                    || directive == ".quit") {
                    std::cout << "Bye\n\n";
                    return;
                } else {
                    std::cout << "Error: unknown interpreter directive \"" << directive << '\"'
                        << std::endl;
                    sql.clear();
                    continue;
                }
            }
        }

        {
            // else, append the input to SQL buffer
            sql += input + ' ';
            if(sql.find(';') != std::string::npos) {
                // execute the statement
                // currently, only one statement per line is supported
                execute_sql(sql);
                std::cout << std::endl;
                sql.clear();
            }
        }
    }
}
