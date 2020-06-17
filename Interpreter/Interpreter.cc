#include "Interpreter.h"

#include <algorithm>
#include <iostream>
#include <fstream>

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
                } else if(directive == ".s" || directive == ".source") {
                    if(!sql.empty()) {
                        std::cout << "Error: cannot source from a file in this state" << std::endl;
                        continue;
                    }
                    auto path_begin = std::find_if(end, input.end(), [](int ch) {
                        return !std::isspace(ch);
                    });
                    auto path_end = input.end() - 1;
                    while(path_end > path_begin && std::isspace(*path_end))
                        --path_end;
                    ++path_end;
                    if(path_begin >= path_end) {
                        std::cout << "Error: empty filename" << std::endl;
                        continue;
                    }
                    std::string path(path_begin, path_end);
                    std::ifstream fin(path);
                    if(!fin) {
                        std::cout << "Error: cannot open file \"" << path << '\"' << std::endl;
                        fin.close();
                        continue;
                    }

                    char *buffer = new char[64 << 10];
                    while(true) {
                        while(!fin.eof() && fin.peek() == ';')
                            fin.ignore(1);
                        fin.get(buffer, (64 << 10), ';');
                        if(fin.eof()) {
                            for(int i = 0; i < fin.gcount(); i++) {
                                if(!isspace(buffer[i])) {
                                    std::cout << "Warning: SQL statements must end with a semicolon" << std::endl;
                                    break;
                                }
                            }
                            break;
                        }
                        if(!fin) {
                            std::cout << "Error: I/O failure" << std::endl;
                            break;
                        }
                        if(fin.gcount() == (64 << 10) - 1) {
                            std::cout << "Error: statement length limit exceeded" << std::endl;
                            break;
                        }
                        buffer[fin.gcount()] = ';';
                        execute_sql(sqltoast::parse_input_t(buffer, buffer + fin.gcount() + 1));
                    }
                    delete[] buffer;
                    fin.close();
                    continue;
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
                execute_sql(sqltoast::parse_input_t(sql.cbegin(), sql.cend()));
                std::cout << std::endl;
                sql.clear();
            }
        }
    }
}
