#include "Interpreter.h"

#include <iostream>
#include <unordered_map>
#include "API/API.h"

void Interpreter::execute_create_table(const sqltoast::create_table_statement_t *stmt)
{
    if(stmt->table_type != sqltoast::TABLE_TYPE_NORMAL) {
        std::cout << "Error: temporary tables are not supported" << std::endl;
        return;
    }

    std::string table_name(stmt->table_name.start, stmt->table_name.end);

    std::vector<std::pair<std::string, SqlValueType> > cols;
    std::unordered_map<std::string, size_t> col_map;
    std::string primary_key;

    for(const auto &col_def : stmt->column_definitions) {
        std::string col_name(col_def->name.start, col_def->name.end);
        if(col_map.count(col_name)) {
            std::cout << "Error: duplicate column name `" << primary_key << '`' << std::endl;
            return;
        }
        // parse data type
        SqlValueBaseType col_type;
        int char_len = 0;
        switch(col_def->data_type->type) {
        case sqltoast::DATA_TYPE_INT: {
            col_type = SqlValueBaseType::MiniSQL_int;
            break;
        }
        case sqltoast::DATA_TYPE_FLOAT: {
            col_type = SqlValueBaseType::MiniSQL_float;
            break;
        }
        case sqltoast::DATA_TYPE_CHAR: {
            col_type = SqlValueBaseType::MiniSQL_char;
            auto char_def = static_cast<const sqltoast::char_string_t *>(col_def->data_type.get());
            char_len = (int) char_def->size;
            break;
        }
        default: {
            std::cout << "Error: unsupported value type "
                << col_def->data_type->type << std::endl;
            return;
        }
        }

        // parse inline constraints
        bool primary = false, unique = false;
        if(col_def->default_descriptor) {
            std::cout << "Error: default values are not supported" << std::endl;
            return;
        }
        for(const auto &constr : col_def->constraints) {
            if(constr->type == sqltoast::CONSTRAINT_TYPE_PRIMARY_KEY) {
                if(!primary_key.empty()) {
                    std::cout << "Error: only one PRIMARY KEY is allowed" << std::endl;
                    return;
                }
                primary_key = col_name;
                primary = true;
            } else if(constr->type == sqltoast::CONSTRAINT_TYPE_UNIQUE) {
                unique = true;
            } else {
                std::cout << "Error: only PRIMARY KEY or UNIQUE constraints are supported" << std::endl;
                return;
            }
        }

        cols.push_back(std::make_pair(col_name, SqlValueType(col_type, primary, unique, char_len)));
        col_map[col_name] = cols.size() - 1;
    }

    // parse out-of-line constraints
    std::cout<<stmt->constraints.size()<<std::endl;
    for(const auto &constr : stmt->constraints) {
        if(constr->type == sqltoast::CONSTRAINT_TYPE_PRIMARY_KEY) {
            if(!primary_key.empty()) {
                std::cout << "Error: only one PRIMARY KEY is allowed" << std::endl;
                return;
            }
            if(constr->columns.size() != 1) {
                std::cout << "Error: number of columns in a PRIMARY KEY must be exactly 1" << std::endl;
                return;
            }
            primary_key = std::string(constr->columns[0].start, constr->columns[0].end);
            if(!col_map.count(primary_key)) {
                std::cout << "Error: column `" << primary_key << "` is not defined" << std::endl;
                return;
            }
            std::cout<<"PK "<<primary_key<<std::endl;
            // col_map[primary_key]->isPrimary = true;
            auto col = std::move(cols[col_map[primary_key]]).second;
            col.isPrimary = true;
            cols[col_map[primary_key]] = std::make_pair(primary_key, std::move(col));
        } else if(constr->type == sqltoast::CONSTRAINT_TYPE_UNIQUE) {
            if(constr->columns.size() != 1) {
                std::cout << "Error: number of columns in a UNIQUE constraint must be exactly 1" << std::endl;
                return;
            }
            std::string unique_key(constr->columns[0].start, constr->columns[0].end);
            if(!col_map.count(unique_key)) {
                std::cout << "Error: column `" << primary_key << "` is not defined" << std::endl;
                return;
            }
            // col_map[unique_key]->isUnique = true;
            auto col = std::move(cols[col_map[unique_key]]).second;
            col.isUnique = true;
            cols[col_map[unique_key]] = std::make_pair(unique_key, std::move(col));
        } else {
            std::cout << "Error: only PRIMARY KEY or UNIQUE constraints are supported" << std::endl;
            return;
        }
    }

    // API::createTable(table_name, cols, primary_key);
    std::cout << "Creating table " << table_name << std::endl;
    for(const auto &col : cols) {
        std::cout << " - " << col.first << ' ' << (int) col.second.type;
        if(col.second.type == SqlValueBaseType::MiniSQL_char)
            std::cout << '(' << col.second.charLength << ')';
        if(col.second.isPrimary)
            std::cout << " primary key";
        if(col.second.isUnique)
            std::cout << " unique";
        std::cout << std::endl;
    }
}
