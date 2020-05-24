/*
 * @Author: Tianyu You 
 * @Date: 2020-05-24 16:50:16 
 * @Last Modified by: Tianyu You
 * @Last Modified time: 2020-05-24 20:25:30
 */

#ifndef MINISQL_COMMON_H
#define MINISQL_COMMON_H

#include <string>
#include <stdexcept>

namespace MiniSQL {

/* Block size of buffer and size of BPlus tree node: 4KB */
const int BlockSize = 4096;
const int MaxCharLength = 255;
const int MaxColomnCnt = 32;

enum class SqlValueBaseType {
    MiniSQL_int,
    MiniSQL_char,
    MiniSQL_float
};

struct SqlValueType {
    SqlValueBaseType type;
    bool isPrimary = false;
    bool isUnique = false;
    short charLength; // '\0' not included

    inline size_t getSize() const {
        switch (type) {
            case SqlValueBaseType::MiniSQL_int:
                return sizeof(int);
            case SqlValueBaseType::MiniSQL_char:
                return sizeof(char) * (charLength + 1);
            case SqlValueBaseType::MiniSQL_float:
                return sizeof(float);
        }
    }
};

struct SqlValue {
    SqlValueBaseType type;
    int int_val;
    std::string char_val;
    float float_val;
    
    inline bool operator< (const SqlValue &rhs) const {
        if (type == rhs.type) {
            switch (type) {
                case SqlValueBaseType::MiniSQL_int:
                    return int_val < rhs.int_val;
                case SqlValueBaseType::MiniSQL_char:
                    return char_val < rhs.char_val;
                case SqlValueBaseType::MiniSQL_float:
                    return float_val < rhs.float_val;
            }
        } else {
            throw std::runtime_error("Value types do not match");
        }
    }

    inline bool operator> (const SqlValue &rhs) const {
        return rhs.operator<(*this);
    }

    inline bool operator== (const SqlValue &rhs) const {
        if (type == rhs.type) {
            switch (type) {
                case SqlValueBaseType::MiniSQL_int:
                    return int_val == rhs.int_val;
                case SqlValueBaseType::MiniSQL_char:
                    return char_val == rhs.char_val;
                case SqlValueBaseType::MiniSQL_float:
                    return float_val == rhs.float_val;
            }
        } else {
            throw std::runtime_error("Value types do not match");
        }
    }

    inline bool operator!= (const SqlValue &rhs) const {
        return !operator==(rhs);
    }

    inline bool operator<= (const SqlValue &rhs) const {
        return operator<(rhs) or operator==(rhs);
    }

    inline bool operator>= (const SqlValue &rhs) const {
        return !operator<(rhs);
    }
};

enum class Operator {
    LT,     // <
    GT,     // >
    LEQ,    //<=
    GEQ,    //>=
    EQ,     // ==
    NEQ,    // !=
};

struct SqlCondition {
    std::string colomnName;
    Operator op;
    SqlValue val;
};

typedef std::vector<SqlValue> Tuple;

struct IndexInfo {
    std::string indexName;
    std::string tableName;
    std::string colomnName;
};

struct TableInfo {
    int colomnCnt, recordCnt;
    std::string tableName;
    std::string primaryKeyName;
    std::vector<std::string> colomnName;
    std::vector<SqlValueType> colomnType;
    std::vector<IndexInfo> indexes;
};


} // namesapce MiniSQL

#endif