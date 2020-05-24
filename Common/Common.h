/*
 * @Author: Tianyu You 
 * @Date: 2020-05-24 16:50:16 
 * @Last Modified by: Tianyu You
 * @Last Modified time: 2020-05-24 23:14:22
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

typedef char BYTE;

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

    SqlValueType(SqlValueBaseType _type, bool _isPrimary, bool _isUnique, short _charLength): type(_type), isPrimary(_isPrimary), isUnique(_isUnique), charLength(_charLength){}

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
    
    SqlValue(SqlValueBaseType _type, int _int_val): type(_type), int_val(_int_val) {
        if (_type != SqlValueBaseType::MiniSQL_int) {
            throw std::runtime_error("Value types do not match");
        }
    }
    SqlValue(SqlValueBaseType _type, std::string _char_val): type(_type), char_val(_char_val) {
        if (_type != SqlValueBaseType::MiniSQL_int) {
            throw std::runtime_error("Value types do not match");
        }
    }
    SqlValue(SqlValueBaseType _type, float _float_val): type(_type), float_val(_float_val) {
        if (_type != SqlValueBaseType::MiniSQL_int) {
            throw std::runtime_error("Value types do not match");
        }
    }

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
    SqlCondition(std::string _colomnName, Operator _op, SqlValue _val): colomnName(_colomnName), op(_op), val(_val) {}
};

typedef std::vector<SqlValue> Tuple;

struct IndexInfo {
    std::string indexName;
    std::string tableName;
    std::string colomnName;
    IndexInfo(std::string _indexName, std::string _tableName, std::string _colomnName): indexName(_indexName), tableName(_tableName), colomnName(_colomnName) {}
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