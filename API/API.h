/*
 * @Author: Tianyu You 
 * @Date: 2020-05-24 16:27:42 
 * @Last Modified by: Tianyu You
 * @Last Modified time: 2020-05-24 17:59:25
 */

#ifndef MINISQL_API_H
#define MINISQL_API_H

#include <vector>
#include <string>

#include "Common.h"

using namespace MiniSQL;

namespace API {
    // Returning true means success, vice versa.

    bool useDatabase(const std::string &dbName);

    // std::pair<ValueName, ValueType>
    bool createTable(const std::string &tableName, const std::vector<std::pair<std::string, SqlValueType>> &schema, const std::string &primaryKeyName = "");

    bool dropTable(const std::string &tableName);

    bool createIndex(const std::string &tableName, const std::string &colomnName, const std::string &indexName);

    bool dropIndex(const std::string &indexName);

    // select * only
    bool select(const std::string &tableName, const std::vector<SqlCondition> &conditions);

    bool insertTuple(const std::string &tableName, const std::vector<SqlValue> &values);

    bool deleteTuple(const std::string &tableName, const std::vector<SqlCondition> &conditions);

} // namespace API

#endif