/*
 * @Author: Tianyu You 
 * @Date: 2020-05-24 18:21:30 
 * @Last Modified by: Tianyu You
 * @Last Modified time: 2020-05-24 20:37:02
 */

#ifndef MINISQL_CATALOG_MANAGER_H
#define MINISQL_CATALOG_MANAGER_H

#include "../BufferManager/BufferManager.h"
#include "../Common/Common.h"

using namespace MiniSQL;

class CatalogManager
{
private:
    /* data */
    BufferManager *bufferManager;
public:
    CatalogManager(BufferManager *_bufferManager): bufferManager(_bufferManager) {};
    ~CatalogManager();

    bool ifTableExist(const std::string &tableName);

    TableInfo getTableInfo(const std::string &tableName);

    bool createTable(const std::string &tableName, const std::vector<std::pair<std::string, SqlValueType>> &schema, const std::string &primaryKeyName);

    bool deleteTable(const std::string &tableName);

    bool createIndex(const IndexInfo &index);

    bool deleteIndex(const std::string &indexName);
};


#endif