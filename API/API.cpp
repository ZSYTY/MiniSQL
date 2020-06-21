#include "API.h"
#include <iostream>

API::APISingleton::~APISingleton() {
    delete bufferManager;
}

bool API::createTable(const std::string &tableName,
                      const std::vector<std::pair<std::string, SqlValueType> > &schema,
                      const std::string &primaryKeyName)
{
    APISingleton &apiSingleton = API::APISingleton::getInstance();
    CatalogManager *cm = apiSingleton.getCatalogManager();
    RecordManager *rm = apiSingleton.getRecordManager();

    // If there exists a table with the same name
    if (cm->ifTableExist(tableName))
    {
        std::cout << "Duplicate table name!" << std::endl;
        return false;
    }
    // If the user selects the prime key, check whether it is validate
    std::string primary_key;
    if (!primaryKeyName.empty())
    {
        bool flag = false;
        for (auto &item : schema)
        {
            // Whether there exists a valid column
            if (item.first == primaryKeyName)
            {
                flag = true;
                primary_key = item.first;
                break;
            }
        }
        if (!flag)
        {
            std::cout << "Invalid primary key name" << std::endl;
            return false;
        }
    }
    else
    {
        bool flag = false;
        for (auto &item: schema)
        {
            if (item.second.isUnique)
            {
                flag = true;
                primary_key = item.first;
                break;
            }
        }
        if (!flag)
        {
            std::cout << "There are no unique columns!" << std::endl;
            return false;
        }
    }

    cm->createTable(tableName, schema, primary_key);
    rm->createTable(tableName);
    API::createIndex(tableName, primary_key, "_" + primary_key);

    return true;
}

bool API::dropTable(const std::string &tableName)
{
    APISingleton &apiSingleton = API::APISingleton::getInstance();
    CatalogManager *cm = apiSingleton.getCatalogManager();

    // If there is no such table
    if (!cm->ifTableExist(tableName))
    {
        std::cout << "No such table!" << std::endl;
        return false;
    }

    return cm->deleteTable(tableName);
}

bool
API::createIndex(const std::string &tableName, const std::string &columnName,
                 const std::string &indexName)
{
    APISingleton &apiSingleton = API::APISingleton::getInstance();
    CatalogManager *cm = apiSingleton.getCatalogManager();
    IndexInfo indexInfo(indexName, tableName, columnName);

    // If there is no such table
    if (!cm->ifTableExist(tableName))
    {
        std::cout << "No such table!" << std::endl;
        return false;
    }
    // Get this table's information
    TableInfo tableInfo = cm->getTableInfo(tableName);
    bool flag = false;
    for (auto &item : tableInfo.columnName)
    {
        if(item == columnName)
        {
            flag = true;
        }
    }
    if (!flag)
    {
        std::cout << "No such column!" << std::endl;
        return false;
    }
    for (auto &item : tableInfo.indexes)
    {
        // If there was already a index in the column
        if (item.columnName == columnName)
        {
            std::cout << "There was already a index!" << std::endl;
            return false;
        }
    }

    // If CatalogManager create index successfully
    if (cm->createIndex(indexInfo))
    {
        IndexManager *im = apiSingleton.getIndexManager();
        if (im->buildIndex(indexInfo))
        {
            return true;
        }
        std::cout << "IndexManager create index failed" << std::endl;
        return false;
    }
    std::cout << "CatalogManager create index failed" << std::endl;
    return false;
}

bool API::dropIndex(const std::string &indexName)
{
    APISingleton &apiSingleton = API::APISingleton::getInstance();
    CatalogManager *cm = apiSingleton.getCatalogManager();

    // If CatalogManager delete index successfully
    if (cm->deleteIndex(indexName))
    {
        IndexManager *im = apiSingleton.getIndexManager();
        if (im->dropIndex(indexName))
        {
            return true;
        }
        std::cout << "IndexManager drop index failed" << std::endl;
        return false;
    }
    std::cout << "CatalogManager delete index failed" << std::endl;
    return false;
}

bool API::select(const std::string &tableName,
                 const std::vector<SqlCondition> &conditions)
{
    APISingleton &apiSingleton = API::APISingleton::getInstance();
    CatalogManager *cm = apiSingleton.getCatalogManager();

    // If there is no such table
    if (!cm->ifTableExist(tableName))
    {
        std::cout << "No such table!" << std::endl;
        return false;
    }
    RecordManager *rm = apiSingleton.getRecordManager();

    return rm->selectRecord(tableName, conditions);
}

bool API::insertTuple(const std::string &tableName,
                      const std::vector<SqlValue> &values)
{
    APISingleton &apiSingleton = API::APISingleton::getInstance();
    CatalogManager *cm = apiSingleton.getCatalogManager();
    // If there is no such table
    if (!cm->ifTableExist(tableName))
    {
        std::cout << "No such table!" << std::endl;
        return false;
    }
    cm->updateTableInfo(tableName, true);
    RecordManager *rm = apiSingleton.getRecordManager();

    std::vector<Tuple> tuple;
    tuple.push_back(values);

    return rm->insertRecord(tableName, tuple);
}

bool API::deleteTuple(const std::string &tableName,
                      const std::vector<SqlCondition> &conditions)
{
    APISingleton &apiSingleton = API::APISingleton::getInstance();
    CatalogManager *cm = apiSingleton.getCatalogManager();
    // If there is no such table
    if (!cm->ifTableExist(tableName))
    {
        std::cout << "No such table!" << std::endl;
        return false;
    }
    RecordManager *rm = apiSingleton.getRecordManager();

    int cnt = rm->deleteRecord(tableName, conditions);
    cm->updateTableInfo(tableName, false, cnt);

    return cnt != 0;
}