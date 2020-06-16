#include "API.h"
#include <iostream>

bool API::createTable(const std::string &tableName,
                      const std::vector<std::pair<std::string, SqlValueType> > &schema,
                      const std::string &primaryKeyName)
{
    APISingleton &apiSingleton = API::APISingleton::getInstance();
    CatalogManager *cm = apiSingleton.getCatalogManager();

    // If there exists a table with the same name
    if (cm->ifTableExist(tableName))
    {
        // TODO duplicate table name
        std::cout << "Duplicate table name!" << std::endl;
        return false;
    }
    // If the user selects the prime key, check whether it is validate
    if (!primaryKeyName.empty())
    {
        for (auto &item : schema)
        {
            // If there exists a valid column
            if (item.first == primaryKeyName)
            {
                // TODO duplicate table name
                std::cout << "Invalid primary key name" << std::endl;
                return false;
            }
        }

    }

    cm->createTable(tableName, schema, primaryKeyName);

    return true;
}

bool API::dropTable(const std::string &tableName)
{
    APISingleton &apiSingleton = API::APISingleton::getInstance();
    CatalogManager *cm = apiSingleton.getCatalogManager();

    // If there is no such table
    if (!cm->ifTableExist(tableName))
    {
        // TODO no such table
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
        // TODO no such table
        std::cout << "No such table!" << std::endl;
        return false;
    }
    // Get this table's information
    TableInfo tableInfo = cm->getTableInfo(tableName);
    for (auto &item : tableInfo.indexes)
    {
        // If there was already a index in the column
        if (item.columnName == columnName)
        {
            // TODO already has a index
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
        // TODO IndexManager create index failed
        std::cout << "IndexManager create index failed" << std::endl;
        return false;
    }
    // TODO CatalogManager create index failed
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
        // TODO IndexManager create index failed
        std::cout << "IndexManager drop index failed" << std::endl;
        return false;
    }
    // TODO CatalogManager delete index failed
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
        // TODO no such table
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
        // TODO no such table
        std::cout << "No such table!" << std::endl;
        return false;
    }
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
        // TODO no such table
        std::cout << "No such table!" << std::endl;
        return false;
    }
    RecordManager *rm = apiSingleton.getRecordManager();

    return rm->deleteRecord(tableName, conditions);
}