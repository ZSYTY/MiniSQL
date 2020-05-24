/*
 * @Author: Tianyu You 
 * @Date: 2020-05-24 16:27:42 
 * @Last Modified by: Tianyu You
 * @Last Modified time: 2020-05-24 20:19:48
 */

#ifndef MINISQL_API_H
#define MINISQL_API_H

#include <vector>
#include <string>

#include "../Common/Common.h"
#include "../RecordManager/RecordManager.h"
#include "../CatalogManager/CatalogManager.h"
#include "../IndexManager/IndexManager.h"


using namespace MiniSQL;

namespace API {
    class APISingleton {
    public:
        ~APISingleton() = default;
        APISingleton(const APISingleton&) = delete;
        APISingleton& operator=(const APISingleton&) = delete;

        // Usage: APISingleton &apiSingleton = API::APISingleton::getInstance();
        static APISingleton& getInstance(){
            static APISingleton instance;
            return instance;
        }

    private:
        APISingleton() {
            BufferManager *bufferManager = new BufferManager();
            recordManager = new RecordManager(bufferManager);
            catalogManager = new CatalogManager(bufferManager);
            indexManager = new IndexManager(bufferManager);
        }
        RecordManager *recordManager = nullptr;
        CatalogManager *catalogManager = nullptr;
        IndexManager *indexManager = nullptr;
    };

    // Returning true means success, vice versa.
    
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