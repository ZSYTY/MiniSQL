/*
 * @Author: Tianyu You 
 * @Date: 2020-05-24 16:27:42 
 * @Last Modified by: Tianyu You
 * @Last Modified time: 2020-05-24 18:33:40
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

        static APISingleton& getInstance(){
            static APISingleton instance;
            return instance;
        }
        
        RecordManager* getRecordManager() {
            if (recordManager == nullptr) {
                recordManager = new RecordManager;
            }
            return recordManager;
        }

        CatalogManager* getCatalogManager() {
            if (catalogManager == nullptr) {
                catalogManager = new CatalogManager;
            }
            return catalogManager;
        }

        IndexManager* getIndexManager() {
            if (indexManager == nullptr) {
                indexManager = new IndexManager;
            }
            return indexManager;
        }
        

    private:
        APISingleton() {}
        RecordManager *recordManager = nullptr;
        CatalogManager *catalogManager = nullptr;
        IndexManager *indexManager = nullptr;
        
    };

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