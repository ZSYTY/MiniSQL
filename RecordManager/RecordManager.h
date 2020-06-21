/*
 * @Author: Tianyu You 
 * @Date: 2020-05-24 18:21:25 
 * @Last Modified by: Tianyu You
 * @Last Modified time: 2020-05-24 21:06:08
 */

#ifndef MINISQL_RECORD_MANAGER_H
#define MINISQL_RECORD_MANAGER_H

#include "../BufferManager/BufferManager.h"
#include "../IndexManager/IndexManager.h"
#include "../Common/Common.h"

using namespace MiniSQL;

class RecordManager
{
private:
    /* data */
    BufferManager *bufferManager;
    IndexManager *indexManager;
public:
    RecordManager(BufferManager *_bufferManager, IndexManager *_indexManager): bufferManager(_bufferManager), indexManager(_indexManager) {};
    ~RecordManager();
    
    bool insertRecord(const std::string &tableName, const std::vector<Tuple> records);

    bool deleteRecord(const std::string &tableName, const std::vector<SqlCondition> &conditions);

    // To avoid large data copy in memory, print result here instead of returning values
    bool selectRecord(const std::string &tableName, const std::vector<SqlCondition> &conditions);

    /* This one may be useful */
    /* std::vector<Tuple> selectRecord(const std::string &tableName, const std::vector<SqlCondition> &conditions); */
    
};


#endif