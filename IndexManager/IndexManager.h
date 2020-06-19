/*
 * @Author: Tianyu You 
 * @Date: 2020-05-24 18:21:29 
 * @Last Modified by: Tianyu You
 * @Last Modified time: 2020-05-24 23:22:03
 */

#ifndef MINISQL_INDEX_MANAGER_H
#define MINISQL_INDEX_MANAGER_H

#include "../BufferManager/BufferManager.h"
#include "../Common/Common.h"

using namespace MiniSQL;

class IndexManager
{
private:
    /* data */
    BufferManager *bufferManager;
public:
    IndexManager(BufferManager *_bufferManager): bufferManager(_bufferManager) {};
    ~IndexManager();
    
    bool buildIndex(const IndexInfo &index);

    bool dropIndex(const std::string &indexName);

    int search(const std::string &tableName, const std::string columnName,SqlValue sqlValue);

    int searchHead(const std::string &tableName, SqlValueBaseType type);

    int searchNext(const std::string &tableName, SqlValueBaseType type);

    bool insertKey(const std::string &filename, const Tuple &tuple, int offset);

    bool removeKey(const std::string &filename, const Tuple &tuple);
};


#endif