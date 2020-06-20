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
#include "../CatalogManager/CatalogManager.h"
#include "../Common/Common.h"
#include<iostream>

using namespace MiniSQL;

class RecordManager
{
private:
    /* data */
    BufferManager *bufferManager;
    IndexManager *indexManager;
    CatalogManager *catalogManager;

    // Check if there is a type conflicts when insert 
    bool checkType(TableInfo &tableInfo,const Tuple record);
    
    // Check if primary key duplicates
    //bool checkUnique(const std::vector<Tuple> &tuples,const Tuple record,TableInfo &tableInfo);

    // Check if predicates match
    bool checkConditions(TableInfo &tableInfo,const std::vector<SqlCondition> conditions,std::shared_ptr<std::vector<SqlValue> > record);

    bool writeRecord(TableInfo &tableInfo,const Tuple record,char* ptr);

    bool insertOneRecord(const std::string &tableName, const Tuple record);

    unsigned int getRecordSize(const std::string tableName);

    std::shared_ptr<std::vector<SqlValue>> readRecord(TableInfo &tableInfo,BYTE* ptr);

    void printResult(TableInfo& tableInfo,const std::vector<Tuple> &results);

    SqlValue* getValue(TableInfo &tableInfo,int indexOffset,int indexPos);

public:
    RecordManager(BufferManager *_bufferManager, IndexManager *_indexManager, CatalogManager *_catalogManager): bufferManager(_bufferManager), indexManager(_indexManager),catalogManager(_catalogManager) {};
    ~RecordManager();

    void createTable(const std::string &tableName);

    void dropTable(const std::string &tableName);

    bool insertRecord(const std::string &tableName,const std::vector<Tuple> records);

    int deleteRecord(const std::string &tableName, const std::vector<SqlCondition> &conditions);

    bool deleteRecord(const std::string &tableName);

    // To avoid large data copy in memory, print result here instead of returning values
    bool selectRecord(const std::string &tableName, const std::vector<SqlCondition> &conditions);

    // Large memory copy, un-efficiency
    //std::vector<Tuple> getTuples(const std::string &tableName);  

    void talbeTraversal(
        TableInfo &tableInfo,
        const std::vector<SqlCondition>& conditions,
        std::function<bool(BYTE*,size_t,std::shared_ptr<std::vector<SqlValue>>)> consumer
    );
    
    void linearTraversal(
        TableInfo &tableInfo,
        const std::vector<SqlCondition>& conditions,
        std::function<bool(BYTE*,size_t,std::shared_ptr<std::vector<SqlValue>>)> consumer
    );

    template <class T>
    inline T getAsType(SqlValue* sqlValue,SqlValueBaseType){
        switch(sqlValue->type){
            case SqlValueBaseType::MiniSQL_int: return sqlValue->int_val; break;
            case SqlValueBaseType::MiniSQL_char: return sqlValue->char_val; break;
            case SqlValueBaseType::MiniSQL_float: return sqlValue->float_val; break;
        }
    }

    void indexTraversal(
        TableInfo& tableInfo,
        int indexOffset,    // This offset marks the index of the record in the table
        std::vector<SqlCondition> conditions,
        std::function<bool(BYTE*,size_t,std::shared_ptr<std::vector<SqlValue>>)> consumer
    );

    void freeRecord(std::shared_ptr<std::vector<SqlValue>> record);

    void saveIndexes(TableInfo &tableInfo,const Tuple record,int offset);

};

#endif