/*
 * @Author: Tianyu You 
 * @Date: 2020-05-24 18:21:29 
 * @Last Modified by: Tianyu You
 * @Last Modified time: 2020-06-20 01:58:24
 */

#ifndef MINISQL_INDEX_MANAGER_H
#define MINISQL_INDEX_MANAGER_H

#include <map>
#include <vector>

#include "../BufferManager/BufferManager.h"
#include "../CatalogManager/CatalogManager.h"
#include "../Common/Common.h"
#include "BPlus.h"

using namespace MiniSQL;

typedef BPlusTree<MiniSQL::SqlValueBaseType::MiniSQL_int> intTree;
typedef BPlusTree<MiniSQL::SqlValueBaseType::MiniSQL_char> charTree;
typedef BPlusTree<MiniSQL::SqlValueBaseType::MiniSQL_float> floatTree;

class IndexManager
{
private:
    /* data */
    BufferManager *bufferManager;
    CatalogManager *catalogManager;
    MiniSQL::TableInfo tableInfo;
    MiniSQL::IndexInfo indexInfo = MiniSQL::IndexInfo("", "", "");
    int columnIdx;

    typedef std::pair<std::string, std::string> treeKey;
    std::map<treeKey, int> intMap, charMap, floatMap;

    std::vector<intTree> intTreeList;
    std::vector<charTree> charTreeList;
    std::vector<floatTree> floatTreeList;

    MiniSQL::SqlValueBaseType getColomnType(const std::string &tableName, const std::string &columnName);

    intTree& getIntTree();
    charTree& getCharTree();
    floatTree& getFloatTree();

public:
    IndexManager(BufferManager *_bufferManager, CatalogManager *_catalogManager): bufferManager(_bufferManager), catalogManager(_catalogManager) {};
    ~IndexManager();
    
    bool buildIndex(const IndexInfo &index);

    bool dropIndex(const std::string &indexName);

    int search(const std::string &tableName, const std::string &columnName, const SqlValue &value);

    int searchEqual(const std::string &tableName, const std::string &columnName, const SqlValue &tuple);

    int searchHead(const std::string &tableName, const std::string &columnName);

    int searchNext(const std::string &tableName, const std::string &columnName);

    bool insertKey(const std::string &tableName, const Tuple &tuple, int offset);

    // bool removeKey(const std::string &tableName, const Tuple &tuple, int offset);
};


#endif