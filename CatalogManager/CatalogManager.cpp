#include "CatalogManager.h"

bool CatalogManager::ifTableExist(const std::string &tableName)
{
    return bufferManager->ifFileExists(tableName + ".def");
}

bool CatalogManager::deleteTable(const std::string &tableName)
{
    if (!ifTableExist(tableName))
    {
        return false;
    }
    // Delete related files
    bufferManager->removeFile(tableName + ".def");
    bufferManager->removeFile(tableName + ".db");
    // Delete related indexes
    if (!bufferManager->ifFileExists("index"))
    {
        return true;
    }
    int block_cnt = bufferManager->getBlockCnt("index");
    std::stringstream ss;
    for (int i = 0; i < block_cnt; ++i) // 遍历所有块
    {
        bool needSetDirty = false;
        auto ptr2index = bufferManager->getBlock("index", i, true);
        // 当 *ptr2index = '\0' 说明该 index 已被删除
        std::string index_name, table_name, column_name;
        for (int j = 0; j < 32; ++j) // 遍历每一条记录
        {
            if (*ptr2index != '\0')
            {
                auto index_info = ptr2index;

                ss.clear();
                ss << index_info;
                ss >> index_name;
                index_info += index_name.size() + 1;

                ss.clear();
                ss << index_info;
                ss >> table_name;
                index_info += table_name.size() + 1;

                ss.clear();
                ss << index_info;
                ss >> column_name;

                if (table_name == tableName)
                {
                    *ptr2index = '\0';
                    needSetDirty = true;
                }
            }
            ptr2index += 128;
        }
        if (needSetDirty)
        {
            bufferManager->setDirty("index", i);
        }
    }
    return true;
}

TableInfo CatalogManager::getTableInfo(const std::string &tableName)
{
    if (!ifTableExist(tableName))
    {
        std::cerr << "No such table!" << std::endl;
    }
    TableInfo tableInfo;
    tableInfo.tableName = tableName;
    auto info = bufferManager->getBlock(tableName + ".def", 0);
//    struct TableInfo {
//        int columnCnt, recordCnt;
//        std::string tableName;
//        std::string primaryKeyName;
//        std::vector<std::string> columnName;
//        std::vector<SqlValueType> columnType;
//        std::vector<IndexInfo> indexes;
//    };
    // cnt 用来得到两个 int 的值
    int *cnt = (int *) info;
    tableInfo.columnCnt = *cnt;
    tableInfo.recordCnt = *(cnt + 1);

    std::stringstream ss; // 用于操作字符串

    // name 用来得到 primaryKeyName
    BYTE *primary_key_name = info + 2 * sizeof(int);
    ss << primary_key_name;
    ss >> tableInfo.primaryKeyName;
    primary_key_name += tableInfo.primaryKeyName.size() + 1; //跳过 '\0'

    // columnInfo 用来得到 column 的信息
    BYTE *columnInfo = primary_key_name;
    for (int i = 0; i < tableInfo.columnCnt; ++i)
    {
        // 开始读取 columnName
        ss.clear();
        ss << columnInfo;
        std::string cName;
        ss >> cName;
        tableInfo.columnName.push_back(cName);
        columnInfo += cName.size() + 1; //跳过 '\0'

        // 开始读取 columnType
        auto svbType = (SqlValueBaseType) (*columnInfo);
        columnInfo += sizeof(SqlValueBaseType);

        auto is_primary = (bool) (*columnInfo);
        columnInfo += sizeof(bool);

        auto is_unique = (bool) (*columnInfo);
        columnInfo += sizeof(bool);

        auto char_length = (short) (*columnInfo);
        columnInfo += sizeof(short);

        SqlValueType svt(svbType, is_primary, is_unique, char_length);
        tableInfo.columnType.push_back(svt);
    }

    int block_cnt = bufferManager->getBlockCnt("index");
    /*
    struct IndexInfo {
    std::string indexName;
    std::string tableName;
    std::string columnName;
    };
    */

    for (int i = 0; i < block_cnt; ++i)
    {
        auto ptr2index = bufferManager->getBlock("index", i);
        // 当 *ptr2index = '\0' 说明该 index 已被删除
        std::string index_name, table_name, column_name;
        for (int j = 0; j < 32; ++j)
        {
            if (*ptr2index != '\0')
            {
                auto index_info = ptr2index;
                ss.clear();
                ss << index_info;
                ss >> index_name;
                index_info += index_name.size() + 1;

                ss.clear();
                ss << index_info;
                ss >> table_name;
                index_info += table_name.size() + 1;

                ss.clear();
                ss << index_info;
                ss >> column_name;

                if (table_name == tableName)
                {
                    IndexInfo indexInfo(index_name, table_name, column_name);
                    tableInfo.indexes.push_back(indexInfo);
                }
            }
            ptr2index += 128;
        }
    }

    return tableInfo;
}

// 每块固定存放 32 条index，每个 index 有 128 byte
bool CatalogManager::createIndex(const IndexInfo &index)
{
    int modified_block_offset = 0;
    // If there is no .index file, initialize one
    if (!bufferManager->ifFileExists("index"))
    {
        bufferManager->createFile("index");
    }
    else
    {
        modified_block_offset = bufferManager->getBlockCnt("index") - 1;
    }
    auto ptr2index = bufferManager->getBlock("index", 0, true);
    // 如果整块都已经满了
    if (*(ptr2index + 31 * 128) != '\0')
    {
        ptr2index = bufferManager->getBlock("index", ++modified_block_offset, true);
    }
    while (*ptr2index != '\0')
    {
        ptr2index += 128;
    }
    auto start = ptr2index;
    index.indexName.copy(ptr2index, index.indexName.size());
    *(ptr2index + index.indexName.size()) = '\0';
    ptr2index += index.indexName.size() + 1;

    index.tableName.copy(ptr2index, index.tableName.size());
    *(ptr2index + index.tableName.size()) = '\0';
    ptr2index += index.tableName.size() + 1;

    index.columnName.copy(ptr2index, index.columnName.size());
    *(ptr2index + index.columnName.size()) = '\0';

    /****** Test Result *******/
//    for (int i = 0; i < index.indexName.size(); ++i)
//    {
//        std::cout << *(start + i);
//    }
//    std::cout << '\n';
//    start += index.indexName.size() + 1;
//
//    for (int i = 0; i < index.tableName.size(); ++i)
//    {
//        std::cout << *(start + i);
//    }
//    std::cout << '\n';
//    start += index.tableName.size() + 1;
//
//    for (int i = 0; i < index.columnName.size(); ++i)
//    {
//        std::cout << *(start + i);
//    }
//    std::cout << '\n';


    bufferManager->setDirty("index", 0);

    return true;
}

bool CatalogManager::createTable(const std::string &tableName,
                                 const std::vector<std::pair<std::string, SqlValueType>> &schema,
                                 const std::string &primaryKeyName)
{
    if (ifTableExist(tableName))
    {
        return false;
    }
    bufferManager->createFile(tableName + ".def");
    auto info = bufferManager->getBlock(tableName + ".def", 0, true);
    // cnt 用来写入两个 int 的值
    int *cnt = (int *) info;
    *cnt = schema.size(); // columnCnt
    *(cnt + 1) = 0; // recordCnt

    /****** Result of columnCnt and recordCnt ******/
//    std::cout << "cnt = " << *cnt << std::endl;
//    std::cout << "recordCnt = " << *(cnt + 1) << std::endl;

    // primary_key_name 用来写入 primary_key 的值
    char *primary_key_name = info + sizeof(int) * 2;
    primaryKeyName.copy(primary_key_name, primaryKeyName.size());
    *(primary_key_name + primaryKeyName.size()) = '\0';
    primary_key_name += primaryKeyName.size() + 1;

    // column_info 用来写入各个 column 的值
    char *column_info = primary_key_name;

    /****** A Simple Test ******/
//    std::string hello = "hello";
//    strcpy(column_info, hello.c_str());
//    *(column_info + hello.size()) = '\0';
//    column_info += hello.size() + 1;
//
//    std::string world = "world";
//    strcpy(column_info, world.c_str());
//    *(column_info + hello.size()) = '\0';
//    column_info += hello.size() + 1;

    for (const auto &item : schema)
    {
        // 写入 columnName
        std::cout << item.first << std::endl;
        item.first.copy(column_info, item.first.size());
        *(column_info + item.first.size()) = '\0';
        column_info += item.first.size() + 1;

        // 写入 SqlBaseValueType
        auto svbType = (SqlValueBaseType *) column_info;
        *svbType = item.second.type;
        column_info += sizeof(SqlValueBaseType);

        // 写入 isPrimary
        auto is_primary = (bool *) column_info;
        *is_primary = item.second.isPrimary;
        column_info += sizeof(bool);

        // 写入 isUnique
        auto is_unique = (bool *) column_info;
        *is_unique = item.second.isUnique;
        column_info += sizeof(bool);

        // 写入 charLength
        auto char_length = (short *) column_info;
        *char_length = item.second.charLength;
        column_info += sizeof(short);
    }
    bufferManager->setDirty(tableName + ".def", 0);

    return true;
}

bool CatalogManager::deleteIndex(const std::string &indexName)
{
    int block_cnt = bufferManager->getBlockCnt("index");
    std::stringstream ss;
    for (int i = 0; i < block_cnt; ++i)
    {
        auto ptr2index = bufferManager->getBlock("index", i);
        // 当 *ptr2index = '\0' 说明该 index 已被删除
        std::string index_name, table_name, column_name;
        for (int j = 0; j < 32; ++j)
        {
            if (*ptr2index != '\0')
            {
                auto index_info = ptr2index;

                ss.clear();
                ss << index_info;
                ss >> index_name;
                index_info += index_name.size() + 1;

                ss.clear();
                ss << index_info;
                ss >> table_name;
                index_info += table_name.size() + 1;

                ss.clear();
                ss << index_info;
                ss >> column_name;

                if (index_name == indexName)
                {
                    *ptr2index = '\0';
                    bufferManager->setDirty("index", i);
                    return true;
                }
            }
            ptr2index += 128;
        }
    }

    return false;
}

void CatalogManager::updateTableInfo(const std::string &tableName, bool isInsert, int num)
{
    auto info = bufferManager->getBlock(tableName + ".def", 0, true);
    int *cnt = (int *) info;
    int record_cnt = *(cnt + 1);
    if (isInsert)
    {
        *(cnt + 1) = record_cnt + 1; // recordCnt
    }
    else
    {
        *(cnt + 1) = record_cnt - num;
    }

    bufferManager->setDirty(tableName + ".def", 0);
}