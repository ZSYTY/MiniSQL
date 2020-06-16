#include "CatalogManager.h"

bool CatalogManager::ifTableExist(const std::string &tableName)
{
    return bufferManager->ifFileExists(tableName + ".def");
}

bool CatalogManager::deleteTable(const std::string &tableName)
{
    // Delete related files
    if (ifTableExist(tableName))
    {
        bufferManager->removeFile(tableName + ".def");
        bufferManager->removeFile(tableName + ".data");
        // Delete related indexes
        // TODO
        if (bufferManager->ifFileExists(".index"))
        {
            auto *ptr2index = (Block *) bufferManager->getBlock(".index", 0);
            auto using_space = (unsigned short *) (ptr2index->content);
            auto info_num = (unsigned short *) (ptr2index->content + sizeof(short));
            for (unsigned int i = 0; i < *info_num; ++i)
            {
                auto ptr = (IndexInfo *) ptr2index->content + sizeof(short) * (2 + i);
                if ((*ptr).indexName == "XXX")
                {
                    // TODO
                }
            }
        }
        return true;
    }

    return false;
}

TableInfo CatalogManager::getTableInfo(const std::string &tableName)
{
    if (!ifTableExist(tableName))
    {
        std::cerr << "No such table!" << std::endl;
    }
    TableInfo tableInfo;
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
    // name 用来得到 tableName 和 primaryKeyName
    BYTE *name = info + 2 * sizeof(int);
    int length = GetLength(name);
    tableInfo.tableName.copy(name, length + 1, 0); // 连同 '\0' 一起复制进去
    name += length + 1;
    length = GetLength(name);
    tableInfo.primaryKeyName.copy(name, length + 1, 0); // 连同 '\0' 一起复制进去
    name += length + 1;
    // columnInfo 用来得到 column 的信息
    BYTE *columnInfo = name;
    for (int i = 0; i < tableInfo.columnCnt; ++i)
    {
        // 开始读取 columnName
        length = GetLength(columnInfo);
        std::string cName;
        cName.copy(columnInfo, length + 1, 0); // 连同 '\0' 一起复制进去
        tableInfo.columnName.push_back(cName);
        columnInfo += length + 1;
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

    int block_cnt = bufferManager->getBlockCnt(".index");
    /*
    struct IndexInfo {
    std::string indexName;
    std::string tableName;
    std::string columnName;
    };
    */
    for (int i = 0; i < block_cnt; ++i)
    {
        auto *tmp = (Block *) bufferManager->getBlock(".index", i);
        auto ptr2index = tmp->content;
        // 当 *ptr2index = '\0' 说明该 index 已被删除
        std::string index_name, table_name, column_name;
        if (*ptr2index != '\0')
        {
            auto index_info = ptr2index;
            length = GetLength(index_info);
            index_name.copy(index_info, length + 1, 0);
            index_info += length + 1;
            length = GetLength(index_info);
            table_name.copy(index_info, length + 1, 0);
            index_info += length + 1;
            length = GetLength(index_info);
            column_name.copy(index_info, length + 1, 0);
            if (table_name == tableName or table_name == tableName + '\0')
            {
                IndexInfo indexInfo(index_name, table_name, column_name);
                tableInfo.indexes.push_back(indexInfo);
            }
        }
        ptr2index += 128;
    }

    return tableInfo;
}

// 每块固定存放 32 条index，每个 index 有 128 byte
bool CatalogManager::createIndex(const IndexInfo &index)
{
    // If there is no .index file, initialize one
    if (!bufferManager->ifFileExists(".index"))
    {
        bufferManager->createFile(".index");
    }
    int block_cnt = bufferManager->getBlockCnt(".index");
    auto *tmp = (Block *) bufferManager->getBlock(".index", block_cnt - 1);
    auto ptr2index = tmp->content;
    while (*ptr2index != '\0')
    {
        ptr2index += 128;
    }
    strcpy(ptr2index, index.indexName.c_str());
    *(ptr2index + index.indexName.size()) = '\0';
    ptr2index += index.indexName.size() + 1;

    strcpy(ptr2index, index.tableName.c_str());
    *(ptr2index + index.tableName.size()) = '\0';
    ptr2index += index.tableName.size() + 1;

    strcpy(ptr2index, index.tableName.c_str());
    *(ptr2index + index.columnName.size()) = '\0';

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

    // column_info 用来写入各个 column 的值
    char *column_info = info + sizeof(int) * 2;
    for (const auto &item : schema)
    {
        // 写入 columnName
        strcpy(column_info, item.first.c_str());
        *(column_info + item.first.size()) = '\0';
        column_info += item.first.size() + 1;
        // 写入 SqlValueType
        auto svbType = (SqlValueBaseType *) column_info;
        *svbType = item.second.type;
        column_info += sizeof(SqlValueBaseType);
        auto is_primary = (bool *) column_info;
        *is_primary = item.second.isPrimary;
        column_info += sizeof(bool);
        auto is_unique = (bool *) column_info;
        *is_unique = item.second.isUnique;
        column_info += sizeof(bool);
        auto char_length = (short *) column_info;
        *char_length = item.second.charLength;
        column_info += sizeof(short);
    }
    bufferManager->setDirty(tableName + ".def", 0);

    return true;
}

bool CatalogManager::deleteIndex(const std::string &indexName)
{
    int block_cnt = bufferManager->getBlockCnt(".index");
    int length = 0;
    for (int i = 0; i < block_cnt; ++i)
    {
        auto *tmp = (Block *) bufferManager->getBlock(".index", i);
        auto ptr2index = tmp->content;
        // 当 *ptr2index = '\0' 说明该 index 已被删除
        std::string index_name, table_name, column_name;
        if (*ptr2index != '\0')
        {
            auto index_info = ptr2index;
            length = GetLength(index_info);
            index_name.copy(index_info, length + 1, 0);
            index_info += length + 1;
            length = GetLength(index_info);
            table_name.copy(index_info, length + 1, 0);
            index_info += length + 1;
            length = GetLength(index_info);
            column_name.copy(index_info, length + 1, 0);
            if (index_name == indexName or index_name == indexName + '\0')
            {
                *ptr2index = '\0';
                bufferManager->setDirty(".index", i);
                return true;
            }
        }
        ptr2index += 128;
    }

    return false;
}

int CatalogManager::GetLength(BYTE *address)
{
    if (*address == 0)
    {
        return 0;
    }
    int length = 0;
    BYTE *ptr = address;
    while (*ptr != '\0')
    {
        ptr++;
        length++;
    }

    return length;
}