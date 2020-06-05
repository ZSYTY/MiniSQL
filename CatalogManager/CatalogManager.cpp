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
        std::cout << "No such table" << std::endl;
    }
    bufferManager->getBlock(tableName + ".def",)
}

// 每次都是 储存本次记录 和 **下一条记录开始的数组下标**
bool CatalogManager::createIndex(const IndexInfo &index)
{
    // If there is no .index file, initialize one
    if (!bufferManager->ifFileExists(".index"))
    {
        bufferManager->createFile(".index");
        auto *ptr2index = (Block *) bufferManager->getBlock(".index", 0);
        auto start = (unsigned short *) (ptr2index->content + BlockSize - sizeof(unsigned short));
        auto using_space = (short *) (ptr2index->content);
        auto info_num = (unsigned short *) (ptr2index->content + sizeof(unsigned short));
        *info_num = 0;
        *using_space = 3 * sizeof(unsigned short);
        *start = 2 * sizeof(unsigned short);
    }
    auto *ptr2index = (Block *) bufferManager->getBlock(".index", 0);
    auto using_space = (short *) (ptr2index->content);
    auto info_num = (unsigned short *) (ptr2index->content + sizeof(unsigned short));
    // 如果可以存放记录
    if ((BlockSize - (*using_space)) > sizeof(index))
    {
        *using_space += sizeof(index);
        auto start = (unsigned short *) (ptr2index->content + BlockSize - sizeof(unsigned short));
        for (unsigned short i = 0; i < *info_num; ++i)
        {
            start--;// 每次减 sizeof(unsigned short) 个长度
        }
        auto info = (IndexInfo *) (ptr2index->content + (*start));
        *info = index;
        *info_num += 1;
        // 如果还能存放指向下一条记录的数组下标
        if (*using_space >= sizeof(unsigned short))
        {
            unsigned short tmp = *start;
            *(--start) = *start + sizeof(index); // TODO 不知道这样写对不对
        }
    }


    return false;
}
/*
struct IndexInfo {
    std::string indexName;
    std::string tableName;
    std::string columnName;
};
*/