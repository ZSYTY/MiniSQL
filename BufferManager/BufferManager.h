/*
 * @Author: Tianyu You 
 * @Date: 2020-05-24 18:21:32 
 * @Last Modified by: Tianyu You
 * @Last Modified time: 2020-05-24 23:21:36
 */

#ifndef MINISQL_BUFFER_MANAGER_H
#define MINISQL_BUFFER_MANAGER_H

#include "../Common/Common.h"

using namespace MiniSQL;

struct Block {
    std::string filename;
    int id; // block id
    unsigned int offset; // block offset in the file
    bool dirty;
    bool busy;
    int LRUCnt;
    char content[BlockSize];
};

class BufferManager
{
private:
    /* data */
    std::vector<Block> blockBuffer;
public:
    BufferManager(/* args */);
    ~BufferManager();
    
    BYTE *getBlock(const std::string &filename, unsigned int offset);

    int getTailBlock(const std::string &filename); // return the id of the tail block

    void setDirty(const std::string &filename, unsigned int offset);

    void flush();

    void createFile(const std::string &filename);

    void removeFile(const std::string &filename);
    
    Block &getLRU();

    void setFree(const std::string &filename, unsigned int offset);
};


#endif