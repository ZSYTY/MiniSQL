/*
 * @Author: Tianyu You 
 * @Date: 2020-05-24 18:21:32 
 * @Last Modified by: Tianyu You
 * @Last Modified time: 2020-06-01 12:12:22
 */

#ifndef MINISQL_BUFFER_MANAGER_H
#define MINISQL_BUFFER_MANAGER_H

#include <set>
#include <map>

#include "../Common/Common.h"

using namespace MiniSQL;

struct Block {
    std::string filename;
    int id; // block id
    unsigned int offset; // block offset in the file
    bool dirty; // dirty means that this block is modified in memory, but not written into the disk
    bool busy;
    int LRUCnt;
    char content[BlockSize];
};

class BufferManager
{
private:
    /* data */
    std::set<int> dirtyList;
    std::vector<Block> blockBuffer;
    std::map<std::pair<std::string, unsigned int>, int> bufferMap;
public:
    static const int maxBlockCnt = 65536; // 65536 * 4KB = 256 MB
    static const std::string defaultDir;

    BufferManager(/* args */);
    ~BufferManager();
    
    /* 
     * @return      the block in the file
     * @filename    the name of file you nead to read
     * @offset      0 means the first block, 1 means the second block, etc
     * @allocate    false means you are not intended to add new block in the end of file, vice versa
     */
    BYTE *getBlock(const std::string &filename, unsigned int offset, bool allocate = false); 

    /* 
     * @return      the number of blocks in this file
     * @filename    the name of file
     */
    int getBlockCnt(const std::string &filename);

    /*
     * if the content of this block is modified, you need to call this!!!
     * @filename    the name of file
     * @offset      the offset of the block
     */
    void setDirty(const std::string &filename, unsigned int offset);

    /*
     * flush ALL dirty blocks in memory, i.e. synchronize the file in the disk with memory
     */
    void flush();

    void createFile(const std::string &filename);

    void removeFile(const std::string &filename);

    bool ifFileExists(const std::string &filename);
    
    /*
     * @return      the least recent used block in memory
     */
    Block &getLRU();

    /*
     * call this when you don't need this stored in memory temporarily
     * @filename    the name of file
     * @offset      the offset of the block
     */
    void setFree(const std::string &filename, unsigned int offset);
};


#endif