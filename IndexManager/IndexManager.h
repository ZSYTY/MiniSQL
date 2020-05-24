/*
 * @Author: Tianyu You 
 * @Date: 2020-05-24 18:21:29 
 * @Last Modified by: Tianyu You
 * @Last Modified time: 2020-05-24 20:21:04
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
    
    
    
};


#endif