/*
 * @Author: Tianyu You 
 * @Date: 2020-05-26 22:10:30 
 * @Last Modified by: Tianyu You
 * @Last Modified time: 2020-06-01 12:22:17
 */

#include <cstdio>
#include <fstream>
#include <iostream>
#include <sys/stat.h>
// #include <filesystem>

#include "BufferManager.h"

using namespace MiniSQL;
// namespace fs = std::filesystem;

const std::string BufferManager::defaultDir = "DBFiles";

void BufferManager::flush(Block &block, bool bulk) {
    if (block.dirty) {
        std::ofstream outfile(defaultDir + "/" + block.filename, std::ios::binary);
        outfile.seekp(block.offset * BlockSize, std::ios::beg);
        outfile.write(block.content, BlockSize);

        if (! bulk) {
            dirtyList.erase(block.id);
        }
        block.dirty = false;
    }
}

BufferManager::BufferManager() {
    struct stat info;
    stat(defaultDir.c_str(), &info);
    if (! (info.st_mode & S_IFDIR)) { // need consideration
        mkdir(defaultDir.c_str(), 0777);
    }
}

BufferManager::~BufferManager() {
    flush();
}

BYTE* BufferManager::getBlock(const std::string &filename, unsigned int offset, bool allocate) {
    auto key = std::make_pair(filename, offset);
    if (bufferMap.count(key)) {
        Block &cur = blockBuffer[bufferMap[key]];
        cur.LRUCnt = LRUCnt;
        cur.busy = true;
        LRUList.push(std::make_pair(cur.id, LRUCnt++));
        flush(cur);
        return cur.content;
    } else {
        Block &cur = getLRU();
        std::ifstream infile(defaultDir + "/" + filename, std::ios::binary);
        int blockCnt = getBlockCnt(infile);

        if (offset < blockCnt) {
            infile.seekg(offset * BlockSize, std::ios::beg);
            infile.read(cur.content, BlockSize);
        } else {
            if (! allocate) {
                std::cerr << "In BufferManager::getBlock: Only " << blockCnt << " blocks found in " << filename << ", but offset=" << offset << " is required." << std::endl;
                return NULL;
            }
            static BYTE empty[BlockSize] = "";
            infile.close();
            std::ofstream outfile(defaultDir + "/" + filename, std::ios::binary | std::ios::ate);
            while (blockCnt++ <= offset) {
                outfile.write(empty, BlockSize);
            }
            std::copy(empty, empty + BlockSize, cur.content);
        }

        bufferMap[key]  = cur.id;

        cur.filename    = filename;
        cur.offset      = offset;
        cur.LRUCnt      = LRUCnt;
        cur.busy        = true;
        cur.dirty       = false;

        LRUList.push(std::make_pair(cur.id, LRUCnt++));
        return cur.content;
    }
}

int BufferManager::getBlockCnt(std::ifstream &infile) {
    std::streampos pos = infile.tellg();
    infile.seekg(0, std::ios::end);
    int rst = infile.tellg() / BlockSize;
    infile.seekg(pos);
    return rst;
}

int BufferManager::getBlockCnt(const std::string &filename) {
    std::ifstream infile(defaultDir + "/" + filename, std::ios::binary);
    return getBlockCnt(infile);
}

void BufferManager::setDirty(const std::string &filename, unsigned int offset) {
    auto key = std::pair(filename, offset);
    if (bufferMap.count(key)) {
        Block &cur = blockBuffer[bufferMap[key]];
        if (! cur.dirty) {
            cur.dirty = true;
            dirtyList.insert(cur.id);
        }
    }
}


bool BufferManager::ifFileExists(const std::string &filename) {
    std::ifstream infile(defaultDir + "/" + filename);
    return infile.good();
}

void BufferManager::createFile(const std::string &filename) {
    if (! ifFileExists(filename)) {
        std::ofstream(defaultDir + "/" + filename);
    }
}

void BufferManager::removeFile(const std::string &filename) {
    if (ifFileExists(filename)) {
        remove((defaultDir + "/" + filename).c_str());
        // TODO: remove buffer
    }
}

void BufferManager::flush() {
    for (auto i : dirtyList) {
        Block &item = blockBuffer[i];
        flush(item, true);
    }
    dirtyList.clear();
}

void BufferManager::restoreMap(const Block &block) {
    auto key = std::make_pair(block.filename, block.offset);
    auto it = bufferMap.find(key);
    if (it != bufferMap.end()) {
        bufferMap.erase(it);
    }
}

Block& BufferManager::getLRU() {
    if (blockBuffer.size() < maxBlockCnt) {
        blockBuffer.push_back(Block(blockBuffer.size()));
        return blockBuffer.back();
    } else {
        while (! freeList.empty()) {
            Block &cur = blockBuffer[freeList.front()];
            freeList.pop();
            if (! cur.busy) {
                restoreMap(cur);
                return cur;
            }
        }
        while (! LRUList.empty()) {
            auto key = LRUList.front();
            LRUList.pop();
            Block &cur = blockBuffer[key.first];
            if (cur.LRUCnt == key.second) {
                restoreMap(cur);
                return cur;
            }
        }
        return blockBuffer.back();
    }
}

void BufferManager::setFree(const std::string &filename, unsigned int offset) {
    auto key = std::pair(filename, offset);
    if (bufferMap.count(key)) {
        Block &cur = blockBuffer[bufferMap[key]];
        cur.busy = false;
        freeList.push(cur.id);
    }
    // TODO: 
}