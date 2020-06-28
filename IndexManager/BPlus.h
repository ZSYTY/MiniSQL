/*
 * @Author: Tianyu You 
 * @Date: 2020-06-11 15:53:58 
 * @Last Modified by: Tianyu You
 * @Last Modified time: 2020-06-28 17:54:24
 */

#ifndef MINISQL_B_PLUS_TREE_H
#define MINISQL_B_PLUS_TREE_H

#include <vector>
#include <string>
#include <iostream>
#include <algorithm>
#include "../Common/Common.h"
#include "../BufferManager/BufferManager.h"

template <SqlValueBaseType T>
class BPlusTree {
private:
    const int cnt_size = 2, valid_bit_size = 2, refs_size = 4;
    int data_size, degree, root = 0;
    BufferManager *bm;
    std::string filename, tableFileName;

    MiniSQL::TableInfo tableInfo;
    int dataOffset, recordSize, recordPerBlock; // param in record
    
    /* status */
    int lastOffset, lastIdx;
    bool lastIsEnd;
    std::vector<std::pair<int, int>> path;
    
    BYTE *getData(BYTE *content, int offset) {
        if (offset >= degree - 1) {
            throw std::out_of_range("Index data out of range");
        }
        return content + data_size * offset + cnt_size + valid_bit_size;
    }

    int *getRefs(BYTE* content, int offset) {
        if (offset >= degree) {
            throw std::out_of_range("Index refs out of range");
        }
        return reinterpret_cast<int *>(content + data_size * (degree - 1) + refs_size * offset + cnt_size + valid_bit_size);
    }

    int *getNxt(BYTE* content) {
        return reinterpret_cast<int *>(content + MiniSQL::BlockSize - refs_size);
    }

    short *getItemCnt(BYTE* content) {
        return reinterpret_cast<short *>(content + valid_bit_size);
    }

    short *getIfLeaf(BYTE* content) {
        return reinterpret_cast<short *>(content);
    }

    BYTE* getBlock(int offset, bool allocate = false) {
        return bm->getBlock(filename, offset, allocate);
    }

    int getBlockCnt() {
        return bm->getBlockCnt(filename);
    }

    BYTE* getNewBlock() {
        return getBlock(getBlockCnt(), true);
    }

    void setDirty(int offset) {
        bm->setDirty(filename, offset);
    }
    
    void writeData(BYTE *ptr, const SqlValue &value) {
        switch (T) {
            case MiniSQL::SqlValueBaseType::MiniSQL_int:
                *reinterpret_cast<int*>(ptr) = value.int_val;
                break;
            case MiniSQL::SqlValueBaseType::MiniSQL_char:
                value.char_val.copy(ptr, MiniSQL::MaxCharLength);
                break;
            case MiniSQL::SqlValueBaseType::MiniSQL_float:
                *reinterpret_cast<float*>(ptr) = value.float_val;
                break;
        }
    }

    SqlValue toValue(BYTE *ptr, MiniSQL::SqlValueBaseType type = T) {
        switch (type) {
            case MiniSQL::SqlValueBaseType::MiniSQL_int:
                return SqlValue(type, *reinterpret_cast<int*>(ptr));
                break;
            case MiniSQL::SqlValueBaseType::MiniSQL_char:
                return SqlValue(type, std::string(ptr, MiniSQL::MaxCharLength));
                break;
            case MiniSQL::SqlValueBaseType::MiniSQL_float:
                return SqlValue(type, *reinterpret_cast<float*>(ptr));
                break;
        }
    }

    void splitOne(BYTE *block, int idx) { // leaf
        short size = *getItemCnt(block);
        if (idx == size) {
            return;
        }
        BYTE *p = getData(block, size) - 1, *rend = getData(block, idx);
        do {
            p[data_size] = p[0];
            p--;
        } while (p != rend);
        // setDirty(lastOffset);
    }

    void insertOne(BYTE* block, int idx, const SqlValue &value, int offset) {
        (*getItemCnt(block))++;
        splitOne(block, idx);
        BYTE *data = getData(block, idx);
        int *refs = getRefs(block, idx);

        writeData(data, value);
        *refs = offset;
    }

    void insertOne(BYTE* block, int idx, BYTE *newData, int newRefs) {
        (*getItemCnt(block))++;
        splitOne(block, idx);
        BYTE *data = getData(block, idx);
        int *refs = getRefs(block, idx);

        std::copy(newData, newData + data_size, data);
        *refs = newRefs;
    }

    void split(BYTE *l, BYTE *r, int mid, int idx) {
        int r_start = mid, size = *getItemCnt(l);
        if (idx > mid) {
            r_start++;
        }
        std::copy(getData(l, r_start), getData(l, size), getData(r, 0));
        std::copy(getRefs(l, r_start), getRefs(l, size + 1), getRefs(r, 0));
        *getItemCnt(r) = size - r_start;
        *getItemCnt(l) = r_start;
    }

    void updateUp(int l_offset, int r_offset) {
        if (path.size()) {
            path.pop_back();
        }
        for (auto it = path.rbegin(); it != path.rend(); it++) {
            int rt_offset = it->first, rt_idx = it->second;
            BYTE *rt = getBlock(rt_offset), *l = getBlock(l_offset), *r = getBlock(r_offset);
            BYTE *cur = getData(l, *getItemCnt(l) - 1);
            if (*getItemCnt(rt) == degree - 1) {
                int mid = (degree - 1) / 2;
                BYTE *rt_r = getNewBlock();
                int newOffset = getBlockCnt() - 1;

                split(rt, rt_r, mid, rt_idx);
                
                if (lastIdx < *getItemCnt(rt)) {
                    insertOne(rt, rt_idx, cur, l_offset);
                } else {
                    insertOne(rt_r, rt_idx - *getItemCnt(rt), cur, l_offset);
                }

                setDirty(rt_offset);
                setDirty(newOffset);
                l_offset = rt_offset;
                r_offset = newOffset;
            } else {
                insertOne(rt, rt_idx, cur, l_offset);
                setDirty(rt_offset);
                break;
            }
        }
        if (l_offset == root) {
            BYTE *rt = getNewBlock(), *l = getBlock(l_offset), *r = getBlock(r_offset);
            root = getBlockCnt() - 1;
            *getItemCnt(rt) = 1;
            BYTE *cur = getData(l, *getItemCnt(l) - 1);

            std::copy(cur, cur + data_size, getData(rt, 0));
            *getRefs(rt, 0) = l_offset;
            *getRefs(rt, 1) = r_offset;
            setDirty(root);
        }
    }

    void updateVal(const SqlValue &value) {
        for (auto it = path.rbegin(); it != path.rend(); it++) {
            if (it->first == lastOffset) {
                continue;
            }
            BYTE *block = getBlock(it->first);
            if (it->second < *getItemCnt(block)) {
                writeData(getData(block, it->second), value);
                setDirty(it->first);
                if (it->second != *getItemCnt(block) - 1) {
                    break;
                }
            }
        }
    }

    void search(const SqlValue &value, bool needStorePath = false) {
        if (getBlockCnt() == 0) {
            lastOffset = lastIdx = -1; // NOTFOUND, needs redefinition
            return;
        }
        lastOffset = root;
        lastIsEnd = true;
        bool found = false;
        while (! found) {
            BYTE *block = getBlock(lastOffset);
            for (int i = 0; i < *getItemCnt(block); i++) {
                SqlValue cur = toValue(getData(block, i));
                if (cur >= value) {
                    lastIdx = i;
                    lastIsEnd = false;
                    break;
                }
            } // needs improvement: binary search
            if (lastIsEnd) {
                lastIdx = *getItemCnt(block);
            }
            if (needStorePath) {
                path.push_back(std::make_pair(lastOffset, lastIdx));
            }
            if (*getIfLeaf(block)) {
                found = true;
            } else {
                lastOffset = *getRefs(block, lastIdx);
            }
        }
    }

public:
    BPlusTree(BufferManager *_bm, MiniSQL::IndexInfo indexInfo, MiniSQL::TableInfo _tableInfo): bm(_bm) {
        tableInfo = _tableInfo;
        filename = indexInfo.indexName + ".index";
        tableFileName = tableInfo.tableName + ".data";
        int sum = 1; // valid byte
        for (int i = 0; i < tableInfo.columnName.size(); i++) {
            if (tableInfo.columnName[i] == indexInfo.columnName) {
                dataOffset = sum;
                data_size = tableInfo.columnType[i].getSize();
            }
            sum += tableInfo.columnType[i].getSize();
        }
        recordSize = sum;
        recordPerBlock = MiniSQL::BlockSize / recordSize;
        // build();
        // data_size = (T == MiniSQL::SqlValueBaseType::MiniSQL_int ? sizeof(int) : (T == MiniSQL::SqlValueBaseType::MiniSQL_char ? sizeof(char) * (MiniSQL::MaxCharLength + 1) : sizeof(float)));
        degree = (MiniSQL::BlockSize + data_size - cnt_size - valid_bit_size) / (refs_size + data_size);
    }
    
    void insertVal(const SqlValue &value, int offset) {
        if (value.type != T) {
            throw std::runtime_error("Value types do not match");
        }
        if (getBlockCnt() == 0) {
            BYTE *block = getBlock(root, true);
            *getItemCnt(block) = 1;
            *getIfLeaf(block) = 1;
            *getNxt(block) = -1;
            BYTE *data = getData(block, 0);
            int *refs = getRefs(block, 0);
            
            writeData(data, value);
            *refs = offset;
            setDirty(root);
        } else {
            search(value, true);
            BYTE *block = getBlock(lastOffset);
            short *itemCnt = getItemCnt(block);
            if (*itemCnt < degree - 1) {
                insertOne(block, lastIdx, value, offset);

                setDirty(lastOffset);
                if (lastIdx == *itemCnt - 1) {
                    updateVal(value);
                }
            } else {
                int mid = (degree - 1) / 2;
                BYTE *l = block, *r = getNewBlock();
                *getIfLeaf(r) = 1;
                int newOffset = getBlockCnt() - 1, l_offset = lastOffset;
                split(l, r, mid, lastIdx);
                *getNxt(r) = *getNxt(l);
                *getNxt(l) = newOffset;

                if (lastIdx < *getItemCnt(l)) {
                    insertOne(l, lastIdx, value, offset);
                } else {
                    lastIdx -= *getItemCnt(l);
                    lastOffset = newOffset;
                    insertOne(r, lastIdx, value, offset);
                }

                setDirty(l_offset);
                setDirty(newOffset);

                updateUp(l_offset, newOffset);
                // insertVal(value, offset);
                if (lastIdx == *getItemCnt(getBlock(lastOffset)) - 1) {
                    updateVal(value);
                }
            }
            path.clear();
        }
    }

    int select(const SqlValue &value) {
        search(value);
        if (! lastIsEnd and lastOffset != -1) {
            BYTE *block = getBlock(lastOffset);
            return *getRefs(block, lastIdx);
        } else {
            return -1;
        }
    }

    int selectNext() {
        if (! lastIsEnd and lastOffset != -1) {
            BYTE *block = getBlock(lastOffset);
            short size = *getItemCnt(block);
            if (++lastIdx < size) {
                return *getRefs(block, lastIdx);
            } else {
                block = getBlock(lastOffset = *getNxt(block));
                return *getRefs(block, lastIdx = 0);
            }
        } else {
            return -1;
        }
    }

    int selectHead() {
        if (getBlockCnt() == 0) {
            lastOffset = lastIdx = -1; // NOTFOUND, needs redefinition
            return -1;
        }
        lastOffset = root;
        lastIsEnd = true;
        bool found = false;
        BYTE *block = nullptr;
        while (! found) {
            block = getBlock(lastOffset);
            if (*getItemCnt(block) == 0) {
                return -1;
            } else {
                lastIdx = 0;
                lastIsEnd = false;
            }
            if (*getIfLeaf(block)) {
                found = true;
            } else {
                lastOffset = *getRefs(block, lastIdx);
            }
        }
        return *getRefs(block, lastIdx);
    }

    Tuple getTuple(int offset) {
        int i = offset / recordPerBlock, j = (offset % recordPerBlock) * recordSize; // valid byte
        BYTE *block = bm->getBlock(tableFileName, i);
        Tuple rst;
        for (auto &item : tableInfo.columnType) {
            rst.push_back(toValue(block + j + 1, item.type));
            j += item.getSize();
        }
        return rst;
    }

    bool getValue(int offset, SqlValue &rst) {
        int i = offset / recordPerBlock, j = (offset % recordPerBlock) * recordSize; // valid byte
        BYTE *block = bm->getBlock(tableFileName, i);
        if (block[j]) {
            rst = toValue(block + j + dataOffset);
            // std::cout << "getValue: " << rst.int_val << std::endl;
            return true;
        } else {
            return false;
        }
    }

    void build() {
        int size = bm->getBlockCnt(tableFileName) * recordPerBlock;
        SqlValue cur;
        bm->createFile(filename);
        for (int i = 0; i < size; i++) {
            if (getValue(i, cur)) {
                insertVal(cur, i);
            }
        }
    }

    // void deleteVal(const SqlValue &value, int offset) {
    //     if (value.type != T) {
    //         throw std::runtime_error("Value types do not match");
    //     }
    //     search(value, true);
    //     if (! lastIsEnd and lastOffset != -1) {
    //         BYTE *block = getBlock(lastOffset);
    //         SqlValue cur = toValue(getData(block, lastIdx));
    //         if (cur == value) {
    //             // TODO:
    //         }
    //     }
    // }
};

#endif
