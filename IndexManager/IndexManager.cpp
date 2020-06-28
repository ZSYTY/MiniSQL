/*
 * @Author: Tianyu You 
 * @Date: 2020-06-10 16:46:54 
 * @Last Modified by: Tianyu You
 * @Last Modified time: 2020-06-28 15:21:15
 */

#include "IndexManager.h"

MiniSQL::SqlValueBaseType IndexManager::getColomnType(const std::string &tableName, const std::string &columnName) {
    tableInfo = catalogManager->getTableInfo(tableName);
    bool found = false;
    for (auto &item : tableInfo.indexes) {
        if (item.columnName == columnName) {
            indexInfo = item;
            found = true;
        }
    }
    if (! found) {
        throw std::runtime_error("No such index");
    }
    for (int i = 0; i < tableInfo.columnName.size(); i++) {
        if (tableInfo.columnName[i] == columnName) {
            columnIdx = i;
            return tableInfo.columnType[i].type;
        }
    }
    throw std::runtime_error("No such column");
}

intTree& IndexManager::getIntTree() {
    auto key = std::make_pair(indexInfo.tableName, indexInfo.columnName);
    if (intMap.count(key)) {
        return intTreeList[intMap[key]];
    } else {
        intTreeList.push_back(intTree(bufferManager, indexInfo, tableInfo));
        intMap[key] = intTreeList.size() - 1;
        return intTreeList.back();
    }
}

charTree& IndexManager::getCharTree() {
    auto key = std::make_pair(indexInfo.tableName, indexInfo.columnName);
    if (charMap.count(key)) {
        return charTreeList[charMap[key]];
    } else {
        charTreeList.push_back(charTree(bufferManager, indexInfo, tableInfo));
        charMap[key] = charTreeList.size() - 1;
        return charTreeList.back();
    }
}

floatTree& IndexManager::getFloatTree() {
    auto key = std::make_pair(indexInfo.tableName, indexInfo.columnName);
    if (floatMap.count(key)) {
        return floatTreeList[floatMap[key]];
    } else {
        floatTreeList.push_back(floatTree(bufferManager, indexInfo, tableInfo));
        floatMap[key] = floatTreeList.size() - 1;
        return floatTreeList.back();
    }
}

IndexManager::~IndexManager() {
    
}

bool IndexManager::buildIndex(const IndexInfo &index) {
    // TODO:
    auto T = getColomnType(index.tableName, index.columnName);
    switch (T) {
        case MiniSQL::SqlValueBaseType::MiniSQL_int:
            getIntTree().build();
            break;
        case MiniSQL::SqlValueBaseType::MiniSQL_char:
            getCharTree().build();
            break;
        case MiniSQL::SqlValueBaseType::MiniSQL_float:
            getFloatTree().build();
            break;
    }
    return true;
}

bool IndexManager::dropIndex(const std::string &indexName) {
    auto table = catalogManager->getTableInfo(indexName);
    bufferManager->removeFile(indexName + ".index");
    return true;
}

int IndexManager::search(const std::string &tableName, const std::string &columnName, const SqlValue &value) {
    auto T = getColomnType(tableName, columnName);
    switch (T) {
        case MiniSQL::SqlValueBaseType::MiniSQL_int:
            return getIntTree().select(value);
            break;
        case MiniSQL::SqlValueBaseType::MiniSQL_char:
            return getCharTree().select(value);
            break;
        case MiniSQL::SqlValueBaseType::MiniSQL_float:
            return getFloatTree().select(value);
            break;
    }
}

int IndexManager::searchNext(const std::string &tableName, const std::string &columnName) {
    auto T = getColomnType(tableName, columnName);
    switch (T) {
        case MiniSQL::SqlValueBaseType::MiniSQL_int:
            return getIntTree().selectNext();
            break;
        case MiniSQL::SqlValueBaseType::MiniSQL_char:
            return getCharTree().selectNext();
            break;
        case MiniSQL::SqlValueBaseType::MiniSQL_float:
            return getFloatTree().selectNext();
            break;
    }
}

int IndexManager::searchEqual(const std::string &tableName, const std::string &columnName, const SqlValue &value) {
    auto T = getColomnType(tableName, columnName);
    int cur = -1;
    SqlValue curValue;
    switch (T) {
        case MiniSQL::SqlValueBaseType::MiniSQL_int:
            cur = getIntTree().select(value);
            if (getIntTree().getValue(cur, curValue) and curValue == value) {
                return cur;
            }
            break;
        case MiniSQL::SqlValueBaseType::MiniSQL_char:
            cur = getCharTree().select(value);
            if (getCharTree().getValue(cur, curValue) and curValue == value) {
                return cur;
            }
            break;
        case MiniSQL::SqlValueBaseType::MiniSQL_float:
            cur = getFloatTree().select(value);
            if (getFloatTree().getValue(cur, curValue) and curValue == value) {
                return cur;
            }
            break;
    }
    return -1;
}

int IndexManager::searchHead(const std::string &tableName, const std::string &columnName) {
    auto T = getColomnType(tableName, columnName);
    switch (T) {
        case MiniSQL::SqlValueBaseType::MiniSQL_int:
            return getIntTree().selectHead();
            break;
        case MiniSQL::SqlValueBaseType::MiniSQL_char:
            return getCharTree().selectHead();
            break;
        case MiniSQL::SqlValueBaseType::MiniSQL_float:
            return getFloatTree().selectHead();
            break;
    }
}

bool IndexManager::insertKey(const std::string &tableName, const Tuple &tuple, int offset) {
    std::cout << "insertKey: (" << tuple[1].int_val << ", " << offset << ")" << std::endl;
    bool isInserted = false;
    tableInfo = catalogManager->getTableInfo(tableName);
    for (int i = 0; i < tableInfo.columnName.size(); i++) {
        for (auto &item : tableInfo.indexes) {
            if (item.columnName == tableInfo.columnName[i]) {
                indexInfo = item;
                columnIdx = i;
                auto &T = tableInfo.columnType[columnIdx].type;
                switch (T) {
                    case MiniSQL::SqlValueBaseType::MiniSQL_int:
                        getIntTree().insertVal(tuple[columnIdx], offset);
                        break;
                    case MiniSQL::SqlValueBaseType::MiniSQL_char:
                        getCharTree().insertVal(tuple[columnIdx], offset);
                        break;
                    case MiniSQL::SqlValueBaseType::MiniSQL_float:
                        getFloatTree().insertVal(tuple[columnIdx], offset);
                        break;
                }
                isInserted = true;
                break;
            }
        }
    }
    return isInserted;
}
