#include "RecordManager.h"
#include <cstring>

// Input table name
// write to the file
void RecordManager::createTable(const std::string &tableName)
{
    std::string fileName = tableName+".db";
    // if table exist
    if(bufferManager->ifFileExists(fileName)){
        std::cerr<<"RecordManager::createTable: "<<"Table already exists."<<std::endl;
        return;
    }
    //write file
    bufferManager->createFile(fileName);
    std::cout<<"table "<<tableName<<" created"<<std::endl;
    return;
}

// Input table name
// drop the file
void RecordManager::dropTable(const std::string &tableName)
{
    std::string fileName = tableName+".db";
    //table doesn't exist
    if(!bufferManager->ifFileExists(fileName)){
        std::cerr<<"RecordManager::dropTable: "<<"Table doesn't exist."<<std::endl;
        return;
    }
    bufferManager->removeFile(fileName);
    std::cout<<"table "<<tableName<<" has been deleted"<<std::endl;
    return;
}


bool RecordManager::insertRecord(const std::string &tableName, const std::vector<Tuple> records)
{
    for(int i = 0;i < records.size();i++){
        if(!insertOneRecord(tableName,records[i])){
            std::cerr<<"RecordManager::insertRecord: "<<"Error when insert records["<<i<<"]"<<std::endl;
            return false;
        }
        bufferManager->flush();
    }
    return true;
}

// Input filename and predicates
// Check colunmtype, primary key
// Insert 1 turple into block
bool RecordManager::insertOneRecord(const std::string &tableName, const Tuple record)
{
    int tailBlockID,flag = 1;
    size_t offset;
    std::string fileName = tableName+".db";
    //std::vector<Tuple> tuples = getTuples(tableName);
    TableInfo tableInfo = catalogManager->getTableInfo(tableName);

    //Check if table exist
    if(!bufferManager->ifFileExists(fileName)){
        std::cerr<<"RecordManager::insertOneRecord: "<<"Table doesn't exist."<<std::endl;
        return false;
    }

    // Check if records illegal
    if(!checkType(tableInfo,record)){
        std::cerr<<"RecordManger::insertOneRecord: "<<"Types conflict."<<std::endl;
        return false;
    }
    for(int i = 0;i < tableInfo.columnCnt;i++){
        //  Check primary key
        if(tableInfo.columnType[i].isUnique || tableInfo.columnType[i].isPrimary){
            bool unique = true;
            std::vector<SqlCondition> conditions;
            SqlCondition condition(tableInfo.columnName[i],Operator::EQ,record[i]);
            conditions.push_back(condition);
            tableTraversal(tableInfo,conditions,[&](BYTE* block,size_t offset,size_t blockOffset,std::shared_ptr<std::vector<SqlValue>> record){
                unique = false;
                return false;
            });
            if(!unique){
                std::cerr<<"RecordManger::insertRecord: "<<"Primarykey duplicates."<<std::endl;
                return false;
            }
        }
    }

    // Check Complete

    //Insert
    int indexOffset = -1;
    int blockNum = bufferManager->getBlockCnt(fileName);
    if(blockNum == 0){
        char* contentPtr = bufferManager->getBlock(fileName,blockNum,true);
        memset(contentPtr,0,BlockSize);
        bufferManager->setDirty(fileName,blockNum);
        if(writeRecord(tableInfo,record,contentPtr)){
            std::cout<<"Successfully write record."<<std::endl;
            tableInfo.recordCnt++;
            saveIndexes(tableInfo,record,0);
            return true;
        }
        // updateIndexes();
        std::cerr<<"RecordManager::insertOneRecord: error insert the first block"<<std::endl;
        return false;
    }
    else{
        // Get last block
        BYTE* blockPtr = bufferManager->getBlock(fileName,blockNum-1);
        unsigned int recordLen = getRecordSize(tableName);
        int recordsPerBlock = BlockSize / recordLen;
        for(int i = 0;i < recordsPerBlock;i++){
            size_t offset = recordLen * i;
            auto re = readRecord(tableInfo,blockPtr+offset);
            if(re == nullptr){
                writeRecord(tableInfo,record,blockPtr+offset);
                std::cout<<"Successfully write record."<<std::endl;
                tableInfo.recordCnt++;
                bufferManager->setDirty(fileName,blockNum-1);
                // save Indexes
                indexOffset = recordsPerBlock*(blockNum - 1) + i;
                saveIndexes(tableInfo,record,indexOffset);
                return true;
            }
            else{
                //freeRecord(re);
            }
        }
        // std::cerr<<"RecordManager::insertOneRecord: error insert record at the tail"<<std::endl;
        // return false;
    }
    char* contentPtr = bufferManager->getBlock(fileName,blockNum,true);
    unsigned int recordLen = getRecordSize(tableName);
    int recordsPerBlock = BlockSize / recordLen;
    indexOffset = recordsPerBlock * blockNum + 1;
    memset(contentPtr,0,BlockSize);
    bufferManager->setDirty(fileName,blockNum);
    if(writeRecord(tableInfo,record,contentPtr)){
        std::cout<<"Successfully write record."<<std::endl;
        tableInfo.recordCnt++;
        saveIndexes(tableInfo,record,indexOffset);
        return true;
    }
    // updateIndexes();
    std::cerr<<"RecordManager::insertOneRecord: error insert record at the tail"<<std::endl;
    return false;
}

// Remove records which satisfy conditions
int RecordManager::deleteRecord(const std::string &tableName,const std::vector<SqlCondition> &conditions) {
    // Initialize
    int count = -1;
    bool flag = false, index_flag = false;
    TableInfo tableInfo = catalogManager->getTableInfo(tableName);
    const std::string fileName = tableInfo.tableName + ".db";
    //Check if table exist
    if (!bufferManager->ifFileExists(fileName)) {
        std::cerr << "RecordManger ERROR: " << "Table doesn't exist." << std::endl;
        return false;
    }

    // Check type conflicts
    for (int i = 0; i < conditions.size(); i++) {
        bool valid = false;
        for (int j = 0; j < tableInfo.columnName.size(); j++) {
            if (conditions[i].columnName == tableInfo.columnName[j]) {
                valid = true;
            }
        }
        if (valid == false) {
            std::cerr << "RecordManger ERROR: " << "Type Conflict" << std::endl;
            return false;
        }
    }

    count = 0;
    // Delete
    tableTraversal(tableInfo, conditions,
                   [&](BYTE *content, size_t offset,size_t blockOffset, std::shared_ptr<std::vector<SqlValue>> record) {
                       bool valid = false;
                       memcpy(content + offset, &valid, sizeof(bool));
                       //block->dirty = true;
                       //tableInfo.recordCnt--;
                       count++;
                       bufferManager->setDirty(fileName, blockOffset);
                       return true;
                   });
    if (count != 0) {
        std::cout <<"Delete "<<count<<" record"<< std::endl;
    }
    else{
        std::cout<<"No record deleted"<<std::endl;
    }
    bufferManager->flush();
    return count;
}

// overload
// remove all records in the table
bool RecordManager::deleteRecord(const std::string &tableName)
{
    std::string fileName = tableName + ".db";
    bufferManager->removeFile(fileName);
    bufferManager->createFile(fileName);
    std::cout<<"Succesfully remove all the records in the table"<<std::endl;
    return true;
}

// We print the result here, instead of return results
bool RecordManager::selectRecord(const std::string &tableName, const std::vector<SqlCondition> &conditions)
{
    TableInfo tableInfo = catalogManager->getTableInfo(tableName);
    std::string fileName = tableName + ".db";
    // Check if talbe exist(already have been done in API)
    if(!bufferManager->ifFileExists(fileName)){
        std::cerr<<"RecordManger::selectRecord: "<<"Table doesn't exist."<<std::endl;
        return false;
    }

    int recordLen = getRecordSize(tableName);
    int recordsPerBlock = BlockSize/recordLen;
    int blockOffset = 0;
    int blockNum = bufferManager->getBlockCnt(fileName);
    //std::cout<<recordsPerBlock<<std::endl;
    // get tuples
    std::vector<Tuple> results;
    BYTE* blockPtr = bufferManager->getBlock(fileName,blockOffset,false);
    if(conditions.size() == 0 && blockOffset < blockNum){
        int i = 0;
        int p = 0;
        while(i < tableInfo.recordCnt && blockOffset<blockNum){
            auto re = readRecord(tableInfo,blockPtr+p);
            if(re != nullptr)
                results.emplace_back(*re);
            p+=recordLen;
            i++;
            if(i>recordsPerBlock){
                blockOffset++;
                i = 0;
                p = 0;
                if(blockOffset<blockNum)
                    blockPtr = bufferManager->getBlock(fileName,blockOffset,false);
            }
        }
        if(!results.empty())
        {
            printResult(tableInfo,results);
            //std::cout<<i<<std::endl;
        }
        else
        {
            std::cout<<"No matching records found."<<std::endl;
        }

        return true;
    }
    tableTraversal(tableInfo,conditions,[&](BYTE* block,size_t offset,size_t blockOffset,std::shared_ptr<std::vector<SqlValue>> record){
        //auto projection = std::make_shared<std::vector<SqlValue>>();
        bool flag = false;
        for (int i = 0; i < conditions.size(); i++) {
            for (int j = 0; j < tableInfo.columnCnt; j++) {
                // If we found one column match, then we go on check whether this tuple satisfy other conditions
                if (tableInfo.columnName[j] == conditions[i].columnName) {
                    results.push_back(*record);
                    flag = true;
                    break;
                }
            }
            if(flag) break;

        }
        //result.push_back(*projection);
        return true; // Don't delete it
    });

    if(!results.empty())
        printResult(tableInfo,results);
    else
    {
        std::cout<<"No matching records found."<<std::endl;
    }

    return true;
}

bool RecordManager::checkType(TableInfo &tableInfo,const Tuple record)
{
    for(int i = 0;i < record.size();i++){
        // here maybe some faults
        if(record[i].type != tableInfo.columnType[i].type){
            return false;
        }
    }
    return true;
}

bool RecordManager::writeRecord(TableInfo &tableInfo,const Tuple record,char* ptr)
{
    int p = 0;
    // valid
    bool valid = true;
    memcpy(ptr+p,&valid,sizeof(bool));
    // write into records
    p+= sizeof(bool);
    for(int i = 0;i < tableInfo.columnCnt;i++){
        switch(record[i].type){
            case SqlValueBaseType::MiniSQL_int:
                memcpy(ptr+p,&record[i].int_val,sizeof(record[i].type));
                p+=sizeof(record[i].type);
                break;
            case SqlValueBaseType::MiniSQL_char:
                for(int k = 0;k < tableInfo.columnType[i].getSize();k++) {
                    if(record[i].char_val.size()>=k+1) {
                        memcpy(ptr + p, &record[i].char_val[k], sizeof(char));
                    }
                    else{
                        char a = '\0';
                        memcpy(ptr+p,&a,sizeof(char));
                    }
                    //std::cout<<record[i].char_val<<std::endl;
//                    memcpy(ptr+p,&record[i].char_val,sizeof(record[i].char_val.size()));
                    p+=sizeof(char);
                }
                break;
            case SqlValueBaseType::MiniSQL_float:
                memcpy(ptr+p,&record[i].float_val,sizeof(record[i].type));
                p+=sizeof(record[i].type);
                break;
        }

    }
    return true;
}

void RecordManager::tableTraversal(
        TableInfo &tableInfo,
        const std::vector<SqlCondition>& conditions,
        std::function<bool(BYTE*,size_t,size_t,std::shared_ptr<std::vector<SqlValue>>)> consumer
){
    int indexPos = -1;
    int conditionPos = -1;
    // for(int i = 0;i < conditions.size();i++)
    // {
    //     int flag = 0;
    //     // Search the id of only one condition
    //     for(int j = 0;j < tableInfo.columnCnt;j++){
    //         if(conditions[i].columnName == tableInfo.columnName[j]){
    //             flag = 1;
    //             conditionPos = i;
    //             indexPos = j;
    //             break;
    //         }
    //     }
    //     if(flag == 1) break;
    // }
    // Use Index
    if(!tableInfo.indexes.empty() && conditionPos != -1){
        SqlValue indexValue = conditions[conditionPos].val;
        int indexOffset = indexManager->search(tableInfo.tableName,tableInfo.columnName[indexPos],indexValue);
        Operator op = conditions[conditionPos].op;
        SqlValue* value = nullptr;
        switch(op)
        {
            case Operator::NEQ: {//!=
                linearTraversal(tableInfo, conditions, consumer);
                break;
            }
            case Operator::LEQ:{
                int indexMov = indexManager->searchHead(tableInfo.tableName,tableInfo.columnName[indexPos]);
                value = getValue(tableInfo,indexMov,indexPos);
                while(indexMov <= indexOffset){
                    if(op == Operator::LT && *value > indexValue) break;
                    if(op == Operator::LEQ && *value >= indexValue) break;
                    indexTraversal(tableInfo,indexMov,conditions,consumer);
                    indexMov = indexManager->searchNext(tableInfo.tableName,tableInfo.columnName[indexPos]);
                    value = getValue(tableInfo,indexMov,indexPos);
                }
                break;
            }
            case Operator::GT:
            case Operator::GEQ: {
                //int indexOffset = indexManager->search(tableInfo.tableName, tableInfo.columnName[indexPos], indexValue);
                value = getValue(tableInfo, indexOffset, indexPos);
                while (indexOffset != -1) {
                    indexTraversal(tableInfo, indexOffset, conditions, consumer);
                    indexOffset = indexManager->searchNext(tableInfo.tableName, tableInfo.columnName[indexPos]);
                }
                break;
            }
            case Operator::EQ: // ==
            {
                indexOffset = indexManager->searchEqual(tableInfo.tableName, tableInfo.columnName[indexPos],
                                                        indexValue);
                value = getValue(tableInfo, indexOffset, indexPos);
                while (indexOffset != -1) {
                    if (op == Operator::EQ && *value != indexValue) {
                        break;
                    }
                    indexTraversal(tableInfo, indexOffset, conditions, consumer);
                    indexOffset = indexManager->searchNext(tableInfo.tableName, tableInfo.columnName[indexPos]);
                    value = getValue(tableInfo, indexOffset, indexPos);
                }
                break;
            }
        }
    }else{
        linearTraversal(tableInfo,conditions,consumer);
    }
}

void RecordManager::linearTraversal(
        TableInfo &tableInfo,
        const std::vector<SqlCondition>& conditions,
        std::function<bool(BYTE*,size_t,size_t,std::shared_ptr<std::vector<SqlValue>>)> consumer
){
    std::string fileName = tableInfo.tableName+".db";
    unsigned int recordLen = getRecordSize(tableInfo.tableName);
    int recordsPerBlock = BlockSize/recordLen;
    int blockNum = bufferManager->getBlockCnt(fileName);
    bool flag = true;

    for(int i = 0;i < blockNum;i++){
        BYTE* blockPtr = bufferManager->getBlock(fileName,i);
        for(int j = 0;j < recordsPerBlock;j++){
            size_t offset = recordLen * j;
            BYTE* ptr = blockPtr + offset;
            auto re = readRecord(tableInfo,ptr);
            if(re == NULL){
                continue;
            }
            if(checkConditions(tableInfo,conditions,re)){
                flag = consumer(blockPtr,offset,i,re);
                if(!flag){
                    //freeRecord(re);
                    return;
                }
            }
            //freeRecord(re);
        }

    }
}

void RecordManager::indexTraversal(
        TableInfo& tableInfo,
        int indexOffset,
        std::vector<SqlCondition> conditions,
        std::function<bool(BYTE*,size_t,size_t,std::shared_ptr<std::vector<SqlValue>>)> consumer
)
{
    std::string fileName = tableInfo.tableName + ".db";
    int recordSize = getRecordSize(tableInfo.tableName);
    int recordsPerBlock = BlockSize / recordSize;
    int blockIndex = indexOffset / recordsPerBlock;
    int remain = indexOffset % recordsPerBlock;
    int offset = remain*recordSize;
    BYTE* blockPtr = bufferManager->getBlock(fileName,blockIndex,false);
    auto record = readRecord(tableInfo,blockPtr+offset);
    if(record!=nullptr && checkConditions(tableInfo,conditions,record)){
        consumer(blockPtr,offset,0,record);
    }
    if(record!=nullptr){
        //freeRecord(record);
    }
    return;
}


unsigned int RecordManager::getRecordSize(const std::string tableName)
{
    TableInfo tableInfo = catalogManager->getTableInfo(tableName);
    unsigned int recordLen = sizeof(bool);
    for(int i = 0;i < tableInfo.columnCnt;i++){
        recordLen += tableInfo.columnType[i].getSize();
    }
    return recordLen;
}

std::shared_ptr<std::vector<SqlValue>> RecordManager::readRecord(TableInfo &tableInfo,BYTE* ptr)
{
    int p = 0;
    // Check if the record is valid
    bool valid;
    memcpy(&valid,ptr+p,sizeof(bool));
    if(!valid){
        return nullptr;
    }
    p+=sizeof(bool);

    auto records = std::make_shared<std::vector<SqlValue>>();
    if(records == nullptr) std::cout<<"You sb"<<std::endl;
    for(int i = 0;i < tableInfo.columnCnt;i++){
        int size = tableInfo.columnType[i].getSize();
        switch(tableInfo.columnType[i].type){
            case SqlValueBaseType::MiniSQL_int: {
                int content;
                memcpy(&content,ptr+p,size);
                records->emplace_back(tableInfo.columnType[i].type,content);
                p+=size;
                break;
            }
            case SqlValueBaseType::MiniSQL_char:{
                size = tableInfo.columnType[i].getSize();
                char temp[size];
                //int tmp = 1;
                //char temp;
                for(int k = 0;k < tableInfo.columnType[i].getSize();k++){
                    //memcpy(&temp,ptr+p,sizeof(char));
                    memcpy(&temp[k],ptr+p,sizeof(char));
                    p+=sizeof(char);
                }
                //std::cout<<temp<<std::endl; 
                std::string content(temp);
                //memcpy(&content,ptr+p,sizeof(char)*size);
                records->emplace_back(tableInfo.columnType[i].type,content);
                //ptr+=tmp;
                break;
            }
            case SqlValueBaseType::MiniSQL_float: {
                float content;
                memcpy(&content,ptr+p,size);
                records->emplace_back(tableInfo.columnType[i].type,content);
                p+=size;
                break;
            }
        }

    }
    return std::shared_ptr<std::vector<SqlValue>>(records);
}

void RecordManager::freeRecord(std::shared_ptr<std::vector<SqlValue>> record)
{
    for(int i = 0;i < record->size();i++){
        switch(record->at(i).type){
            case SqlValueBaseType::MiniSQL_int:
                delete &record->at(i).int_val;
                break;
            case SqlValueBaseType::MiniSQL_char:
                delete &record->at(i).char_val;
                break;
            case SqlValueBaseType::MiniSQL_float:
                delete &record->at(i).float_val;
                break;
        }
    }
    return;
}

bool RecordManager::checkConditions(TableInfo &tableInfo,const std::vector<SqlCondition> conditions,std::shared_ptr<std::vector<SqlValue>> record)
{
    bool valid = true;
    for(int i = 0;i < conditions.size() && valid;i++)
    {
        for(int j = 0;j < tableInfo.columnCnt && valid;j++){
            if(tableInfo.columnName[j] == conditions[i].columnName){
                auto value = record->at(j);
                switch(conditions[i].op){
                    case Operator::EQ:
                        valid = value == conditions[i].val; break;
                    case Operator::NEQ:
                        valid = value != conditions[i].val; break;
                    case Operator::LT:
                        valid =  value < conditions[i].val; break;
                    case Operator::LEQ:
                        valid = value <= conditions[i].val; break;
                    case Operator::GT:
                        valid = value > conditions[i].val; break;
                    case Operator::GEQ:
                        valid = value >= conditions[i].val; break;
                }
            }
        }
    }
    return valid;
}


void RecordManager::printResult(TableInfo &tableInfo,const std::vector<Tuple> &results)
{
    for(int i = 0;i < tableInfo.columnCnt;i++)
    {
        std::cout<<tableInfo.columnName[i]<<" ";
    }
    std::cout<<std::endl;
    for(int i = 0;i < results.size();i++){
        for(int j = 0;j < results[i].size();j++){
            switch(results[i][j].type){
                case SqlValueBaseType::MiniSQL_int:{
                    int p = results[i][j].int_val; std::cout<<p;
                    break;
                }
                case SqlValueBaseType::MiniSQL_char:
                {
                    std::string s = results[i][j].char_val;
                    std::cout<<s;
                    break;
                }
                case SqlValueBaseType::MiniSQL_float:
                {
                    float q = results[i][j].float_val;
                    std::cout<<q;
                    break;
                }
            }
            std::cout<<" ";
        }
        std::cout<<std::endl;
    }
}

SqlValue* RecordManager::getValue(TableInfo &tableInfo,int indexOffset,int indexPos)
{
    const std::string fileName = tableInfo.tableName + ".db";
    int recordSize = getRecordSize(tableInfo.tableName);
    int recordsPerBlock = BlockSize / recordSize;
    int blockIndex = indexOffset / recordsPerBlock;
    int remain = indexOffset % recordsPerBlock;
    int offset = remain*recordSize;
    // Get the block before it
    BYTE* blockPtr = bufferManager->getBlock(fileName,blockIndex);
    auto record = readRecord(tableInfo,blockPtr+offset);
    return &record->at(indexPos);
}

void RecordManager::saveIndexes(TableInfo &tableInfo,const Tuple record,int offset)
{
    std::string fileName = tableInfo.tableName + ".db";
    for(int i = 0;i < tableInfo.indexes.size();i++){
        indexManager->insertKey(fileName,record,offset);
    }
}