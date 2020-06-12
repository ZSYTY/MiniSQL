#include "RecordManager.h"

bool NO_CATALOG_MANAGER =  true;

// Input table name
// write to the file
void RecordManager::createTable(const std::string &tableName)
{
    std::string fileName = tableName+".db";
    // if table exist
    #ifdef MINISQL_CATALOG_MANAGER_H
    // if(NO_CATALOG_MANAGER && catalogManager->ifTableExist(tableName)){
    //     std::cout<<"Table alread exists."<<std::endl;
    // }
    #endif
    // write file to data
    bufferManager->createFile(fileName);
    return;
}

// Input table name
// drop the file
void RecordManager::dropTable(const std::string &tableName)
{
    std::string fileName = tableName+".db";
    // table doesn't exist
    // if(NO_CATALOG_MANAGER &&!catalogManager->ifTableExist(tableName)){
    //     std::cout<<"Table doesn't exist."<<std::endl;
    // }
    bufferManager->removeFile(fileName);
    return;
}


bool RecordManager::insertRecord(const std::string &tableName, const std::vector<Tuple> records)
{
    for(int i = 0;i < records.size();i++){
        if(!insertOneRecord(tableName,records[i])){
            std::cout<<"Error when insert records["<<i<<"]"<<std::endl;
            return false;
        }
    }
    // write dirty block to the disk
    bufferManager->flush();
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
    
    // Check if table exist
    // if(!catalogManager->ifTableExist(tableName)){
    //     std::cout<<"Table doesn't exist."<<std::endl;
    //     return false;
    // }

    // Check if records illegal
    if(!checkType(tableInfo,record)){
        std::cout<<"Types conflict."<<std::endl;
        return false;
    }

    //  Check primary key   
    if(!tableInfo.primaryKeyName.empty()){
        bool unique = true;
        std::vector<SqlCondition> conditions;
        talbeTraversal(tableInfo,conditions,[&](Block* block,size_t offset,std::shared_ptr<std::vector<SqlValue>> record){
            unique = false;
            return false;
        });
        std::cout<<"Primarykey duplicates"<<std::endl;
        return false;
    }

    // Check Complete

    //Insert
    int blockNum = bufferManager->getBlockCnt(fileName);
    if(blockNum == 0){
        Block* newBlock = NULL;
        newBlock = (Block*)malloc(sizeof(struct Block));
        newBlock->filename = tableName+".db";
        bufferManager->setDirty(fileName,blockNum);
        memset(newBlock->content,0,BlockSize);
        if(writeRecord(record,newBlock->content))
            std::cout<<"Successfully write record"<<std::endl;
        // updateIndexes();
        return true;
    }
    else{
        // Get last block
        BYTE* blockPtr = bufferManager->getBlock(fileName,blockNum-1);
        int vacant;
        unsigned int recordLen = getRecordSize(tableName); 
        for(vacant = 0; blockPtr[vacant]!= '\0' < BlockSize;vacant++);
        if(BlockSize - vacant >= recordLen){
            writeRecord(record,blockPtr+vacant);
            bufferManager->setDirty(fileName,blockNum-1);
            // Update the index
            return true;
        }
    }
    // New block is needed
    Block* newBlock = NULL;
    newBlock = (Block*)malloc(sizeof(struct Block));
    memset(newBlock->content,0,BlockSize);
    newBlock->filename = tableName+".db";
    newBlock->dirty = true;
    //bufferManager->setDirty(fileName,blockNum);
    writeRecord(record,newBlock->content);
    return true;
}

// Remove records which satisfy conditions
bool RecordManager::deleteRecord(const std::string &tableName,const std::vector<SqlCondition> &conditions)
{
    // Initialize
    int count = 0;
    bool flag = false,index_flag = false;
    TableInfo tableInfo = catalogManager->getTableInfo(tableName);

    // Check if table exist
    // if(!catalogManager->ifTableExist(tableName)){
    //     std::cout<<"Table doesn't exist."<<std::endl;
    //     return false;
    // }

    // Check type conflicts
    for(int i = 0; i < conditions.size();i++){
        bool valid = false;
        for(int j = 0; j < tableInfo.columnName.size();j++){
            if(conditions[i].columnName == tableInfo.columnName[j]){
                valid = true;
            }
        }
        if(valid == false){
            std::cout<<"Type Conflict"<<std::endl;
            return false;
        }
    }

    // Delete
    talbeTraversal(tableInfo,conditions,[&](Block* block,size_t offset,std::shared_ptr<std::vector<SqlValue>> record){
        bool valid = false;
        memcpy(block->content+offset,&valid,sizeof(bool));
        block->dirty = true;
        return true;
    });

    return true;
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

    // Check if talbe exist(already have been done in API)
    // if(!catalogManager->ifTableExist(tableName)){
    //     std::cout<<"Table doesn't exist."<<std::endl;
    //     return false;
    // }

    // get tuples
    std::vector<Tuple> result;

    talbeTraversal(tableInfo,conditions,[&](Block* block,size_t offset,std::shared_ptr<std::vector<SqlValue>> record){
        auto projection = std::make_shared<std::vector<SqlValue>>();
        for (int i = 0; i < conditions.size(); i++) {
				for (int j = 0; j < tableInfo.columnCnt; j++) {
					if (tableInfo.columnName[j] == conditions[i].columnName) {
						 SqlValue* sqlValue = NULL;
                         switch(tableInfo.columnType[i].type){
                             
                                case SqlValueBaseType::MiniSQL_int:
                                    sqlValue = new SqlValue(SqlValueBaseType::MiniSQL_int,record->at(j).int_val);
                                break;
                                case SqlValueBaseType::MiniSQL_char:
                                    sqlValue = new SqlValue(SqlValueBaseType::MiniSQL_char,record->at(j).char_val);
                                break;
                                case SqlValueBaseType::MiniSQL_float:
                                    sqlValue = new SqlValue(SqlValueBaseType::MiniSQL_float,record->at(j).char_val);
                                break;
                        }
						projection->push_back(*sqlValue);
						break;
					}
				}
			}
            result.push_back(*projection);
        return true;
    });

    printResult(result);

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

bool RecordManager::writeRecord(const Tuple record,char* ptr)
{
    int p = 0;
    // valid 
    bool valid = true;
    memcpy(ptr+p,&valid,sizeof(bool));
    // write into records
    p+= sizeof(bool);
    for(int i = 0;i < record.size();i++){
        memcpy(ptr+p,&record[i],sizeof(record[i].type));
    }
    return true;
}

void RecordManager::talbeTraversal(
        TableInfo &tableInfo,
        const std::vector<SqlCondition>& conditions,
        std::function<bool(Block*,size_t,std::shared_ptr<std::vector<SqlValue>>)> consumer
){
    IndexInfo* index = NULL;
    if(index){

    }else{
        linearTraversal(tableInfo,conditions,consumer);
    }
}

void RecordManager::linearTraversal(
        TableInfo &tableInfo,
        const std::vector<SqlCondition>& conditions,
        std::function<bool(Block*,size_t,std::shared_ptr<std::vector<SqlValue>>)> consumer
){
    std::string fileName = tableInfo.tableName+".db";
    unsigned int recordLen = getRecordSize(tableInfo.tableName);
    int recordsPerBlock = BlockSize/recordLen;
    int blockNum = bufferManager->getBlockCnt(fileName);
    bool flag = true;

    for(int i = 0;i < blockNum;i++){
        BYTE* blockPtr = bufferManager->getBlock(fileName,i);
        Block* block = (struct Block*)blockPtr;
        for(int j = 0;j < recordsPerBlock;j++){
            size_t offset = recordLen * j;
            BYTE* ptr = block->content + offset;
            auto re = readRecord(tableInfo,ptr);
            if(re == NULL){
                continue;
            }
            if(checkConditions(tableInfo,conditions,re)){
                flag = consumer(block,offset,re);
                if(!flag){
                    freeRecord(re);
                    return;
                }
            }
            freeRecord(re);
        }

    }
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
    for(int i = 0;i < tableInfo.columnCnt;i++){
        unsigned int size = tableInfo.columnType[i].getSize();
        char* content = new char(size);
        memcpy(content,ptr+p,size);
        // switch(tableInfo.columnType[i].type){
        //     case SqlValueBaseType::MiniSQL_int:
        //         records->operator[](records->size()-1).int_val = *content;
        //         break;
        //     case SqlValueBaseType::MiniSQL_char:
        //         records->operator[](records->size()-1).char_val = *content;
        //         break;
        //     case SqlValueBaseType::MiniSQL_float:
        //         records->operator[](records->size()-1).float_val = *content;
        //         break;
        // }
        // records->operator[](records->size()-1).type = tableInfo.columnType[i].type;
        records->emplace_back(tableInfo.columnType[i].type,*content);
        p+=size;
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
				    valid = value == conditions[i].val;
			        case Operator::NEQ:
				    valid = value != conditions[i].val;
			        case Operator::LT:
				    valid =  value < conditions[i].val;
			        case Operator::LEQ:
				    valid = value <= conditions[i].val;
			        case Operator::GT:
				    valid = value > conditions[i].val;
			        case Operator::GEQ:
				    valid = value >= conditions[i].val;
                }
            }
        }
    }
    return valid;
}


void RecordManager::printResult(const std::vector<Tuple> &results)
{
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
