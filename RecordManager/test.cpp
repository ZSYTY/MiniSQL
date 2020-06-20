#include "RecordManager.h"

const std::string tableName = "Test";

Tuple tuple;
std::vector<Tuple> records;
std::vector<SqlCondition> cons;

void makeTuple();
void makeConditions();
//void MakeTableInfo(); // I revised CatalogManager::getTableInfo for test

int main(int argc,char *argv[])
{   
    BufferManager bufferManager = BufferManager();
    IndexManager* indexManager = new IndexManager(&bufferManager);
    CatalogManager* catalogManager = new CatalogManager(&bufferManager);
    RecordManager* recordManager = new RecordManager(&bufferManager,indexManager,catalogManager);
    int Op;
    std::cout<<"Please enter the function you want to test"<<std::endl;
    std::cout<<"1 create table\n"<<"2 delete table\n"<<"3 insert one record\n"<<"4 delete record\n"<<"5 select all records\n"<<"6 select with specific conditions\n";
    // 1 create
    // 2 delete
    // 3 insert
    // 4 delete record
    // 5 select all records
    // 6 select specfic record
    std::cin>>Op;
    switch(Op){
        case 1:
            //catalogManager->createTable(tableName);
            recordManager->createTable(tableName);break;
        case 2: recordManager->dropTable(tableName);break;
        case 3:
            makeTuple();
            records.push_back(tuple);
            recordManager->insertRecord(tableName,records);
            break;
        case 4:
            makeConditions();
            recordManager->deleteRecord(tableName,cons);
            break;
        case 5:
            recordManager->selectRecord(tableName,cons);
            break;
        case 6:
            makeConditions();
            recordManager->selectRecord(tableName,cons);
            break;
        default:
            std::cout<<"Wrong input"<<std::endl;
            break;
    }
    return 0;
}

void makeTuple()
{
    float f = 6.25;
    SqlValue* sqlValue1 = new SqlValue(SqlValueBaseType::MiniSQL_char,"MiSQL");
    SqlValue* sqlValue2 = new SqlValue(SqlValueBaseType::MiniSQL_int,2020);
    SqlValue* sqlValue3 = new SqlValue(SqlValueBaseType::MiniSQL_float,f);
    tuple.push_back(*sqlValue1);
    tuple.push_back(*sqlValue2);
    tuple.push_back(*sqlValue3);
    return;
}

void makeConditions()
{
//    const std::string columnName = "Name";
//    SqlValue *sqlValue = new SqlValue(SqlValueBaseType::MiniSQL_char,"MiSQL");
//    SqlCondition* sqlCon = new SqlCondition(columnName,Operator::EQ,*sqlValue);
//    cons.push_back(*sqlCon);
    float f = 6.00;
    const std::string columnName1= "Score";
    SqlValue *sqlValue1 = new SqlValue(SqlValueBaseType::MiniSQL_float,f);
    SqlCondition* sqlCon1 = new SqlCondition(columnName1,Operator::GT,*sqlValue1);
    cons.push_back(*sqlCon1);
    return;
}


