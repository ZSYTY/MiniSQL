#include "RecordManager.h"

const std::string tableName = "table";

int main(int argc,char *argv[])
{   
    BufferManager bufferManager = BufferManager();
    IndexManager* indexManager = new IndexManager(&bufferManager);
    CatalogManager* catalogManager = new CatalogManager(&bufferManager);
    RecordManager* recordManager = new RecordManager(&bufferManager,indexManager,catalogManager);
    // Test create tabel
    recordManager->createTable(tableName);
    // Test drop table
    return 0;
}
