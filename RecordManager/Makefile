all : testRecordManager

testRecordManager : test.cpp recordManager.cpp ../BufferManager/BufferManager.cpp ../CatalogManager/CatalogManager.cpp
	g++ test.cpp recordManager.cpp ../BufferManager/BufferManager.cpp ../CatalogManager/CatalogManager.cpp -std=c++11 -o testRecordManager.elf
