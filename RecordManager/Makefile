all : testRecordManager

testRecordManager : test.cpp recordManager.cpp ../BufferManager/BufferManager.cpp
	g++ test.cpp recordManager.cpp ../BufferManager/BufferManager.cpp -std=c++11 -o testRecordManager.elf