#include "StaticBuffer.h"


// Defining the variables in cpp files due to static nature
unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];
StaticBuffer::StaticBuffer() {
    // Initialize buffer with default values
    for(int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++) {
        metainfo[bufferIndex].free = true;
    }
}

StaticBuffer::~StaticBuffer() {}

int StaticBuffer::getFreeBuffer(int blockNum) {
    if(blockNum < 0 || blockNum > DISK_BLOCKS) {
        return E_OUTOFBOUND;
    }
    int allocatedBuffer;

    for(int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++) {
        if(metainfo[bufferIndex].free == true) {
            allocatedBuffer = bufferIndex;
            break;
        }
    }
    metainfo[allocatedBuffer].free = false;
    metainfo[allocatedBuffer].blockNum = blockNum;
    return allocatedBuffer;
}

int StaticBuffer::getBufferNum(int blockNum) {
    if(blockNum < 0 || blockNum > DISK_BLOCKS) {
        return E_OUTOFBOUND;
    }
    for(int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++) {
        if(metainfo[bufferIndex].blockNum == blockNum) {
            return bufferIndex;
        }
    }
    return E_BLOCKNOTINBUFFER;
}