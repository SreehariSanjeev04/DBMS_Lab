#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>
#include <iostream>

BlockBuffer::BlockBuffer(int blockNum) {
    this->blockNum = blockNum;
}

RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {} /* Constructor Initializer*/

int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **bufferPtr) {
    int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

    if(bufferNum == E_BLOCKNOTINBUFFER) {
        bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

        if(bufferNum == E_OUTOFBOUND) {
            return E_OUTOFBOUND;
        }

        Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
    }

    *bufferPtr = StaticBuffer::blocks[bufferNum];

    return SUCCESS;
}
int BlockBuffer::getHeader(struct HeadInfo* head) {
    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret != SUCCESS) {
        return ret;
    }

    unsigned char buffer[BLOCK_SIZE];
    Disk::readBlock(buffer, this->blockNum);

    memcpy(&head->numSlots, buffer+24, 4);
    memcpy(&head->numAttrs, buffer+20, 4);
    memcpy(&head->numEntries, buffer+16, 4);
    memcpy(&head->rblock, buffer+12, 4);
    memcpy(&head->lblock, buffer+8, 4);
    memcpy(&head->pblock, buffer+4, 4);

    return SUCCESS;
}

/*
    Need Review
*/
int RecBuffer::getRecord(union Attribute *rec, int slotNum) {

    unsigned char* bufferPtr;
    int ret = BlockBuffer::loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret != SUCCESS) {
        return ret;
    }
    HeadInfo head;

    BlockBuffer::getHeader(&head);

    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;

    unsigned char buffer[BLOCK_SIZE];
    Disk::readBlock(buffer, this->blockNum);

    int recordStart = HEADER_SIZE + slotCount + slotNum * (ATTR_SIZE * attrCount);
    int recordSize = ATTR_SIZE * attrCount;

    unsigned char *start = buffer + recordStart;

    memcpy(rec, start, recordSize);
    return SUCCESS;
}

int RecBuffer::setRecord(union Attribute *rec, int slotNum) {

    unsigned char *bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret!= SUCCESS) {
        return ret;
    }
    HeadInfo head;
    
    BlockBuffer::getHeader(&head);

    unsigned char buffer[BLOCK_SIZE];

    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;

    int recordSize = ATTR_SIZE * attrCount;
    int recordStart = HEADER_SIZE + slotCount + slotNum * recordSize;
    unsigned char *start = buffer + recordStart;

    memcpy(start, rec, recordSize);
    Disk::writeBlock(buffer, this->blockNum);
    
    return SUCCESS;
}


