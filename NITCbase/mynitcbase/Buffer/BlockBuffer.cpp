#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>

BlockBuffer::BlockBuffer(int blockNum) {
    this->blockNum = blockNum;
}

RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

int BlockBuffer::getHeader(struct HeadInfo* head) {
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

int RecBuffer::getRecord(union Attribute *rec, int slotNum) {
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