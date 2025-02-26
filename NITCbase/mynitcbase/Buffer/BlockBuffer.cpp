#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>
#include <cstdio>

BlockBuffer::BlockBuffer(int blockNum)
{

    this->blockNum = blockNum;
}

BlockBuffer::BlockBuffer(char blockType)
{
    int blockTypeConst = -1;
    if (blockType == 'R')
        blockTypeConst = REC;
    if (blockType == 'I')
        blockTypeConst = IND_INTERNAL;
    if (blockType == 'L')
        blockTypeConst = IND_LEAF;

    if (blockTypeConst == -1)
    {
        printf("Invalid Block Type\n");
        return;
    }

    int blockNum = BlockBuffer::getFreeBlock(blockTypeConst);

    this->blockNum = blockNum;
}

IndBuffer::IndBuffer(char blockType) : BlockBuffer(blockType) {}

IndBuffer::IndBuffer(int blockNum) : BlockBuffer(blockNum) {}

IndInternal::IndInternal() : IndBuffer('I') {}

IndInternal::IndInternal(int blockNum) : IndBuffer(blockNum) {}

IndLeaf::IndLeaf() : IndBuffer('L') {}

IndLeaf::IndLeaf(int blockNum) : IndBuffer(blockNum) {}

int IndLeaf::getEntry(void *ptr, int indexNum) {
    if(indexNum < 0 || indexNum >= MAX_KEYS_LEAF) {
        return E_OUTOFBOUND;
    }
    unsigned char* bufferPtr;
    
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret != SUCCESS) {
        printf("Failed to load the block\n");
        return ret;
    }

    unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);
    memcpy((struct Index*)ptr, entryPtr, LEAF_ENTRY_SIZE);
    return SUCCESS;
}


// what ?
int IndInternal::getEntry(void *ptr, int indexNum) {
    if(indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL) {
        return E_OUTOFBOUND;
    }
    unsigned char* bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret != SUCCESS) {
        printf("Failed to load the block\n");
        return ret;
    }
    
    struct InternalEntry *internalEntry = (struct InternalEntry*)ptr;

    unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * 20); // why?
    
    memcpy(&(internalEntry->lChild), entryPtr, sizeof(int32_t));
    memcpy(&(internalEntry->attrVal), entryPtr + 4, sizeof(Attribute));
    memcpy(&(internalEntry->rChild), entryPtr + 20, 4);

    return SUCCESS;
}

int IndLeaf::setEntry(void *ptr, int indexNum) {
    if(indexNum < 0 || indexNum >= MAX_KEYS_LEAF) {
        return E_OUTOFBOUND;
    }
    unsigned char *bufferPtr;   
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret != SUCCESS) {
        printf("Could not load the block\n");
        return ret;
    }
    
    struct Index *index = (struct Index*)ptr;
    
    unsigned char* entryPtr = bufferPtr + HEADER_SIZE + (LEAF_ENTRY_SIZE * indexNum);
    memcpy(entryPtr, index, LEAF_ENTRY_SIZE);
    return SUCCESS;
}

int IndInternal::setEntry(void *ptr, int indexNum) {
    if(indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL) {
        return E_OUTOFBOUND;
    }
    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret != SUCCESS) {
        printf("Could not load the block\n");
        return ret;
    }

    struct InternalEntry *internalEntry = (struct InternalEntry*)ptr;
    unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * 20);

    memcpy(entryPtr, &(internalEntry->lChild), sizeof(int32_t));
    memcpy(entryPtr + 4, &(internalEntry->attrVal), sizeof(Attribute));
    memcpy(entryPtr + 20, &(internalEntry->rChild), sizeof(int32_t));
    return SUCCESS; 
}

int IndLeaf::setEntry(void *ptr, int indexNum) {
    return 0;
}
int IndInternal::setEntry(void *ptr, int indexNum) {
    return 0;
}

RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

RecBuffer::RecBuffer() : BlockBuffer('R') {}

int BlockBuffer::getHeader(struct HeadInfo *head)
{

    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
    {
        return ret; 
    }

    memcpy(&head->blockType, bufferPtr + 0, 4);
    memcpy(&head->pblock, bufferPtr + 4, 4);
    memcpy(&head->lblock, bufferPtr + 8, 4);
    memcpy(&head->rblock, bufferPtr + 12, 4);
    memcpy(&head->numEntries, bufferPtr + 16, 4);
    memcpy(&head->numAttrs, bufferPtr + 20, 4);
    memcpy(&head->numSlots, bufferPtr + 24, 4);
    memcpy(&head->reserved, bufferPtr + 28, 4);
    return SUCCESS;
}

int BlockBuffer::setHeader(struct HeadInfo *head)
{

    unsigned char *bufferPtr;
    int ret = BlockBuffer::loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
        return ret;

    struct HeadInfo *bufferHeader = (struct HeadInfo *)bufferPtr;
    bufferHeader->lblock = head->lblock;
    bufferHeader->rblock = head->rblock;
    bufferHeader->pblock = head->pblock;
    bufferHeader->numAttrs = head->numAttrs;
    bufferHeader->numEntries = head->numEntries;
    bufferHeader->numSlots = head->numSlots;

    ret = StaticBuffer::setDirtyBit(this->blockNum);
    if (ret != SUCCESS)
        return ret;
    return SUCCESS;
}

int RecBuffer::getRecord(union Attribute *rec, int slotNum)
{

    struct HeadInfo head;
    this->getHeader(&head);

    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;

    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
    {
        return ret;
    }


    int recordSize = attrCount * ATTR_SIZE;
    unsigned char *slotPointer = bufferPtr + HEADER_SIZE + slotCount + (recordSize * slotNum);

    memcpy(rec, slotPointer, recordSize);

    return SUCCESS;
}

int RecBuffer::setRecord(union Attribute *rec, int slotNum)
{

    unsigned char *bufferPtr;
    int retval = loadBlockAndGetBufferPtr(&bufferPtr);
    if (retval != SUCCESS)
    {
        return retval;
    }

    HeadInfo head;
    BlockBuffer::getHeader(&head);

    int numOfAttr = head.numAttrs;
    int numOfSlots = head.numSlots;


    if (slotNum < 0 || slotNum >= numOfSlots)
        return E_OUTOFBOUND;

    int recordSize = ATTR_SIZE * numOfAttr;
    memcpy(bufferPtr + HEADER_SIZE + numOfSlots + (slotNum * recordSize), rec, recordSize);

    int response = StaticBuffer::setDirtyBit(this->blockNum);

    if (response != SUCCESS)
    {
        printf("Error setting dirty bit.\n");
        return response;
    }

    return SUCCESS;
}

int RecBuffer::getSlotMap(unsigned char *slotMap)
{
    unsigned char *bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
    {
        return ret;
    }

    struct HeadInfo head;
    this->getHeader(&head);

    int slotCount = head.numSlots;

    unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;

    memcpy(slotMap, slotMapInBuffer, slotCount);

    return SUCCESS;
}

int RecBuffer::setSlotMap(unsigned char *slotMap)
{
    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
        return ret;

    HeadInfo header;
    BlockBuffer::getHeader(&header);

    int numSlots = header.numSlots;

    memcpy(bufferPtr + HEADER_SIZE, slotMap, numSlots);

    ret = StaticBuffer::setDirtyBit(this->blockNum);
    return ret;
}

int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr)
{
    int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

    if (bufferNum != E_BLOCKNOTINBUFFER)
    {
        for (int i = 0; i < BUFFER_CAPACITY; i++)
        {
            if (StaticBuffer::metainfo[i].free == false)
            {
                StaticBuffer::metainfo[i].timeStamp++;
            }
        }
        StaticBuffer::metainfo[bufferNum].timeStamp = 0;
    }
    else
    {
        bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

        if (bufferNum == E_OUTOFBOUND)
            return E_OUTOFBOUND;
        Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
    }
    *buffPtr = StaticBuffer::blocks[bufferNum];
    return SUCCESS;
}

int BlockBuffer::setBlockType(int blockType)
{

    unsigned char *bufferPtr;
    int retval = loadBlockAndGetBufferPtr(&bufferPtr);
    if (retval != SUCCESS)
    {
        return retval;
    }


    *((int32_t *)bufferPtr) = blockType;


    StaticBuffer::blockAllocMap[this->blockNum] = blockType;

    retval = StaticBuffer::setDirtyBit(this->blockNum);
    if (retval != SUCCESS)
    {
        return retval;
    }

    return SUCCESS;
}

int BlockBuffer::getFreeBlock(int blockType)
{

    int blockNum = -1;
    for (int i = 0; i < DISK_BLOCKS; i++)
    {
        if (StaticBuffer::blockAllocMap[i] == UNUSED_BLK)
        {
            blockNum = i;
            break;
        }
    }
    if (blockNum == -1)
        return E_DISKFULL;

    this->blockNum = blockNum;

    StaticBuffer::getFreeBuffer(this->blockNum);

    struct HeadInfo header;
    header.pblock = -1;
    header.lblock = -1;
    header.rblock = -1;
    header.numAttrs = 0;
    header.numEntries = 0;
    header.numSlots = 0;
    BlockBuffer::setHeader(&header);

    BlockBuffer::setBlockType(blockType);

    return blockNum;
}

int BlockBuffer::getBlockNum()
{
    return this->blockNum;
}

void BlockBuffer::releaseBlock()
{

    if (this->blockNum == INVALID_BLOCKNUM || StaticBuffer::blockAllocMap[this->blockNum] == UNUSED_BLK)
        return;

    int buffNum = StaticBuffer::getBufferNum(this->blockNum);
    if (buffNum != E_BLOCKNOTINBUFFER)
    {
        StaticBuffer::metainfo[buffNum].free = true;
    }

    StaticBuffer::blockAllocMap[this->blockNum] = UNUSED_BLK;
    this->blockNum = -1;
}

int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType)
{

    double diff;
    if (attrType == STRING)
        diff = strcmp(attr1.sVal, attr2.sVal);
    else
        diff = attr1.nVal - attr2.nVal;

    if (diff > 0)
        return 1;
    else if (diff < 0)
        return -1;
    else
        return 0;
}
