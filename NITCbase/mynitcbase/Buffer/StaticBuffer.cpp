#include "StaticBuffer.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];

StaticBuffer::StaticBuffer()
{
    for (int i = 0; i < 4; i++)
    {
        Disk::readBlock(blockAllocMap + i * BLOCK_SIZE, i);
    }

    for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++)
    {
        metainfo[bufferIndex].free = true;
        metainfo[bufferIndex].dirty = false;
        metainfo[bufferIndex].timeStamp = -1;
        metainfo[bufferIndex].blockNum = -1;
    }
}

StaticBuffer::~StaticBuffer()
{

    for (int i = 0; i < 4; i++)
    {
        Disk::writeBlock(blockAllocMap + i * BLOCK_SIZE, i);
    }

    for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++)
    {
        if (metainfo[bufferIndex].free == false && metainfo[bufferIndex].dirty == true)
        {
            Disk::writeBlock(blocks[bufferIndex], metainfo[bufferIndex].blockNum);
        }
    }
}

int StaticBuffer::getFreeBuffer(int blockNum)
{
    if (blockNum < 0 || blockNum > DISK_BLOCKS)
    {
        return E_OUTOFBOUND;
    }

    for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++)
    {
        if (metainfo[bufferIndex].free == false)
        {
            metainfo[bufferIndex].timeStamp++;
        }
    }

    int allocatedBuffer;
    for (allocatedBuffer = 0; allocatedBuffer < BUFFER_CAPACITY; allocatedBuffer++)
    {
        if (metainfo[allocatedBuffer].free == true)
            break;
    }

    if (allocatedBuffer == BUFFER_CAPACITY)
    {
        allocatedBuffer = -1;
        for (int i = 0; i < BUFFER_CAPACITY; i++)
        {
            if (metainfo[i].timeStamp > allocatedBuffer)
            {
                allocatedBuffer = i;
            }
        }
        if (metainfo[allocatedBuffer].dirty == true)
        {
            Disk::writeBlock(blocks[allocatedBuffer], metainfo[allocatedBuffer].blockNum);
        }
    }

    metainfo[allocatedBuffer].free = false;
    metainfo[allocatedBuffer].dirty = false;
    metainfo[allocatedBuffer].blockNum = blockNum;
    metainfo[allocatedBuffer].timeStamp = 0;

    return allocatedBuffer;
}

int StaticBuffer::getBufferNum(int blockNum)
{
    if (blockNum < 0 || blockNum > DISK_BLOCKS)
    {
        return E_OUTOFBOUND;
    }
    int bufferIndex;
    for (bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++)
    {
        if (metainfo[bufferIndex].blockNum == blockNum)
            break;
    }

    if (bufferIndex == BUFFER_CAPACITY)
        return E_BLOCKNOTINBUFFER;
    else
        return bufferIndex;
}

int StaticBuffer::setDirtyBit(int blockNum)
{
    int bufIndex = getBufferNum(blockNum);

    if (bufIndex == E_BLOCKNOTINBUFFER)
        return E_BLOCKNOTINBUFFER;
    else if (bufIndex == E_OUTOFBOUND)
        return E_OUTOFBOUND;
    else
        metainfo[bufIndex].dirty = true;

    return SUCCESS;
}

int StaticBuffer::getStaticBlockType(int blockNum) {
    if(blockNum < 0 || blockNum >= DISK_BLOCKS) {
        return E_OUTOFBOUND;
    }
    int type = (int)blockAllocMap[blockNum];
    return type;
}
