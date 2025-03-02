#include "BPlusTree.h"
#include <cstring>
#include <cstdio>
#include <cstdlib>

int BPlusTree::numOfComparions = 0;

RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int op)
{
    IndexId searchIndex;
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getSearchIndex(relId, attrName, &searchIndex);
    if (ret != SUCCESS)
    {
        printf("Search index not available\n");
        exit(FAILURE);
    }
    ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if (ret != SUCCESS)
    {
        printf("Cannot get the Attribute Cache Entry.\n");
        exit(FAILURE);
    }
    int block = -1, index = -1;
    if (searchIndex.block == -1 && searchIndex.index == -1)
    {
        // search is done for the first time
        block = attrCatEntry.rootBlock;
        index = 0;

        if (block == -1)
        {
            return RecId{-1, -1};
        }
    }
    else
    {
        block = searchIndex.block;
        index = searchIndex.index + 1;

        IndLeaf leaf(block);
        HeadInfo leafHead;
        leaf.getHeader(&leafHead);

        if (index >= leafHead.numEntries)
        {
            // traversing to the next block
            block = leafHead.rblock;
            index = 0;

            if (block == -1)
            {
                return RecId{-1, -1};
            }
        }
    }

    while (StaticBuffer::getStaticBlockType(block) == IND_INTERNAL)
    {
        IndInternal internalBlock(block);

        HeadInfo intHead;
        internalBlock.getHeader(&intHead);

        InternalEntry intEntry;
        // moving the pointer
        if (op == NE || op == LT || op == LE)
        {
            // for the three cases, we have to move left
            internalBlock.getEntry(&intEntry, 0);
            block = intEntry.lChild;
        }
        else
        {
            index = 0;
            bool found = false;
            while (index < intHead.numEntries)
            {
                ret = internalBlock.getEntry(&intEntry, index);
                int cmpVal = compareAttrs(intEntry.attrVal, attrVal, NUMBER);
                BPlusTree::numOfComparions++;
                if ((op == EQ && cmpVal == 0) || (op == GE && cmpVal >= 0) || (op == GT && cmpVal > 0))
                {
                    found = true;
                    break;
                }
                index++;
            }

            if (found)
            {
                block = intEntry.lChild;
            }
            else
            {
                block = intEntry.rChild;
            }
        }
    }
    // now in the leaf index block, find the first leaf index entry that satisfies our condition
    while (block != -1)
    {
        IndLeaf leafBlk(block);
        HeadInfo leafHead;
        leafBlk.getHeader(&leafHead);

        Index leafEntry;
        while (index < leafHead.numEntries)
        {
            leafBlk.getEntry(&leafEntry, index);
            int cmpVal = compareAttrs(leafEntry.attrVal, attrVal, NUMBER);
            BPlusTree::numOfComparions++;
            if ((op == EQ && cmpVal == 0) || (op == LE && cmpVal <= 0) ||
                (op == LT && cmpVal < 0) || (op == GT && cmpVal > 0) || (op == GE && cmpVal >= 0) || (op == NE && cmpVal != 0))
            {
                searchIndex.block = block;
                searchIndex.index = index;
                AttrCacheTable::setSearchIndex(relId, attrName, &searchIndex);
                return RecId{leafEntry.block, leafEntry.slot};
            }
            else if ((op == EQ || op == LE || op == LT) && cmpVal > 0)
            {
                // future entries will not satisfy EQ, LE, LT
                return RecId{-1, -1};
            }

            ++index;
        }

        if (op != NE)
        {
            break;
        }

        block = leafHead.rblock;
        index = 0;
    };
    return RecId{-1, -1};
}

// Initializaes new rootBlock of type Internal Nodes and sets up Lchild, RChild, along with the attrVal

/*

    Updates the header of the newRoot such that numOfEntries = 1
    Updates the InternalEntry with lChild, rChild and the attrVal
    Sets the entry at the 0th Index
*/
int BPlusTree::createNewRoot(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int lChild, int rChild)
{

    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    IndInternal newRootBlock;

    int newRootBlockNum = newRootBlock.getBlockNum();

    if (newRootBlockNum == E_DISKFULL)
    {

        // only the right Block child is created, needs to be destroyed
        BPlusTree::bPlusDestroy(rChild);
        return E_DISKFULL;
    }

    HeadInfo blockHeader;
    newRootBlock.getHeader(&blockHeader);

    blockHeader.numEntries = 1;
    newRootBlock.setHeader(&blockHeader);

    InternalEntry internalEntry;

    internalEntry.lChild = lChild;
    internalEntry.rChild = rChild;
    internalEntry.attrVal = attrVal;

    newRootBlock.setEntry(&internalEntry, 0);

    HeadInfo leftChildHeader, rightChildHeader;

    BlockBuffer leftChildBlock(lChild);
    BlockBuffer rightChildBlock(rChild);

    leftChildBlock.getHeader(&leftChildHeader);
    rightChildBlock.getHeader(&rightChildHeader);

    leftChildHeader.pblock = newRootBlockNum;
    rightChildHeader.pblock = newRootBlockNum;

    leftChildBlock.setHeader(&leftChildHeader);
    rightChildBlock.setHeader(&rightChildHeader);

    // setting up the rootBlockNum in the attribute catalog entry of the relation
    attrCatEntry.rootBlock = newRootBlockNum;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);

    return SUCCESS;
}

// Converts the existing leaf block into left Leaf and creates a right Leaf Block
/*
    Creates the rightBlock using constructor 1 
    updates the header values for the the two blocks
    Inserts the first 32 values in leftBlock and the rest in the rightBlock
*/

int BPlusTree::splitLeaf(int leafBlockNum, Index indices[])
{
    IndLeaf rightBlock;

    // declaring the existing leafBlockNum as left leaf block
    IndLeaf leftBlock(leafBlockNum);

    int rightBlockNum = rightBlock.getBlockNum();
    int leftBlockNum = leftBlock.getBlockNum();

    if (rightBlockNum == E_DISKFULL)
    {
        return E_DISKFULL;
    }

    HeadInfo leftBlockHeader, rightBlockHeader;

    leftBlock.getHeader(&leftBlockHeader);
    rightBlock.getHeader(&rightBlockHeader);

    // set rightBlkHeader with the following values
    // - number of entries = (MAX_KEYS_LEAF+1)/2 = 32,
    // - pblock = pblock of leftBlk
    // - lblock = leftBlkNum
    // - rblock = rblock of leftBlk

    // set leftBlkHeader with the following values
    // - number of entries = (MAX_KEYS_LEAF+1)/2 = 32
    // - rblock = rightBlkNum
    // and update the header of leftBlk using BlockBuffer::setHeader() */

    rightBlockHeader.numEntries = (MAX_KEYS_LEAF + 1) / 2;
    rightBlockHeader.pblock = leftBlockHeader.pblock;
    rightBlockHeader.lblock = leftBlockNum;
    rightBlockHeader.rblock = leftBlockHeader.rblock;
    rightBlock.setHeader(&rightBlockHeader);

    leftBlockHeader.numEntries = (MAX_KEYS_LEAF + 1) / 2;
    leftBlockHeader.rblock = rightBlockNum;

    leftBlock.setHeader(&leftBlockHeader);

    for (int entry = 0; entry <= MIDDLE_INDEX_LEAF; entry++)
    {
        leftBlock.setEntry(&indices[entry], entry);
        rightBlock.setEntry(&indices[entry + MIDDLE_INDEX_LEAF + 1], entry);
    }

    return rightBlockNum;
}

int BPlusTree::insertIntoLeaf(int relId, char attrName[ATTR_SIZE], int leafBlockNum, Index indexEntry)
{
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    IndLeaf leafBlock(leafBlockNum);
    HeadInfo blockHeader;

    leafBlock.getHeader(&blockHeader);

    // existing indices + new index to insert

    /*
        Seg Fault
    */
    Index indices[blockHeader.numEntries + 1];

    bool inserted = false;
    int j = 0;
    for (int i = 0; i < blockHeader.numEntries; i++)
    {
        Index entry;
        leafBlock.getEntry(&entry, i);

        // insert the new value where the entry Val >= indexEntry Val
        if (compareAttrs(entry.attrVal, indexEntry.attrVal, attrCatEntry.attrType) <= 0)
        {

            indices[j++] = entry;
        }
        else if (!inserted)
        {
            indices[j++] = indexEntry;
            indices[j++] = entry;
            inserted = true;
        }
        else
            indices[j++] = entry;
    }

    // insert at the end if not inserted already
    if (!inserted)
        indices[blockHeader.numEntries] = indexEntry;

    // leaf block has not reached the max limit
    if (blockHeader.numEntries < MAX_KEYS_LEAF)
    {
        blockHeader.numEntries++;
        leafBlock.setHeader(&blockHeader);

        for (int i = 0; i < blockHeader.numEntries; i++)
        {
            leafBlock.setEntry(&indices[i], i);
        }

        return SUCCESS;
    }
    // if exceeded the max capacity

    int newRightBlock = splitLeaf(leafBlockNum, indices);

    if (newRightBlock == E_DISKFULL)
        return E_DISKFULL;

    if (blockHeader.pblock != -1)
    {

        // insert the middle value to the internal block

        InternalEntry middleEntry;
        middleEntry.attrVal = indices[MIDDLE_INDEX_LEAF].attrVal;
        middleEntry.lChild = leafBlockNum;
        middleEntry.rChild = newRightBlock;

        return insertIntoInternal(relId, attrName, blockHeader.pblock, middleEntry);
    }
    else
    {

        // create a new root block
        if (createNewRoot(relId, attrName, indices[MIDDLE_INDEX_LEAF].attrVal, leafBlockNum, newRightBlock) == E_DISKFULL)
        {
            return E_DISKFULL;
        }
    }

    return SUCCESS;
}

int BPlusTree::insertIntoInternal(int relId, char attrName[ATTR_SIZE], int intBlockNum, InternalEntry intEntry)
{

    int ret;
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    IndInternal internalBlock(intBlockNum);

    HeadInfo blockHeader;
    internalBlock.getHeader(&blockHeader);

    InternalEntry internalEntries[blockHeader.numEntries + 1];

    int inserted = false;
    int entryIndex = 0;
    for (int entry = 0; entry < blockHeader.numEntries; entry++)
    {
        InternalEntry internalEntry;
        internalBlock.getEntry(&internalEntry, entry);

        if (compareAttrs(internalEntry.attrVal, intEntry.attrVal, attrCatEntry.attrType) <= 0)
        {
            internalEntries[entryIndex++] = internalEntry;
        }
        else if (!inserted)
        {
            // updating the next internalEntry's LChild
            internalEntry.lChild = intEntry.rChild;
            internalEntries[entryIndex++] = intEntry;
            internalEntries[entryIndex++] = internalEntry;
            inserted = true;
        }
        else
            internalEntries[entryIndex++] = internalEntry;
    }

    if (!inserted)
        internalEntries[blockHeader.numEntries] = intEntry;

    // internal index Blocks has not reached max limit

    if (blockHeader.numEntries < MAX_KEYS_INTERNAL)
    {
        blockHeader.numEntries++;
        internalBlock.setHeader(&blockHeader);

        for (int i = 0; i < blockHeader.numEntries; i++)
        {
            internalBlock.setEntry(&internalEntries[i], i);
        }
        return SUCCESS;
    }

    // we have reached the limit

    // creating a new right block and keeping the existing as the left
    int newRightBlock = splitInternal(intBlockNum, internalEntries);

    if (newRightBlock == E_DISKFULL)
    {
        bPlusDestroy(intEntry.rChild);
        return E_DISKFULL;
    }

    if (blockHeader.pblock != -1)
    {
        // insert the middle value from the internalEntries into the parent block

        InternalEntry parentInternalEntry;
        parentInternalEntry.attrVal = internalEntries[MIDDLE_INDEX_INTERNAL].attrVal;
        parentInternalEntry.lChild = intBlockNum;
        parentInternalEntry.rChild = newRightBlock;

        ret = insertIntoInternal(relId, attrName, blockHeader.pblock, parentInternalEntry);

        if (ret != SUCCESS)
            return ret;
    }
    else
    {
        // the curent block was the root block and is now split, a new internal index
        // block needs to be allocated and made the root of the tree

        ret = createNewRoot(relId, attrName, internalEntries[MIDDLE_INDEX_INTERNAL].attrVal, intBlockNum, newRightBlock);
        if (ret != SUCCESS)
            return ret;
    }
    return SUCCESS;
}

int BPlusTree::splitInternal(int intBlockNum, InternalEntry internalEntries[]) {
    IndInternal rightBlock;
    IndInternal leftBlock(intBlockNum);

    int rightBlockNum = rightBlock.getBlockNum();
    int leftBlockNum = intBlockNum;

    if(rightBlockNum == E_DISKFULL) {
        return E_DISKFULL;
    }

    HeadInfo leftBlockHeader, rightBlockHeader;
    
    rightBlock.getHeader(&rightBlockHeader);
    leftBlock.getHeader(&leftBlockHeader);

    rightBlockHeader.numEntries = (MAX_KEYS_INTERNAL)/2;
    rightBlockHeader.pblock = leftBlockHeader.pblock;
    rightBlock.setHeader(&rightBlockHeader);

    leftBlockHeader.numEntries = (MAX_KEYS_INTERNAL) / 2;
    leftBlock.setHeader(&leftBlockHeader);

    // inserting the internal entries

    for(int i = 0; i < MIDDLE_INDEX_INTERNAL; i++) {
        leftBlock.setEntry(&internalEntries[i], i);
        rightBlock.setEntry(&internalEntries[i + MIDDLE_INDEX_INTERNAL + 1], i);
    }

    // Reassigning parents form the left Child to the right child

    int type = StaticBuffer::getStaticBlockType(internalEntries[0].rChild);

    // updating the parent block of the entries of right Block
    for(int i = MIDDLE_INDEX_INTERNAL; i <= MAX_KEYS_INTERNAL; i++) {
        BlockBuffer child(internalEntries[i].rChild);

        HeadInfo childHeader;
        child.getHeader(&childHeader);
        childHeader.pblock = rightBlockNum;
        child.setHeader(&childHeader);
    }

    return rightBlockNum;

}
int BPlusTree::findLeafToInsert(int rootBlock, Attribute attrVal, int attrType)
{
    int blockNum = rootBlock;

    while (StaticBuffer::getStaticBlockType(blockNum) != IND_LEAF)
    {
        IndInternal internalBlock(blockNum);

        HeadInfo blockHeader;
        internalBlock.getHeader(&blockHeader);

        // iterate through all the entires to find the first entry whose attribute
        // value >= attrVal

        int index = 0;
        while (index < blockHeader.numEntries)
        {
            InternalEntry entry;
            internalBlock.getEntry(&entry, index);
            if (compareAttrs(attrVal, entry.attrVal, attrType) <= 0)
                break;

            index++;
        }

        if (index == blockHeader.numEntries)
        {

            InternalEntry entry;
            internalBlock.getEntry(&entry, blockHeader.numEntries - 1);
            blockNum = entry.rChild;
        }
        // left child of block that satisfied
        else
        {
            InternalEntry entry;
            internalBlock.getEntry(&entry, index);
            blockNum = entry.lChild;
        }
    }
    return blockNum;
}

int BPlusTree::bPlusCreate(int relId, char attrName[ATTR_SIZE])
{
    int ret;
    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)
        return E_NOTPERMITTED;
    AttrCatEntry attrCatEntry;
    ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if (ret != SUCCESS)
        return ret;

        // root Block already exists

    if (attrCatEntry.rootBlock != -1)
    {
        return SUCCESS;
    }

    // get a free leaf block using constructor 1
    // root block is a leaf block
    IndLeaf rootBlockBuf;
    int rootBlock = rootBlockBuf.getBlockNum();

    if (rootBlock == E_DISKFULL)
    {
        return E_DISKFULL;
    }

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);
    int block = relCatEntry.firstBlk;

    attrCatEntry.rootBlock = rootBlock;

    ret = AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);

    // Traverse all the blocks in the relation and insert them one by one into B+ tree
    while (block != -1)
    {
        printf("%d\n", block);
        RecBuffer recBuf(block);

        unsigned char slotMap[relCatEntry.numSlotsPerBlk];
        recBuf.getSlotMap(slotMap);

        for (int slot = 0; slot < relCatEntry.numSlotsPerBlk; slot++)
        {
            if (slotMap[slot] == SLOT_OCCUPIED)
            {
                Attribute record[relCatEntry.numAttrs];
                recBuf.getRecord(record, slot);
                RecId recId{block, slot};
                ret = BPlusTree::bPlusInsert(relId, attrName, record[attrCatEntry.offset], recId);
                if (ret == E_DISKFULL)
                    return E_DISKFULL;
            }
        }
        HeadInfo head;
        recBuf.getHeader(&head);
        block = head.rblock;
    }
    printf("Done\n");
    return SUCCESS;
}

int BPlusTree::bPlusInsert(int relId, char attrName[ATTR_SIZE], Attribute attrVal, RecId recId)
{
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if (ret != SUCCESS)
    {
        printf("Could not fetch the Attribute Catalog Record\n");
        return ret;
    }

    int rootBlock = attrCatEntry.rootBlock;

    if (rootBlock == -1)
        return E_NOINDEX;

    // find the leaf block to which insertion is to be done

    int leafBlkNum = findLeafToInsert(rootBlock, attrVal, attrCatEntry.attrType);

    // insertion to the leaf block at the blockNum

    Index indexEntry;
    indexEntry.attrVal = attrVal;
    indexEntry.block = recId.block;
    indexEntry.slot = recId.slot;

    if (insertIntoLeaf(relId, attrName, leafBlkNum, indexEntry) == E_DISKFULL)
    {
        BPlusTree::bPlusDestroy(rootBlock);

        attrCatEntry.rootBlock = -1;
        AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);
        return E_DISKFULL;
    }
    return SUCCESS;
}

int BPlusTree::bPlusDestroy(int rootBlockNum)
{
    if (rootBlockNum < 0 || rootBlockNum >= DISK_BLOCKS)
    {
        return E_OUTOFBOUND;
    }

    int type = StaticBuffer::getStaticBlockType(rootBlockNum);

    if (type == IND_LEAF)
    {
        IndLeaf leafBlock(rootBlockNum);
        leafBlock.releaseBlock();
        return SUCCESS;
    }

    else if (type == IND_INTERNAL)
    {

        IndInternal internalBlock(rootBlockNum);

        HeadInfo blockHeader;
        internalBlock.getHeader(&blockHeader);

        InternalEntry blockEntry;
        internalBlock.getEntry(&blockEntry, 0);

        BPlusTree::bPlusDestroy(blockEntry.lChild);

        for (int entry = 0; entry < blockHeader.numEntries; entry++)
        {
            internalBlock.getEntry(&blockEntry, entry);
            BPlusTree::bPlusDestroy(blockEntry.rChild);
        }

        internalBlock.releaseBlock();

        return SUCCESS;
    }
    else
    {
        return E_INVALIDBLOCK;
    }
}