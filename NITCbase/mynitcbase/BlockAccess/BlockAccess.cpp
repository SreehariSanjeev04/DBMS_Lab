#include "BlockAccess.h"

#include <cstring>
#include <cstdio>
#include <cstdlib>

int BlockAccess::numOfComparisons = 0;

RecId BlockAccess::linearSearch(int relId,char attrName[ATTR_SIZE],union Attribute attrVal,int op)
{

    RecId prevRecId;

    int ret = RelCacheTable::getSearchIndex(relId, &prevRecId);
    if (ret != SUCCESS)
    {
        printf("Invalid Search Index.\n");
        exit(FAILURE);
    }
    int block;
    int slot;
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        block = relCatEntry.firstBlk;
        slot = 0;
    }
    else
    {

        block = prevRecId.block;
        slot = prevRecId.slot + 1;
    }

    while (block != -1)
    {

        RecBuffer searchBlock(block);

        Attribute searchRecord[relCatEntry.numAttrs];
        int response = searchBlock.getRecord(searchRecord, slot);

        if (response != SUCCESS)
        {
            printf("Record not found.\n");
            exit(FAILURE);
        }

        struct HeadInfo header;
        response = searchBlock.getHeader(&header);
        if (response != SUCCESS)
        {
            printf("Header not found.\n");
            exit(FAILURE);
        }

        unsigned char slotMap[header.numSlots];
        response = searchBlock.getSlotMap(slotMap);
        if (response != SUCCESS)
        {
            printf("Slotmap not found.\n");
            exit(FAILURE);
        }

        if (slot >= header.numSlots)
        {
            block = header.rblock;
            slot = 0;
            continue;
        }

        if (slotMap[slot] == SLOT_UNOCCUPIED)
        {
            slot++;
            continue;
        }

        AttrCatEntry attrCatBuf;
        AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);

        Attribute currRecordAttr = searchRecord[attrCatBuf.offset];
        int cmpVal = compareAttrs(currRecordAttr, attrVal, attrCatBuf.attrType);
        BlockAccess::numOfComparisons++;

        if (
            (op == NE && cmpVal != 0) || 
            (op == LT && cmpVal < 0) ||  
            (op == LE && cmpVal <= 0) || 
            (op == EQ && cmpVal == 0) ||
            (op == GT && cmpVal > 0) ||
            (op == GE && cmpVal >= 0))
        {
            RecId recordId = {block, slot};
            RelCacheTable::setSearchIndex(relId, &recordId);
            return recordId;
        }
        slot++;
    }

    return RecId{-1, -1};
}

int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE])
{

    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    Attribute newRelationName;
    strcpy(newRelationName.sVal, newName);

    char relcatAttrName[] = RELCAT_ATTR_RELNAME;
    RecId searchRes = linearSearch(RELCAT_RELID, relcatAttrName, newRelationName, EQ);

    if (searchRes.slot != -1 || searchRes.block != -1)
    {
        return E_RELEXIST;
    }

    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    Attribute oldRelationName;
    strcpy(oldRelationName.sVal, oldName);

    searchRes = linearSearch(RELCAT_RELID, relcatAttrName, oldRelationName, EQ);

    if (searchRes.slot == -1 && searchRes.block == -1)
    {
        return E_RELNOTEXIST;
    }

    RecBuffer recBuffer(RELCAT_BLOCK);
    Attribute relCatRecord[RELCAT_NO_ATTRS];

    int ret = recBuffer.getRecord(relCatRecord, searchRes.slot);
    if (ret != SUCCESS)
    {
        printf("Record not found\n");
        exit(FAILURE);
    }

    strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, newName);

    ret = recBuffer.setRecord(relCatRecord, searchRes.slot);
    if (ret != SUCCESS)
    {
        printf("Relation name could not be changed\n");
        exit(FAILURE);
    }

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    int numOfAttr = relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
    char attrcatAttrName[] = ATTRCAT_ATTR_RELNAME;

    for (int i = 0; i < numOfAttr; i++)
    {
        searchRes = linearSearch(ATTRCAT_RELID, attrcatAttrName, oldRelationName, EQ);

        RecBuffer buf(searchRes.block);
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

        ret = buf.getRecord(attrCatRecord, searchRes.slot);
        if (ret != SUCCESS)
        {
            printf("Failed to get record from this slot in ATTRCAT.\n");
            exit(FAILURE);
        }

        strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, newName);

        ret = buf.setRecord(attrCatRecord, searchRes.slot);
        if (ret != SUCCESS)
        {
            printf("Failed to change record in this slot in ATTRCAT.\n");
            exit(FAILURE);
        }
    }

    return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE],char oldName[ATTR_SIZE],char newName[ATTR_SIZE])
{

    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    Attribute relNameAttr;
    strcpy(relNameAttr.sVal, relName);
    char relcatAttrName[] = RELCAT_ATTR_RELNAME;

    RecId searchRes = linearSearch(RELCAT_RELID, relcatAttrName, relNameAttr, EQ);
    if (searchRes.slot == -1 && searchRes.block == -1)
    {
        return E_RELNOTEXIST;
    }


    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    RecId attrToRenameRecId{-1, -1};
    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

    while (true)
    {
        RecId searchRecId = linearSearch(ATTRCAT_RELID, (char*)ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);
        if (searchRecId.slot == -1 && searchRecId.block == -1)
        {
            break;
        }

        RecBuffer attrRecord(searchRecId.block);
        attrRecord.getRecord(attrCatEntryRecord, searchRecId.slot);

        if (strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName) == 0)
        {
            attrToRenameRecId.block = searchRecId.block;
            attrToRenameRecId.slot = searchRecId.slot;
        }

        if (strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName) == 0)
            return E_ATTREXIST;
    }

    if (attrToRenameRecId.slot == -1 && attrToRenameRecId.block == -1)
    {
        return E_ATTRNOTEXIST;
    }

    RecBuffer attrBuf(attrToRenameRecId.block);
    attrBuf.getRecord(attrCatEntryRecord, attrToRenameRecId.slot);

    strcpy(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName);

    int ret = attrBuf.setRecord(attrCatEntryRecord, attrToRenameRecId.slot);
    if (ret != SUCCESS)
    {
        printf("Attribute name could not be changed in AttrCat.\n");
        exit(FAILURE);
    }

    return SUCCESS;
}

int BlockAccess::insert(int relId, Attribute *record)
{

    RelCatEntry relCatEntry;
    int ret = RelCacheTable::getRelCatEntry(relId, &relCatEntry);
    int blockNum = relCatEntry.lastBlk;

    RecId rec_id = {-1, -1};

    int numOfSlots = relCatEntry.numSlotsPerBlk;
    int numOfAttributes = relCatEntry.numAttrs;

    int prevBlockNum = -1;


    while (blockNum != -1)
    {
        RecBuffer recBuf(blockNum);

        HeadInfo header;
        recBuf.getHeader(&header);

        unsigned char slotMap[numOfSlots];
        recBuf.getSlotMap(slotMap);

        for (int i = 0; i < numOfSlots; i++)
        {
            if (slotMap[i] == SLOT_UNOCCUPIED)
            {
                rec_id.slot = i;
                rec_id.block = blockNum;
                break;
            }
        }
        if (rec_id.block != -1 && rec_id.slot != -1)
            break;
        prevBlockNum = blockNum;
        blockNum = header.rblock;
    }


    if (rec_id.slot == -1 && rec_id.block == -1)
    {
        if (relId == RELCAT_RELID)
            return E_MAXRELATIONS;

        RecBuffer newRecBuf;
        int newBlockNum = newRecBuf.getBlockNum();
        if (newBlockNum == E_DISKFULL)
        {
            return E_DISKFULL;
        }

        rec_id.block = newBlockNum;
        rec_id.slot = 0;

        HeadInfo newHeader;
        newHeader.blockType = REC;
        newHeader.pblock = -1;
        newHeader.lblock = prevBlockNum;
        newHeader.rblock = -1;
        newHeader.numEntries = 0;
        newHeader.numAttrs = numOfAttributes;
        newHeader.numSlots = numOfSlots;
        newRecBuf.setHeader(&newHeader);

        unsigned char slotMap[numOfSlots];
        for (int i = 0; i < numOfSlots; i++)
        {
            slotMap[i] = SLOT_UNOCCUPIED;
        }
        newRecBuf.setSlotMap(slotMap);

        if (prevBlockNum != -1)
        {
            RecBuffer prevBlock(prevBlockNum);

            HeadInfo prevHeader;
            prevBlock.getHeader(&prevHeader);
            prevHeader.rblock = rec_id.block;
            prevBlock.setHeader(&prevHeader);
        }
        else
        {
            relCatEntry.firstBlk = rec_id.block;
            RelCacheTable::setRelCatEntry(relId, &relCatEntry);
        }
        relCatEntry.lastBlk = rec_id.block;
        RelCacheTable::setRelCatEntry(relId, &relCatEntry);
    }

    RecBuffer block(rec_id.block);
    block.setRecord(record, rec_id.slot);

    unsigned char slotMap[numOfSlots];
    block.getSlotMap(slotMap);
    slotMap[rec_id.slot] = SLOT_OCCUPIED;
    block.setSlotMap(slotMap);

    HeadInfo header;
    block.getHeader(&header);
    header.numEntries++;
    block.setHeader(&header);

    relCatEntry.numRecs++;
    RelCacheTable::setRelCatEntry(relId, &relCatEntry);

    // B+ Tree insertions

    int flag = SUCCESS;

    for(int offset = 0; offset < numOfAttributes; offset++) {
        AttrCatEntry attrCatBuf;
        AttrCacheTable::getAttrCatEntry(relId, offset, &attrCatBuf);

        int rootBlock = attrCatBuf.rootBlock;

        if(rootBlock != -1) {
            int ret = BPlusTree::bPlusInsert(relId, attrCatBuf.attrName, record[offset], rec_id);
            if(ret == E_DISKFULL) {
                flag = E_INDEX_BLOCKS_RELEASED;
            }
        }
    }

    return flag;
}


int BlockAccess::search(int relId,Attribute *record,char attrName[ATTR_SIZE],Attribute attrVal,int op)
{

    RecId recId;
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if(ret != SUCCESS) {
        printf("Failed to fetch Attribute Cache Entry\n");
        return ret;
    }
    
    int rootBlock = attrCatEntry.rootBlock;
    if(rootBlock==-1) {
        recId = linearSearch(relId, attrName, attrVal, op);
    } else {
        recId = BPlusTree::bPlusSearch(relId, attrName, attrVal, op);
    }

    if (recId.slot == -1 && recId.block == -1)
        return E_NOTFOUND;

    RecBuffer block(recId.block);
    block.getRecord(record, recId.slot);

    return SUCCESS;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE])
{

    if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
        return E_NOTPERMITTED;

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr;
    strcpy(relNameAttr.sVal, relName);

    RecId rec_id = linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, relNameAttr, EQ);

    if (rec_id.slot == -1 && rec_id.block == -1)
        return E_RELNOTEXIST;

    Attribute relCatEntryRecord[RELCAT_NO_ATTRS];

    RecBuffer relCatBlock(rec_id.block);
    relCatBlock.getRecord(relCatEntryRecord, rec_id.slot);

    int firstBlock = relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
    int numAttrs = relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;

    while (firstBlock != -1)
    {
        BlockBuffer tempBlock(firstBlock);

        HeadInfo header;
        tempBlock.getHeader(&header);
        firstBlock = header.rblock;

        tempBlock.releaseBlock();
    }
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    int numberOfAttributesDeleted = 0;

    while (true)
    {


        RecId attrCatRecId;
        attrCatRecId = linearSearch(ATTRCAT_RELID, (char*)ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);

        if (attrCatRecId.block == -1 && attrCatRecId.slot == -1)
            break;

        numberOfAttributesDeleted++;

        RecBuffer attrCatBlock(attrCatRecId.block);

        HeadInfo header;
        attrCatBlock.getHeader(&header);

        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        attrCatBlock.getRecord(attrCatRecord, attrCatRecId.slot);

        int rootBlock = attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal;

        unsigned char slotMap[header.numSlots];
        attrCatBlock.getSlotMap(slotMap);
        slotMap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
        attrCatBlock.setSlotMap(slotMap);

        header.numEntries--;
        attrCatBlock.setHeader(&header);

        if (header.numEntries == 0)
        {

            int lBlockNum = header.lblock;

            if (lBlockNum != -1)
            {
                RecBuffer leftBlock(lBlockNum);

                HeadInfo leftBlockHeader;
                leftBlock.getHeader(&leftBlockHeader);
                leftBlockHeader.rblock = header.rblock;
                leftBlock.setHeader(&leftBlockHeader);
            }

            if (header.rblock != -1)
            {
                int rBlockNum = header.rblock;
                RecBuffer rightBlock(rBlockNum);

                HeadInfo rightBlockHeader;
                rightBlock.getHeader(&rightBlockHeader);
                rightBlockHeader.lblock = lBlockNum;
                rightBlock.setHeader(&rightBlockHeader);
            }
            else
            {

                RelCatEntry attrCatInRelCatEntry;
                RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &attrCatInRelCatEntry);
                attrCatInRelCatEntry.lastBlk = lBlockNum;
                RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &attrCatInRelCatEntry);
            }

            attrCatBlock.releaseBlock();
        }

        if(rootBlock != -1) {
            BPlusTree::bPlusDestroy(rootBlock);
        }
    }

    HeadInfo header;
    relCatBlock.getHeader(&header);
    header.numEntries--;
    relCatBlock.setHeader(&header);

    unsigned char slotMap[header.numSlots];
    relCatBlock.getSlotMap(slotMap);
    slotMap[rec_id.slot] = SLOT_UNOCCUPIED;
    relCatBlock.setSlotMap(slotMap);

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatEntry);
    relCatEntry.numRecs--;
    RelCacheTable::setRelCatEntry(RELCAT_RELID, &relCatEntry);

    RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatEntry);
    relCatEntry.numRecs -= numberOfAttributesDeleted;
    RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &relCatEntry);

    return SUCCESS;
}

int BlockAccess::project(int relId, Attribute *record)
{

    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);

    int block, slot;

    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {

        RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(relId, &relCatEntry);

        block = relCatEntry.firstBlk;
        slot = 0;
    }

    else
    {
        block = prevRecId.block;
        slot = prevRecId.slot + 1;
    }

    while (block != -1)
    {
        RecBuffer recBuffer(block);

        HeadInfo header;
        recBuffer.getHeader(&header);

        unsigned char slotMap[header.numSlots];
        recBuffer.getSlotMap(slotMap);
        if (slot >= header.numSlots)
        {
            block = header.rblock;
            slot = 0;
        }

        else if (slotMap[slot] == SLOT_UNOCCUPIED)
            slot++;
        else
            break;
    }

    if (block == -1)
        return E_NOTFOUND;

    RecId nextRecId{block, slot};
    RelCacheTable::setSearchIndex(relId, &nextRecId);
    RecBuffer recBuffer1(nextRecId.block);
    int response = recBuffer1.getRecord(record, nextRecId.slot);
    if (response != SUCCESS)
    {
        printf("Record not found.\n");
        exit(FAILURE);
    }

    return SUCCESS;
}