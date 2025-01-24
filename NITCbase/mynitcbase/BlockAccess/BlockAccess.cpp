#include "BlockAccess.h"

#include <cstring>
#include <iostream>

RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op) {
    RecId prevSearchIndex;
    RelCacheTable::getSearchIndex(relId, &prevSearchIndex);
    int block = -1;
    int slot = -1;
    if(prevSearchIndex.block == -1 && prevSearchIndex.slot == -1) {
        // previous hit
        RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(relId, &relCatEntry);
        block = relCatEntry.firstBlk;
        slot = 0 ;
    } else {
        block = prevSearchIndex.block;
        slot = prevSearchIndex.slot + 1;
    } 

    // search for the record
    RelCatEntry relCatBuffer;
    RelCacheTable::getRelCatEntry(relId, &relCatBuffer);
    while(block != -1) {
        // creating buffer object
        RecBuffer Buffer(block);
        HeadInfo header;
        Buffer.getHeader(&header);

        unsigned char *slotMap = (unsigned char*)malloc(sizeof(unsigned char) * header.numSlots);
        Buffer.getSlotMap(slotMap);

        if(slot >= relCatBuffer.numSlotsPerBlk) {
            block = header.rblock, slot = 0;
            continue;
        }

        if(slotMap[slot] == SLOT_UNOCCUPIED) {
            slot++;
            continue;
        }

        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

        Attribute *record = (Attribute *)malloc(sizeof(Attribute) * header.numAttrs);
        Buffer.getRecord(record, slot);

        int attrOffset = attrCatEntry.offset;
        int cmpVal = compareAttrs(record[attrOffset], attrVal, attrCatEntry.attrType);

        if((op == NE && cmpVal != 0 ) ||
        (op == LT && cmpVal < 0) ||
        (op == LE && cmpVal <= 0) ||
        (op == EQ && cmpVal == 0) ||
        (op == GT && cmpVal > 0) ||
        (op == GE && cmpVal >= 0)) {
            RecId newSearchIndex;
            newSearchIndex.block = block;
            newSearchIndex.slot = slot;
            RelCacheTable::setSearchIndex(relId, &newSearchIndex);
            return RecId{block, slot};
        }   
        slot++;
    }
    return RecId{-1,-1};
}

int BlockAccess::renameRelation(char* oldRelName, char *newRelName) {
    
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute newRelationName;
    strcpy(newRelationName.sVal, newRelName);
    
    RecId searchIndex = linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, newRelationName, EQ);
    if(searchIndex.block != -1 && searchIndex.slot != -1) {
        return E_RELEXIST;
    }

    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    
    Attribute oldRelationName;
    strcpy(oldRelationName.sVal, oldRelName);

    searchIndex = linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, oldRelationName, EQ);
    if(searchIndex.block == -1 && searchIndex.slot == -1) {
        return E_RELNOTEXIST;
    }

    RecBuffer buffer(searchIndex.block);
    Attribute record[RELCAT_NO_ATTRS];
    buffer.getRecord(record, searchIndex.slot);
    strcpy(record[RELCAT_REL_NAME_INDEX].sVal, newRelName);

    buffer.setRecord(record, searchIndex.slot);

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    for(int i = 0; i < record[RELCAT_NO_ATTRIBUTES_INDEX].nVal; i++) {
        searchIndex = linearSearch(ATTRCAT_RELID, (char*)ATTRCAT_ATTR_RELNAME, oldRelationName, EQ);

        RecBuffer attrBlock(searchIndex.block);
        Attribute attrRecord[ATTRCAT_NO_ATTRS];
        attrBlock.getRecord(attrRecord, searchIndex.slot);
        strcpy(attrRecord[ATTRCAT_REL_NAME_INDEX].sVal, newRelName);
        attrBlock.setRecord(attrRecord, searchIndex.slot);
    }
    return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr;
    strcpy(relNameAttr.sVal, relName);
    RecId searchIndex = linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, relNameAttr, EQ);
    if(searchIndex.block == -1 && searchIndex.slot == -1) {
        return E_RELNOTEXIST;
    }
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    
    RecId attrToRenameRecId = {-1,-1};
    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

    while(true) {
        searchIndex = BlockAccess::linearSearch(ATTRCAT_RELID, (char*)ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);
        if(searchIndex.block == -1 && searchIndex.slot == -1) {
            break;
        }

        RecBuffer attrBuffer(searchIndex.block);
        Attribute attrRecord[ATTRCAT_NO_ATTRS];

        attrBuffer.getRecord(attrRecord, searchIndex.slot);

        if(strcmp(attrRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName) == 0) {
            attrToRenameRecId = searchIndex;
            break;
        }
        if(strcmp(attrRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName) == 0) {
            return E_ATTREXIST;
        }
    }
    if(attrToRenameRecId.block == -1 && attrToRenameRecId.slot == -1) {
        return E_ATTRNOTEXIST;
    }

    RecBuffer attrBuffer(attrToRenameRecId.block);
    Attribute attrRecord[ATTRCAT_NO_ATTRS];
    attrBuffer.getRecord(attrRecord, attrToRenameRecId.slot);
    strcpy(attrRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName);
    attrBuffer.setRecord(attrRecord, attrToRenameRecId.slot);

    return SUCCESS;


}

int BlockAccess::insert(int relId, Attribute* record) {
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    int blockNum = relCatEntry.firstBlk;

    RecId rec_id = {-1,-1};
    int numOfSlots = relCatEntry.numSlotsPerBlk;
    int numOfAttributes = relCatEntry.numAttrs;
    int prevBlockNum = -1;

    while(blockNum != -1) {
        RecBuffer recBuffer(blockNum);
        HeadInfo header;
        recBuffer.getHeader(&header);
        unsigned char* slotMap = (unsigned char*)malloc(sizeof(unsigned char) * header.numSlots);
        recBuffer.getSlotMap(slotMap);

        for(int i = 0; i < header.numSlots; i++) {
            if(slotMap[i] == SLOT_UNOCCUPIED) {
                rec_id.block = blockNum;
                rec_id.slot = i;
                break;
            }
        }
        if(rec_id.slot != -1 && rec_id.block != -1) {
            break;
        }
        prevBlockNum = blockNum;
        blockNum = header.rblock;
    }

    // if no more free slot is found in existing record blocks

    if(rec_id.block == -1 && rec_id.slot == -1) {
        if(relId == RELCAT_RELID) {
            return E_MAXRELATIONS;
        }

        // allocate a new block for the record

        RecBuffer blockBuffer;
        blockNum = blockBuffer.getBlockNum();


        if(blockNum == E_DISKFULL) {
            return E_DISKFULL;
        }

        rec_id.block = blockNum;
        rec_id.slot = 0;

        // setting the header of the new record block such that it links with them

        HeadInfo blockHeader;
        blockHeader.pblock = blockHeader.rblock = -1;
        blockHeader.blockType = REC;
        if(relCatEntry.numRecs == 0) {
            blockHeader.lblock = -1;
        } else blockHeader.lblock = prevBlockNum;
        blockHeader.numAttrs = relCatEntry.numAttrs;
        blockHeader.numEntries = 0;
        blockHeader.numSlots = relCatEntry.numSlotsPerBlk;
        blockBuffer.setHeader(&blockHeader);

        unsigned char* slotMap = (unsigned char*)malloc(sizeof(unsigned char) * relCatEntry.numSlotsPerBlk);
        for(int i = 0; i < relCatEntry.numSlotsPerBlk; i++) {
            slotMap[i] = SLOT_UNOCCUPIED;
        }
        blockBuffer.setSlotMap(slotMap);

        if(prevBlockNum != -1) {

            RecBuffer prevBuffer(prevBlockNum);
            HeadInfo prevHeader;
            prevBuffer.getHeader(&prevHeader);
            prevHeader.rblock = blockNum;
            prevBuffer.setHeader(&prevHeader);
        } else {
            // first record block for the relation

            relCatEntry.firstBlk = rec_id.block;
            RelCacheTable::setRelCatEntry(relId, &relCatEntry);

        }

        relCatEntry.lastBlk = rec_id.block;
        RelCacheTable::setRelCatEntry(relId, &relCatEntry);
    }

    RecBuffer buffer(rec_id.block);
    int ret = buffer.setRecord(record, rec_id.slot);
    if(ret != SUCCESS) {
        printf("Record save unsuccessful.\n");
        exit(FAILURE);
    }

    unsigned char* slotMap = (unsigned char*)malloc(sizeof(unsigned char) * relCatEntry.numSlotsPerBlk);
    slotMap[rec_id.slot] = SLOT_OCCUPIED;

    buffer.setSlotMap(slotMap);


    // increment the numEntries field in the header of the block

    HeadInfo header;
    buffer.getHeader(&header);
    header.numEntries+=1;
    buffer.setHeader(&header);
    
    relCatEntry.numRecs++;
    RelCacheTable::setRelCatEntry(relId, &relCatEntry);

    return SUCCESS;
}

// RecId BlockAccess::linearSearch(int recId, char attrName[ATTR_SIZE], union Attribute attVal, int op) {
//     RecId prevSearchIndex;
//     RelCacheTable::getSearchIndex(recId, &prevSearchIndex);
    
//     int block = prevSearchIndex.block;
//     int slot = prevSearchIndex.slot;
    
//     if(prevSearchIndex.block == -1 && prevSearchIndex.slot == -1) {
//         // no previous hits
//         RelCatEntry relCatEntry;
//         RelCacheTable::getRelCatEntry(recId, &relCatEntry);
        
//         block = relCatEntry.firstBlk;
//         slot = 0;
//     } else {
//         // there was hit from prev search
//         block = prevSearchIndex.block;
//         slot = prevSearchIndex.slot + 1;
//     }

//     while(block != -1) {
//         RecBuffer blockBuffer(block);
        
//         Attribute CatRecord[RELCAT_NO_ATTRS];
//         HeadInfo header;

//         blockBuffer.getRecord(CatRecord, slot);

//         blockBuffer.getHeader(&header);

//         unsigned char *slotMap = (unsigned char*)malloc(sizeof(unsigned char) * header.numSlots);
//         blockBuffer.getSlotMap(slotMap);

//         if(slot >= header.numSlots) {
//             block = header.rblock;
//             slot = 0;
//             continue;
//         }

//         if(slotMap[slot] == SLOT_UNOCCUPIED) {
//             slot++;
//             continue;
//         }

//         AttrCatEntry attrCatBuf;
//         AttrCacheTable::getAttrCatEntry(recId, attrName, &attrCatBuf);

//         Attribute *record = (Attribute *)malloc(sizeof(Attribute) * header.numAttrs);
//         blockBuffer.getRecord(record, slot);
//         int attrOffset = attrCatBuf.offset;

//         int cmpVal = compareAttrs(record[attrOffset], attVal, attrCatBuf.attrType);

//         if((op == NE && cmpVal != 0) ||
//             (op == LT && cmpVal < 0) ||
//             (op == LE && cmpVal <= 0) ||
//             (op == EQ && cmpVal == 0) ||
//             (op == GT && cmpVal > 0) ||
//             (op == GE && cmpVal >= 0)) {
//                 // setting the search index as the new recId

//                 RecId newSearchIndex;
//                 newSearchIndex.block = block;
//                 newSearchIndex.slot = slot;
//                 RelCacheTable::setSearchIndex(recId, &newSearchIndex);
//                 return RecId{block, slot};
//             }
//             slot++;
//     }
//     // no record in the relation that satisfies the condition
//     return RecId{-1,-1};
// }