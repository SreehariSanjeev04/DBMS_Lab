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
        (op == GE && cmpVal) > 0) {
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