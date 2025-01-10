#include "OpenRelTable.h"

#include <cstring>
#include <stdlib.h>
#include <iostream>

int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {
    if(strcmp(relName, RELCAT_RELNAME) == 0) {
        return RELCAT_RELID;
    } else if(strcmp(relName, ATTRCAT_RELNAME) == 0) {
        return ATTRCAT_RELID;
    } else if(strcmp(relName, "Students") == 0) {
        return ATTRCAT_RELID + 1;
    } 
    else return E_RELNOTOPEN;
}

OpenRelTable::OpenRelTable() {
    for(int i = 0; i < MAX_OPEN; i++) {
        RelCacheTable::relCache[i] = nullptr;
        AttrCacheTable::attrCache[i] = nullptr;
    }
    
    RecBuffer relCatBuffer(RELCAT_BLOCK);
    Attribute relCatRecord[RELCAT_NO_ATTRS];

    HeadInfo relCatHeader;
    relCatBuffer.getHeader(&relCatHeader);
    
    for(int i = 0; i < relCatHeader.numEntries; i++) {
        relCatBuffer.getRecord(relCatRecord, i); 
        RelCacheEntry relCacheEntry;
        RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
        relCacheEntry.recId.block = RELCAT_BLOCK;
        relCacheEntry.recId.slot = i;

        RelCacheTable::relCache[i] = (RelCacheEntry*)malloc(sizeof(RelCacheEntry));
        *(RelCacheTable::relCache[i]) = relCacheEntry;
    }
    // relCatBuffer.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

    // RelCacheEntry relCacheEntry;
    // RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
    // relCacheEntry.recId.block = RELCAT_BLOCK;
    // relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;

    // RelCacheTable::relCache[RELCAT_RELID] = (RelCacheEntry*)malloc(sizeof(RelCacheEntry));
    // *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;


    // // setting up Attribute catalog relation in relation catalog table

    // relCatBuffer.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);
    // RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
    // relCacheEntry.recId.block = RELCAT_BLOCK;
    // relCacheEntry.recId.slot  = RELCAT_SLOTNUM_FOR_ATTRCAT;

    // RelCacheTable::relCache[ATTRCAT_RELID] = (RelCacheEntry*)malloc(sizeof(RelCacheEntry));
    // *(RelCacheTable::relCache[ATTRCAT_RELID]) = relCacheEntry;


    RecBuffer attrCatBuffer(ATTRCAT_BLOCK);
    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    AttrCacheEntry* attrCacheEntry = nullptr;
    AttrCacheEntry* head = nullptr;
    AttrCacheEntry* prev = nullptr;

   for(int i = 0, attrSlotIndex = 0; i < relCatHeader.numEntries; i++) {
        RelCatEntry relCatEntry;
        HeadInfo attrCatHeader;
        attrCatBuffer.getHeader(&attrCatHeader);
        relCatEntry = RelCacheTable::relCache[i]->relCatEntry;
        int noOfAttributes = relCatEntry.numAttrs;
        for(int j = 0; j < noOfAttributes; j++, attrSlotIndex++) {
            attrCatBuffer.getRecord(attrCatRecord, attrSlotIndex);
            attrCacheEntry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
            AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(attrCacheEntry->attrCatEntry));
            attrCacheEntry->recId.block = ATTRCAT_BLOCK;
            attrCacheEntry->recId.slot = attrSlotIndex; 

            if(!head) {
                head = attrCacheEntry;
                prev = attrCacheEntry; 
                continue;
            }
            if(attrSlotIndex == attrCatHeader.numSlots - 1) {
                attrSlotIndex = -1;
                HeadInfo attrCatHeader; 
                attrCatBuffer.getHeader(&attrCatHeader);
                attrCatBuffer = RecBuffer(attrCatHeader.rblock);
            }
            prev->next = attrCacheEntry;
            prev = attrCacheEntry;
        }
        attrCacheEntry->next = nullptr;
        AttrCacheTable::attrCache[i] = head;
        head = nullptr;
        prev = nullptr;
   }
    // for(int i = 0; i < RELCAT_NO_ATTRS; i++) {
    //     attrCatBuffer.getRecord(attrCatRecord, i);
    //     attrCacheEntry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    //     AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(attrCacheEntry->attrCatEntry));
    //     if(!head) {
    //         head = attrCacheEntry;
    //         prev = attrCacheEntry;
    //     }
    //     prev->next = attrCacheEntry;
    //     prev = attrCacheEntry;
    // }
    // attrCacheEntry->next = nullptr;

    // AttrCacheTable::attrCache[RELCAT_RELID] = head;

    // /* Simply printing the linked list for debugging*/

    // // Setting up Attribute Catalog relation in the Attribute Cache Table // 

    // prev = nullptr;
    // head = nullptr;
    // for(int i = 6; i < ATTRCAT_NO_ATTRS + 6; i++) {
    //     attrCatBuffer.getRecord(attrCatRecord, i);
    //     attrCacheEntry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    //     AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(attrCacheEntry->attrCatEntry));
    //     if(!head) {
    //         prev = attrCacheEntry;
    //         head = attrCacheEntry;
    //     }
    //     prev->next = attrCacheEntry;
    //     prev = attrCacheEntry;
    // }

    // attrCacheEntry->next = nullptr;
    // AttrCacheTable::attrCache[ATTRCAT_RELID] = head;
}
void freeLinkedList(AttrCacheEntry** head) {
    if (!head || !*head) return;  
    
    AttrCacheEntry* current = *head;
    while (current) {
        AttrCacheEntry* nextNode = current->next;  
        free(current);  
        current = nextNode; 
    }
    
    *head = nullptr;
}


OpenRelTable::~OpenRelTable() {
    for(int i = RELCAT_RELID; i <= ATTRCAT_RELID; i++) {
        freeLinkedList(&AttrCacheTable::attrCache[i]);
    }
}