#include "OpenRelTable.h"

#include <cstring>
#include <stdlib.h>

OpenRelTable::OpenRelTable() {
    /*
        RelCacheEntry - 
            relCatEntry -> header values
            dirty
            recId
            searchIndex

    */
    // initializing relCache and attrCache with null valueds
    for(int i = 0; i < MAX_OPEN; i++) {
        RelCacheTable::relCache[i] = nullptr;
        AttrCacheTable::attrCache[i] = nullptr;
    }

    // populating through relation catalog for the relation cache

    RecBuffer relCatBuffer(RELCAT_BLOCK);
    Attribute relCatRecord[RELCAT_NO_ATTRS];

    relCatBuffer.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

    RelCacheEntry relCacheEntry;
    RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
    relCacheEntry.recId.block = RELCAT_BLOCK;
    relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;

    RelCacheTable::relCache[RELCAT_RELID] = (RelCacheEntry*)malloc(sizeof(RelCacheEntry));
    *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;


    // setting up Attribute catalog relation in relation catalog table

    relCatBuffer.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);
    RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
    relCacheEntry.recId.block = RELCAT_BLOCK;
    relCacheEntry.recId.slot  = RELCAT_SLOTNUM_FOR_ATTRCAT;

    RelCacheTable::relCache[ATTRCAT_RELID] = (RelCacheEntry*)malloc(sizeof(RelCacheEntry));
    *(RelCacheTable::relCache[ATTRCAT_RELID]) = relCacheEntry;


    // Setting up the Attribute cache entries //
    RecBuffer attrCatBuffer(ATTRCAT_BLOCK);
    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    AttrCacheEntry* attrCacheEntry = nullptr;
    AttrCacheEntry* head = nullptr;
    AttrCacheEntry* prev = nullptr;
    AttrCacheEntry* attrHead = nullptr; 
    for(int i = 0; i < RELCAT_NO_ATTRS; i++) {
        attrCatBuffer.getRecord(attrCatRecord, i);
        attrCacheEntry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
        if(!head) {
            head = attrCacheEntry;
            prev = attrCacheEntry;
        }
        prev->next = attrCacheEntry;
        prev = attrCacheEntry;
    }
    attrCacheEntry->next = nullptr;

    AttrCacheTable::attrCache[RELCAT_RELID] = head;
    // head -> node 1 -> node 2 -> nullptr

    // Setting up Attribute Catalog relation in the Attribute Cache Table
    for(int i = 6; i < ATTRCAT_NO_ATTRS + 6; i++) {
        attrCatBuffer.getRecord(attrCatRecord, i);
        attrCacheEntry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
        if(!attrHead) {
            prev = attrCacheEntry;
            head = attrCacheEntry;
        }
        prev->next = attrCacheEntry;
        prev = attrCacheEntry;
    }

    attrCacheEntry->next = nullptr;
    AttrCacheTable::attrCache[ATTRCAT_RELID] = attrHead;

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