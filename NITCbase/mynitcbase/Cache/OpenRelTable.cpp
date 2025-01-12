#include "OpenRelTable.h"

#include <cstring>
#include <stdlib.h>
#include <iostream>
OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];
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

OpenRelTable::OpenRelTable() {
    for(int i = 0; i < MAX_OPEN; i++) {
        tableMetaInfo[i].free = true;
        RelCacheTable::relCache[i] = nullptr;
        AttrCacheTable::attrCache[i] = nullptr;
    }
    
    RecBuffer relCatBuffer(RELCAT_BLOCK);
    Attribute relCatRecord[RELCAT_NO_ATTRS];

    HeadInfo relCatHeader;
    relCatBuffer.getHeader(&relCatHeader);
    
    for(int i = 0; i <= ATTRCAT_RELID; i++) {
        relCatBuffer.getRecord(relCatRecord, i); 
        RelCacheEntry relCacheEntry;
        RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
        relCacheEntry.recId.block = RELCAT_BLOCK;
        relCacheEntry.recId.slot = i;
        relCacheEntry.searchIndex = {-1, -1};

        RelCacheTable::relCache[i] = (RelCacheEntry*)malloc(sizeof(RelCacheEntry));
        *(RelCacheTable::relCache[i]) = relCacheEntry;
    }

    RecBuffer attrCatBuffer(ATTRCAT_BLOCK);
    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    AttrCacheEntry* attrCacheEntry = nullptr;
    AttrCacheEntry* head = nullptr;
    AttrCacheEntry* prev = nullptr;

   for(int i = RELCAT_RELID, attrSlotIndex = 0; i <= ATTRCAT_RELID ; i++) {
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

    tableMetaInfo[RELCAT_RELID].free = false;
    tableMetaInfo[ATTRCAT_RELID].free = false;
    strcpy(tableMetaInfo[RELCAT_RELID].relName, "RELATIONCAT");
    strcpy(tableMetaInfo[ATTRCAT_RELID].relName, "ATTRIBUTECAT");
}

OpenRelTable::~OpenRelTable() {
    for(int i = 2; i < MAX_OPEN; i++) {
        if(tableMetaInfo[i].free == false) {
            closeRel(i);
        }
    }
    // free the cache for rel id 0 and 1
    free(RelCacheTable::relCache[0]);
    free(RelCacheTable::relCache[1]);

    freeLinkedList(&AttrCacheTable::attrCache[0]);
    freeLinkedList(&AttrCacheTable::attrCache[1]);
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {
  for (int i = 0; i < MAX_OPEN; i++) {
    if (strcmp(relName, tableMetaInfo[i].relName) == 0 and tableMetaInfo[i].free == false) {
      return i;
    }
  }
  return E_RELNOTOPEN;
}

int OpenRelTable::getFreeOpenRelTableEntry() {
    for(int i = 0; i < MAX_OPEN; i++) {
        if(tableMetaInfo[i].free == true) {
            return i;
        }
    }
    return E_CACHEFULL;
}


int OpenRelTable::openRel(char relName[ATTR_SIZE]) {
    int relCheck = getRelId(relName);
    if(relCheck != E_RELNOTOPEN) {
        return relCheck;
    }
    
    int emptySlot = getFreeOpenRelTableEntry();
    if(emptySlot == E_CACHEFULL) {
        return E_CACHEFULL;
    }

    int relId = emptySlot;
    // Setting up Relation Cache Entry of the relation
    // first search for the relation entry in the relation catalog

    Attribute attrVal;
    strcpy(attrVal.sVal, relName);

    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    RecId searchIndex = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, attrVal, EQ);

    if(searchIndex.block == -1 && searchIndex.slot == -1) {
        return E_RELNOTEXIST;
    }

    // loading the records with block and slot into relation catalog

    RecBuffer relationBuffer(searchIndex.block);
    Attribute record[RELCAT_NO_ATTRS];
    relationBuffer.getRecord(record, searchIndex.slot);

    RelCacheEntry *relCacheEntry = (RelCacheEntry*)malloc(sizeof(RelCacheEntry));
    RelCacheTable::recordToRelCatEntry(record, &relCacheEntry->relCatEntry);

    relCacheEntry->recId = searchIndex;
    RelCacheTable::relCache[relId] = relCacheEntry;

    // loading the attributes to the attribute cache

    Attribute attrRecord[ATTRCAT_NO_ATTRS];
    AttrCacheEntry* prev = nullptr;
    AttrCacheEntry* head = nullptr;

    int numberOfAttributes = RelCacheTable::relCache[relId]->relCatEntry.numAttrs;

    for(int i = 0; i < numberOfAttributes; i++) {
        RecId attributeSearchIndex = BlockAccess::linearSearch(ATTRCAT_RELID, (char*)RELCAT_ATTR_RELNAME, attrVal, EQ);

        RecBuffer attrCatBlock = RecBuffer(attributeSearchIndex.block);
        attrCatBlock.getRecord(attrRecord, attributeSearchIndex.slot);
        AttrCacheEntry* attrCacheEntry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
        AttrCacheTable::recordToAttrCatEntry(attrRecord, &(attrCacheEntry->attrCatEntry));
        attrCacheEntry->recId = attributeSearchIndex;
        
        if(!head) {
            head = attrCacheEntry;
        } else {
            prev->next = attrCacheEntry;
        }
        prev = attrCacheEntry;
    }
    prev->next = nullptr;
    AttrCacheTable::attrCache[relId] = head;

    //update the metainfoa
    tableMetaInfo[relId].free = false;
    strcpy(tableMetaInfo[relId].relName, relName);
    
    // debugging


    return relId;
}

int OpenRelTable::closeRel(int relId) {
    if(relId == RELCAT_RELID || relId == ATTRCAT_RELID) {
        return E_NOTPERMITTED;
    }
    if(relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }
    if(tableMetaInfo[relId].free == true) {
        return E_RELNOTOPEN;
    }

    // releasing the relation cache entry of the relation
    free(RelCacheTable::relCache[relId]);
    freeLinkedList(&AttrCacheTable::attrCache[relId]);

    //update the metainfo

    tableMetaInfo[relId].free = true;
    strcpy(tableMetaInfo[relId].relName, "");
    return SUCCESS;
}

