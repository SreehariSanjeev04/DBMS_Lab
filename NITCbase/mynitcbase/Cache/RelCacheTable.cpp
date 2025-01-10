#include "RelCacheTable.h"

#include <cstring>
#include <iostream>

RelCacheEntry* RelCacheTable::relCache[MAX_OPEN];

int RelCacheTable::getRelCatEntry(int relId, RelCatEntry* relCatBuf) {
    if(relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if(relCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    *relCatBuf = relCache[relId]->relCatEntry;

    return SUCCESS;
}

int RelCacheTable::getSearchIndex(int recId, RecId* searchIndex) {
    if(recId < 0 || recId > MAX_OPEN) {
        return E_OUTOFBOUND;
    }
    if(relCache[recId] == nullptr) {
        return E_RELNOTOPEN;
    }

    *searchIndex = relCache[recId]->searchIndex;
    return SUCCESS;
}

void RelCacheTable::recordToRelCatEntry(union Attribute record[RELCAT_NO_ATTRS],
RelCatEntry* relCatEntry) {
    strcpy(relCatEntry->relName, record[RELCAT_REL_NAME_INDEX].sVal);
    relCatEntry->numAttrs = (int)record[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
    relCatEntry->numRecs = (int)record[RELCAT_NO_RECORDS_INDEX].nVal;
    relCatEntry->firstBlk = (int)record[RELCAT_FIRST_BLOCK_INDEX].nVal;
    relCatEntry->lastBlk = (int)record[RELCAT_LAST_BLOCK_INDEX].nVal;
    relCatEntry->numSlotsPerBlk = (int)record[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal;
}

int RelCacheTable::setSearchIndex(int recId, RecId* searchIndex) {
    if(recId < 0 || recId > MAX_OPEN) {
        return E_OUTOFBOUND;
    }
    if(relCache[recId] == nullptr) {
        return E_RELNOTOPEN;
    }
    
    relCache[recId]->searchIndex = *searchIndex;
    return SUCCESS;
}


int RelCacheTable::resetSearchIndex(int recId) {
    if(recId < 0 || recId > MAX_OPEN) {
        return E_OUTOFBOUND;
    }
    if(relCache[recId] == nullptr) {
        return E_RELNOTOPEN;
    }

    RelCacheTable::relCache[recId]->searchIndex = {-1, -1};
    return SUCCESS;
}
