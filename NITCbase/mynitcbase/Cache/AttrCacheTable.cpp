#include "AttrCacheTable.h"

#include <cstring>
#include <iostream>

AttrCacheEntry* AttrCacheTable::attrCache[MAX_OPEN];

int AttrCacheTable::getAttrCatEntry(int relId, int attrOffset, AttrCatEntry* attrCatBuf) {
    if(relId < 0 || relId > MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if(attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }
    for(AttrCacheEntry* entry = attrCache[relId]; entry != nullptr; entry = entry->next) {
        if(entry->attrCatEntry.offset == attrOffset) {
            *attrCatBuf = entry->attrCatEntry;
            return SUCCESS;
        }
    }
    return E_ATTRNOTEXIST;
}

int AttrCacheTable::getAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry* attrCatEntry) {
    if(relId < 0 || relId > MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if(attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }
    for(int i = 0; i < MAX_OPEN; i++) {
        AttrCacheEntry* ptr = attrCache[i];
        while(ptr) {
            if(strcmp(ptr->attrCatEntry.attrName, attrName) == 0) {
                *(attrCatEntry) = ptr->attrCatEntry;
                return SUCCESS;
            }
            ptr = ptr->next;
        }
    }
    return E_ATTRNOTEXIST;
}

void AttrCacheTable::recordToAttrCatEntry(union Attribute record[ATTRCAT_NO_ATTRS],
    AttrCatEntry* AttrCatEntry) {
        strcpy(AttrCatEntry->relName, record[ATTRCAT_REL_NAME_INDEX].sVal);
        strcpy(AttrCatEntry->attrName, record[ATTRCAT_ATTR_NAME_INDEX].sVal);
        AttrCatEntry->offset = (int)record[ATTRCAT_OFFSET_INDEX].nVal;
        AttrCatEntry->attrType = (int)record[ATTRCAT_ATTR_TYPE_INDEX].nVal;
        AttrCatEntry->rootBlock= (int)record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
        AttrCatEntry->primaryFlag=(bool)record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal;
    }