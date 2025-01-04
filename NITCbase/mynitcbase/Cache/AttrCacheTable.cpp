#include "AttrCacheTable.h"

#include <cstring>

AttrCacheEntry* AttrCacheTable::attrCache[MAX_OPEN];

int AttrCacheTable::getAttrCatEntry(int relId, int attrOffset, AttrCatEntry* attrCatBuf) {
    if(relId < 0 || relId > MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if(attrCache[relId] == nullptr) {
        E_RELNOTOPEN;
    }

    for(AttrCacheEntry* entry = attrCache[relId]; entry != nullptr; entry = entry->next) {
        if(entry->attrCatEntry.offset == attrOffset) {
            *attrCatBuf = attrCache[relId]->attrCatEntry;
            return SUCCESS;
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