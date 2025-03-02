#include "AttrCacheTable.h"

#include <cstring>
#include <iostream>

AttrCacheEntry *AttrCacheTable::attrCache[MAX_OPEN];

int AttrCacheTable::getAttrCatEntry(int relId, int attrOffset, AttrCatEntry *attrCatBuf)
{
    if (relId < 0 || relId > MAX_OPEN)
    {
        return E_OUTOFBOUND;
    }

    if (attrCache[relId] == nullptr)
    {
        return E_RELNOTOPEN;
    }
    for (AttrCacheEntry *entry = attrCache[relId]; entry != nullptr; entry = entry->next)
    {
        if (entry->attrCatEntry.offset == attrOffset)
        {
            *attrCatBuf = entry->attrCatEntry;
            return SUCCESS;
        }
    }
    return E_ATTRNOTEXIST;
}

int AttrCacheTable::getAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry *attrCatEntry)
{
    if (relId < 0 || relId > MAX_OPEN)
    {
        return E_OUTOFBOUND;
    }

    if (attrCache[relId] == nullptr)
    {
        return E_RELNOTOPEN;
    }
    AttrCacheEntry *ptr = attrCache[relId];
    while (ptr)
    {
        if (strcmp(ptr->attrCatEntry.attrName, attrName) == 0)
        {
            *(attrCatEntry) = ptr->attrCatEntry;
            return SUCCESS;
        }
        ptr = ptr->next;
    }
    return E_ATTRNOTEXIST;
}

void AttrCacheTable::recordToAttrCatEntry(union Attribute record[ATTRCAT_NO_ATTRS],
                                          AttrCatEntry *AttrCatEntry)
{
    strcpy(AttrCatEntry->relName, record[ATTRCAT_REL_NAME_INDEX].sVal);
    strcpy(AttrCatEntry->attrName, record[ATTRCAT_ATTR_NAME_INDEX].sVal);
    AttrCatEntry->offset = (int)record[ATTRCAT_OFFSET_INDEX].nVal;
    AttrCatEntry->attrType = (int)record[ATTRCAT_ATTR_TYPE_INDEX].nVal;
    AttrCatEntry->rootBlock = (int)record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
    AttrCatEntry->primaryFlag = (bool)record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal;
}

int AttrCacheTable::setAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry* attrCatBuf) {
    if(relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }
    if(attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }
    AttrCacheEntry* attrCacheHead = attrCache[relId];
    while(attrCacheHead) {
        if(strcmp(attrCacheHead->attrCatEntry.attrName, attrName) == 0) {
            attrCacheHead->attrCatEntry = *attrCatBuf;
            attrCacheHead->dirty = true;
            return SUCCESS;
        }
        attrCacheHead = attrCacheHead->next;
    }
    return E_ATTRNOTEXIST;
}
int AttrCacheTable::setAttrCatEntry(int relId, int attrOffset, AttrCatEntry* attrCatBuf) {
    if(relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }
    if(attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }
    AttrCacheEntry* attrCacheHead = attrCache[relId];
    while(attrCacheHead) {
        if(attrCacheHead->attrCatEntry.offset == attrOffset) {
            attrCacheHead->attrCatEntry = *attrCatBuf;
            attrCacheHead->dirty = true;
            return SUCCESS;
        }
        attrCacheHead = attrCacheHead->next;
        
    }
    return E_ATTRNOTEXIST;
}

void AttrCacheTable::attrCatEntryToRecord(AttrCatEntry *attrCatEntry, union Attribute record[ATTRCAT_NO_ATTRS]){
    strcpy(record[ATTRCAT_REL_NAME_INDEX].sVal, attrCatEntry->relName);
    strcpy(record[ATTRCAT_ATTR_NAME_INDEX].sVal, attrCatEntry->attrName);
    record[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrCatEntry->attrType;
    record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = attrCatEntry->primaryFlag;
    record[ATTRCAT_ROOT_BLOCK_INDEX].nVal = attrCatEntry->rootBlock;
    record[ATTRCAT_OFFSET_INDEX].nVal = attrCatEntry->offset;
  }
int AttrCacheTable::getSearchIndex(int relId, char attrName[ATTR_SIZE], IndexId *searchIndex)
{
    if (relId < 0 || relId >= MAX_OPEN)
    {
        return E_OUTOFBOUND;
    }

    if (attrCache[relId] == nullptr)
    {
        return E_RELNOTOPEN;
    }
    AttrCacheEntry *attrCacheEntry = AttrCacheTable::attrCache[relId];
    while (attrCacheEntry)
    {
        if (strcmp(attrCacheEntry->attrCatEntry.attrName, attrName) == 0)
        {
            *searchIndex = attrCacheEntry->searchIndex;
            return SUCCESS;
        }
        attrCacheEntry = attrCacheEntry->next;
    }
    return E_ATTRNOTEXIST;
}

int AttrCacheTable::getSearchIndex(int relId, int attrOffset, IndexId *searchIndex)
{
    if (relId < 0 || relId >= MAX_OPEN)
    {
        return E_OUTOFBOUND;
    }
    if (attrCache[relId] == nullptr)
    {
        return E_RELNOTOPEN;
    }
    AttrCacheEntry *attrCacheEntry = attrCache[relId];
    while (attrCacheEntry)
    {
        if (attrOffset == attrCacheEntry->attrCatEntry.offset)
        {
            *searchIndex = attrCacheEntry->searchIndex;
            return SUCCESS;
        }
        attrCacheEntry = attrCacheEntry->next;
    }
    return E_ATTRNOTEXIST;
}

int AttrCacheTable::setSearchIndex(int relId, char attrName[ATTR_SIZE], IndexId* searchIndex) {
    if(relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }
    if(attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }
    AttrCacheEntry *current = attrCache[relId];
    while(current) {
        if(strcmp(current->attrCatEntry.attrName, attrName) == 0) {
            current->searchIndex = *searchIndex;
            return SUCCESS;
        }
        current = current->next;
    }
    return E_ATTRNOTEXIST;
}
int AttrCacheTable::setSearchIndex(int relId, int attrOffset, IndexId* searchIndex) {
    if(relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }
    if(attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }
    AttrCacheEntry *current = attrCache[relId];
    while(current) {
        if(current->attrCatEntry.offset == attrOffset) {
            current->searchIndex = *searchIndex;
            return SUCCESS;
        }
        current = current->next;
    }
    return E_ATTRNOTEXIST;
}

int AttrCacheTable::resetSearchIndex(int relId, int attrOffset) {
    IndexId IndexId = {-1,-1};
    return AttrCacheTable::setSearchIndex(relId, attrOffset, &IndexId);
}

int AttrCacheTable::resetSearchIndex(int relId, char attrName[ATTR_SIZE]) {
    IndexId IndexId = {-1,-1};
    return AttrCacheTable::setSearchIndex(relId, attrName, &IndexId);
}