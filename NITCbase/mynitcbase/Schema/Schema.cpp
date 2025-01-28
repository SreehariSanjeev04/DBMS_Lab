#include "Schema.h"

#include <cmath>
#include <cstring>
#include <iostream>

int Schema::openRel(char relName[ATTR_SIZE]) {
    int ret = OpenRelTable::openRel(relName);

    if(ret >= 0) {
        return SUCCESS;
    }

    return ret;
}

int Schema::closeRel(char relName[ATTR_SIZE]) {
    if(strcmp(relName, ATTRCAT_RELNAME) == 0 || strcmp(relName, RELCAT_RELNAME)) {
        E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relName);

    if(relId == E_RELNOTOPEN) {
        return E_RELNOTOPEN;
    }

    return OpenRelTable::closeRel(relId);
}

int Schema::renameRel(char oldRelName[ATTR_SIZE], char newRelName[ATTR_SIZE]) {
    if(strcmp(oldRelName, RELCAT_RELNAME) == 0 ||
        strcmp(oldRelName, ATTRCAT_RELNAME) == 0 ||
        strcmp(newRelName, RELCAT_RELNAME) == 0 ||
        strcmp(newRelName, ATTRCAT_RELNAME) == 0) {
            return E_NOTPERMITTED;
        }

    int ret = OpenRelTable::getRelId(oldRelName);
    if(ret != E_RELNOTOPEN) {
        return E_RELOPEN;
    } 
    int retVal = BlockAccess::renameRelation(oldRelName, newRelName);
    return retVal;
}

int Schema::renameAttr(char *relName, char *oldAttrName, char *newAttrName) {
    if(strcmp(relName, RELCAT_RELNAME) == 0 ||
        strcmp(relName, RELCAT_RELNAME) == 0) {
            return E_NOTPERMITTED;
        }
    int ret = OpenRelTable::getRelId(relName);
    if(ret != E_RELNOTOPEN) {
        return E_RELOPEN;
    } 
    return BlockAccess::renameAttribute(relName, oldAttrName, newAttrName);
    
}

int Schema::createRel(char relname[], int nAttrs, char attrs[][ATTR_SIZE], int attrType[]) {
    Attribute relNameAsAttribute;
    strcpy(relNameAsAttribute.sVal, relname);
    
    RecId targetRelId = {-1,-1};
    int relId = OpenRelTable::getRelId(relname);
    RelCacheTable::resetSearchIndex(relId);
    targetRelId = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, relNameAsAttribute, EQ);

    if(targetRelId.block != -1 && targetRelId.slot != -1) {
        return E_RELEXIST;
    }

    // checking for duplicate attributes
    for(int i = 0; i < nAttrs; i++) {
        for(int j = i+1; j < nAttrs; j++) {
            if(strcmp(attrs[i], attrs[j]) == 0) return E_DUPLICATEATTR;
        }
    }

    Attribute relCatRecord[RELCAT_NO_ATTRS];

    // filling the relCatRecord

    strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, relname);
    relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal = nAttrs;
    relCatRecord[RELCAT_NO_RECORDS_INDEX].nVal = 0;
    relCatRecord[RELCAT_FIRST_BLOCK_INDEX].nVal = -1;
    relCatRecord[RELCAT_LAST_BLOCK_INDEX].nVal = -1;
    relCatRecord[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = floor((2016) / (16 * nAttrs + 1));

    int retVal = BlockAccess::insert(RELCAT_RELID, relCatRecord);

    if(retVal != SUCCESS) {
        return retVal;
    }
    
    // filling the attribute catalog
    for(int i = 0; i < nAttrs; i++) {
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relname);
        strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrs[i]);
        attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrType[i];
        attrCatRecord[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = -1;
        attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal = -1;
        attrCatRecord[ATTRCAT_OFFSET_INDEX].nVal = i;

        retVal = BlockAccess::insert(ATTRCAT_RELID, attrCatRecord);
        if(retVal != SUCCESS) {
            // delete the relation
            return E_DISKFULL;
        }
    }
    return SUCCESS;
}

int Schema::deleteRel(char* relName) {
    if(strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0) {
        return E_NOTPERMITTED;
    }
    int relId = OpenRelTable::getRelId(relName);

    if(relId != E_RELNOTOPEN) {
        return E_RELOPEN;
    }

    int ret = BlockAccess::deleteRelation(relName);
    
    return ret;
}