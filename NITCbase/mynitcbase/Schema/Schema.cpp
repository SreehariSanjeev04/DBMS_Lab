#include "Schema.h"

#include <cmath>
#include <cstring>
#include <cstdio>

int Schema::openRel(char relName[ATTR_SIZE])
{
    int ret = OpenRelTable::openRel(relName);

    if (ret >= 0)
    {
        return SUCCESS;
    }

    return ret;
}

int Schema::closeRel(char relName[ATTR_SIZE])
{
    if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
    {
        return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relName);

    if (relId == E_RELNOTOPEN)
    {
        return E_RELNOTOPEN;
    }

    return OpenRelTable::closeRel(relId);
}

int Schema::renameRel(char oldRelName[ATTR_SIZE], char newRelName[ATTR_SIZE])
{
    if (strcmp(oldRelName, RELCAT_RELNAME) == 0 || strcmp(oldRelName, ATTRCAT_RELNAME) == 0 || strcmp(newRelName, RELCAT_RELNAME) == 0 || strcmp(newRelName, ATTRCAT_RELNAME) == 0)
    {
        return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(oldRelName);
    if (relId != E_RELNOTOPEN)
    {
        return E_RELOPEN;
    }

    int retVal = BlockAccess::renameRelation(oldRelName, newRelName);
    return retVal;
}

int Schema::renameAttr(char *relName, char *oldAttrName, char *newAttrName)
{

    if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
    {
        return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relName);
    if (relId != E_RELNOTOPEN)
    {
        return E_RELOPEN;
    }

    int retVal = BlockAccess::renameAttribute(relName, oldAttrName, newAttrName);
    return retVal;
}

int Schema::createRel(char relName[], int nAttrs, char attrs[][ATTR_SIZE], int attrtype[])
{

    Attribute relNameAsAttribute;
    strcpy(relNameAsAttribute.sVal, relName);
    char relnameAttrRelcat[] = RELCAT_ATTR_RELNAME;

    RecId targetRecId;
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    targetRecId = BlockAccess::linearSearch(RELCAT_RELID, relnameAttrRelcat, relNameAsAttribute, EQ);

    if (targetRecId.slot != -1 || targetRecId.block != -1)
        return E_RELEXIST;

    for (int i = 0; i < nAttrs; i++)
    {
        for (int j = 0; j < nAttrs; j++)
        {
            if (i != j && strcmp(attrs[i], attrs[j]) == 0)
                return E_DUPLICATEATTR;
        }
    }

    Attribute relCatRecord[RELCAT_NO_ATTRS];
    strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, relName);
    relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal = nAttrs;
    relCatRecord[RELCAT_NO_RECORDS_INDEX].nVal = 0;
    relCatRecord[RELCAT_FIRST_BLOCK_INDEX].nVal = -1;
    relCatRecord[RELCAT_LAST_BLOCK_INDEX].nVal = -1;
    relCatRecord[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = floor((2016 / (16 * nAttrs + 1)));

    int retVal = BlockAccess::insert(RELCAT_RELID, relCatRecord);
    if (retVal != SUCCESS)
        return retVal;

    for (int i = 0; i < nAttrs; i++)
    {

        Attribute attrCatRecord[RELCAT_NO_ATTRS];
        strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relName);
        strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrs[i]);
        attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrtype[i];
        attrCatRecord[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = -1;
        attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal = -1;
        attrCatRecord[ATTRCAT_OFFSET_INDEX].nVal = i;

        int retVal = BlockAccess::insert(ATTRCAT_RELID, attrCatRecord);
        if (retVal != SUCCESS)
        {
            Schema::deleteRel(relName);
            return E_DISKFULL;
        }
    }

    return SUCCESS;
}

int Schema::deleteRel(char *relName)
{
    if (strcmp(relName, ATTRCAT_RELNAME) == 0 || strcmp(relName, RELCAT_RELNAME) == 0)
        return E_NOTPERMITTED;

    int relId = OpenRelTable::getRelId(relName);
    if (relId != E_RELNOTOPEN)
    {
        return E_RELOPEN;
    }

    int ret = BlockAccess::deleteRelation(relName);

    return ret;

}

int Schema::createIndex(char relName[ATTR_SIZE], char attrName[ATTR_SIZE]) {
    if(strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0) {
        return E_NOTPERMITTED;
    }
    int relId = OpenRelTable::getRelId(relName);
    if(relId == E_RELNOTOPEN) {
        return E_RELNOTOPEN;
    }
    return BPlusTree::bPlusCreate(relId, attrName);
}

int Schema::dropIndex(char *relName, char *attrName) {
    if(strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0) {
        return E_NOTPERMITTED;
    }
    int relId = OpenRelTable::getRelId(relName);
    if(relId == E_RELNOTOPEN) {
        return E_RELNOTOPEN;
    }
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if(ret != SUCCESS) return E_ATTRNOTEXIST;

    int rootBlock = attrCatEntry.rootBlock;
    if(rootBlock == -1) return E_NOINDEX;

    BPlusTree::bPlusDestroy(rootBlock);

    attrCatEntry.rootBlock = -1;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);
    return SUCCESS;
}
