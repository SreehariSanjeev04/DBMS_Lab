#include "Algebra.h"

#include <cstring>
#include <cstdlib>
#include <cstdio>
bool isNumber(char *str)
{
    int len;
    float ignore;
    /*
      sscanf returns the number of elements read, so if there is no float matching
      the first %f, ret will be 0, else it'll be 1

      %n gets the number of characters read. this scanf sequence will read the
      first float ignoring all the whitespace before and after. and the number of
      characters read that far will be stored in len. if len == strlen(str), then
      the string only contains a float with/without whitespace. else, there's other
      characters.
    */
    int ret = sscanf(str, "%f %n", &ignore, &len);
    return ret == 1 && len == strlen(str);
}

int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE])
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

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    if (nAttrs != relCatEntry.numAttrs)
    {
        return E_NATTRMISMATCH;
    }

    Attribute recordValues[relCatEntry.numAttrs];

    for (int i = 0; i < nAttrs; i++)
    {
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId, i, &attrCatEntry);

        int type = attrCatEntry.attrType;
        if (type == NUMBER)
        {

            if (isNumber(record[i]))
            {
                recordValues[i].nVal = atof(record[i]);
            }
            else
                return E_ATTRTYPEMISMATCH;
        }
        else if (type == STRING)
        {
            strcpy(recordValues[i].sVal, record[i]);
        }
    }

    int retVal = BlockAccess::insert(relId, recordValues);
    return retVal;
}

int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE])
{

    int srcRelId = OpenRelTable::getRelId(srcRel);
    if (srcRelId == E_RELNOTOPEN)
    {
        return E_RELNOTOPEN;
    }

    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);
    if (ret != SUCCESS)
    {
        return E_ATTRNOTEXIST;
    }

    int type = attrCatEntry.attrType;
    Attribute attrVal;
    if (type == NUMBER)
    {
        if (isNumber(strVal))
        {
            attrVal.nVal = atof(strVal);
        }
        else
        {
            return E_ATTRTYPEMISMATCH;
        }
    }
    else if (type == STRING)
    {
        strcpy(attrVal.sVal, strVal);
    }

    // printf("|");
    // for (int i = 0; i < relCatEntry.numAttrs; ++i) {
    //   AttrCatEntry attrCatEntry;
    //   AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
    //   printf(" %s |", attrCatEntry.attrName);
    // }
    // printf("\n");

    // while (true) {

    //   RecId searchRes = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);

    //   if (searchRes.block != -1 && searchRes.slot != -1) {

    //     // get the record at searchRes using BlockBuffer.getRecord
    //     RecBuffer block( searchRes.block );

    //     HeadInfo header;
    //     block.getHeader(&header);

    //     Attribute record[header.numAttrs];
    //     block.getRecord( record, searchRes.slot );
    //     // print the attribute values in the same format as above
    //     printf("|");
    //     for (int i = 0; i < relCatEntry.numAttrs; ++i)
    //     {
    //         AttrCatEntry attrCatEntry;
    //         // get attrCatEntry at offset i using AttrCacheTable::getAttrCatEntry()
    //         int response = AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
    //         if (response != SUCCESS)
    //         {
    //             printf("Invalid Attribute ID.\n");
    //             exit(1);
    //         }

    //         if (attrCatEntry.attrType == NUMBER)
    //         {
    //             printf(" %d |", (int) record[i].nVal);
    //         }
    //         else
    //         {
    //             printf(" %s |", record[i].sVal);
    //         }
    //     }
    //     printf("\n");
    //   }
    //   else {
    //     break;
    //   }
    // }

    RelCatEntry srcRelCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &srcRelCatEntry);
    int src_nAttrs = srcRelCatEntry.numAttrs;

    char attr_names[src_nAttrs][ATTR_SIZE];

    int attr_types[src_nAttrs];

    for (int i = 0; i < src_nAttrs; i++)
    {
        AttrCatEntry srcAttrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &srcAttrCatEntry);
        strcpy(attr_names[i], srcAttrCatEntry.attrName);
        attr_types[i] = srcAttrCatEntry.attrType;
    }

    ret = Schema::createRel(targetRel, src_nAttrs, attr_names, attr_types);
    if (ret != SUCCESS)
        return ret;

    int tarRelId = OpenRelTable::openRel(targetRel);

    if (tarRelId < 0)
        return tarRelId;

    Attribute record[src_nAttrs];

    RelCacheTable::resetSearchIndex(srcRelId);
    AttrCacheTable::resetSearchIndex(srcRelId, attr);

    while (BlockAccess::search(srcRelId, record, attr, attrVal, op) == SUCCESS)
    {

        int ret = BlockAccess::insert(tarRelId, record);
        if (ret != SUCCESS)
        {
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            printf("Failed to insert\n");
            return ret;
        }
    }

    Schema::closeRel(targetRel);
    return SUCCESS;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE])
{

    int ret;

    int srcRelId = OpenRelTable::getRelId(srcRel);
    if (srcRelId == E_RELNOTOPEN)
        return E_RELNOTOPEN;

    RelCatEntry srcRelCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &srcRelCatEntry);

    int numAttrs = srcRelCatEntry.numAttrs;

    char attrNames[numAttrs][ATTR_SIZE];
    int attrTypes[numAttrs];

    for (int i = 0; i < numAttrs; i++)
    {
        AttrCatEntry srcAttrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &srcAttrCatEntry);
        strcpy(attrNames[i], srcAttrCatEntry.attrName);
        attrTypes[i] = srcAttrCatEntry.attrType;
    }

    ret = Schema::createRel(targetRel, numAttrs, attrNames, attrTypes);
    if (ret != SUCCESS)
        return ret;

    int tarRelId = OpenRelTable::openRel(targetRel);

    if (tarRelId < 0)
    {
        Schema::deleteRel(targetRel);
        return tarRelId;
    }

    RelCacheTable::resetSearchIndex(srcRelId);
    Attribute record[numAttrs];

    while (BlockAccess::project(srcRelId, record) == SUCCESS)
    {
        ret = BlockAccess::insert(tarRelId, record);

        if (ret != SUCCESS)
        {
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }
    Schema::closeRel(targetRel);
    return SUCCESS;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], int tar_nAttrs, char tar_Attrs[][ATTR_SIZE])
{

    int ret;

    int srcRelId = OpenRelTable::getRelId(srcRel);
    if (srcRelId == E_RELNOTOPEN)
        return E_RELNOTOPEN;

    RelCatEntry srcRelCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &srcRelCatEntry);

    int src_nAttrs = srcRelCatEntry.numAttrs;

    int attr_offset[tar_nAttrs];
    int attr_types[tar_nAttrs];

    for (int i = 0; i < tar_nAttrs; i++)
    {
        AttrCatEntry tarAttrCatEntry;
        ret = AttrCacheTable::getAttrCatEntry(srcRelId, tar_Attrs[i], &tarAttrCatEntry);
        if (ret != SUCCESS)
            return E_ATTRNOTEXIST;
        attr_offset[i] = tarAttrCatEntry.offset;
        attr_types[i] = tarAttrCatEntry.attrType;
    }

    ret = Schema::createRel(targetRel, tar_nAttrs, tar_Attrs, attr_types);
    if (ret != SUCCESS)
        return ret;

    int tarRelId = OpenRelTable::openRel(targetRel);

    if (tarRelId < 0)
    {
        Schema::deleteRel(targetRel);
        return tarRelId;
    }

    RelCacheTable::resetSearchIndex(srcRelId);
    Attribute record[src_nAttrs];

    while (BlockAccess::project(srcRelId, record) == SUCCESS)
    {
        Attribute proj_record[tar_nAttrs];

        for (int i = 0; i < tar_nAttrs; i++)
        {
            proj_record[i] = record[attr_offset[i]]; // copying the values at each attribute for the insertion
        }

        ret = BlockAccess::insert(tarRelId, proj_record);

        if (ret != SUCCESS)
        {
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }
    Schema::closeRel(targetRel);
    return SUCCESS;
}

int Algebra::join(char srcRelation1[ATTR_SIZE], char srcRelation2[ATTR_SIZE], char targetRelation[ATTR_SIZE], char attribute1[ATTR_SIZE], char attribute2[ATTR_SIZE])
{
    int ret;
    int srcRelId_1 = OpenRelTable::getRelId(srcRelation1);
    int srcRelId_2 = OpenRelTable::getRelId(srcRelation2);

    if (srcRelId_1 == E_RELNOTOPEN || srcRelId_2 == E_RELNOTOPEN)
    {
        return E_RELNOTOPEN;
    }

    AttrCatEntry attrCatEntry1, attrCatEntry2;
    ret = AttrCacheTable::getAttrCatEntry(srcRelId_1, attribute1, &attrCatEntry1);
    if (ret != SUCCESS)
    {
        return E_ATTRNOTEXIST;
    }
    ret = AttrCacheTable::getAttrCatEntry(srcRelId_2, attribute2, &attrCatEntry2);
    if (ret != SUCCESS)
    {
        return E_ATTRNOTEXIST;
    }

    if(attrCatEntry1.attrType != attrCatEntry2.attrType) {
        return E_ATTRTYPEMISMATCH;
    }

    RelCatEntry relCatEntryBuf1, relCatEntryBuf2;

    RelCacheTable::getRelCatEntry(srcRelId_1, &relCatEntryBuf1);
    RelCacheTable::getRelCatEntry(srcRelId_2, &relCatEntryBuf2);

    int numOfAttributes_1 = relCatEntryBuf1.numAttrs;
    int numOfAttributes_2 = relCatEntryBuf2.numAttrs;

    for(int i = 0; i < numOfAttributes_1; i++) {
        AttrCatEntry attrCatEntry_1;
        AttrCacheTable::getAttrCatEntry(srcRelId_1, i, &attrCatEntry_1);
        
        if(strcmp(attrCatEntry_1.attrName, attribute1) == 0) continue;

        for(int j = 0; j < numOfAttributes_2; j++) {
            AttrCatEntry attrCatEntry_2;
            AttrCacheTable::getAttrCatEntry(srcRelId_2, j, &attrCatEntry_2);

            if(strcmp(attrCatEntry_2.attrName, attribute2) == 0) continue;

            if(strcmp(attrCatEntry_1.attrName, attrCatEntry_2.attrName) == 0) {
                return E_DUPLICATEATTR;
            }
        }
    }

    int rootBlock = attrCatEntry2.rootBlock;
    
    // creating B+ Tree for the 2nd relation
    if(rootBlock == -1) {
        int ret = BPlusTree::bPlusCreate(srcRelId_2, attribute2);
        if(ret == E_DISKFULL) return E_DISKFULL;
        
        rootBlock =attrCatEntry2.rootBlock;
    }

    int numOfAttributesInTarget = numOfAttributes_1 + numOfAttributes_2 - 1;
    char targetRelAttrNames[numOfAttributesInTarget][ATTR_SIZE];
    int targetRelAttrTypes[numOfAttributesInTarget];

    for(int i = 0; i < numOfAttributes_1; i++) {
        AttrCatEntry attrCatEntry;

        AttrCacheTable::getAttrCatEntry(srcRelId_1, i, &attrCatEntry);
        strcpy(targetRelAttrNames[i], attrCatEntry.attrName);
        targetRelAttrTypes[i] = attrCatEntry.attrType;
    }

    bool inserted = false;

    for(int i = 0; i < numOfAttributes_2; i++) {
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId_2, i, &attrCatEntry);

        if(strcmp(attrCatEntry.attrName, attribute2) == 0) {
            inserted = true;
            continue;
        }

        if(!inserted) {
            strcpy(targetRelAttrNames[numOfAttributes_1 + i], attrCatEntry.attrName);
            targetRelAttrTypes[numOfAttributes_1 + i] = attrCatEntry.attrType;
        } else {
            strcpy(targetRelAttrNames[numOfAttributes_1 + i - 1], attrCatEntry.attrName);
            targetRelAttrTypes[numOfAttributes_1 + i - 1] = attrCatEntry.attrType;
        }
    }

    ret = Schema::createRel(targetRelation, numOfAttributesInTarget, targetRelAttrNames, targetRelAttrTypes);

    if(ret != SUCCESS) return ret;

    int targetRelId = OpenRelTable::openRel(targetRelation);

    if(targetRelId < 0) {
        Schema::deleteRel(targetRelation);
        return targetRelId;
    }

    Attribute record1[numOfAttributes_1];
    Attribute record2[numOfAttributes_2];
    Attribute targetRecord[numOfAttributesInTarget];

    RelCacheTable::resetSearchIndex(srcRelId_1);

    while(BlockAccess::project(srcRelId_1, record1) == SUCCESS) {
        RelCacheTable::resetSearchIndex(srcRelId_2);
        AttrCacheTable::resetSearchIndex(srcRelId_2, attribute2);


        while(BlockAccess::search(srcRelId_2, record2, attribute2, record1[attrCatEntry1.offset], EQ) == SUCCESS) {
            
            for(int i = 0; i < numOfAttributes_1; i++) {
                targetRecord[i] = record1[i];
            }

            bool inserted = false;
            for(int i = 0; i < numOfAttributes_2; i++) {
                if(i == attrCatEntry2.offset) {
                    inserted = true;
                    continue;
                }

                if(!inserted) {
                    targetRecord[numOfAttributes_1 + i] = record2[i];
                } else {
                    targetRecord[numOfAttributes_1 + i - 1] = record2[i];
                }
            }

            ret = BlockAccess::insert(targetRelId, targetRecord);

            if(ret == E_DISKFULL) {
                ret = OpenRelTable::closeRel(targetRelId);
                
                ret = Schema::deleteRel(targetRelation);

                return E_DISKFULL;
            }
        }
    }

    return SUCCESS;
}
