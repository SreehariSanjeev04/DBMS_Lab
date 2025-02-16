#include "Algebra.h"

#include <cstring>
#include <iostream>

bool isNumber(char *str) {
    int len;
    float ignore;
    int ret = sscanf(str, "%f %n", &ignore, &len);
    return ret == 1 && len == strlen(str);
}

int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
    
    int srcRelId = OpenRelTable::getRelId(srcRel);
    if(srcRelId == E_RELNOTOPEN) {
        return E_RELNOTOPEN;
    }
    // convert the strval

    AttrCatEntry attrCatEntry;
    if(AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry) == E_ATTRNOTEXIST) {
        return E_ATTRNOTEXIST;
    }
    int type = attrCatEntry.attrType;
    Attribute attrVal;
    if(type == NUMBER) {
        if(isNumber(strVal)) {
            attrVal.nVal = atof(strVal);
        } else return E_ATTRTYPEMISMATCH;
    } else if(type == STRING) {
        strcpy(attrVal.sVal, strVal);
    }
    // reset the search index
    RelCacheTable::resetSearchIndex(srcRelId);
    // printing the attributes of the relation

    
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

    printf("|");
    for(int i = 0; i < relCatEntry.numAttrs; i++) {
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
        printf(" %s |", attrCatEntry.attrName);
    }
    printf("\n");

    // actual business
    while(true) {
        RecId searchRes = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);
        if(searchRes.block != -1 && searchRes.slot != -1) {
            
            AttrCatEntry attrCatEnt;

            RecBuffer blockBuffer(searchRes.block);
            HeadInfo header;
            blockBuffer.getHeader(&header);
            Attribute record[header.numAttrs];
            blockBuffer.getRecord(record, searchRes.slot);
            
            for(int i = 0; i < header.numAttrs; i++) {
                AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEnt);
                if(attrCatEnt.attrType == NUMBER) {
                    printf(" %d |", (int)record[i].nVal);
                } else {
                    printf(" %s |", record[i].sVal);
                }
            }
            printf("\n");
        } else {
            // all done 
            break;
        }
    }
    return SUCCESS;

}

int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE]){

    if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0 ){
      return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relName);

    if ( relId == E_RELNOTOPEN) {
      return E_RELNOTOPEN;
    }

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    if ( nAttrs != relCatEntry.numAttrs ){
      return E_NATTRMISMATCH;
    }

    Attribute recordValues[relCatEntry.numAttrs];

    
    for (int i = 0; i < nAttrs; i++ ){
      AttrCatEntry attrCatEntry;
      AttrCacheTable::getAttrCatEntry(relId, i, &attrCatEntry);

      int type = attrCatEntry.attrType;
      if (type == NUMBER){
        if ( isNumber(record[i]) ){
          recordValues[i].nVal = atof(record[i]);
        }
        else return E_ATTRTYPEMISMATCH;
      }
      else if (type == STRING){
        strcpy(recordValues[i].sVal, record[i]);
      }
    }

    int retVal = BlockAccess::insert(relId, recordValues);
    return retVal;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE]) {
    int srcRelId = OpenRelTable::getRelId(srcRel);
    if(srcRelId < 0 || srcRelId >= MAX_OPEN) {
        return E_RELNOTOPEN;
    }
    RelCatEntry srcRelCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &srcRelCatEntry);
    int numAttributes = srcRelCatEntry.numAttrs;
    char attrNames[numAttributes][ATTR_SIZE];
    int attrTypes[numAttributes];

    for(int i = 0; i < numAttributes; i++) {
        AttrCatEntry srcAttrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &srcAttrCatEntry);
        strcpy(attrNames[i], srcAttrCatEntry.attrName);
        attrTypes[i] = srcAttrCatEntry.attrType;
    }

    int ret = Schema::createRel(targetRel, numAttributes, attrNames, attrTypes);
    if(ret != SUCCESS) return ret;
    
    int targetRelId = OpenRelTable::openRel(targetRel);
    if(targetRelId < 0 || targetRelId >= MAX_OPEN) return targetRelId;

    RelCacheTable::resetSearchIndex(srcRelId);
    
    Attribute record[numAttributes];
    
    while(BlockAccess::project(srcRelId, record) == SUCCESS) {
        ret = BlockAccess::insert(targetRelId, record);
        
        if(ret != SUCCESS) {
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    Schema::closeRel(targetRel);
    return SUCCESS;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], int tar_nAttrs, char tar_Attrs[][ATTR_SIZE]) {
    
    int srcRelId = OpenRelTable::getRelId(srcRel);
    if(srcRelId < 0 || srcRelId >= MAX_OPEN) return E_RELNOTOPEN;

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);
    int numAttrs = relCatEntry.numAttrs;

    int attr_offset[tar_nAttrs];
    int attr_types[tar_nAttrs];
    
    for(int i = 0; i < tar_nAttrs; i++) {
        AttrCatEntry attrCatEntry;
        int ret = AttrCacheTable::getAttrCatEntry(srcRelId, tar_Attrs[i], &attrCatEntry);
        if(ret == E_ATTRNOTEXIST) {
            return E_ATTRNOTEXIST;
        }
        attr_offset[i] = attrCatEntry.offset;
        attr_types[i] = attrCatEntry.attrType;
    }

    int ret = Schema::createRel(targetRel, tar_nAttrs, tar_Attrs, attr_types);
    
    if(ret != SUCCESS) return ret;
    
    int targetRelId = OpenRelTable::openRel(targetRel);

    if(targetRelId < 0) {
        Schema::deleteRel(targetRel);
        return targetRelId;
    }

    RelCacheTable::resetSearchIndex(srcRelId);

    Attribute record[numAttrs];

    while(BlockAccess::project(srcRelId, record) == SUCCESS) {
        Attribute proj_record[tar_nAttrs];
        for(int i = 0; i < tar_nAttrs; i++) {
            proj_record[i] = record[attr_offset[i]];
        }

        ret = BlockAccess::insert(targetRelId, proj_record);
        if(ret != SUCCESS) {
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);

            return ret;
        }
    }
    Schema::closeRel(targetRel);
    return SUCCESS;
}

// int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
//     int srcRelId = OpenRelTable::getRelId(srcRel);
//     if(srcRelId == E_RELNOTOPEN) {
//         return E_RELNOTOPEN;
//     }

//     AttrCatEntry attrCatEntry;
//     if(AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry) == E_ATTRNOTEXIST) {
//         return E_ATTRNOTEXIST;
//     };

//     int type = attrCatEntry.attrType;
//     Attribute attrVal;
//     if(type == NUMBER) {
//         if(isNumber(strVal)) {
//             attrVal.nVal = atof(strVal);
//         } else E_ATTRTYPEMISMATCH;
//     } else if(type == STRING) {
//         strcpy(attrVal.sVal, strVal);
//     }

//     RelCacheTable::resetSearchIndex(srcRelId);

//     RelCatEntry relCatEntry;
//     RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

//     printf("|");

//     while(true) {
//         RecId searchRes = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);
//         if(searchRes.block != -1 && searchRes.slot != -1) {
//             AttrCatEntry attrCatEntry;
//             RecBuffer BlockBuffer(searchRes.block);
//             HeadInfo blockHeader;
//             BlockBuffer.getHeader(&blockHeader);

//             Attribute recordBuffer[blockHeader.numAttrs];
//             BlockBuffer.getRecord(recordBuffer, searchRes.slot);

//             for(int i = 0; i < blockHeader.numAttrs; i++) {
//                 AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
//                 if(attrCatEntry.attrType == NUMBER) {
//                     printf(" %d |", (int)recordBuffer[i].nVal);
//                 } else {
//                     printf(" %s |", recordBuffer[i].sVal);
//                 }
//             }
//             printf("\n");
//         } else {
//             break;
//         }
//         return SUCCESS;
//     }
    
// }

