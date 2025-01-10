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

