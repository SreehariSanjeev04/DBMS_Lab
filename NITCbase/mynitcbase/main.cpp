#include <cstring>
#include <iostream>
#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
using namespace std;

void displayRelations() {
  /*
    Instiantiating relCatBuffer and AttrCatBuffer with RecBuffer
  */

  RecBuffer relCatBuffer(RELCAT_BLOCK); // index of the start of relation catalog buffer
  RecBuffer attrCatBuffer(ATTRCAT_BLOCK);

  /*
    Getting the headers from the buffers
  */

  HeadInfo relCatHeader;
  HeadInfo attrCatHeader;
  
  /* Loading the header data*/

  relCatBuffer.getHeader(&relCatHeader);
  attrCatBuffer.getHeader(&attrCatHeader);

  for(int i = 0; i < relCatHeader.numEntries; i++) {
    /* Loading the relation one by one */
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    relCatBuffer.getRecord(relCatRecord, i);

    printf("Relation: %s\n", relCatRecord[RELCAT_REL_NAME_INDEX].sVal);

    for(int j = 0, attrIndex = 0; j < attrCatHeader.numEntries; j++) {
      /* Loading the attribute one by one and checking the relation name*/
      Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
      attrCatBuffer.getRecord(attrCatRecord, j);

      if(strcmp(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal) == 0) {
        const char *attrType = attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER? "NUM" : "STR"; // If number value is 0
        printf(" %s: %s\n", attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrType);
      }
      if(attrIndex == attrCatHeader.numSlots - 1) {
        attrIndex = -1;
        attrCatBuffer = RecBuffer(attrCatHeader.rblock);
        attrCatBuffer.getHeader(&attrCatHeader);
      }
    }
  }
}

void updateAttributeName(const char* relName, const char* oldAttrName, const char* newAttrName) {
  
  RecBuffer relCatBuffer(RELCAT_BLOCK);
  RecBuffer attrCatBuffer(ATTRCAT_BLOCK);

  HeadInfo relCatHeader;
  HeadInfo attrCatHeader;

  relCatBuffer.getHeader(&relCatHeader);
  attrCatBuffer.getHeader(&attrCatHeader);

  for(int i = 0, attrIndex = 0; i < attrCatHeader.numEntries; i++) {
    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    attrCatBuffer.getRecord(attrCatRecord, i);

    if(strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relName) == 0 && 
    strcmp(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldAttrName) == 0) {
      strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newAttrName);
      if(attrCatBuffer.setRecord(attrCatRecord, i)) {
        printf("Attribute name updated successfully\n");
      };
      break;
    }
    if(attrIndex == attrCatHeader.numSlots - 1) {
      attrIndex = -1;
      attrCatBuffer = RecBuffer(attrCatHeader.rblock);
      attrCatBuffer.getHeader(&attrCatHeader);
    }
  }
}

int main(int argc, char *argv[]) {
  
  Disk disk_run;
  updateAttributeName ("Students", "Class", "Batch");
  displayRelations();
  // return FrontendInterface::handleFrontend(argc, argv);
  return 0;
}