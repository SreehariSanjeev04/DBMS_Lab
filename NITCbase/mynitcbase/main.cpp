#include <cstring>
#include <iostream>
#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
using namespace std;


int main(int argc, char *argv[]) {
  /* Initialize the Run Copy of Disk */
  
  Disk disk_run;
  RecBuffer relCatBuffer(RELCAT_BLOCK);
  RecBuffer attrCatBuffer(ATTRCAT_BLOCK);

  HeadInfo relCatHeader;
  HeadInfo attrCatHeader;

  /*
    Loading the headers into
  */
  relCatBuffer.getHeader(&relCatHeader);
  attrCatBuffer.getHeader(&attrCatHeader);
  printf("Num Entries: %d\n", relCatHeader.numEntries);
  for(int i = 0; i < relCatHeader.numEntries; i++) {
    Attribute relCatRecord[RELCAT_NO_ATTRS]; // Number of attributes in relation catalog block 6
    relCatBuffer.getRecord(relCatRecord, i);

    printf("Relation: %s\n", relCatRecord[RELCAT_REL_NAME_INDEX].sVal);

    // Loading the attributes

    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

    for(int j = 0; j < attrCatHeader.numEntries; j++) {
      attrCatBuffer.getRecord(attrCatRecord, j);

      if(strcmp(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal) == 0) {
        const char *attrType = attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER ? "NUM" : "STR";
        printf(" %s: %s\n", attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrType);
      }
    }

  }

  // return FrontendInterface::handleFrontend(argc, argv);
  return 0;
}