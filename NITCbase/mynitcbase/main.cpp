#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include <cstring>

// local headers
#include <iostream>


void printRelations() {
  RecBuffer relCatBuffer(RELCAT_BLOCK);
  RecBuffer attrCatBuffer(ATTRCAT_BLOCK);
  
  HeadInfo relCatHeader;
  HeadInfo attrCatHeader;
  
  relCatBuffer.getHeader(&relCatHeader);
  attrCatBuffer.getHeader(&attrCatHeader);

  for(int i = 0, attrCatSlotIndex = 0; i < relCatHeader.numEntries; i++) {
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    relCatBuffer.getRecord(relCatRecord, i);

    printf("RELATION: %s\n", relCatRecord[RELCAT_REL_NAME_INDEX].sVal);
    for(int j = 0; j < relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal; j++, attrCatSlotIndex++) {
      Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
      attrCatBuffer.getRecord(attrCatRecord, attrCatSlotIndex);
      if(strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relCatRecord[RELCAT_REL_NAME_INDEX].sVal) == 0) {
        const char* attrType = attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER ? "NUM" : "STR";
        printf(" %s %s\n", attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrType);
      }
      if(attrCatSlotIndex == attrCatHeader.numSlots - 1) {
        attrCatSlotIndex = -1;
        attrCatBuffer = RecBuffer(attrCatHeader.rblock);
        attrCatBuffer.getHeader(&attrCatHeader);
      }
    }
  }
  printf("\n");
}

void printRelationCache() {	
	RelCatEntry relCatEntry;
	for(int i = RELCAT_RELID; i <= ATTRCAT_RELID; i++) {
		if(RelCacheTable::getRelCatEntry(i, &relCatEntry) != SUCCESS) {
			printf("Relation Catalogue Not Found.\n");
		}
		printf("Relation: %s\n", relCatEntry.relName);
		for(int j = 0; j < relCatEntry.numAttrs; j++) {
			AttrCatEntry attrCatEntry;
			const int response = AttrCacheTable::getAttrCatEntry(i,j,&attrCatEntry);
			if(response != SUCCESS) {
				printf("Failed to fetch entries.\n");
			}
			const char* attrType = attrCatEntry.attrType == NUMBER ? "NUM" : "STR";
			printf(" %s %s\n", attrCatEntry.attrName, attrType);
		}
		printf("\n");
	}
	printf("\n");
}
void printSchema () {
	RecBuffer relCatBuffer(RELCAT_BLOCK);
	RecBuffer attrCatBuffer(ATTRCAT_BLOCK);

	HeadInfo relCatHeader;
	HeadInfo attrCatHeader;

	relCatBuffer.getHeader(&relCatHeader);
	attrCatBuffer.getHeader(&attrCatHeader);

	for (int i = 0, attrCatSlotIndex = 0; i < relCatHeader.numEntries; i++)
	{
		Attribute relCatRecord[RELCAT_NO_ATTRS]; 
		relCatBuffer.getRecord(relCatRecord, i);

		printf("Relation: %s\n", relCatRecord[RELCAT_REL_NAME_INDEX].sVal);

		int j = 0;
		for (; j < relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal; j++, attrCatSlotIndex++)
		{
			Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
			attrCatBuffer.getRecord(attrCatRecord, attrCatSlotIndex);

			if (strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal,
					   relCatRecord[RELCAT_REL_NAME_INDEX].sVal) == 0)
			{
				const char *attrType = attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER
										   ? "NUM" : "STR";
				printf("  %s: %s\n", attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrType);
			}

			if (attrCatSlotIndex == attrCatHeader.numSlots-1) {
				attrCatSlotIndex = -1; 
				attrCatBuffer = RecBuffer (attrCatHeader.rblock);
				attrCatBuffer.getHeader(&attrCatHeader);
			}
		}

		printf("\n");
	}
}

void updateAttributeName (const char* relName, 
									const char* oldAttrName, const char* newAttrName) {
	// used to hold reference to the block which referred to 
	// for getting records, headers and updating them
	RecBuffer attrCatBuffer (ATTRCAT_BLOCK);
	
	HeadInfo attrCatHeader;
	attrCatBuffer.getHeader(&attrCatHeader);

	// iterating the records in the Attribute Catalog
	// to find the correct entry of relation and attribute
	for (int recIndex = 0; recIndex < attrCatHeader.numEntries; recIndex++) {
		Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
		attrCatBuffer.getRecord(attrCatRecord, recIndex);

		// matching the relation name, and attribute name
		if (strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relName) == 0
			&& strcmp(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldAttrName) == 0) 
		{
			strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newAttrName);
			attrCatBuffer.setRecord(attrCatRecord, recIndex);
			std::cout << "Update successful!\n\n";
			break;
		}

		// reaching at the end of the block, and thus loading
		// the next block and setting the attrCatHeader & recIndex
		if (recIndex == attrCatHeader.numSlots-1) {
			recIndex = -1;
			attrCatBuffer = RecBuffer (attrCatHeader.rblock);
			attrCatBuffer.getHeader(&attrCatHeader);
		}
	}

}

int main(int argc, char *argv[])
{
	Disk disk_run;
  	StaticBuffer buffer;
	OpenRelTable cache;
	printRelationCache();
	// updateAttributeName ("Students", "Class", "Batch");
	// printAttributeCatalog();

	return 0;
}