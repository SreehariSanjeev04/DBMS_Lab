#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include <cstring>
#include <iostream>

void printBMAP() {
	unsigned char buffer[BLOCK_SIZE];
	Disk::readBlock(buffer, 0);
	for(int i = 0; i < BLOCK_SIZE; i++) {
		printf("%u ", buffer[i]);
	}
	printf("\n");
}

void stage_1() {
	unsigned char buffer[BLOCK_SIZE];
	Disk::readBlock(buffer, 7000);
	char message[] = "hello";
	memcpy(buffer+20, message, 6);
	Disk::writeBlock(buffer, 7000);

	unsigned char buffer2[BLOCK_SIZE];
	char message2[6];
	Disk::readBlock(buffer, 7000);
	memcpy(message2, buffer+20, 6);
	std::cout << message2 << std::endl;
}


void printSchemaCache() {	
	RelCatEntry relCatEntry;
	for(int i = RELCAT_RELID; i <= 6; i++) {
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
	}
	printf("\n");
}
void stage_2 () {
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
		for (; j < attrCatHeader.numEntries; j++, attrCatSlotIndex++)
		{
			Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
			attrCatBuffer.getRecord(attrCatRecord, j);

			if (strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal,
					   relCatRecord[RELCAT_REL_NAME_INDEX].sVal) == 0)
			{
				const char *attrType = attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER
										   ? "NUM" : "STR";
				printf("  %s: %s\n", attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrType);
			}

			if(j == attrCatHeader.numSlots - 1) {
				j = 0;
				attrCatBuffer = RecBuffer(attrCatHeader.rblock);
				attrCatBuffer.getHeader(&attrCatHeader);
			}
		}

		printf("\n");
	}
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
	RecBuffer attrCatBuffer (ATTRCAT_BLOCK);
	
	HeadInfo attrCatHeader;
	attrCatBuffer.getHeader(&attrCatHeader);

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
	return FrontendInterface::handleFrontend(argc, argv);
}