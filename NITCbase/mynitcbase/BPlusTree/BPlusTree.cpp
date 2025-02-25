#include "BPlusTree.h"
#include <cstring>
#include <cstdio>
#include <cstdlib>

RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int op)
{
    IndexId searchIndex;
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getSearchIndex(relId, attrName, &searchIndex);
    if (ret != SUCCESS)
    {
        printf("Search index not available\n");
        exit(FAILURE);
    }
    ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if (ret != SUCCESS)
    {
        printf("Cannot get the Attribute Cache Entry.\n");
        exit(FAILURE);
    }
    int block = -1, index = -1;
    if (searchIndex.block = -1 && searchIndex.index == -1)
    {
        block = attrCatEntry.rootBlock;
        index = 0;

        if (block == -1)
        {
            RecId RecId{-1, -1};
        }
        else
        {
            block = searchIndex.block;
            index = searchIndex.index + 1;

            IndLeaf leaf(block);
            HeadInfo leafHead;
            leaf.getHeader(&leafHead);

            if (index >= leafHead.numEntries)
            {
                block = leafHead.rblock;
                index = 0;

                if (block == -1)
                {
                    return RecId{-1, -1};
                }
            }
        }

        while (StaticBuffer::getStaticBlockType(block) == IND_INTERNAL)
        {
            IndInternal internalBlock(block);

            HeadInfo intHead;
            internalBlock.getHeader(&intHead);

            InternalEntry intEntry;
            if (op == NE || op == LT || op == LE)
            {
                internalBlock.getEntry(&intEntry, 0);
                block = intEntry.lChild;
            }
            else
            {
                index = 0;
                bool found = false;
                while (index < intHead.numEntries)
                {
                    ret = internalBlock.getEntry(&intEntry, index);
                    int cmpVal = compareAttrs(intEntry.attrVal, attrVal, NUMBER);
                    if ((op == EQ && cmpVal == 0) || (op == GE && cmpVal >= 0) || (op == GT && cmpVal > 0))
                    {
                        found = true;
                        break;
                    }
                    index++;
                }

                if (found)
                {
                    block = intEntry.lChild;
                }
                else
                {
                    block = intEntry.rChild;
                }
            }
        }
        // now in the leaf index block, find the first leaf index entry that satisfies our condition
        while (block != -1)
        {
        }
    }
}
