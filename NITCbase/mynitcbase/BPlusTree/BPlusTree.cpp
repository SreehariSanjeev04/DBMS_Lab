#include "BPlusTree.h"
#include <cstring>
#include <cstdio>
#include <cstdlib>

int BPlusTree::numOfComparions = 0;

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
    if (searchIndex.block == -1 && searchIndex.index == -1)
    {
        // search is done for the first time
        block = attrCatEntry.rootBlock;
        index = 0;

        if (block == -1)
        {
            return RecId{-1, -1};
        }
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
            // traversing to the next block
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
        // moving the pointer
        if (op == NE || op == LT || op == LE)
        {
            // for the three cases, we have to move left
            internalBlock.getEntry(&intEntry, 0);
            block = intEntry.lChild;
        }
        else
        {
            int entryIndex = 0;
            while (entryIndex < intHead.numEntries)
            {
                ret = internalBlock.getEntry(&intEntry, entryIndex);
                int cmpVal = compareAttrs(intEntry.attrVal, attrVal, attrCatEntry.attrType);
                BPlusTree::numOfComparions++;
                if ((op == EQ && cmpVal >= 0) || (op == GE && cmpVal >= 0) || (op == GT && cmpVal > 0))
                {
                    break;
                }
                entryIndex++;
            }

            if (entryIndex < intHead.numEntries)
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
        printf("%d\n", block);
        IndLeaf leafBlk(block);
        HeadInfo leafHead;
        leafBlk.getHeader(&leafHead);

        Index leafEntry;
        while (index < leafHead.numEntries)
        {
            leafBlk.getEntry(&leafEntry, index);
            int cmpVal = compareAttrs(leafEntry.attrVal, attrVal, attrCatEntry.attrType);
            BPlusTree::numOfComparions++;
            if ((op == EQ && cmpVal == 0) || (op == LE && cmpVal <= 0) ||
                (op == LT && cmpVal < 0) || (op == GT && cmpVal > 0) || (op == GE && cmpVal >= 0) || (op == NE && cmpVal != 0))
            {
                searchIndex.block = block;
                searchIndex.index = index;
                AttrCacheTable::setSearchIndex(relId, attrName, &searchIndex);
                return RecId{leafEntry.block, leafEntry.slot};
            }
            else if ((op == EQ || op == LE || op == LT) && cmpVal > 0)
            {
                // future entries will not satisfy EQ, LE, LT
                return RecId{-1, -1};
            }

            ++index;
        }

        if (op != NE)
        {
            break;
        }

        block = leafHead.rblock;
        index = 0;
    };
    return RecId{-1, -1};
}
