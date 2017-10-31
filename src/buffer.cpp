/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb {

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++)
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


// TODO(sahibgoa)
BufMgr::~BufMgr() {
    delete hashTable;
    delete[] bufPool;
    delete[] bufDescTable;
}

// TODO(sreejita)
void BufMgr::advanceClock()
{
}

// TODO(sahibgoa)
void BufMgr::allocBuf(FrameId & frame) {
    FrameId freeFrameIdx = numBufs, beginClock = clockHand;

		// Use Clock Algorithm to find a free frame
		do {
			// If it's valid check other fields otherwise we have a free frame
			if (bufDescTable[clockHand].valid) {
				// If the refbit's set clear it otherwise continue checking other fields
				if (!bufDescTable[clockHand].refbit) {
					// Ignore pages which are pinned
					if (bufDescTable[clockHand].pinCnt == 0) {
						// If the dirty bit is set, flush the page to disk and then proceed
						if (bufDescTable[clockHand].dirty) {
							flushFile(bufDescTable[clockHand].file);
						}

						// Remove the valid entry from the hash table
						hashTable->remove(
							bufDescTable[clockHand].file,
							bufDescTable[clockHand].pageNo
						);

						// Clear the fields to prepare frame for new user
						bufDescTable[clockHand].Clear();

						freeFrameIdx = clockHand;
					}
				} else {
					bufDescTable[clockHand].refbit = false;
				}
			} else {
				freeFrameIdx = clockHand;
			}

			// Advance the clock
			advanceClock();

		} while (freeFrameIdx == numBufs && clockHand != beginClock);

		// If we didn't find a frame to evict (all were pinned) then throw exception
		if (freeFrameIdx == numBufs)
			throw BufferExceededException();

		// Return frame by reference
		frame = freeFrameIdx;
}

// TODO(sahibgoa)
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page) {
    FrameId frame;
    try {
        hashTable->lookup(file, pageNo, frame);
				bufDescTable[frame].refbit = true;
				bufDescTable[frame].pinCnt++;
				page = &bufPool[frame];
    } catch (const HashNotFoundException& e) {
        allocBuf(frame);
				file->readPage(pageNo);
        hashTable->insert(file, pageNo, frame);
				bufDescTable[frame].Set(file, pageNo);
				page = &bufPool[frame];
    }
}

// TODO(hayleejane)
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
{
}

// TODO(hayleejane)
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
{
}

// TODO(sreejita)
void BufMgr::flushFile(const File* file)
{
}

// TODO(sreejita)
void BufMgr::disposePage(File* file, const PageId PageNo)
{
}

void BufMgr::printSelf(void)
{
  BufDesc* tmpbuf;
	int validFrames = 0;

  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
