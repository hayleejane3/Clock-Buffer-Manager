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
}

// TODO(sreejita)
void BufMgr::advanceClock()
{
}

// TODO(sahibgoa)
void BufMgr::allocBuf(FrameId & frame)
{
}

// TODO(sahibgoa)
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
}

// TODO(hayleejane)
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
{
	for (std::uint32_t i = 0; i < numBufs; i++)
  {
  	if (bufDescTable[i].file == file && bufDescTable[i].pageNo == pageNo)
		{
			if (bufDescTable[i].pinCnt == 0)
			{
				throw new PageNotPinnedException(file.filename_, pageNo,
					bufDescTable[i].frameNo);
			}
			else
			{
				bufDescTable[i].pinCnt--;
			}

			if (dirty == true)
			{
				bufDescTable[i].dirty = true;
			}

			// Assuming that only one frame matches for a (file, PageNo)
			break;
		}
  }
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
