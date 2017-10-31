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
	clockHand = (clockHand + 1) % numBufs;
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
}

// TODO(hayleejane)
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
}

// TODO(sreejita)
void BufMgr::flushFile(const File* file) 
{
	for(FrameId i = 0; i < numBufs; i++) {
		if(bufDescTable[i].file == file) {
			if(!bufDescTable[i].valid)
				throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
			
			if(bufDescTable[i].pinCnt > 0)
				throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
				
			if(bufDescTable[i].dirty) {
				File* f = bufDescTable[i].file;
				Page pflush = bufPool[bufDescTable[i].frameNo];
				f->writePage(pflush);
				bufDescTable[i].dirty = false;
			}
			
			hashTable->remove(file, bufDescTable[i].pageNo);
			bufDescTable[i].Clear();
		}
	}
}

// TODO(sreejita)
void BufMgr::disposePage(File* file, const PageId PageNo)
{	
	FrameId frameNo;
	
	try{
		hashTable->lookup(file, PageNo, frameNo);
		hashTable->remove(file, PageNo);
		bufDescTable[frameNo].Clear();
		
	} catch(HashNotFoundException h) {
		
	}

	file->deletePage(PageNo); 
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
