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

	  for (FrameId i = 0; i < bufs; i++) {
	  	bufDescTable[i].frameNo = i;
	  	bufDescTable[i].valid = false;
	  }

	  bufPool = new Page[bufs];

	  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
	  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

	  clockHand = bufs - 1;
	}

	BufMgr::~BufMgr() {
	  delete hashTable;
	  delete[] bufPool;
	  delete[] bufDescTable;
	}

	void BufMgr::advanceClock() {
		clockHand = (clockHand + 1) % numBufs;
	}

	void BufMgr::allocBuf(FrameId & frame) {
		// FrameId freeFrameIdx = numBufs;
	  bool found = false;
		std::uint32_t traversedFrames = 0;
		// Use Clock Algorithm to find a free frame
		while(traversedFrames <= numBufs) {
			// Advance the clock
			advanceClock();
			// If it's valid check other fields otherwise we have a free frame
			if (bufDescTable[clockHand].valid) {
				// If the refbit's set clear it otherwise continue checking other fields
				if (!bufDescTable[clockHand].refbit) {
					// Ignore pages which are pinned
					if (bufDescTable[clockHand].pinCnt == 0) {
						// If the dirty bit is set, flush the page to disk and then proceed
						if (bufDescTable[clockHand].dirty) {
							bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
							bufStats.diskwrites++;
							bufStats.accesses++;
							bufDescTable[clockHand].dirty= false;
						}

						// Remove the valid entry from the hash table
						hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);

					  // freeFrameIdx = clockHand;
						found = true;
						break;
					}
				} else {
					bufDescTable[clockHand].refbit = false;
				}
			} else {
				// freeFrameIdx = clockHand;
				found = true;
				break;
			}

			traversedFrames++;
		}

		// If we didn't find a frame to evict (all were pinned) then throw exception
		if (!found && traversedFrames>numBufs)
			throw BufferExceededException();

		// Clear the fields to prepare frame for new user
		bufDescTable[clockHand].Clear();

		// Return frame by reference
		frame = clockHand;
	}

	void BufMgr::readPage(File* file, const PageId pageNo, Page*& page) {
	  FrameId frame;
	  try {
			// Check if page is already in the buffer pool
	    hashTable->lookup(file, pageNo, frame);

			// Set the refbit for the frame
			bufDescTable[frame].refbit = true;

			// Increment pin count for the page
		  bufDescTable[frame].pinCnt++;

			// Return a pointer to the frame
			page = &bufPool[frame];
	  } catch (const HashNotFoundException& e) {
			// Page is not in the buffer pool

			// Allocate a buffer frame
	    allocBuf(frame);

			// Read the page from disk into the buffer pool frame
		  bufPool[frame] = file->readPage(pageNo);
			bufStats.diskreads++;

			// Insert an entry for the page in the hash table
	    hashTable->insert(file, pageNo, frame);

			// Set up the frame
		  bufDescTable[frame].Set(file, pageNo);

			// Return a pointer to the frame
		  page = &bufPool[frame];
	  }
		bufStats.accesses++;
	}

	void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) {
		FrameId frame;
	  try {
			// Look for frame containing (file, PageNo)
	    hashTable->lookup(file, pageNo, frame);

			// Decrement pin count of frame. Throw exception if pin count is already 0
			if (bufDescTable[frame].pinCnt == 0) {
				throw PageNotPinnedException(file->filename(), pageNo, frame);
			} else {
				bufDescTable[frame].pinCnt--;
			}

			// Set dirty bit to true if 'dirty' is true
			if (dirty == true) {
				bufDescTable[frame].dirty = true;
			}
		} catch (const HashNotFoundException& e) {
			// Do nothing if page is not found in the hash table lookup
		}
	}

	void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) {
		// Allocate an empty page in the file
		Page alloc_page = file->allocatePage();
		// Set page number of allocated page
		pageNo = alloc_page.page_number();

		// Obtain a buffer pool frame
		FrameId frame;
		allocBuf(frame);
		bufPool[frame] = alloc_page;
		bufStats.accesses++;

		// Insert entry in hash table
		hashTable->insert(file, pageNo, frame);

		// Set up frame
		bufDescTable[frame].Set(file, pageNo);
		page = &bufPool[frame];
		bufStats.accesses++;
	}

	void BufMgr::flushFile(const File* file) {
		for (FrameId i = 0; i < numBufs; i++) {
			if (bufDescTable[i].file == file) {
				// Throw BadBufferException if an invalid page belonging to the file is encountered
				if (!bufDescTable[i].valid)
					throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);

				// Throw PagePinnedException if some page of the file is pinned
				if (bufDescTable[i].pinCnt > 0)
					throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);

				// Flush dirty pages to disk and clear the dirty bit
				if (bufDescTable[i].dirty) {
					File* f = bufDescTable[i].file;
					Page pflush = bufPool[bufDescTable[i].frameNo];
					bufStats.accesses++;
					f->writePage(pflush);
					bufStats.diskwrites++;
					bufDescTable[i].dirty = false;
				}

				// Remove page from the hash table
				hashTable->remove(file, bufDescTable[i].pageNo);

				// Clear the page frame
				bufDescTable[i].Clear();
			}
		}
	}

	void BufMgr::disposePage(File* file, const PageId PageNo) {
		FrameId frameNo;

		try {
			// Ensure that page to be deleted has an allocated frame in the buffer pool
			hashTable->lookup(file, PageNo, frameNo);

			// Remove entry for page from the buffer pool
			hashTable->remove(file, PageNo);

			// Free the frame
			bufDescTable[frameNo].Clear();
		} catch(HashNotFoundException h) {
			// In case lookup fails, do nothing
		}

		// Delete the page from the file
		file->deletePage(PageNo);
	}

	void BufMgr::printSelf(void) {
	  BufDesc* tmpbuf;
		int validFrames = 0;

	  for (std::uint32_t i = 0; i < numBufs; i++) {
	  	tmpbuf = &(bufDescTable[i]);
			std::cout << "FrameNo:" << i << " ";
			tmpbuf->Print();

	  	if (tmpbuf->valid == true)
	    	validFrames++;
	  }

		std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
	}
}
