/**
 * @author Kevin Hutchins
 *
 *Author of BufMgr: Kevin Hutchins
 *Student ID: 9080171847
 *
 * @section 9080171847
 */

#include "buffer.h"

#include <iostream>
#include <memory>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/invalid_page_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb {

constexpr int HASHTABLE_SZ(int bufs) { return ((int)(bufs * 1.2) & -2) + 1; }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs),
      hashTable(HASHTABLE_SZ(bufs)),
      bufDescTable(bufs),
      bufPool(bufs) {
  for (FrameId i = 0; i < bufs; i++) {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }

  clockHand = bufs - 1;
}

// Dune
/**
 * Advance clock to next frame in the buffer pool
 */
void BufMgr::advanceClock() {
  // Advance clock to next frame in the buffer pool
  if (clockHand >= numBufs - 1) {
    clockHand = 0;
  } else {
    clockHand = clockHand + 1;
  }
}
// Dune

/**
 * Allocates a free frame .using the clock algorithm
 */
void BufMgr::allocBuf(FrameId& frame) {
  // Allocates a free frame using the clock algorithm
  unsigned int counter = 0;
  // Iterates through bufDescTable
  do {
    // Checks valid pin
    if (bufDescTable[clockHand].valid) {
      // Checks if frame is still pinned
      if (bufDescTable[clockHand].pinCnt > 0) {
        counter++;
        advanceClock();
      } else {
        // Checks refBit, reverts to 0 if not already
        if (bufDescTable[clockHand].refbit) {
          bufDescTable[clockHand].refbit = false;
          advanceClock();
        } else {
          // Frame has been picked by clock algorithm, checks to see if you need
          // to write old page back
          if (bufDescTable[clockHand].dirty) {
            // Writes old page back to disk
            File dFile = bufDescTable[clockHand].file;
            FrameId pNum = bufDescTable[clockHand].frameNo;
            Page sPage = bufPool[pNum];
            dFile.writePage(sPage);

            // remove the appropriate entry from the hash table.
            hashTable.remove(bufDescTable[clockHand].file,
                             bufDescTable[clockHand].pageNo);

            // If file is not used in any frame, close the file
            int count = 0;
            for (FrameId i = 0; i < numBufs; i++) {
              if (bufDescTable[i].file == dFile) {
                if (i != clockHand) {
                  count++;
                }
              }
            }
            if (count == 0) {
              dFile.close();
            }

            // updates frame variable to be frame to be allocated
            frame = bufDescTable[clockHand].frameNo;
            return;
          } else {
            // remove the appropriate entry from the hash table.
            hashTable.remove(bufDescTable[clockHand].file,
                             bufDescTable[clockHand].pageNo);

            // updates frame variable to be frame to be allocated
            frame = bufDescTable[clockHand].frameNo;
            return;
          }
        }
      }
    } else {
      // Frame has been picked to be allocated

      // updates frame variable to be frame to be allocated
      frame = bufDescTable[clockHand].frameNo;
      return;
    }
  } while (numBufs > counter);
  // Throws BufferExceededException if all buffer frames are pinned.
  throw BufferExceededException();
}

/**
 * Reads the given page from the file into a frame and returns the pointer to
 * page. If the requested page is already present in the buffer pool pointer
 * to that frame is returned otherwise a new frame is allocated from the *
 * buffer pool for reading the page.
 */
void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {
  FrameId val;
  try {
    // check whether page is already in buffer by invoking lookup() method
    hashTable.lookup(file, pageNo, val);

    // sets the appropriate refbit
    bufDescTable[val].refbit = true;

    // increments pinCnt for the page
    bufDescTable[val].pinCnt++;

    // return pointer to frame containing page via page parameter
    page = &bufPool[val];
  } catch (const HashNotFoundException& e) {
    // File/pageNo not found in hashTable

    // Checks to see if file + page exist; if it's not the method will throw an
    // exception
    file.readPage(pageNo);

    // find frame to be allocated
    allocBuf(val);

    // Get page from disk and write into bufPool
    bufPool[val] = file.readPage(pageNo);

    // insert page into hashtable
    hashTable.insert(file, pageNo, val);

    // Set() on frame so bufDescTable is accurate
    bufDescTable[val].Set(file, pageNo);

    // Return a pointer to frame containing page via page parameter
    page = &bufPool[val];
  }
}

// Dune
/**
 * Unpin a page from memory since it is no longer required for it to remain in
 * memory.
 */
void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {
  FrameId val;
  try {
    hashTable.lookup(file, pageNo, val);

    // throws PAGENOTPINNED exception if pin count is already 0
    if (bufDescTable[val].pinCnt == 0) {
      throw PageNotPinnedException(file.filename_, pageNo, val);
    }

    // Decrement the pinCnt of the frame containing (file, PageNo)
    bufDescTable[val].pinCnt--;

    // if dirty == true, sets dirty bit
    if (dirty) {
      bufDescTable[val].dirty = true;
    }

  } catch (const HashNotFoundException& e) {
    // does nothing if page is not found in hash table lookup
  }
}

// Dune
/**
 * Allocates a new, empty page in the file and returns the Page object .
 * The newly allocated page is also assigned a frame in the buffer pool.
 */
void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {
  // file.allocatePage() returns newly allocated page
  Page nPage = file.allocatePage();

  // return page number of newly allocated page to caller
  pageNo = nPage.page_number();

  // allocBuf() gets buffer pool frame that will be newly allocated
  FrameId frame;
  allocBuf(frame);
  bufPool[frame] = nPage;

  // entry sent into hashtable
  hashTable.insert(file, pageNo, frame);

  // Set() invoked to update bufDescTable
  bufDescTable[frame].Set(file, pageNo);

  // return pointer to buffer frame allocated for the page via the page
  // parameter
  page = &bufPool[frame];
}

// Dune
/**
 * Writes out all dirty pages of the file to disk.
 * All the frames assigned to the f i l e need to be unpinned from buffer pool
 * before this function can be successfully called. Otherwise Error returned .
 */
void BufMgr::flushFile(File& file) {
  // scan bufTable for pages belonging to file
  for (unsigned int i = 0; i < numBufs; i++) {
    // If file in bufTable is equal to file you're looking for
    if (bufDescTable[i].file == file) {
      // Throws PagePinnedException if some page of the file is pinned
      if (bufDescTable[i].pinCnt > 0) {
        throw PagePinnedException(file.filename_, bufDescTable[i].pageNo,
                                  bufDescTable[i].frameNo);
      }

      // if page is dirty, call file.writePage
      if (bufDescTable[i].dirty) {
        try {
          file.writePage(bufPool[bufDescTable[i].pageNo]);
        } catch (InvalidPageException* e) {
          // Throws BadBufferException if an invalid page belonging to file is
          // encountered
          throw BadBufferException(bufDescTable[i].frameNo,
                                   bufDescTable[i].dirty, bufDescTable[i].valid,
                                   bufDescTable[i].refbit);
        }

        // set dirty bit to false as page written to disk
        bufDescTable[i].dirty = false;
      }

      // remove page from hashtable
      hashTable.remove(file, bufDescTable[i].pageNo);

      // If file is not used in any frame, close the file
      int count = 0;
      for (FrameId i = 0; i < numBufs; i++) {
        if (bufDescTable[i].file == file) {
          if (i != clockHand) {
            count++;
          }
        }
      }
      if (count == 0) {
        file.close();
      }

      // invoke Clear() method of BufDesc for page frame
      bufDescTable[i].clear();
    }
  }
  file.close();
}

// Dune
/**
 * Delete page from file and also from buffer pool if present.
 * Since the page is entirely deleted from file , its unnecessary to see if the
 * page is dirty.
 */
void BufMgr::disposePage(File& file, const PageId PageNo) {
  FrameId val;
  try {
    // find frame in hashTable
    hashTable.lookup(file, PageNo, val);

    // if the frame is dirty, flush it to disk
    if (bufDescTable[val].dirty) {
      file.writePage(bufPool[PageNo]);
    }
    // set the valid bit to false
    bufDescTable[val].valid = false;

    // delete corresponding entry in hashTable
    hashTable.remove(file, PageNo);

    // delete page in disk
    file.deletePage(PageNo);

  } catch (HashNotFoundException* e) {
    // file/page does not exist in hashTable, is not used anywhere
  }

  // If file is not used in any frame, close the file
  int count = 0;
  for (FrameId i = 0; i < numBufs; i++) {
    if (bufDescTable[i].file == file) {
      if (i != clockHand) {
        count++;
      }
    }
  }
  if (count == 0) {
    file.close();
  }
}

/**
 * Print member variable values .
 * Note: Not written by Kevin Hutchins
 */
void BufMgr::printSelf(void) {
  int validFrames = 0;

  for (FrameId i = 0; i < numBufs; i++) {
    std::cout << "FrameNo:" << i << " ";
    bufDescTable[i].Print();
    if (bufDescTable[i].valid) validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb
