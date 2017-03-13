import io, math, struct

from collections import OrderedDict
from struct      import Struct
from collections import OrderedDict

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema      import DBSchema

import Storage.FileManager

class BufferPool:
  """
  A buffer pool implementation.

  Since the buffer pool is a cache, we do not provide any serialization methods.

  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> bp = BufferPool()
  >>> fm = Storage.FileManager.FileManager(bufferPool=bp)
  >>> bp.setFileManager(fm)

  # Check initial buffer pool size
  >>> len(bp.pool.getbuffer()) == bp.poolSize
  True

  """

  # Default to a 10 MB buffer pool.
  defaultPoolSize = 10 * (1 << 20)

  # Buffer pool constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments, with defaults if not present:
  # pageSize       : the page size to be used with this buffer pool
  # poolSize       : the size of the buffer pool
  def __init__(self, **kwargs):
    self.pageSize     = kwargs.get("pageSize", io.DEFAULT_BUFFER_SIZE)
    self.poolSize     = kwargs.get("poolSize", BufferPool.defaultPoolSize)
    self.pool         = io.BytesIO(b'\x00' * self.poolSize)

    ####################################################################################
    # DESIGN QUESTION: what other data structures do we need to keep in the buffer pool?
    # -Ans: 1. the dict for free frames start point offset <frame_offset, page>
    #       2. pincount "hashtable" for the page(<frame_offset, pincount>)
    #       3. a list for free frames(containing frame_offset in the buffer)
    #       4. a <pageId, frame_offset> hashtable getting existing page
    #           It should pop out the page when the page is accessed and then put it at the end of dictionary
    self.frame_offset = {x: None for x in range(0, self.poolSize, self.pageSize)}
    self.pincount     = {x: 0 for x in range(0, self.poolSize, self.pageSize)}
    self.freeList     = list(self.frame_offset.keys())
    self.pageList     = OrderedDict()

  def setFileManager(self, fileMgr):
    self.fileMgr = fileMgr

  # Basic statistics

  def numPages(self):
    return math.floor(self.poolSize / self.pageSize)

  def numFreePages(self):
    return len(self.freeList)

  def size(self):
    return self.poolSize

  def freeSpace(self):
    return self.numFreePages() * self.pageSize

  def usedSpace(self):
    return self.size() - self.freeSpace()


  # Buffer pool operations

  def hasPage(self, pageId):
    return (pageId in self.pageList.keys())
  
  def getPage(self, pageId):
    # if the page is already in the bufferpool, then fetch that page in the frame
    if self.hasPage(pageId):
      page_startPoint = self.pageList[pageId]
      # the page has been accessed, so we should put this page to the end of pageList and pincount+1
      self.pageList.move_to_end(pageId)
      self.pincount[page_startPoint] += 1
      return self.frame_offset[page_startPoint]
    else:
      # if the bufferpool is full, we should evict a page with self.evictPage()
      if self.numFreePages == 0:
        self.evictpage()
      # find the offset of the frame that has recently evicted page, or it is the first candidate in the freeList
      page_startPoint = self.freeList.pop(0)
      # insert the page into the bufferpool
      page_buffer = self.pool.getbuffer()[page_startPoint: page_startPoint+self.pageSize]
      page = self.fileMgr.readPage(pageId, page_buffer)
      # insert the page into the pageList, frame_offset and pin it once
      self.pageList[pageId] = page_startPoint
      self.pincount[page_startPoint] += 1
      self.frame_offset[page_startPoint] = page
      return self.frame_offset[page_startPoint]

  # Removes a page from the page map, returning it to the free 
  # page list without flushing the page to the disk.
  def discardPage(self, pageId):
    if not self.hasPage(pageId):
      raise ValueError("This page is not contained in the bufferpool; nothing is implemented")
    # find the offset of that page in the buffer
    page_offset = self.pageList[pageId]
    # Insert this page_offset into freeList
    self.freeList.append(page_offset)
    # Remove the item corresponding to the page_offset and pageList
    self.frame_offset[page_offset] = None
    self.pageList.pop(pageId)

  def flushPage(self, pageId):
    if not self.hasPage(pageId):
      raise ValueError("This page is not contained in the bufferpool; nothing is implemented")
    page_startPoint = self.pageList[pageId]
    page = self.frame_offset[page_startPoint]
    self.fileMgr.writePage(page)

  # Evict using LRU policy. 
  # We implement LRU through the use of an OrderedDict, and by moving pages
  # to the end of the ordering every time it is accessed through getPage()
  def evictPage(self):
    # Since self.pageList put the page to the end every time the page get accessed,
    # therefore we can choose the first candidate of the self.pageList to release the frame
    (pageId, page_startPoint) = self.pageList.popitem()
    # write the page to the file
    if self.frame_offset[page_startPoint].header.isDirty():
      self.flushPage(pageId)
    # clear the page in frame_offset
    self.pincount[page_startPoint] = 0
    self.frame_offset[page_startPoint] = None
    # add the offset to the freeList
    self.freeList.append(page_startPoint)

  # Flushes all dirty pages
  def clear(self):
    for offest in self.frame_offset.keys():
      page = self.frame_offset[offset]
      # write the page into the disk
      if page.header.isDirty():
        self.flushPage(page.pageId)
      # remove the page from frame, pageList
      self.pincount[offset] = 0
      self.frame_offset[offset] = None
      self.pageList.pop(page.pageId)
      # add this offset into freeList
      self.freeList.append(offset)

if __name__ == "__main__":
    import doctest
    doctest.testmod()
