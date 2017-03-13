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
    #       2. a <pageId, (frame_offset, page, pinCount)> hashtable for getting existing page
    #           It should pop out the page when the page is accessed and then put it at the end of dictionary
    self.freeList     = list(range(0, self.poolSize, self.pageSize))
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
    return (pageId in self.pageList)
  
  def getPage(self, pageId):
    # if the page is already in the bufferpool, then fetch that page in the frame
    if self.hasPage(pageId):
      # increment the pinning count
      (frame_offset, page, pinCount) = self.pageList[pageId]
      self.pageList[pageId] = (frame_offset, page, pinCount+1)
      # return the page back
      return self.pageList[pageId][1]
    else:
      # This means the page isn't in cache, then fetch it in file
      if len(self.freeList) == 0:
        self.evictPage()
      frame_offset = self.freeList.pop(0)
      page_buffer = self.pool.getbuffer()[frame_offset: frame_offset+self.pageSize]
      page = self.fileMgr.readPage(pageId, page_buffer)
      self.pageList[pageId] = (frame_offset, page, 0)
      self.pageList.move_to_end(pageId)
      return page

  # Removes a page from the page map, returning it to the free 
  # page list without flushing the page to the disk.
  def discardPage(self, pageId):
    if self.hasPage(pageId):
      (frame_offset, page, pinCount) = self.pageList[pageId]
      self.freeList.append(frame_offset)
      del self.pageList[pageId]

  def flushPage(self, pageId):
    if self.hasPage(pageId):
      (frame_offset, page, pinCount) = self.pageList[pageId]
      self.freeList.append(frame_offset)
      del self.pageList[pageId]

      if page.isDirty():
        self.fileMgr.writePage(page)

  # Evict using LRU policy. 
  # We implement LRU through the use of an OrderedDict, and by moving pages
  # to the end of the ordering every time it is accessed through getPage()
  def evictPage(self):
    # find the first page in pageList
    (pageId, (frame_offset, page, _)) = self.pageList.popitem()
    self.freeList.append(frame_offset)

    if page.isDirty():
        self.fileMgr.writePage(page)
    #self.flushPage(pageId)

  # Flushes all dirty pages
  def clear(self):
    items = list(self.pageList.items())
    for (pageId, (frame_offset, page, pinCount)) in items:
      if page.isDirty():
        self.flushPage(pageId)

if __name__ == "__main__":
    import doctest
    doctest.testmod()
