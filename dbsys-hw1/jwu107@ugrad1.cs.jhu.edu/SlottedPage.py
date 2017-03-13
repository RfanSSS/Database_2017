import functools, math, struct
from struct import Struct
from io     import BytesIO

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema import DBSchema
from Storage.Page import PageHeader, Page

###########################################################
# DESIGN QUESTION 1: should this inherit from PageHeader?
# If so, what methods can we reuse from the parent?
# Yes!
class SlottedPageHeader(PageHeader):
  """
  A slotted page header implementation. This should store a slot bitmap
  implemented as a memoryview on the byte buffer backing the page
  associated with this header. Additionally this header object stores
  the number of slots in the array, as well as the index of the next
  available slot.

  The binary representation of this header object is: (numSlots, nextSlot, slotBuffer)

  >>> import io
  >>> buffer = io.BytesIO(bytes(4096))
  >>> ph     = SlottedPageHeader(buffer=buffer.getbuffer(), tupleSize=16)
  >>> ph2    = SlottedPageHeader.unpack(buffer.getbuffer())

  ## Dirty bit tests
  >>> ph.isDirty()
  False
  >>> ph.setDirty(True)
  >>> ph.isDirty()
  True
  >>> ph.setDirty(False)
  >>> ph.isDirty()
  False

  ## Tuple count tests
  >>> ph.hasFreeTuple()
  True

  # First tuple allocated should be at the first slot.
  # Notice this is a slot index, not an offset as with contiguous pages.
  >>> ph.nextFreeTuple() == 0
  True

  >>> ph.numTuples()
  1

  >>> tuplesToTest = 10
  >>> [ph.nextFreeTuple() for i in range(0, tuplesToTest)]
  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
  
  >>> ph.numTuples() == tuplesToTest+1
  True

  >>> ph.hasFreeTuple()
  True

  # Check space utilization
  >>> ph.usedSpace() == (tuplesToTest+1)*ph.tupleSize
  True

  >>> ph.freeSpace() == 4096 - (ph.headerSize() + ((tuplesToTest+1) * ph.tupleSize))
  True

  >>> remainingTuples = int(ph.freeSpace() / ph.tupleSize)

  # Fill the page.
  >>> [ph.nextFreeTuple() for i in range(0, remainingTuples)] # doctest:+ELLIPSIS
  [11, 12, ...]

  >>> ph.hasFreeTuple()
  False

  # No value is returned when trying to exceed the page capacity.
  >>> ph.nextFreeTuple() == None
  True
  
  >>> ph.freeSpace() < ph.tupleSize
  True
  """
  binrepr   = struct.Struct("HHH")
  reprSize      = binrepr.size

  # Flag bitmasks
  dirtyMask = 0b1

  # Fields:
  #   - numSlots: determine number of slots in the page(given pagesize, tuplesize, headersize) 
  #   - tupleSize(Inherited from pageHeader)
  #   - slotBuffer: an array with size "numSlots"
  #   - usedSlots: a queue for recording used slots
  #   - freeSlots: a queue for recording free slots in the page
  #   - nextSlot : keep track of the space for slot insertion
  #   - usedSlots_num: number of used slots in this page
  def __init__(self, **kwargs):
    buffer     = kwargs.get("buffer", None)
    self.flags = kwargs.get("flags", b'\x00')
    if buffer:
      self.tupleSize = kwargs.get("tupleSize", None)
      self.pageCapacity = len(buffer)
      self.numSlots = math.floor((self.pageCapacity - 2*3)/float(self.tupleSize+0.125))
      self.slotBuffer = [0 for x in range(0, self.numSlots)]

      self.usedSlots = []
      self.freeSlots = []
      self.nextSlot = 0
      self.usedSlots_num = 0
    else:
      raise ValueError("No backing buffer provided to page constructor.")
    # Initialize the freeSlots
    for i in range(0, self.numSlots):
      self.freeSlots.append(i)
    buffer[0: self.headerSize()] = self.pack()

  def __eq__(self, other):
    return (    self.flags == other.flags
            and self.tupleSize == other.tupleSize
            and self.pageCapacity == other.pageCapacity 
            and self.slotBuffer == other.slotBuffer
            and self.nextSlot == other.nextSlot
    )

  def __hash__(self):
    return hash((self.flags, self.tupleSize, self.pageCapacity, self.slotBuffer, self.nextSlot))

  def headerSize(self):
    return self.reprSize + math.ceil(self.numSlots/8)

  # Flag operations.
  def flag(self, mask):
    return (ord(self.flags) & mask) > 0

  def setFlag(self, mask, set):
    if set:
      self.flags = bytes([ord(self.flags) | mask])
    else:
      self.flags = bytes([ord(self.flags) & ~mask])

  # Dirty bit accessors
  def isDirty(self):
    return self.flag(PageHeader.dirtyMask)

  def setDirty(self, dirty):
    self.setFlag(PageHeader.dirtyMask, dirty)

  def numTuples(self):
    # Inherit from PageHeader class
    return super(SlottedPageHeader, self).numTuples()

  # Returns the space available in the page associated with this header.
  def freeSpace(self):
    # Inherit from PageHeader class
    return super(SlottedPageHeader, self).freeSpace()

  # Returns the space used in the page associated with this header.
  def usedSpace(self):
    return self.usedSlots_num * self.tupleSize


  # Slot operations.
  def offsetOfSlot(self, slot):
    return self.headerSize() + slot * self.tupleSize

  def hasSlot(self, slotIndex):
    if slotIndex in self.usedSlots:
      return True
    else:
      return False

  def getSlot(self, slotIndex):
    if self.hasSlot(slotIndex):
      start = self.headerSize() + slotIndex * self.tupleSize
      end = start + self.tupleSize 
      return buffer[start : end]
    else:
      return None

  def setSlot(self, slotIndex, slot):
    raise NotImplementedError 

  def resetSlot(self, slotIndex):
    raise NotImplementedError

  def freeSlots(self):
    return self.freeSlots

  def usedSlots(self):
    return self.usedSlots

  # return the offset of certain slotIndex
  def slotOffset(self, slotIndex):
    return slotIndex * self.tupleSize + self.headerSize()


  # Tuple allocation operations.
  
  # Returns whether the page has any free space for a tuple.
  def hasFreeTuple(self):
    return (self.usedSlots_num < self.numSlots)


  # Returns the tupleIndex of the next free tuple.
  # This should also "allocate" the tuple, such that any subsequent call
  # does not yield the same tupleIndex.
  def nextFreeTuple(self):
    if self.hasFreeTuple():
      # Find the nextSlot for insertion
      nextSlot = self.nextSlot
      self.slotBuffer[nextSlot] = 1
      # Check if the self.freeSlots still have available slots lying in
      if len(self.freeSlots) > 1:
        self.nextSlot = self.freeSlots[1]
      else:
        # We should set the nextSlot greater than the greatest slot index
        self.nextSlot = self.numSlots

      # Pop out the first element in freeSlots
      tupleIndex = self.freeSlots.pop(0)
      self.usedSlots.append(tupleIndex)
      self.usedSlots_num += 1
      return tupleIndex
    else:
      return None


  def nextTupleRange(self):
    if self.hasFreeTuple():
      slotIndex = self.nextFreeTuple
      start = self.slotOffset(slotIndex)
      end = start + self.tupleSize
      return (slotIndex, start, end)

  # Create a binary representation of a slotted page header.
  # The binary representation should include the slot contents.
  def pack(self):
    headerPacked = Struct("HHH").pack(self.numSlots, self.nextSlot, self.tupleSize)
    # Pack the bitmap(self.slotBuffer)
    byteSlot = 0
    bitmove = 0
    for i in range(0, self.numSlots):
      if i == 0: 
        byteSlot = self.slotBuffer[0]
      elif (i % 8 == 0) and i != 0:
        headerPacked += Struct("B").pack(byteSlot)
        bitmove = 0
        byteSlot = self.slotBuffer[i]
      else:
        bitmove += 1
        byteSlot |= self.slotBuffer[i] << bitmove
      # if this is the last element in slotBuffer, it should call struct.pack immediately
      if i == self.numSlots - 1:
        headerPacked += Struct("B").pack(byteSlot)

    return headerPacked


  # Create a slotted page header instance from a binary representation held in the given buffer.
  @classmethod
  def unpack(cls, buffer):
    # first, unpack from the buffer with binrepr
    values = Struct("HHH").unpack_from(buffer)
    numSlots = values[0]
    nextSlot = values[1]
    tupleSize = values[2]
    new_ph = cls(buffer=BytesIO(buffer).getbuffer(), tupleSize=tupleSize)
    new_ph.numSlots, new_ph.nextSlot = numSlots, nextSlot

    # reconstruct the original bitmap from buffer
    for i in range(0, numSlots):
      if i%8 == 0:
          temp = buffer[int(i/8+6)]
      new_ph.slotBuffer[i] = temp & 1
      temp >>= 1

    # reconstruct the original usedSlots and freeSlots
    for i in range(0, numSlots):
      if new_ph.slotBuffer[i] == 0:
        new_ph.freeSlots.append(i)
      elif new_ph.slotBuffer[i] == 1:
        new_ph.usedSlots.append(i)
    new_ph.usedSlots_num = len(new_ph.usedSlots)
    return new_ph



######################################################
# DESIGN QUESTION 2: should this inherit from Page?
# If so, what methods can we reuse from the parent?
#
class SlottedPage(Page):
  """
  A slotted page implementation.

  Slotted pages use the SlottedPageHeader class for its headers, which
  maintains a set of slots to indicate valid tuples in the page.

  A slotted page interprets the tupleIndex field in a TupleId object as
  a slot index.

  >>> from Catalog.Identifiers import FileId, PageId, TupleId
  >>> from Catalog.Schema      import DBSchema

  # Test harness setup.
  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> pId    = PageId(FileId(1), 100)
  >>> p      = SlottedPage(pageId=pId, buffer=bytes(4096), schema=schema)

  # Validate header initialization
  >>> p.header.numTuples() == 0 and p.header.usedSpace() == 0
  True

  # Create and insert a tuple
  >>> e1 = schema.instantiate(1,25)
  >>> tId = p.insertTuple(schema.pack(e1))

  >>> tId.tupleIndex
  0

  # Retrieve the previous tuple
  >>> e2 = schema.unpack(p.getTuple(tId))
  >>> e2
  employee(id=1, age=25)

  # Update the tuple.
  >>> e1 = schema.instantiate(1,28)
  >>> p.putTuple(tId, schema.pack(e1))

  # Retrieve the update
  >>> e3 = schema.unpack(p.getTuple(tId))
  >>> e3
  employee(id=1, age=28)

  # Compare tuples
  >>> e1 == e3
  True

  >>> e2 == e3
  False

  # Check number of tuples in page
  >>> p.header.numTuples() == 1
  True

  # Add some more tuples
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(10)]:
  ...    _ = p.insertTuple(tup)
  ...

  # Check number of tuples in page
  >>> p.header.numTuples()
  11

  # Test iterator
  >>> [schema.unpack(tup).age for tup in p]
  [28, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Test clearing of first tuple
  >>> tId = TupleId(p.pageId, 0)
  >>> sizeBeforeClear = p.header.usedSpace()  
  >>> p.clearTuple(tId)
  
  >>> schema.unpack(p.getTuple(tId))
  employee(id=0, age=0)

  >>> p.header.usedSpace() == sizeBeforeClear
  True

  # Check that clearTuple only affects a tuple's contents, not its presence.
  >>> [schema.unpack(tup).age for tup in p]
  [0, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Test removal of first tuple
  >>> sizeBeforeRemove = p.header.usedSpace()
  >>> p.deleteTuple(tId)

  >>> [schema.unpack(tup).age for tup in p]
  [20, 22, 24, 26, 28, 30, 32, 34, 36, 38]
  
  # Check that the page's slots have tracked the deletion.
  >>> p.header.usedSpace() == (sizeBeforeRemove - p.header.tupleSize)
  True

  """

  headerClass = SlottedPageHeader

  # Slotted page constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments:
  # buffer       : a byte string of initial page contents.
  # pageId       : a PageId instance identifying this page.
  # header       : a SlottedPageHeader instance.
  # schema       : the schema for tuples to be stored in the page.
  # Also, any keyword arguments needed to construct a SlottedPageHeader.
  def __init__(self, **kwargs):
    buffer = kwargs.get("buffer", None)
    if buffer:
      BytesIO.__init__(self, buffer)
      self.pageId = kwargs.get("pageId", None)
      header      = kwargs.get("header", None)
      schema      = kwargs.get("schema", None)

      if self.pageId and header:
        self.header = header
      elif self.pageId:
        self.header = self.initializeHeader(**kwargs)
      else:
        raise ValueError("No page identifier provided to page constructor.")
      

    else:
      raise ValueError("No backing buffer provided to page constructor.")


  # Header constructor override for directory pages.
  def initializeHeader(self, **kwargs):
    schema = kwargs.get("schema", None)
    if schema:
      return SlottedPageHeader(buffer=self.getbuffer(), tupleSize=schema.size)
    else:
      raise ValueError("No schema provided when constructing a slotted page.")

  # Tuple iterator.
  def __iter__(self):
    self.iterTupleIdx = 0
    return self

  def __next__(self):

    if self.iterTupleIdx < self.header.usedSlots_num:
      t = self.getTuple(TupleId(self.pageId, self.header.usedSlots[self.iterTupleIdx]));

      self.iterTupleIdx += 1
      return t
    else:
      raise StopIteration

  # Tuple accessor methods

  # Returns a byte string representing a packed tuple for the given tuple id.
  def getTuple(self, tupleId):
    tupleIndex = tupleId.tupleIndex
    if self.header.hasSlot(tupleIndex): 
      start = self.header.slotOffset(tupleIndex)
      end = start + self.header.tupleSize
      return self.getvalue()[start: end]
    else:
      return None

  # Updates the (packed) tuple at the given tuple id.
  def putTuple(self, tupleId, tupleData):
    tupleIndex = tupleId.tupleIndex
    if self.header.hasSlot(tupleIndex) and (tupleIndex in self.header.usedSlots):
      start = self.header.slotOffset(tupleIndex)
      end = start + self.header.tupleSize
      self.getbuffer()[start: end] = tupleData
      self.setDirty(True)
    else:
      return None

  # Adds a packed tuple to the page. Returns the tuple id of the newly added tuple.
  def insertTuple(self, tupleData):
    if self.header.hasFreeTuple():
      slotIndex = self.header.nextFreeTuple()
      tupleIndex = self.header.offsetOfSlot(slotIndex)
      self.getbuffer()[tupleIndex: (tupleIndex+self.header.tupleSize)] = tupleData
      self.header.setDirty(True)
      return TupleId(self.pageId, slotIndex)
    else:
      raise ValueError("Page is full!")


  # Zeroes out the contents of the tuple at the given tuple id.
  def clearTuple(self, tupleId):
    tupleIndex = tupleId.tupleIndex
    if self.header.hasSlot(tupleIndex):
      start = self.header.slotOffset(tupleIndex)
      end = start + self.header.tupleSize
      self.getbuffer()[start:end] = bytearray(b'\x00' * self.header.tupleSize);
      self.setDirty(True)
    else:
      return None

  # Removes the tuple at the given tuple id, shifting subsequent tuples.
  def deleteTuple(self, tupleId):
    tupleIndex = tupleId.tupleIndex
    if self.header.hasSlot(tupleIndex):
      self.clearTuple(tupleId)
      self.header.freeSlots.append(tupleIndex)
      self.header.usedSlots.remove(tupleIndex)
      self.header.slotBuffer[tupleIndex] = 0
      self.header.usedSlots_num -= 1
      self.setDirty(True)

  # Returns a binary representation of this page.
  # This should refresh the binary representation of the page header contained
  # within the page by packing the header in place.
  def pack(self):
    self.getbuffer()[0: self.header.headerSize()] = self.header.pack()
    return self.getvalue()

  # Creates a Page instance from the binary representation held in the buffer.
  # The pageId of the newly constructed Page instance is given as an argument.
  @classmethod
  def unpack(cls, pageId, buffer):
    header = cls.headerClass.unpack( BytesIO(buffer).getbuffer() )
    return cls(pageId = pageId, header = header, buffer = buffer)


if __name__ == "__main__":
    import doctest
    doctest.testmod()