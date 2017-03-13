from Catalog.Schema import DBSchema
from Query.Operator import Operator

import itertools
import types

class GroupBy(Operator):
  def __init__(self, subPlan, **kwargs):
    super().__init__(**kwargs)

    if self.pipelined:
      raise ValueError("Pipelined group-by-aggregate operator not supported")

    self.subPlan     = subPlan
    self.subSchema   = subPlan.schema()
    self.groupSchema = kwargs.get("groupSchema", None)
    self.aggSchema   = kwargs.get("aggSchema", None)
    self.groupExpr   = kwargs.get("groupExpr", None)
    self.aggExprs    = kwargs.get("aggExprs", None)
    self.groupHashFn = kwargs.get("groupHashFn", None)
    self.validateGroupBy()
    self.initializeSchema()

  # Perform some basic checking on the group-by operator's parameters.
  def validateGroupBy(self):
    requireAllValid = [self.subPlan, \
                       self.groupSchema, self.aggSchema, \
                       self.groupExpr, self.aggExprs, self.groupHashFn ]

    if any(map(lambda x: x is None, requireAllValid)):
      raise ValueError("Incomplete group-by specification, missing a required parameter")

    if not self.aggExprs:
      raise ValueError("Group-by needs at least one aggregate expression")

    if len(self.aggExprs) != len(self.aggSchema.fields):
      raise ValueError("Invalid aggregate fields: schema mismatch")

  # Initializes the group-by's schema as a concatenation of the group-by
  # fields and all aggregate fields.
  def initializeSchema(self):
    schema = self.operatorType() + str(self.id())
    fields = self.groupSchema.schema() + self.aggSchema.schema()
    self.outputSchema = DBSchema(schema, fields)

  # Returns the output schema of this operator
  def schema(self):
    return self.outputSchema

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.subPlan.schema()]

  # Returns a string describing the operator type
  def operatorType(self):
    return "GroupBy"

  # Returns child operators if present
  def inputs(self):
    return [self.subPlan]

  # Iterator abstraction for selection operator.
  def __iter__(self):
    self.initializeOutput()
    # Pipelined join operator is not supported
    self.outputIterator = self.processAllPages()

    return self

  def __next__(self):
    return next(self.outputIterator)

  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    raise ValueError("Page-at-a-time processing not supported for joins")

  # Set-at-a-time operator processing
  def processAllPages(self):
    Map = dict()
    # partition the schema into several files in different attributes
    self.partition(Map)

    for key, title in Map.items():
      # Generate a pageIterator in the file
      pageIterator = self.storage.pages(title)

      # Generate an dictionary on intermediate aggregation results
      aggregator = {}
      # Get the tuple in the page
      for _, page in pageIterator:
        for Tuple in page:
          tuple_Unpacked = self.subSchema.unpack(Tuple)
          key = self.groupExpr(tuple_Unpacked)
          if type(key) is tuple:
            key = key
          else:
            key = key,

          val = self.groupHashFn(key)
          intermediate_results = aggregator.get(val, None)
          
          # if the intermediate_result has not generated, form one
          if not intermediate_results:
            intermediate_results = list()
            aggregator[val] = intermediate_results
            for aggExpr in self.aggExprs:
              intermediate_results.append(aggExpr[0])
          index = 0
          # Perform the aggregation function
          for aggExpr in self.aggExprs:
            intermediate_result = intermediate_results[index]
            intermediate_results[index] = aggExpr[1](intermediate_result, tuple_Unpacked)
            index += 1

      for val, intermediate_results in aggregator.items():
        index = 0
        for aggExpr in self.aggExprs:
          intermediate_result = intermediate_results[index]
          intermediate_results[index] = aggExpr[2](intermediate_result)
          index += 1

        outputList = itertools.chain([val], intermediate_results)
        outputTuple = self.outputSchema.instantiate(*outputList)
        self.emitOutputTuple(self.outputSchema.pack(outputTuple))

      if self.outputPages:
        self.outputPages = [self.outputPages[-1]]

    # remove the temporary relation created
    for _, title in Map.items():
      self.storage.removeRelation(title)

    return self.storage.pages(self.relationId())

  def partition(self, relMap):
    for(pageId, page) in iter(self.subPlan):
      for Tuple in page:
        tuple_Unpacked = self.subSchema.unpack(Tuple)
        key = self.groupExpr(tuple_Unpacked)
        if type(key) is tuple:
          key = key
        else:
          key = key,

        value = self.groupHashFn(key)
        # if this key is not in relation map, we should create a temperory file to contain these tuples with this key
        if not value in relMap:
          title = str(self.id()) + "_grp_" + str(value)
          self.storage.createRelation(title, self.subSchema)
          relMap[value] = title
        self.storage.insertTuple(relMap[value], Tuple)
  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    return super().explain() + "(groupSchema=" + self.groupSchema.toString() \
                             + ", aggSchema=" + self.aggSchema.toString() + ")"
