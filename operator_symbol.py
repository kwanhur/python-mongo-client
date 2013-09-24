#!/usr/bin/env python
#_*_ coding:utf8 _*_
'''
Created on 2013-4-10

@author: huanghua
'''


class QueryUpdateOperator(object):
    '''
    { field: { $all: [ <value> , <value1> ... ] }
    '''
    ALL = '$all'

    '''
    { $and: [ { <expression1> }, { <expression2> } , ... , { <expressionN> } ] }
    '''
    AND = '$and'

    '''
    update({ field: value }, { $addToSet: { field: value1 } }) 
    '''
    ADD_TO_SET = '$addToSet'

    '''
    update( { field: 1 }, { $bit: { field: { and: 5 } } } )
    '''
    BIT = '$bit'

    '''
    { <location field> : { $geoWithin : { $box :
                                       [ [ <bottom left coordinates> ] ,
                                         [ <upper right coordinates> ] ] } } }
    '''
    BOX = '$box'

    '''
    { <location field> : { $geoWithin : { $center : [ [ <x>, <y> ] , <radius> ] } } }
    '''
    CENTER = '$center'

    '''
    find( { <location field> :
                         { $geoWithin :
                            { $centerSphere : [ [ <x>, <y> ] , <radius> ] }
                      } } )
    '''
    CENTER_SPHERE = '$centerSphere'

    '''
    find( { <query> } )._addSpecial( "$comment", <comment> )
    find( { $query: { <query> }, $comment: <comment> } )
    '''
    COMMENT = '$comment'

    '''
    update( <query>,
                      {
                        $addToSet: { <field>: { $each: [ <value1>, <value2> ... ] } }
                      }
                    )
    '''
    EACH = '$each'

    '''
    find( { array: { $elemMatch: { value1: 1, value2: { $gt: 1 } } } } )
    '''
    ELEM_MATCH = '$elemMatch'

    '''
    usage:{ field: { $exists: <boolean> } }
    '''
    EXISTS = '$exists'

    '''
    find().explain()
    '''
    EXPLAIN = '$explain'

    '''
    find( { <location field> :
                         { $geoIntersects :
                            { $geometry :
                               { type : "<GeoJSON object type>" ,
                                 coordinates : [ <coordinates> ]
                      } } } } )
    '''
    GEO_INTERSECTS = '$geoInteresects'

    '''
    find( { <location field> :
                         { $geoWithin :
                            { $geometry :
                               { type : "Polygon" ,
                                 coordinates : [ [ [ <lng1>, <lat1> ] , [ <lng2>, <lat2> ] ... ] ]
                      } } } } )
    '''
    GEO_WITHIN = '$geoWith'

    '''
    '''
    GEO_METRY = '$geometry'

    '''
    find( { qty: { $gt: 20 } } )
    '''
    GT = '$gt'

    '''
    find( { qty: { $gte: 20 } } )
    '''
    GTE = '$gte'

    '''
    find().hint( { age: 1 } )
    returns all documents using the index on the age field.
    '''
    HINT = '$hint'

    '''
    { field: { $in: [<value1>, <value2>, ... <valueN> ] } }
    '''
    IN = '$in'

    '''
    update:{ field: value },{ $inc: { field1: amount } }
    '''
    INC = '$inc'

    '''
    update( { field1 : 1 , $isolated : 1 }, { $inc : { field2 : 1 } } , { multi: true } )
    '''
    ISOLATED = '$isolated'

    '''
    find( { qty: { $lt: 20 } } )
    '''
    LT = '$lt'

    '''
    find( { qty: { $lte: 20 } } )
    '''
    LTE = '$lte'

    '''
    find( { <query> } ).max( { field1: <max value>, ... fieldN: <max valueN> } )
    '''
    MAX = '$max'

    '''
    find( { loc : { $near : [ 100 , 100 ] ,
                          $maxDistance: 10 }
                } )
    '''
    MAX_DISTANCE = '$maxDistance'

    '''
    db.collection.find( { <query> } )._addSpecial( "$maxScan" , <number> )
    db.collection.find( { $query: { <query> }, $maxScan: <number> } )
    '''
    MAX_SCAN = '$maxScan'

    '''
    db.collection.find( { <query> } ).min( { field1: <min value>, ... fieldN: <min valueN>} )
    '''
    MIN = '$min'

    '''
    { field: { $mod: [ divisor, remainder ]} }
    '''
    MOD = '$mod'

    '''
    db.collection.sort( { $natural: 1 } ) 
    return documents in the order they exist on disk:
    '''
    NATURAL = '$natural'

    '''
     {field: {$ne: value} }
    '''
    NE = '$ne'

    '''
    find( { <location field> :
                         { $near :
                            { $geometry :
                                { type : "Point" ,
                                  coordinates : [ <longitude> , <latitude> ] } ,
                              $maxDistance : <distance in meters>
                      } } } )
    '''
    NEAR = '$near'

    '''
    find( { <location field> :
                         { $nearSphere :
                           { $geometry :
                              { type : "Point" ,
                                coordinates : [ <longitude> , <latitude> ] } ,
                             $maxDistance : <distance in meters>
                   } } } )
    '''
    NEAR_SPHERE = '$nearSphere'

    '''
    { field: { $nin: [<value1>, <value2>, ... <valueN> ] } }
    '''
    NIN = '$nin'

    '''
    { $nor: [ { <expression1> }, { <expression2> }, ...  { <expressionN> } ] }
    '''
    NOR = '$nor'

    '''
     { field: { $not: { <operator-expression> } } }
    '''
    NOT = '$not'

    '''
    { $or: [ { <expression1> }, { <expression2> }, ... , { <expressionN> } ] }
    '''
    OR = '$or'

    '''
    db.collection.find()._addSpecial( "$orderby", { age : -1 } )
    db.collection.find( { $query: {}, $orderby: { age : -1 } } )
    '''
    ORDER_BY = '$orderby'

    '''
    { <location field> : { $geoWithin : { $polygon : [ [ <x1> , <y1> ] ,
                                                   [ <x2> , <y2> ] ,
                                                   [ <x3> , <y3> ] ] } } }
    '''
    POLY_GON = '$polygon'

    '''
    update( {field: value }, { $pop: { field: 1 } } )
    usage:{$pop:{field:-1}} remove last record order by field asc
    {$pop:{field:1}} remove first record order by field asc
    '''
    POP = '$pop'

    '''
    update( { field: value }, { $pull: { field: value1 } } )
    usage:{$pullAll:{field:value}} remove record where field's value is value
    '''
    PULL = '$pull'

    '''
    update( { field: value }, { $pullAll: { field1: [ value1, value2, value3 ] } } )
    usage:{$pullAll:{field:value_array}} remove record where field's value in value_array
    '''
    PULL_ALL = '$pullall'

    '''
    update( <query>,
                      { $push: { <field>: <value> } }
                   )
    usage:{$push:{field:value}} append
    '''
    PUSH = '$push'

    '''
    update( { field: value }, { $pushAll: { field1: [ value1, value2, value3 ] } } )
    usage:{$pushall:{field:value_array}}
    '''
    PUSH_ALL = '$pushall'

    '''
    find( { $query: { age : 25 } } )
    '''
    QUERY = '$query'

    '''
    find( { field: { $regex: 'acme.*corp', $options: 'i' } } )
    '''
    REGEX = '$regex'

    '''
    {$rename: { <old name1>: <new name1>, <old name2>: <new name2>, ... } }
    The new field name must differ from the existing field name.
    '''
    RENAME = '$rename'

    '''
    db.collection.find( { <query> } )._addSpecial( "$returnKey", true )
    db.collection.find( { $query: { <query> }, $returnKey: true } )
    '''
    RETURN_KEY = '$returnKey'

    '''
    update( { field: value1 }, { $set: { field1: value2 } } )
    '''
    SET = '$set'

    '''
    update( <query>,
                      { $setOnInsert: { <field1>: <value1>, ... } },
                      { upsert: true }
                    )
    '''
    SET_ON_INSERT = '$setOnInsert'

    '''
    db.collection.find().showDiskLoc()
    '''
    SHOW_DISK_LOC = '$showDiskLoc'

    '''
    find( { field: { $size: 2 } } )
    '''
    SIZE = '$size'

    '''
    update( <query>,
                      { $push: {
                                 <field>: {
                                            $each: [ <value1>, <value2>, ... ],
                                            $slice: <num>
                                          }
                               }
                      }
                    )
    '''
    SLICE = '$slice'

    '''
    db.collection.find().snapshot()
    '''
    SNAPSHOT = '$snapshot'

    '''
    update( <query>,
                      { $push: {
                                 <field>: {
                                            $each: [ <document1>,
                                                     <document2>,
                                                     ...
                                                   ],
                                            $slice: <num>,
                                            $sort: <sort document>,
                                          }
                               }
                      }
                    )
    '''
    SORT = '$sort'
    '''
     { field: { $type: <BSON type> } }
     Type    Number
    Double    1
    String    2
    Object    3
    Array    4
    Binary data    5
    Object id    7
    Boolean    8
    Date    9
    Null    10
    Regular Expression    11
    JavaScript    13
    Symbol    14
    JavaScript (with scope)    15
    32-bit integer    16
    Timestamp    17
    64-bit integer    18
    Min key    255
    Max key    127
     '''
    TYPE = '$type'

    UNIQUE_DOCS = '$uniqueDocs'

    '''
    update( { field: value1 }, { $unset: { field1: "" } } )
    usage:{'$unset':{field:1}}
    '''
    UNSET = '$unset'

    WHERE = '$where'


class AggregationOperator(object):
    ADD = '$add'

    ADD_TO_SET = '$addToSet'

    AND = '$and'

    AVG = '$avg'

    CMP = '$cmp'

    CONCAT = '$concat'

    COND = '$cond'

    DAY_OF_MONTH = '$dayOfMonth'

    DAY_OF_WEEK = '$dayOfWeek'

    DAY_OF_YEAR = 'dayOfYear'

    DIVIDE = '$divide'

    EQ = '$eq'

    FIRST = '$first'

    GEO_NEAR = '$geoNear'

    GROUP = '$group'

    GT = '$gt'

    GTE = '$gte'

    HOUR = '$hour'

    IF_NULL = '$ifNull'

    LAST = '$last'

    LIMIT = '$limit'

    LT = '$lt'

    LTE = '$lte'

    MATCH = '$match'

    MAX = '$max'

    MILLI_SECOND = '$millisecond'

    MIN = '$min'

    MINUTE = '$minute'

    MOD = '$mod'

    MONTH = '$month'

    MULTIPLY = '$multiply'

    NE = '$ne'

    NOT = '$not'

    OR = '$or'

    PROJECT = '$project'

    PUSH = '$push'

    SECOND = '$second'

    SKIP = '$skip'

    SORT = '$sort'

    STRCASECMP = '$strcasecmp'

    SUBSTR = '$substr'

    SUBTRACT = '$subtract'

    SUM = '$sum'

    TO_LOWER = '$toLower'

    TO_UPPER = '$toUpper'

    UNWIND = '$unwind'

    WEEK = '$week'

    YEAR = '$year'