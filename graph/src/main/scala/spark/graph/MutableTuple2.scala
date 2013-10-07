package spark.graph

case class MutableTuple2[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) U,
                         @specialized(Char, Int, Boolean, Byte, Long, Float, Double) V](
  var _1: U, var _2: V)
