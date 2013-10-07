package spark.graph.util

object AggregationFunctions extends Serializable {

  def minAggr[A <% Ordered[A]](vertexValue: A, msgs: Seq[A]): A = {
    var minValue = vertexValue
    println("starting minValue: " + minValue + " msgs: " + msgs)
    for (msg <- msgs) {
      println("msg: " + msg)
      print("min of minValue: " + minValue + " msg: " + msg + " is: ")
      if (msg < minValue) {
        minValue = msg
      }
      println("" + minValue)
    }
    return minValue
  }

  def maxAggr[A <% Ordered[A]](vertexValue: A, msgs: Seq[A]): A = {
    var maxValue = vertexValue
    println("starting maxValue: " + maxValue + " msgs: " + msgs)
    for (msg <- msgs) {
      println("msg: " + msg)
      print("max of maxValue: " + maxValue + " msg: " + msg + " is: ")
      if (msg > maxValue) {
        maxValue = msg
      }
      println("" + maxValue)
    }
    return maxValue
  }
  
  def minAggrSeq[A <% Ordered[A]](msgs: Seq[A]): A = {
    var minValue = msgs(0)
    println("starting minValue: " + minValue + " msgs: " + msgs)
    for (index <- 1 to (msgs.length - 1)) {
      val msgIndex = msgs(index)
      println("msg: " + msgs(index))
      print("min of maxValue: " + minValue + " msg: " + msgIndex + " is: ")
      if (msgIndex < minValue) {
        minValue = msgIndex
      }
      println("" + minValue)
    }
    return minValue
  }
  
  def maxAggrSeq[A <% Ordered[A]](msgs: Seq[A]): A = {
    var maxValue = msgs(0)
    println("starting maxValue: " + maxValue + " msgs: " + msgs)
    for (index <- 1 to (msgs.length - 1)) {
      val msgIndex = msgs(index)
      println("msg: " + msgs(index))
      print("max of maxValue: " + maxValue + " msg: " + msgIndex + " is: ")
      if (msgIndex > maxValue) {
        maxValue = msgIndex
      }
      println("" + maxValue)
    }
    return maxValue
  }
}