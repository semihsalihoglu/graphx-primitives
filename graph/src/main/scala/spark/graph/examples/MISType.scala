package spark.graph.examples

sealed abstract class MISType {
  def reverse: MISType = this match {
    case MISType.InSet   => MISType.InSet
    case MISType.NotInSet  => MISType.NotInSet
    case MISType.Unknown => MISType.Unknown
  }
}

object MISType {
  case object InSet extends MISType
  case object NotInSet extends MISType
  case object Unknown extends MISType
}