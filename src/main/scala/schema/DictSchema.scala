package schema

import fs2.{Pure, Stream}
import schema.DictSchema.Weights

import scala.collection.immutable.HashMap
import scala.util.Random


object DictSchema {
  type Weights[S] = HashMap[Vector[S], HashMap[S, Int]]

  def apply[S : StringSerializable](ngrams: Stream[Pure, Vector[S]]): DictSchema[S] = {
    val weights = ngrams.fold(
      new Weights[S]()
    )((weights, ngram) => {
      val successors = weights.getOrElse(ngram.init, new HashMap[S, Int]())
      val count = successors.getOrElse(ngram.last, 0)
      val newSuccessors = successors.updated(ngram.last, count + 1)
      weights.updated(ngram.init, newSuccessors)
    }).toVector.head

    val seeds = weights.size match {
      case 0 => Vector()
      case _ => Vector(ngrams.head.toVector.head.head)
    }

    new DictSchema(weights, seeds)
  }
}

class DictSchema[S : StringSerializable](val weights: Weights[S], val seeds: Seq[S]) extends Schema[S] {
  def +(other: Schema[S]): DictSchema[S] = {
    val otherAsDict = other.toDictSchema

    val newWeights = weights.merged(otherAsDict.weights) { case ((firstWord, thisSuccessors), (_, otherSuccessors)) =>
      firstWord -> thisSuccessors.merged(otherSuccessors) { case ((secondWord, thisCount), (_, otherCount)) =>
        secondWord -> (thisCount + otherCount)
      }
    }

    new DictSchema(newWeights, seeds ++ otherAsDict.seeds)
  }

  override def successorsOf(tokens: Vector[S]): Option[HashMap[S, Int]] = {
    weights.get(tokens)
  }

  override def getSeed(random: Random): Option[S] = {
    seeds.size match {
      case 0 => None
      case n => Some(seeds(random.nextInt(n)))
    }
  }

  override def toDictSchema: DictSchema[S] = this

  override def toFileSchema: FileSchema[S] = {
    FileSchema("default.schema", this)
  }

  override def toString: String = {
    val sortedWeights = weights.toVector.sortBy { case (ngram, _) => (-ngram.length, ngram.mkString) }
    sortedWeights.map { case (ngram, successors) =>
      val ngramString = ngram.mkString("(", ", ", "):")

      val successorsSorted = successors.toVector.sortBy(_._2).reverse
      val successorsStrings = successorsSorted.map { case (token, count) => s"\t$token => $count" }.toList

      (ngramString :: successorsStrings).mkString("\n")
    }.mkString("\n")
  }
}
