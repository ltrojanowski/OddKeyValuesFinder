package com.ltrojanowski.oddkeyvaluesfinder

/*
The complexity of the solution is O(n)
 */
object Logic {

  def numberOfOccurrencesPerNumber(values: Iterable[Int]): Map[Int, Int] = {
    values.foldLeft(Map.empty[Int, Int]) {
      case (acc: Map[Int, Int], value: Int) => {
        val currentCount = acc.getOrElse(value, 0)
        val newAcc       = acc.+((value, currentCount + 1))
        newAcc
      }
    }
  }

  def keyOfOddValue(valuesCount: Map[Int, Int]): Int = {
    valuesCount
      .find { case (number, count) => count % 2 != 0 }
      .getOrElse(throw new Exception("No value for key occuress odd number of times"))
      ._1
  }

  def findNumberOccurringOddNumberOfTimes(values: Iterable[Int]): Int = {
    val valuesCount = numberOfOccurrencesPerNumber(values)
    keyOfOddValue(valuesCount)
  }

}
