case class AccumulatorWithTwoArgument(var equalToCounter: Int, var commaCounter: Int) extends Serializable {
  def reset(): Unit = {
    equalToCounter = 0
    commaCounter = 0
  }

  def add(p: AccumulatorWithTwoArgument): AccumulatorWithTwoArgument = {
    equalToCounter = equalToCounter + p.equalToCounter
    commaCounter = commaCounter + p.commaCounter
    this
  }

}
