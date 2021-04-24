import org.apache.spark.util.AccumulatorV2

class CustomAccumulator(var accumulatorWithTwoArgument: AccumulatorWithTwoArgument) extends AccumulatorV2[AccumulatorWithTwoArgument,AccumulatorWithTwoArgument]{

  override def isZero: Boolean = true
    //if(accumulatorWithTwoArgument.equalToCounter!=0 && accumulatorWithTwoArgument.commaCounter!=0) true else false

  override def copy(): AccumulatorV2[AccumulatorWithTwoArgument, AccumulatorWithTwoArgument] = new CustomAccumulator(accumulatorWithTwoArgument)

  override def reset(): Unit = accumulatorWithTwoArgument.reset()

  override def add(v: AccumulatorWithTwoArgument): Unit = accumulatorWithTwoArgument.add(v)

  override def merge(other: AccumulatorV2[AccumulatorWithTwoArgument, AccumulatorWithTwoArgument]): Unit =accumulatorWithTwoArgument.add(other.value)

  override def value: AccumulatorWithTwoArgument = accumulatorWithTwoArgument
}
