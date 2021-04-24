import org.apache.spark.sql.SparkSession


object MainRunCustomAccumulator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CustomAccumulator")
      .master("local[*]").getOrCreate()

    val accum = new CustomAccumulator(AccumulatorWithTwoArgument(0, 0))

    spark.sparkContext.register(accum,"custAccumulator")

    var withoutCollectEqualCounter = 0
    var withoutCollectCommaCounter = 0

    spark.sparkContext.textFile("C:\\Users\\r289\\Desktop\\spark-accumulator\\src\\main\\scala\\MainRunCustomAccumulator.scala")
      .foreach(x => {
        if (x.contains("=")) {
          accum.add(AccumulatorWithTwoArgument(1,0))
          withoutCollectEqualCounter = withoutCollectEqualCounter + 1
        }
        if(x.contains(",")){
          accum.add(AccumulatorWithTwoArgument(0,1))
          withoutCollectCommaCounter = withoutCollectCommaCounter + 1
        }
      })
    println("this is equal accumalator value " + accum.value.equalToCounter)
    println("this is comma accumalator value " + accum.value.commaCounter)
    println("this is the equal counter value without collect " + withoutCollectEqualCounter)
    println("this is the comma counter value without collect " + withoutCollectCommaCounter)
  }
}
