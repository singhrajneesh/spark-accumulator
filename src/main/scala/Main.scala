import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark-accumulator").master("local[*]")
      .getOrCreate()
    //this is code to count the "=" in the file present without collect option
    val accumulator = spark.sparkContext.longAccumulator("accum")
    var withoutCollectCounter=0
    spark.sparkContext.textFile("C:\\Users\\r289\\Desktop\\spark-accumulator\\src\\main\\scala\\Main.scala")
      .foreach(x => {
        if (x.contains("="))
          {
            accumulator.add(1)
            withoutCollectCounter=withoutCollectCounter+1
          }
      })
    println("this is accumalator value "+accumulator.value)
    println("this is the counter value without collect " + withoutCollectCounter)


    //this is code to count the "=" in the file present with collect option
    var withCollectCounter=0
    spark.sparkContext.textFile("C:\\Users\\r289\\Desktop\\spark-accumulator\\src\\main\\scala\\Main.scala")
      .collect.foreach(x => {
        if (x.contains("="))
          withCollectCounter=withCollectCounter+1
      })
     println("this is the counter value with collect " + withCollectCounter)

  }
}