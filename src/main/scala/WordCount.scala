import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat_ws, explode, lower, regexp_replace, split, trim}

object WordCount {
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val stopWordsFile = args(2)
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits.StringToColumn
    val stopWords = spark.read.option("header", "true").textFile(stopWordsFile).collect()
    spark
      .read
      .option("header", "true")
      .csv(input)
      .select(concat_ws(" ", $"class", $"comment") as "docs")
      .select(split($"docs", "\\s+") as "words")
      .select(explode($"words") as "word")
      .select(trim($"word") as "word")
      .select(regexp_replace($"word", "\\pP", "") as "word")
      .select(lower($"word") as "word")
      .filter(row => filterPair(row.get(row.fieldIndex("word")).toString, stopWords))
      .groupBy($"word")
      .count()
      .select($"count", $"word")
      .sort($"count".asc)
      .write
      .mode("overwrite")
      .csv(output)
    spark.stop()
  }

  def filterPair(word: String, stopWordsLines: Array[String]): Boolean = {
    for (stopWordsLine <- stopWordsLines) {
      val splitedLines = stopWordsLine.trim().replaceAll("\\pP", "").toLowerCase().split("\\s")
      for (stopWord <- splitedLines) {
        if (stopWord.equals(word)) return false
      }
    }
    true
  }
}
