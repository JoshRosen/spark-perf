package spark.perf

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object TestRunner {
  def main(args: Array[String]) {
    if (args.size < 1) {
      println(
        "spark.perf.TestRunner requires 1 or more args, you gave %s, exiting".format(args.size))
      System.exit(1)
    }
    val testName = args(0)
    val perfTestArgs = args.slice(1, args.length)
    val sc = new SparkContext(new SparkConf().setAppName("TestRunner: " + testName))

    val test: PerfTest =
      testName match {
        case "aggregate-by-key" => new AggregateByKey(sc)
        case "aggregate-by-key-int" => new AggregateByKeyInt(sc)
        case "aggregate-by-key-naive" => new AggregateByKeyNaive(sc)
        case "sort-by-key" => new SortByKey(sc)
        case "sort-by-key-int" => new SortByKeyInt(sc)
        case "count" => new Count(sc)
        case "count-with-filter" => new CountWithFilter(sc)
        case "scheduling-throughput" => new SchedulerThroughputTest(sc)
    }
    test.initialize(perfTestArgs)
    test.createInputData()
    val (options, results) = test.run()
    val json: JValue =
      ("options" -> options) ~
      ("sparkConf" -> sc.getConf.getAll.toMap) ~
      ("sparkVersion" -> sc.version) ~
      ("systemProperties" -> System.getProperties.asScala.toMap) ~
      ("results" -> results)
    println("results: " + compact(render(json)))
  }
}
