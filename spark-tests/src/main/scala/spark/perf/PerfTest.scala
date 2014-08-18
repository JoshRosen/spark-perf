package spark.perf

import org.json4s._

trait PerfTest {
  /** Initialize internal state based on arguments */
  def initialize(args: Array[String])

  /** Create stored or otherwise materialized input data */
  def createInputData()

  /**
   * Runs the test and returns a JSON object that captures performance metrics, such as time taken
   * or training accuracy.
   *
   * @return A sequence of JSON results containing per-run metrics (e.g. ("time" -> time))
   */
  def run(): Seq[JValue]
}
