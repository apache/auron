/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.auron.integration

import java.io.File

import org.apache.spark.sql.auron.Shims
import scopt.OParser

import org.apache.auron.integration.runner.TPCDSSuite

object Main {
  val parser = {
    val builder = OParser.builder[SuiteArgs]
    import builder._
    OParser.sequence(
      programName("auron-it"),
      head("auron-it", "v1.0"),
      opt[String]('t', "type")
        .action((x, c) => c.copy(benchType = x))
        .required()
        .text("benchmark type (tpcds)"),
      opt[String]('d', "data-location")
        .action((x, c) => c.copy(dataLocation = x))
        .required()
        .text("data directory path"),
      opt[String]('q', "query-filter")
        .action((x, c) => c.copy(queryFilter = x.split(",").map(_.trim).filter(_.nonEmpty).toSeq))
        .text("query filter (e.g. q1,q2,q3); empty for all queries"),
      opt[String]("conf")
        .unbounded()
        .valueName("k=v")
        .action { (x, c) =>
          val Array(k, v) = x.split("=", 2)
          c.copy(extraSparkConf = c.extraSparkConf + (k -> v))
        }
        .validate { x =>
          if (x.contains("=")) Right(()) else Left(s"--conf expects k=v, got: $x")
        }
        .text("Spark configuration, repeatable: --conf k=v --conf a=b"),
      opt[Unit]("auron-only")
        .action((_, c) => c.copy(auronOnly = true))
        .text("run Auron only; skip baseline and result comparison (default: false)"),
      opt[Unit]("result-check")
        .action((_, c) => c.copy(auronOnly = false))
        .text("enable query result check (default: enabled)"),
      opt[Unit]("plan-check")
        .action((_, c) => c.copy(enablePlanCheck = true))
        .text("enable plan stability check(default: false)"),
      opt[Unit]("regen-golden")
        .action((_, c) => c.copy(regenGoldenFiles = true))
        .text("regenerate golden files"),
      opt[Unit]("print-plan")
        .action((_, c) => c.copy(printPlan = true))
        .text("print the Auron physical plan of every query (default: false)"),
      help('h', "help"))
  }

  def parseArgs(args: Array[String]): Option[SuiteArgs] = {
    OParser.parse(parser, args, SuiteArgs())
  }

  // scalastyle:off
  def main(mainArgs: Array[String]): Unit = {
    parseArgs(mainArgs) match {
      case Some(args) =>
        if (!new File(args.dataLocation).exists()) {
          println(s"Error: Data location ${args.dataLocation} does not exist.")
          sys.exit(1)
        }

        printConfigurationSummary(args)

        val suite = createSuite(args)
        var exitCode = 0
        try {
          exitCode = suite.run()
        } finally {
          suite.close()
        }

        if (exitCode != 0) {
          println("Integration tests failed.")
          sys.exit(exitCode)
        } else {
          println("Integration tests completed successfully.")
        }
      case None =>
        sys.exit(1)
    }
  }

  private def createSuite(args: SuiteArgs): Suite = args.benchType match {
    case "tpcds" => TPCDSSuite(args)
    case other =>
      println(s"Unsupported benchmark type: $other")
      sys.exit(1)
  }

  private def printConfigurationSummary(args: SuiteArgs): Unit = {
    println("\n" + "=" * 60)
    println(s"""|Auron Integration Test (type: ${args.benchType})
               |Spark Version: ${Shims.get.shimVersion}
               |Data: ${args.dataLocation}
               |Queries: [${args.queryFilter.mkString(", ")}] (${if (args.queryFilter.isEmpty)
                 "all"
               else args.queryFilter.length} queries)
               |Extra Spark Conf: ${args.extraSparkConf}""".stripMargin)

    if (args.auronOnly) println("Mode: Auron-only (skip baseline)")
    if (args.enablePlanCheck) println("Plan Check: Enabled")
    if (args.regenGoldenFiles) println("Regenerate golden files: Enabled")
    printClassLoaderSummary()
    println("-" * 60)
    println("")
  }

  /**
   * Reports which loader actually defined Auron. Auron classes reach the JVM either through the
   * application class loader (a jar in $SPARK_HOME/jars) or through Spark's MutableURLClassLoader
   * (spark-submit --jars, and the application jar itself). The two behave differently during
   * deserialization, so tests covering class-loading defects such as AURON #2386 need to state
   * which arrangement they ran under rather than infer it from how the job was set up.
   */
  private def printClassLoaderSummary(): Unit = {
    val auronClass = Shims.get.getClass
    val loader = auronClass.getClassLoader
    val codeSource = Option(auronClass.getProtectionDomain.getCodeSource)
      .map(_.getLocation.toString)
      .getOrElse("unknown")
    println(s"Auron Class Loader: ${loader.getClass.getName}")
    println(s"Auron Code Source: $codeSource")
    println(s"Auron On System Classpath: ${loader eq ClassLoader.getSystemClassLoader}")
  }
}
// scalastyle:on
