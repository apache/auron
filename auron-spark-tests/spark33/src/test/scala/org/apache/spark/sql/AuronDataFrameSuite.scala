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
package org.apache.spark.sql

import java.io.ByteArrayOutputStream

import scala.util.Random

import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression}
import org.apache.spark.sql.execution.auron.plan.{NativeAggExec, NativeShuffleExchangeExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestData.TestData2

class AuronDataFrameSuite extends DataFrameSuite with SparkQueryTestsBase {

  testAuron("repartitionByRange") {
    val partitionNum = 10
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> partitionNum.toString) {
      import testImplicits._
      val data1d = Random.shuffle(0.to(partitionNum - 1))
      val data2d = data1d.map(i => (i, data1d.size - i))

      checkAnswer(
        data1d
          .toDF("val")
          .repartitionByRange(data1d.size, $"val".asc)
          .select(spark_partition_id().as("id"), $"val"),
        data1d.map(i => Row(i, i)))

      checkAnswer(
        data1d
          .toDF("val")
          .repartitionByRange(data1d.size, $"val".desc)
          .select(spark_partition_id().as("id"), $"val"),
        data1d.map(i => Row(i, data1d.size - 1 - i)))

      checkAnswer(
        data1d
          .toDF("val")
          .repartitionByRange(data1d.size, lit(42))
          .select(spark_partition_id().as("id"), $"val"),
        data1d.map(i => Row(0, i)))

      checkAnswer(
        data1d
          .toDF("val")
          .repartitionByRange(data1d.size, lit(null), $"val".asc, rand())
          .select(spark_partition_id().as("id"), $"val"),
        data1d.map(i => Row(i, i)))

      checkAnswer(
        data2d
          .toDF("a", "b")
          .repartitionByRange(data2d.size, $"a".desc, $"b")
          .select(spark_partition_id().as("id"), $"a", $"b"),
        data2d
          .toDF("a", "b")
          .repartitionByRange(data2d.size, $"a".desc, $"b".asc)
          .select(spark_partition_id().as("id"), $"a", $"b"))

      intercept[IllegalArgumentException] {
        data1d.toDF("val").repartitionByRange(data1d.size)
      }
      intercept[IllegalArgumentException] {
        data1d.toDF("val").repartitionByRange(data1d.size, Seq.empty: _*)
      }
    }
  }

  testAuron("distributeBy and localSort") {
    import testImplicits._
    val data = spark.sparkContext.parallelize((1 to 100).map(i => TestData2(i % 10, i))).toDF()

    var partitionNum = 1
    val original = testData.repartition(partitionNum)
    assert(original.rdd.partitions.length == partitionNum)

    val df6 = data.repartition(partitionNum, $"a").sortWithinPartitions("b")
    df6.rdd.foreachPartition { p =>
      var previousValue: Int = -1
      var allSequential: Boolean = true
      p.foreach { r =>
        val v: Int = r.getInt(1)
        if (previousValue != -1) {
          if (previousValue > v) throw new Exception("Partition is not ordered.")
          if (v - 1 != previousValue) allSequential = false
        }
        previousValue = v
      }
      if (!allSequential) {
        throw new Exception("Partition should contain all sequential values")
      }
    }

    partitionNum = 5
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> partitionNum.toString) {
      val df = original.repartition(partitionNum, $"key")
      assert(df.rdd.partitions.length == partitionNum)
      checkAnswer(original.select(), df.select())

      val df4 = data.repartition(partitionNum, $"a").sortWithinPartitions($"b".desc)
      df4.rdd.foreachPartition { p =>
        if (p.hasNext) {
          var previousValue: Int = -1
          var allSequential: Boolean = true
          p.foreach { r =>
            val v: Int = r.getInt(1)
            if (previousValue != -1) {
              if (previousValue < v) throw new Exception("Partition is not ordered.")
              if (v + 1 != previousValue) allSequential = false
            }
            previousValue = v
          }
          if (allSequential) throw new Exception("Partition should not be globally ordered")
        }
      }
    }

    partitionNum = 10
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> partitionNum.toString) {
      val df2 = original.repartition(partitionNum, $"key")
      assert(df2.rdd.partitions.length == partitionNum)
      checkAnswer(original.select(), df2.select())
    }

    val df3 = testData.repartition($"key").groupBy("key").count()
    verifyNonExchangingAgg(df3)
    verifyNonExchangingAgg(
      testData
        .repartition($"key", $"value")
        .groupBy("key", "value")
        .count())

    verifyExchangingAgg(
      testData
        .repartition($"key", $"value")
        .groupBy("key")
        .count())

    partitionNum = 2
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> partitionNum.toString) {
      val df5 = data.repartition(partitionNum, $"a").sortWithinPartitions($"b".asc, $"a".asc)
      df5.rdd.foreachPartition { p =>
        var previousValue: Int = -1
        var allSequential: Boolean = true
        p.foreach { r =>
          val v: Int = r.getInt(1)
          if (previousValue != -1) {
            if (previousValue > v) throw new Exception("Partition is not ordered.")
            if (v - 1 != previousValue) allSequential = false
          }
          previousValue = v
        }
        if (allSequential) throw new Exception("Partition should not be all sequential")
      }
    }
  }

  testAuron("reuse exchange") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2") {
      val df = spark.range(100).toDF()
      val join = df.join(df, "id")
      checkAnswer(join, df)
      val shuffleCount = collect(join.queryExecution.executedPlan) {
        case e: NativeShuffleExchangeExec =>
          true
      }.size
      assert(shuffleCount === 1, s"Expected 1 shuffle exchange, got $shuffleCount")
      assert(collect(join.queryExecution.executedPlan) { case e: ReusedExchangeExec =>
        true
      }.size === 1)
      val broadcasted = broadcast(join)
      val join2 = join.join(broadcasted, "id").join(broadcasted, "id")
      checkAnswer(join2, df)
      val shuffleCount2 = collect(join2.queryExecution.executedPlan) {
        case e: NativeShuffleExchangeExec =>
          true
      }.size
      assert(shuffleCount2 == 1, s"Expected 1 shuffle exchange in join2, got $shuffleCount2")
      assert(collect(join2.queryExecution.executedPlan) { case e: ReusedExchangeExec =>
        true
      }.size == 4)
    }
  }

  testAuron("SPARK-22520: support code generation for large CaseWhen") {
    import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
    val N = 30
    var expr1 = when(equalizer($"id", lit(0)), 0)
    var expr2 = when(equalizer($"id", lit(0)), 10)
    (1 to N).foreach { i =>
      expr1 = expr1.when(equalizer($"id", lit(i)), -i)
      expr2 = expr2.when(equalizer($"id", lit(i + 10)), i)
    }
    val df = spark.range(1).select(expr1, expr2.otherwise(0))
    checkAnswer(df, Row(0, 10) :: Nil)
  }

  testAuron("SPARK-27439: Explain result should match collected result after view change") {
    withTempView("test", "test2", "tmp") {
      spark.range(10).createOrReplaceTempView("test")
      spark.range(5).createOrReplaceTempView("test2")
      spark.sql("select * from test").createOrReplaceTempView("tmp")
      val df = spark.sql("select * from tmp")
      spark.sql("select * from test2").createOrReplaceTempView("tmp")

      val captured = new ByteArrayOutputStream()
      Console.withOut(captured) {
        df.explain(extended = true)
      }
      checkAnswer(df, spark.range(10).toDF)
      val output = captured.toString
      assert(output.contains("""== Parsed Logical Plan ==
                               |'Project [*]
                               |+- 'UnresolvedRelation [tmp]""".stripMargin))
    }
  }

  private def withExpr(newExpr: Expression): Column = new Column(newExpr)

  def equalizer(expr: Expression, other: Any): Column = withExpr {
    val right = lit(other).expr
    if (expr == right) {
      logWarning(
        s"Constructing trivially true equals predicate, '$expr = $right'. " +
          "Perhaps you need to use aliases.")
    }
    EqualTo(expr, right)
  }

  private def verifyNonExchangingAgg(df: DataFrame): Unit = {
    var atFirstAgg: Boolean = false
    df.queryExecution.executedPlan.foreach {
      case _: NativeAggExec =>
        atFirstAgg = !atFirstAgg
      case _ =>
        if (atFirstAgg) {
          fail("Should not have operators between the two aggregations")
        }
    }
  }

  private def verifyExchangingAgg(df: DataFrame): Unit = {
    var atFirstAgg: Boolean = false
    df.queryExecution.executedPlan.foreach {
      case _: NativeAggExec =>
        if (atFirstAgg) {
          fail("Should not have back to back Aggregates")
        }
        atFirstAgg = true
      case _: NativeShuffleExchangeExec =>
        atFirstAgg = false
      case _ =>
    }
  }
}
