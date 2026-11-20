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
package org.apache.spark.sql.execution.auron.plan

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.Future
import java.util.concurrent.TimeoutException
import java.util.concurrent.TimeUnit

import scala.collection.immutable.SortedMap
import scala.concurrent.Promise
import scala.jdk.CollectionConverters._

import org.apache.spark.OneToOneDependency
import org.apache.spark.Partition
import org.apache.spark.SparkException
import org.apache.spark.TaskContext
import org.apache.spark.broadcast
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.auron.NativeConverters
import org.apache.spark.sql.auron.NativeHelper
import org.apache.spark.sql.auron.NativeRDD
import org.apache.spark.sql.auron.NativeSupports
import org.apache.spark.sql.auron.Shims
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.InterpretedUnsafeProjection
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.plans.physical.BroadcastPartitioning
import org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.auron.plan.NativeBroadcastExchangeBase.buildBroadcastData
import org.apache.spark.sql.execution.auron.shuffle.BlockObject
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.BinaryType

import org.apache.auron.{protobuf => pb, sparkver}
import org.apache.auron.jni.JniBridge
import org.apache.auron.metric.SparkMetricNode

abstract class NativeBroadcastExchangeBase(
    @transient mode: BroadcastMode,
    override val child: SparkPlan)
    extends BroadcastExchangeLike
    with NativeSupports {

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = BroadcastPartitioning(mode)

  def broadcastMode: BroadcastMode = this.mode

  protected val nativeSchema: pb.Schema = Util.getNativeSchema(output)
  private val broadcastSchema = mode match {
    case IdentityBroadcastMode => nativeSchema
    case _: HashedRelationBroadcastMode =>
      val hashMapOutput = output.map(_.withNullability(true)) :+
        AttributeReference("~TABLE", BinaryType, nullable = true)()
      Util.getNativeSchema(hashMapOutput)
  }

  def getRunId: UUID
  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(
        sparkContext,
        Set(
          "stage_id",
          "output_rows",
          "output_batches",
          "elapsed_compute",
          "build_hash_map_time",
          "probed_side_hash_time",
          "probed_side_search_time",
          "probed_side_compare_time",
          "build_output_time",
          "mem_spill_count",
          "mem_spill_size",
          "mem_spill_iotime",
          "disk_spill_size",
          "disk_spill_iotime",
          "shuffle_write_total_time",
          "shuffle_read_total_time",
          "input_batch_count",
          "input_row_count",
          "input_batch_mem_size"))
      .toSeq :+
      ("dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size")) :+
      ("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows")) :+
      ("collectTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to collect")) :+
      ("buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build")) :+
      ("broadcastTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to broadcast")): _*)

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "BroadcastExchange does not support the execute() code path.")
  }

  override def doPrepare(): Unit = {
    // Materialize the future.
    relationFuture
  }

  // Mixed native/Spark execution needs a second JVM broadcast; otherwise the transformed relation
  // would be serialized with every Spark task.
  // Build it under a separate lock because relationFuture initializes lazy fields on another thread.
  @transient
  private lazy val sparkBroadcastLock = new Object

  @transient
  private var sparkBroadcast: Broadcast[Any] = _

  private def createSparkBroadcast(): Broadcast[Any] = {
    val singlePartition = new Partition() {
      override def index: Int = 0
    }
    val broadcastReadNativePlan = doExecuteNative().nativePlan(singlePartition, null)
    val rowsIter = NativeHelper.executeNativePlan(
      broadcastReadNativePlan,
      SparkMetricNode(Map(), Nil, None),
      singlePartition,
      None)
    val pruneKeyField = new InterpretedUnsafeProjection(
      output.zipWithIndex
        .map(v => BoundReference(v._2, v._1.dataType, v._1.nullable))
        .toArray)

    val dataRows = rowsIter
      .map(pruneKeyField)
      .map(_.copy())
      .toArray

    sparkContext.broadcast(mode.transform(dataRows))
  }

  override def doExecuteBroadcast[T](): Broadcast[T] = sparkBroadcastLock.synchronized {
    if (sparkBroadcast == null) {
      sparkBroadcast = createSparkBroadcast()
    }
    sparkBroadcast.asInstanceOf[Broadcast[T]]
  }

  def doExecuteBroadcastNative[T](): broadcast.Broadcast[T] = {
    val timeout: Long = SQLConf.get.broadcastTimeout
    try {
      relationFuture.get(timeout, TimeUnit.SECONDS).asInstanceOf[broadcast.Broadcast[T]]
    } catch {
      case ex: TimeoutException =>
        logError(s"Could not execute broadcast in $timeout secs.", ex)
        if (!relationFuture.isDone) {
          sparkContext.cancelJobGroup(getRunId.toString)
          relationFuture.cancel(true)
        }
        throw new SparkException("Native broadcast exchange timed out.", ex)
    }
  }

  override def doExecuteNative(): NativeRDD = {
    val broadcast = doExecuteBroadcastNative[Array[Array[Byte]]]()
    val readerSchema = broadcastSchema
    val nativeMetrics = SparkMetricNode(metrics, Nil)
    val partitions = Array(new Partition() {
      override def index: Int = 0
    })

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions,
      None,
      Nil,
      rddShuffleReadFull = true,
      (_, context) => {
        val resourceId = s"ArrowBroadcastExchangeExec:${UUID.randomUUID()}"
        val provideIpcIterator = () => {
          broadcast.value.iterator.map(bytes =>
            new BlockObject {
              override def hasByteBuffer: Boolean = true
              override def getByteBuffer: ByteBuffer = ByteBuffer.wrap(bytes)
              override def close(): Unit = {}
            })
        }

        JniBridge.putResource(resourceId, () => provideIpcIterator())
        // A cached native build can finish the task without opening this reader.
        if (context != null) {
          context.addTaskCompletionListener[Unit](_ => JniBridge.getResource(resourceId))
        }
        pb.PhysicalPlanNode
          .newBuilder()
          .setIpcReader(
            pb.IpcReaderExecNode
              .newBuilder()
              .setSchema(readerSchema)
              .setNumPartitions(1)
              .setIpcProviderResourceId(resourceId)
              .build())
          .build()
      },
      friendlyName = "NativeRDD.BroadcastRead")
  }

  def collectNative(): Array[Array[Byte]] = {
    val inputRDD = NativeHelper.executeNative(child match {
      case child if NativeHelper.isNative(child) => child
      case child => Shims.get.createConvertToNativeExec(child)
    })
    val modifiedMetrics = metrics ++ Map("output_rows" -> metrics("numOutputRows"))
    val nativeMetrics = SparkMetricNode(modifiedMetrics, inputRDD.metrics :: Nil)

    val ipcRDD =
      new RDD[Array[Byte]](sparkContext, new OneToOneDependency(inputRDD) :: Nil) {
        setName("NativeRDD.BroadcastWrite")
        Shims.get.setRDDShuffleReadFull(this, inputRDD.isShuffleReadFull)

        override protected def getPartitions: Array[Partition] = inputRDD.partitions

        override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
          val resourceId = s"ArrowBroadcastExchangeExec.input:${UUID.randomUUID()}"
          val bos = new ByteArrayOutputStream()
          JniBridge.putResource(
            resourceId,
            (byteBuffer: ByteBuffer) => {
              val byteArray = new Array[Byte](byteBuffer.capacity())
              byteBuffer.get(byteArray)
              bos.write(byteArray)
              metrics("dataSize") += byteArray.length
            })

          val input = inputRDD.nativePlan(split, context)
          val nativeIpcWriterExec = pb.PhysicalPlanNode
            .newBuilder()
            .setIpcWriter(
              pb.IpcWriterExecNode
                .newBuilder()
                .setInput(input)
                .setIpcConsumerResourceId(resourceId)
                .build())
            .build()

          // execute ipc writer and fill output channels
          val iter =
            NativeHelper.executeNativePlan(
              nativeIpcWriterExec,
              nativeMetrics,
              split,
              Some(context))
          assert(iter.isEmpty)

          if (bos.size() == 0) Iterator.empty else Iterator.single(bos.toByteArray)
        }
      }

    val collectedData = ipcRDD.collect()
    mode match {
      case IdentityBroadcastMode =>
        NativeBroadcastExchangeBase.prepareRowBlocks(collectedData, nativeSchema)
      case mode: HashedRelationBroadcastMode =>
        buildBroadcastData(collectedData, mode.key, nativeSchema)
    }
  }

  @transient
  lazy val relationFuturePromise: Promise[Broadcast[Any]] = Promise[Broadcast[Any]]()

  @transient
  lazy val relationFuture: Future[Broadcast[Any]] = getRelationFuture

  @sparkver("3.0 / 3.1 / 3.2 / 3.3 / 3.4 / 3.5")
  private def getRelationFuture = {
    SQLExecution.withThreadLocalCaptured[Broadcast[Any]](
      Shims.get.getSqlContext(this).sparkSession,
      BroadcastExchangeExec.executionContext) {
      executeBroadcastJob()
    }
  }

  @sparkver("4.0 / 4.1 / 4.2")
  private def getRelationFuture = {
    SQLExecution.withThreadLocalCaptured[Broadcast[Any]](
      this.session.sqlContext.sparkSession,
      BroadcastExchangeExec.executionContext) {
      executeBroadcastJob()
    }
  }

  private def executeBroadcastJob(): Broadcast[Any] = {
    try {
      sparkContext.setJobGroup(
        getRunId.toString,
        s"native broadcast exchange (runId $getRunId)",
        interruptOnCancel = true)
      val broadcasted = sparkContext.broadcast(collectNative().asInstanceOf[Any])
      relationFuturePromise.trySuccess(broadcasted)
      broadcasted
    } catch {
      case e: Throwable =>
        relationFuturePromise.tryFailure(e)
        throw e
    }
  }

  override protected def doCanonicalize(): SparkPlan =
    Shims.get.createNativeBroadcastExchangeExec(mode.canonicalized, child.canonicalized)
}

object NativeBroadcastExchangeBase {

  def nativeExecutionTag: TreeNodeTag[Boolean] = TreeNodeTag("arrowBroadcastNativeExecution")

  private def prepareRowBlocks(
      blocks: Array[Array[Byte]],
      schema: pb.Schema): Array[Array[Byte]] = {
    if (blocks.length < 128) return packRowBlocks(blocks)
    val prepared = Array.newBuilder[Array[Byte]]
    var forwardFrom = 0
    var i = 0
    while (i < blocks.length) {
      val start = i
      while (i < blocks.length && blocks(i).length <= 4096) i += 1
      // Coalesce rows across long runs of tiny codec frames. Each rewrite reads at most
      // 16 MiB of compressed input and leaves larger partitions in their original encoding.
      if (i - start >= 128) {
        prepared ++= packRowBlocks(blocks.slice(forwardFrom, start))
        var groupStart = start
        while (groupStart < i) {
          val groupEnd = groupStart + math.min(i - groupStart, 4096)
          val group = packRowBlocks(blocks.slice(groupStart, groupEnd))
          prepared ++= rewriteBroadcastData(group, schema)(identity)
          groupStart = groupEnd
        }
        forwardFrom = i
      }
      if (i < blocks.length) i += 1
    }
    if (forwardFrom == 0) return packRowBlocks(blocks)
    prepared ++= packRowBlocks(blocks.drop(forwardFrom))
    prepared.result()
  }

  // Auron IPC frames are self-delimiting. Pack tiny partitions so executors can read several
  // frames per JNI block, and forward larger partitions without copying their contents.
  private def packRowBlocks(blocks: Array[Array[Byte]]): Array[Array[Byte]] = {
    if (blocks.length < 2) return blocks
    val targetSize = 64 * 1024
    val packed = Array.newBuilder[Array[Byte]]
    var start = 0
    while (start < blocks.length) {
      var end = start + 1
      var size = blocks(start).length
      while (end < blocks.length && blocks(end).length <= targetSize - size) {
        size += blocks(end).length
        end += 1
      }
      if (end == start + 1) {
        packed += blocks(start)
      } else {
        val bytes = new Array[Byte](size)
        var offset = 0
        var i = start
        while (i < end) {
          System.arraycopy(blocks(i), 0, bytes, offset, blocks(i).length)
          offset += blocks(i).length
          i += 1
        }
        packed += bytes
      }
      start = end
    }
    packed.result()
  }

  def buildBroadcastData(
      collectedData: Array[Array[Byte]],
      keys: Seq[Expression],
      nativeSchema: pb.Schema): Array[Array[Byte]] =
    rewriteBroadcastData(collectedData, nativeSchema) { input =>
      val buildHashMap = pb.BroadcastJoinBuildHashMapExecNode
        .newBuilder()
        .setInput(input)
        .addAllKeys(keys.map(key => NativeConverters.convertExpr(key)).asJava)
      pb.PhysicalPlanNode.newBuilder().setBroadcastJoinBuildHashMap(buildHashMap).build()
    }

  private def rewriteBroadcastData(collectedData: Array[Array[Byte]], nativeSchema: pb.Schema)(
      transform: pb.PhysicalPlanNode => pb.PhysicalPlanNode): Array[Array[Byte]] = {
    val readerIpcProviderResourceId = s"BuildBroadcastDataReader:${UUID.randomUUID()}"
    val readerExec = pb.IpcReaderExecNode
      .newBuilder()
      .setSchema(nativeSchema)
      .setIpcProviderResourceId(readerIpcProviderResourceId)

    val writerIpcProviderResourceId = s"BuildBroadcastDataWriter:${UUID.randomUUID()}"
    val writerExec = pb.IpcWriterExecNode
      .newBuilder()
      .setInput(transform(pb.PhysicalPlanNode.newBuilder().setIpcReader(readerExec).build()))
      .setIpcConsumerResourceId(writerIpcProviderResourceId)

    val exec = pb.PhysicalPlanNode
      .newBuilder()
      .setIpcWriter(writerExec)
      .build()

    // input
    val provideIpcIterator = () => {
      collectedData.iterator.map(bytes =>
        new BlockObject {
          override def hasByteBuffer: Boolean = true
          override def getByteBuffer: ByteBuffer = ByteBuffer.wrap(bytes)
          override def close(): Unit = {}
        })
    }
    JniBridge.putResource(readerIpcProviderResourceId, () => provideIpcIterator())

    // output
    val bos = new ByteArrayOutputStream()
    val consumeIpc = (byteBuffer: ByteBuffer) => {
      val byteArray = new Array[Byte](byteBuffer.capacity())
      byteBuffer.get(byteArray)
      bos.write(byteArray)
    }
    JniBridge.putResource(writerIpcProviderResourceId, consumeIpc)

    // execute
    val singlePartition = new Partition {
      override def index: Int = 0
    }
    assert(NativeHelper.executeNativePlan(exec, null, singlePartition, None).isEmpty)
    Array(bos.toByteArray)
  }
}
