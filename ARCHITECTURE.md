# Apache Auron Architecture - Flink End-to-End Integration

This document provides a comprehensive architectural overview of Apache Auron's integration with Apache Flink, covering the complete data flow from SQL query to native execution and result processing.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [System Components](#system-components)
3. [End-to-End Data Flow](#end-to-end-data-flow)
4. [Serialization and Communication](#serialization-and-communication)
5. [Module Descriptions](#module-descriptions)
6. [Key Files and Locations](#key-files-and-locations)
7. [Memory Management](#memory-management)
8. [Configuration and Extension Points](#configuration-and-extension-points)

---

## Architecture Overview

Apache Auron is a native vectorized execution accelerator for big data engines. It leverages Apache DataFusion (Rust) for high-performance query processing while integrating seamlessly with Apache Flink's batch execution mode.

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Flink Table API / SQL Layer                      │
│                       (User Query Interface)                        │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                │ SQL Query
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  Flink Table Planner (Apache Calcite)               │
│              - Parse SQL                                            │
│              - Optimize logical plan                                │
│              - Generate physical plan (RelNodes)                    │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                │ Physical Plan (RelNode tree)
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│             Auron Flink Planner Extension (JVM)                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  AuronFlinkPlannerExtension                                  │  │
│  │  - Plan interception and conversion                          │  │
│  │  - Rule-based operator matching                              │  │
│  └──────────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  FlinkTypeConverter                                          │  │
│  │  - Flink LogicalType → Arrow Type                           │  │
│  │  - Schema translation                                        │  │
│  └──────────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  FlinkExpressionConverter                                    │  │
│  │  - RexNode → PhysicalExprNode (protobuf)                    │  │
│  │  - Literal serialization (Arrow IPC)                        │  │
│  └──────────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  AuronFlinkConverters                                        │  │
│  │  - Physical plan node conversion                             │  │
│  │  - Scan, Filter, Projection operators                        │  │
│  └──────────────────────────────────────────────────────────────┘  │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                │ PhysicalPlanNode (protobuf)
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│              Auron Batch Execution Wrapper (JVM)                    │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  AuronBatchExecutionWrapperOperator                          │  │
│  │  - Flink DataStream operator                                 │  │
│  │  - Manages execution lifecycle                               │  │
│  │  - Batch iteration and collection                            │  │
│  └──────────────────────────────────────────────────────────────┘  │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                │ Initiate native execution
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Auron Core JNI Bridge (JVM)                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  FlinkAuronAdaptor extends AuronCallNativeWrapper           │  │
│  │  - Adaptor pattern for Flink-specific behavior              │  │
│  │  - Configuration management                                  │  │
│  │  - Arrow allocator handling                                  │  │
│  └──────────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  AuronCallNativeWrapper (Core)                              │  │
│  │  - JNI method declarations                                   │  │
│  │  - TaskDefinition serialization                             │  │
│  │  - Arrow FFI result deserialization                         │  │
│  │  - Batch callback handling                                   │  │
│  └──────────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  JniBridge (Static Native Methods)                          │  │
│  │  - native long callNative(memory, logLevel, wrapper)        │  │
│  │  - native boolean nextBatch(runtimePtr)                     │  │
│  │  - native void freeBatch(runtimePtr)                        │  │
│  └──────────────────────────────────────────────────────────────┘  │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                │ JNI boundary (libauron.so/dylib)
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   Auron Native Runtime (Rust)                       │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  JNI Entry Points (rt.rs)                                    │  │
│  │  - Java_*_JniBridge_callNative                              │  │
│  │  - Java_*_JniBridge_nextBatch                               │  │
│  │  - Java_*_JniBridge_freeBatch                               │  │
│  └──────────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  Task Deserialization                                        │  │
│  │  - Get raw bytes: wrapper.getRawTaskDefinition()            │  │
│  │  - Protobuf decode: TaskDefinition::decode()                │  │
│  │  - Extract PartitionId and PhysicalPlanNode                 │  │
│  └──────────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  PhysicalPlanner (Plan Conversion)                          │  │
│  │  - Convert protobuf → DataFusion ExecutionPlan              │  │
│  │  - Handle all operator types                                │  │
│  │  - Build execution DAG                                       │  │
│  └──────────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  DataFusion Execution Engine                                │  │
│  │  - Vectorized query execution                                │  │
│  │  - Columnar processing (Apache Arrow)                       │  │
│  │  - SIMD optimizations                                        │  │
│  │  - Parallel execution                                        │  │
│  └──────────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  Result Batch Processing                                     │  │
│  │  - Execute plan → Stream<RecordBatch>                       │  │
│  │  - Convert to Arrow FFI format                              │  │
│  │  - Call JVM: wrapper.importBatch(ffiArrayPtr)              │  │
│  └──────────────────────────────────────────────────────────────┘  │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                │ Arrow FFI (zero-copy)
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  Result Processing (JVM)                            │
│  - Deserialize Arrow FFI → VectorSchemaRoot                        │
│  - Convert to Flink RowData                                        │
│  - Emit to Flink DataStream                                        │
│  - Continue pipeline processing                                    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## System Components

### 1. Flink Table API / SQL Layer

**Purpose**: User-facing query interface

**Responsibilities**:
- Accept SQL queries or Table API calls
- Provide execution environment configuration
- Handle result collection and presentation

**Key Classes**:
- `StreamTableEnvironment` - Main entry point
- `Configuration` - Auron-specific settings

### 2. Flink Table Planner (Calcite)

**Purpose**: Query parsing, validation, and optimization

**Responsibilities**:
- Parse SQL into abstract syntax tree (AST)
- Validate semantics and types
- Apply logical optimizations
- Generate physical execution plan (RelNode tree)

**Key Concepts**:
- `RelNode` - Relational operator nodes
- `RexNode` - Row expressions (predicates, projections)
- Optimization rules and cost-based planning

### 3. Auron Flink Planner Extension

**Location**: `auron-flink-extension/auron-flink-planner/`

**Purpose**: Convert Flink physical plans to Auron protobuf plans

**Key Components**:

#### 3.1 AuronFlinkPlannerExtension
**File**: `src/main/java/org/apache/auron/flink/planner/AuronFlinkPlannerExtension.java`

**Responsibilities**:
- Entry point for explicit API (`createAuronParquetScan`)
- Configuration validation
- Operator construction

**Example API**:
```java
public static DataStream<RowData> createAuronParquetScan(
    StreamExecutionEnvironment env,
    List<String> filePaths,
    RowType outputSchema,
    RowType physicalSchema,
    int[] projectedFields,
    List<RexNode> filterPredicates,
    int parallelism
)
```

#### 3.2 FlinkTypeConverter
**File**: `src/main/java/org/apache/auron/flink/planner/FlinkTypeConverter.java:23`

**Responsibilities**:
- Convert Flink `LogicalType` → Protobuf `ArrowType`
- Handle complex types (structs, arrays, maps)
- Schema construction

**Type Mappings**:
```
Flink LogicalType          →  Arrow Type (Protobuf)
─────────────────────────────────────────────────────
IntType                    →  INT32
BigIntType                 →  INT64
DoubleType                 →  FLOAT64
VarCharType/CharType       →  UTF8
BooleanType                →  BOOL
DateType                   →  DATE32
TimestampType              →  TIMESTAMP(microsecond, UTC)
DecimalType                →  DECIMAL128/DECIMAL256
ArrayType                  →  LIST
MapType                    →  MAP
RowType                    →  STRUCT
```

#### 3.3 FlinkExpressionConverter
**File**: `src/main/java/org/apache/auron/flink/planner/FlinkExpressionConverter.java:38`

**Responsibilities**:
- Convert Calcite `RexNode` → Protobuf `PhysicalExprNode`
- Serialize literals to Arrow IPC format
- Handle expression trees (binary ops, function calls, etc.)

**Expression Types Handled**:
- Literals (with Arrow IPC serialization)
- Input references (column references)
- Binary expressions (AND, OR, comparison operators)
- Function calls (scalar functions)
- Cast operations

**Literal Serialization Example**:
```java
private static PhysicalExprNode convertLiteral(RexLiteral literal) {
    // Create Arrow schema for the literal value
    Schema schema = new Schema(...);

    // Write value to Arrow vector
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, ALLOCATOR)) {
        root.allocateNew();
        writeValueToVector(root.getVector(0), literal.getValue(), literal.getType());
        root.setRowCount(1);

        // Serialize to Arrow IPC streaming format
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }

        byte[] ipcBytes = out.toByteArray();
        return PhysicalExprNode.newBuilder()
            .setLiteral(ScalarValue.newBuilder()
                .setIpcBytes(ByteString.copyFrom(ipcBytes)))
            .build();
    }
}
```

#### 3.4 AuronFlinkConverters
**File**: `src/main/java/org/apache/auron/flink/planner/AuronFlinkConverters.java:22`

**Responsibilities**:
- Build complete `PhysicalPlanNode` protobuf messages
- Convert Parquet scan parameters
- Construct filter and projection operators

### 4. Auron Batch Execution Wrapper

**Location**: `auron-flink-extension/auron-flink-planner/src/main/java/org/apache/auron/flink/planner/execution/`

#### AuronBatchExecutionWrapperOperator
**File**: `AuronBatchExecutionWrapperOperator.java:33`

**Purpose**: Flink DataStream operator that drives native execution

**Responsibilities**:
- Initialize `FlinkAuronAdaptor` with protobuf plan
- Iterate through result batches via `loadNextBatch()`
- Convert Arrow `VectorSchemaRoot` → Flink `RowData`
- Emit results to downstream operators
- Handle cleanup and resource disposal

**Key Methods**:
```java
public class AuronBatchExecutionWrapperOperator
    extends AbstractStreamOperator<RowData>
    implements OneInputStreamOperator<RowData, RowData> {

    @Override
    public void open() throws Exception {
        // Initialize adaptor with protobuf plan
        adaptor = new FlinkAuronAdaptor(
            arrowAllocator,
            nativePlan,
            metrics,
            partitionId,
            stageId,
            taskId,
            nativeMemory,
            config
        );
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        // Consume result batches from native execution
        while (adaptor.loadNextBatch(vectorSchemaRoot -> {
            // Convert Arrow batch to Flink RowData
            for (int i = 0; i < vectorSchemaRoot.getRowCount(); i++) {
                RowData row = convertArrowRowToFlinkRow(vectorSchemaRoot, i);
                output.collect(new StreamRecord<>(row));
            }
        })) {
            // Continue loading batches
        }
    }
}
```

### 5. Auron Core JNI Bridge

**Location**: `auron-core/src/main/java/org/apache/auron/jni/`

#### 5.1 FlinkAuronAdaptor
**File**: `auron-flink-extension/auron-flink-runtime/src/main/java/org/apache/auron/jni/FlinkAuronAdaptor.java:29`

**Purpose**: Flink-specific adaptor extending core JNI wrapper

**Responsibilities**:
- Wrap Flink configuration
- Provide engine-specific implementations
- Manage Flink-specific Arrow allocators

#### 5.2 AuronCallNativeWrapper
**File**: `auron-core/src/main/java/org/apache/auron/jni/AuronCallNativeWrapper.java:57`

**Purpose**: Core JNI bridge to native runtime

**Key Methods**:

```java
public class AuronCallNativeWrapper {
    // Initialize and start native execution
    public AuronCallNativeWrapper(
        BufferAllocator arrowAllocator,
        PhysicalPlanNode nativePlan,
        MetricNode metrics,
        int partitionId,
        int stageId,
        int taskId,
        long nativeMemory
    ) {
        // Calls JniBridge.callNative() which invokes Rust
        this.nativeRuntimePtr = JniBridge.callNative(nativeMemory, logLevel, this);
    }

    // Load next batch from native execution
    public boolean loadNextBatch(Consumer<VectorSchemaRoot> batchConsumer) {
        this.batchConsumer = batchConsumer;
        if (nativeRuntimePtr != 0 && JniBridge.nextBatch(nativeRuntimePtr)) {
            return true;  // Batch was processed via callback
        }
        close();
        return false;  // No more batches
    }

    // Called by native code to serialize plan
    protected byte[] getRawTaskDefinition() {
        PartitionId partition = PartitionId.newBuilder()
            .setPartitionId(partitionId)
            .setStageId(stageId)
            .setTaskId(taskId)
            .build();

        TaskDefinition taskDefinition = TaskDefinition.newBuilder()
            .setTaskId(partition)
            .setPlan(nativePlan)  // PhysicalPlanNode protobuf
            .build();

        return taskDefinition.toByteArray();  // Protobuf serialization
    }

    // Called by native code with result batch (Arrow FFI)
    protected void importBatch(long ffiArrayPtr) {
        try (ArrowArray ffiArray = ArrowArray.wrap(ffiArrayPtr)) {
            try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, arrowAllocator)) {
                // Deserialize Arrow C Data Interface
                Data.importIntoVectorSchemaRoot(arrowAllocator, ffiArray, root, dictionaryProvider);

                // Pass to consumer
                batchConsumer.accept(root);
            }
        }
    }
}
```

#### 5.3 JniBridge
**File**: `auron-core/src/main/java/org/apache/auron/jni/JniBridge.java:24`

**Purpose**: Native method declarations

```java
public class JniBridge {
    static {
        System.loadLibrary("auron");  // Load libauron.so/dylib
    }

    // Initialize native runtime and start execution
    public static native long callNative(
        long memory,
        int logLevel,
        AuronCallNativeWrapper wrapper
    );

    // Get next batch from native execution
    public static native boolean nextBatch(long nativeRuntimePtr);

    // Free native runtime resources
    public static native void freeBatch(long nativeRuntimePtr);
}
```

### 6. Auron Native Runtime (Rust)

**Location**: `native-engine/auron/`

#### 6.1 JNI Entry Points
**File**: `native-engine/auron/src/rt.rs:124`

**JNI Function**: `Java_org_apache_auron_jni_JniBridge_callNative`

**Execution Flow**:
```rust
#[no_mangle]
pub extern "system" fn Java_org_apache_auron_jni_JniBridge_callNative(
    mut env: JNIEnv,
    _class: JClass,
    memory: jlong,
    log_level: jint,
    native_wrapper: JObject,
) -> jlong {
    // 1. Get TaskDefinition bytes from JVM
    let raw_task_definition = jni_call!(
        AuronCallNativeWrapper(native_wrapper.as_obj())
            .getRawTaskDefinition() -> JObject
    )?;
    let raw_task_definition = jni_convert_byte_array!(raw_task_definition.as_obj())?;

    // 2. Deserialize protobuf
    let task_definition = TaskDefinition::decode(raw_task_definition.as_slice())
        .or_else(|err| df_execution_err!("cannot decode execution plan: {err:?}"))?;

    // 3. Extract plan and partition info
    let plan = task_definition.plan.ok_or_else(|| /* error */)?;
    let partition_id = task_definition.task_id.ok_or_else(|| /* error */)?;

    // 4. Create DataFusion execution plan
    let planner = PhysicalPlanner::new(partition_id);
    let execution_plan: Arc<dyn ExecutionPlan> = planner
        .create_plan(plan)
        .or_else(|err| df_execution_err!("cannot create execution plan: {err:?}"))?;

    // 5. Create runtime context
    let runtime = NativeRuntime {
        execution_plan,
        batch_stream: None,  // Will be populated on first nextBatch call
        // ...
    };

    // 6. Return runtime pointer to JVM
    Box::into_raw(Box::new(runtime)) as jlong
}
```

**JNI Function**: `Java_org_apache_auron_jni_JniBridge_nextBatch`

```rust
#[no_mangle]
pub extern "system" fn Java_org_apache_auron_jni_JniBridge_nextBatch(
    mut env: JNIEnv,
    _class: JClass,
    native_runtime_ptr: jlong,
) -> jboolean {
    let runtime = unsafe { &mut *(native_runtime_ptr as *mut NativeRuntime) };

    // Initialize stream on first call
    if runtime.batch_stream.is_none() {
        let stream = runtime.execution_plan.execute(0, runtime.task_ctx.clone())?;
        runtime.batch_stream = Some(stream);
    }

    // Get next batch
    let batch = runtime_block_on(runtime.batch_stream.as_mut().unwrap().next())?;

    match batch {
        Some(record_batch) => {
            // Convert RecordBatch to Arrow FFI
            let (ffi_schema, ffi_array) = record_batch_to_ffi(&record_batch)?;

            // Call JVM callback with FFI pointers
            jni_call!(
                AuronCallNativeWrapper(runtime.wrapper_global_ref.as_obj())
                    .importBatch(ffi_array.into_raw() as jlong) -> ()
            )?;

            JNI_TRUE  // More batches available
        }
        None => JNI_FALSE  // No more batches
    }
}
```

#### 6.2 PhysicalPlanner
**File**: `native-engine/auron-planner/src/planner.rs:42`

**Purpose**: Convert protobuf plans to DataFusion execution plans

**Key Method**:
```rust
impl PhysicalPlanner {
    pub fn create_plan(&self, plan: PhysicalPlanNode) -> Result<Arc<dyn ExecutionPlan>> {
        match plan.physical_plan_type {
            Some(PhysicalPlanType::ParquetScan(scan)) => {
                self.create_parquet_scan_exec(scan)
            }
            Some(PhysicalPlanType::Filter(filter)) => {
                self.create_filter_exec(filter)
            }
            Some(PhysicalPlanType::Projection(proj)) => {
                self.create_projection_exec(proj)
            }
            Some(PhysicalPlanType::HashAgg(agg)) => {
                self.create_hash_agg_exec(agg)
            }
            Some(PhysicalPlanType::HashJoin(join)) => {
                self.create_hash_join_exec(join)
            }
            // ... handle other operator types
            None => Err(DataFusionError::Plan("Missing plan type".to_string()))
        }
    }
}
```

#### 6.3 DataFusion Execution

DataFusion executes the plan:
- Reads Parquet files using columnar scanning
- Applies filters and projections with SIMD vectorization
- Executes joins and aggregations
- Produces `RecordBatch` results (Apache Arrow format)

---

## End-to-End Data Flow

### Complete Execution Sequence

```
┌─────────────────────────────────────────────────────────────────────┐
│ 1. USER SUBMITS QUERY                                               │
└─────────────────────────────────────────────────────────────────────┘
    SQL: SELECT id, name FROM sales WHERE amount > 100
         (via Table API or SQL string)
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 2. FLINK CALCITE PLANNER                                            │
│    - Parse SQL to AST                                               │
│    - Validate and type-check                                        │
│    - Logical optimization                                           │
│    - Generate physical plan (RelNode tree)                          │
└─────────────────────────────────────────────────────────────────────┘
                              │
              RelNode Tree:   │  LogicalFilter (amount > 100)
              LogicalProject  │    │
              (id, name)      │    └─ LogicalTableScan (sales)
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 3. AURON PLANNER EXTENSION (Plan Conversion)                        │
│                                                                     │
│  FlinkExpressionConverter.convert():                               │
│    - Filter: amount > 100 → PhysicalExprNode                      │
│      {                                                             │
│        binary_expr: {                                              │
│          left: {column: {name: "amount"}},                        │
│          op: GT,                                                   │
│          right: {literal: {ipc_bytes: [Arrow IPC bytes for 100]}} │
│        }                                                           │
│      }                                                             │
│                                                                     │
│  FlinkTypeConverter.convert():                                     │
│    - Schema: [                                                     │
│        {name: "id", type: INT32},                                 │
│        {name: "name", type: UTF8},                                │
│        {name: "amount", type: FLOAT64}                            │
│      ]                                                             │
│                                                                     │
│  AuronFlinkConverters.createParquetScan():                        │
│    - Build PhysicalPlanNode:                                       │
│      {                                                             │
│        parquet_scan: {                                            │
│          base_conf: {schema: ..., file_groups: ...},              │
│          projection: [0, 1],  // id, name                         │
│          predicate: {/* PhysicalExprNode for filter */}           │
│        }                                                           │
│      }                                                             │
└─────────────────────────────────────────────────────────────────────┘
                              │
              PhysicalPlanNode protobuf message
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 4. BATCH EXECUTION WRAPPER OPERATOR                                 │
│                                                                     │
│  open():                                                           │
│    adaptor = new FlinkAuronAdaptor(                               │
│      arrowAllocator,                                              │
│      nativePlan,  // ← PhysicalPlanNode                           │
│      metrics,                                                      │
│      partitionId, stageId, taskId,                                │
│      nativeMemory,                                                 │
│      config                                                        │
│    )                                                               │
│                                                                     │
│  processElement():                                                 │
│    while (adaptor.loadNextBatch(batchConsumer)) {                 │
│      // Process each batch                                         │
│    }                                                               │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 5. AURON CALL NATIVE WRAPPER (JNI Bridge)                          │
│                                                                     │
│  constructor():                                                    │
│    nativeRuntimePtr = JniBridge.callNative(                       │
│      nativeMemory, logLevel, this                                 │
│    )                                                               │
│    // Native code will call back:                                  │
│    //   this.getRawTaskDefinition()                               │
│    //   this.importSchema()                                        │
│                                                                     │
│  getRawTaskDefinition():                                           │
│    TaskDefinition taskDef = TaskDefinition.newBuilder()           │
│      .setTaskId(PartitionId.newBuilder()                          │
│        .setPartitionId(partitionId)                               │
│        .setStageId(stageId)                                       │
│        .setTaskId(taskId))                                        │
│      .setPlan(nativePlan)  // PhysicalPlanNode                    │
│      .build();                                                     │
│    return taskDef.toByteArray();  // ← Protobuf binary           │
└─────────────────────────────────────────────────────────────────────┘
                              │
              Protobuf bytes: TaskDefinition
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 6. NATIVE RUNTIME (Rust) - Initialization                          │
│                                                                     │
│  Java_*_JniBridge_callNative():                                    │
│    // 1. Get TaskDefinition bytes                                  │
│    let bytes = wrapper.getRawTaskDefinition();                    │
│                                                                     │
│    // 2. Deserialize protobuf                                      │
│    let task_def = TaskDefinition::decode(&bytes)?;                │
│    let plan = task_def.plan.unwrap();                             │
│    let partition_id = task_def.task_id.unwrap();                  │
│                                                                     │
│    // 3. Convert protobuf → DataFusion plan                        │
│    let planner = PhysicalPlanner::new(partition_id);              │
│    let exec_plan = planner.create_plan(plan)?;                    │
│    // exec_plan: ParquetExec                                       │
│    //   → FilterExec (amount > 100)                               │
│    //   → ProjectionExec (id, name)                               │
│                                                                     │
│    // 4. Store in runtime struct                                   │
│    let runtime = NativeRuntime {                                  │
│      execution_plan: exec_plan,                                   │
│      batch_stream: None,                                          │
│      wrapper_ref,                                                 │
│    };                                                              │
│                                                                     │
│    // 5. Return pointer to JVM                                     │
│    Box::into_raw(Box::new(runtime)) as jlong                      │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 7. BATCH ITERATION (Pull Model)                                    │
│                                                                     │
│  [JVM] loadNextBatch(batchConsumer):                              │
│    bool hasMore = JniBridge.nextBatch(nativeRuntimePtr);         │
│                                                                     │
│  [Rust] Java_*_JniBridge_nextBatch():                             │
│    let runtime = &mut *(ptr as *mut NativeRuntime);              │
│                                                                     │
│    // Initialize stream on first call                              │
│    if runtime.batch_stream.is_none() {                            │
│      let stream = runtime.execution_plan                          │
│        .execute(0, task_ctx.clone())?;                            │
│      runtime.batch_stream = Some(stream);                         │
│    }                                                               │
│                                                                     │
│    // Get next RecordBatch                                         │
│    let batch_opt = stream.next().await?;                          │
│                                                                     │
│    match batch_opt {                                              │
│      Some(record_batch) => {                                      │
│        // DataFusion produced a batch:                            │
│        // RecordBatch {                                           │
│        //   schema: [id: INT32, name: UTF8],                      │
│        //   columns: [Int32Array, StringArray],                   │
│        //   row_count: 8192                                       │
│        // }                                                        │
│                                                                     │
│        // Convert to Arrow C Data Interface (FFI)                 │
│        let ffi_schema = FFI_ArrowSchema::try_from(                │
│          record_batch.schema())?;                                 │
│        let ffi_array = FFI_ArrowArray::new(                       │
│          &record_batch.into_struct_array())?;                     │
│                                                                     │
│        // Call JVM with FFI pointers                              │
│        wrapper.importBatch(                                       │
│          ffi_array.into_raw() as jlong                            │
│        );                                                          │
│                                                                     │
│        return JNI_TRUE;  // More batches may be available        │
│      }                                                             │
│      None => {                                                    │
│        return JNI_FALSE;  // Stream exhausted                     │
│      }                                                             │
│    }                                                               │
└─────────────────────────────────────────────────────────────────────┘
                              │
              Arrow FFI pointer (zero-copy)
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 8. RESULT DESERIALIZATION (JVM)                                    │
│                                                                     │
│  [JVM] importBatch(long ffiArrayPtr):                             │
│    // Wrap FFI pointer                                             │
│    try (ArrowArray ffiArray = ArrowArray.wrap(ffiArrayPtr)) {    │
│      // Create VectorSchemaRoot                                    │
│      try (VectorSchemaRoot root = VectorSchemaRoot.create(        │
│          arrowSchema, arrowAllocator)) {                          │
│                                                                     │
│        // Import Arrow data (zero-copy where possible)            │
│        Data.importIntoVectorSchemaRoot(                           │
│          arrowAllocator, ffiArray, root, dictProvider);           │
│                                                                     │
│        // VectorSchemaRoot now contains:                          │
│        // - IntVector for "id" column                             │
│        // - VarCharVector for "name" column                       │
│        // - 8192 rows of data                                     │
│                                                                     │
│        // Pass to consumer callback                               │
│        batchConsumer.accept(root);                                │
│      }                                                             │
│    }                                                               │
└─────────────────────────────────────────────────────────────────────┘
                              │
              VectorSchemaRoot (Arrow columnar data)
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 9. FLINK ROW CONVERSION & EMISSION                                 │
│                                                                     │
│  [AuronBatchExecutionWrapperOperator] batchConsumer:              │
│    for (int row = 0; row < root.getRowCount(); row++) {          │
│      // Extract values from Arrow vectors                         │
│      IntVector idVector = (IntVector) root.getVector("id");      │
│      VarCharVector nameVector = (VarCharVector)                   │
│        root.getVector("name");                                    │
│                                                                     │
│      int id = idVector.get(row);                                  │
│      String name = nameVector.getObject(row).toString();          │
│                                                                     │
│      // Create Flink RowData                                      │
│      GenericRowData rowData = new GenericRowData(2);             │
│      rowData.setField(0, id);                                     │
│      rowData.setField(1, StringData.fromString(name));           │
│                                                                     │
│      // Emit to Flink DataStream                                  │
│      output.collect(new StreamRecord<>(rowData));                │
│    }                                                               │
└─────────────────────────────────────────────────────────────────────┘
                              │
              Flink DataStream<RowData>
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 10. FLINK PIPELINE CONTINUATION                                    │
│     - Further transformations                                      │
│     - Sinks (file, database, console, etc.)                       │
│     - Result collection                                            │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Serialization and Communication

### Three Serialization Formats

Auron uses three distinct serialization formats for different purposes:

#### 1. Protobuf (Execution Plans)
**Direction**: JVM → Native
**Purpose**: Serialize execution plans and metadata
**Format**: Protocol Buffers v3

**Schema Location**: `native-engine/auron-planner/proto/auron.proto`

**Key Messages**:
- `TaskDefinition` - Top-level task container
- `PhysicalPlanNode` - Operator tree nodes
- `PhysicalExprNode` - Expression trees
- `Schema` / `Field` / `ArrowType` - Type system

**Build Process**:
```bash
# Java code generation (Maven)
protobuf-maven-plugin → generates Java classes in target/generated-sources/

# Rust code generation (Cargo)
tonic_build in build.rs → generates prost types
```

**Example Protobuf Message**:
```protobuf
TaskDefinition {
  task_id: PartitionId {
    stage_id: 1,
    partition_id: 0,
    task_id: 42
  },
  plan: PhysicalPlanNode {
    parquet_scan: ParquetScanExecNode {
      base_conf: {
        schema: Schema {
          fields: [
            Field { name: "id", arrow_type: INT32 },
            Field { name: "name", arrow_type: UTF8 }
          ]
        },
        file_groups: [
          FileGroup {
            files: [FileScanConfig {...}]
          }
        ]
      },
      projection: [0, 1],
      predicate: PhysicalExprNode {
        binary_expr: {
          left: {column: {name: "amount"}},
          op: GT,
          right: {literal: {ipc_bytes: <Arrow IPC bytes>}}
        }
      }
    }
  }
}
```

#### 2. Arrow IPC Streaming Format (Literal Values)
**Direction**: JVM → Native
**Purpose**: Embed literal values in protobuf expressions
**Format**: Apache Arrow IPC (Streaming)

**Usage**: `ScalarValue.ipc_bytes` field in `PhysicalExprNode`

**Why Arrow IPC?**
- Supports all Arrow data types consistently
- Self-describing (includes schema)
- Efficient binary representation
- Native deserialization in Rust via arrow-rs

**Example Serialization** (Java):
```java
// Serialize integer literal: 100
Schema schema = new Schema(Arrays.asList(
    new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)
));

try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
    root.allocateNew();
    IntVector vector = (IntVector) root.getVector(0);
    vector.set(0, 100);
    root.setRowCount(1);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null,
         Channels.newChannel(out))) {
        writer.start();
        writer.writeBatch();
        writer.end();
    }

    byte[] ipcBytes = out.toByteArray();
    // Store in ScalarValue.ipc_bytes
}
```

**Deserialization** (Rust):
```rust
use arrow::ipc::reader::StreamReader;

let cursor = Cursor::new(&scalar_value.ipc_bytes);
let mut reader = StreamReader::try_new(cursor, None)?;
let batch = reader.next().unwrap()?;
let array = batch.column(0);
let value = ScalarValue::try_from_array(array, 0)?;
```

#### 3. Arrow C Data Interface (Result Batches)
**Direction**: Native → JVM
**Purpose**: Zero-copy data transfer for result batches
**Format**: Arrow C Data Interface (FFI)

**Key Structures**:
```c
// C ABI structures
struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};
```

**Rust Export**:
```rust
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

// Convert DataFusion RecordBatch to FFI
let struct_array = StructArray::from(record_batch);
let ffi_array = FFI_ArrowArray::new(&struct_array);
let ffi_schema = FFI_ArrowSchema::try_from(record_batch.schema().as_ref())?;

// Pass raw pointers to JVM
let array_ptr = ffi_array.into_raw() as jlong;
wrapper.importBatch(array_ptr);
```

**Java Import**:
```java
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.Data;

// Wrap FFI pointer
try (ArrowArray ffiArray = ArrowArray.wrap(ffiArrayPtr)) {
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        // Zero-copy import (where possible)
        Data.importIntoVectorSchemaRoot(allocator, ffiArray, root, dictionaryProvider);

        // root now contains the data
    }
}
```

### Serialization Format Summary

| Format | Direction | Purpose | Size Efficiency | Parsing Speed |
|--------|-----------|---------|-----------------|---------------|
| **Protobuf** | JVM → Native | Plans, metadata | High (binary) | Fast |
| **Arrow IPC** | JVM → Native | Literal values | Medium (self-describing) | Fast |
| **Arrow FFI** | Native → JVM | Result batches | Highest (zero-copy) | Instant |

---

## Module Descriptions

### Core Modules

#### auron-core
**Location**: `auron-core/`

**Purpose**: Core JNI bridge and native interop

**Key Classes**:
- `AuronCallNativeWrapper` - Base JNI wrapper for all engines
- `JniBridge` - Native method declarations
- `AuronConfiguration` - Configuration abstraction

**Responsibilities**:
- Load native library (`libauron.so`/`libauron.dylib`)
- Manage native runtime lifecycle
- Arrow FFI deserialization
- Generic metric collection

#### auron-flink-runtime
**Location**: `auron-flink-extension/auron-flink-runtime/`

**Purpose**: Flink-specific runtime adaptor

**Key Classes**:
- `FlinkAuronAdaptor` - Flink adaptor extending `AuronCallNativeWrapper`
- `FlinkAuronAdaptorProvider` - SPI provider for adaptor discovery
- `FlinkAuronConfiguration` - Flink configuration wrapper

**Responsibilities**:
- Provide Flink-specific adaptor implementation
- Manage Flink configuration integration
- Handle Flink-specific Arrow allocators

**SPI Registration**:
```
src/main/resources/META-INF/services/
  org.apache.auron.jni.AuronEngineAdaptorProvider
    → org.apache.auron.jni.FlinkAuronAdaptorProvider
```

#### auron-flink-planner
**Location**: `auron-flink-extension/auron-flink-planner/`

**Purpose**: Query planning and execution

**Key Packages**:
- `org.apache.auron.flink.planner` - Plan conversion
- `org.apache.auron.flink.planner.execution` - Execution operators

**Dependencies**:
- `auron-flink-runtime` - Adaptor access
- `auron-core` - Core JNI functionality
- `flink-table-planner` - Calcite integration

#### native-engine/auron-planner
**Location**: `native-engine/auron-planner/`

**Purpose**: Protobuf schema and plan conversion

**Key Files**:
- `proto/auron.proto` - Protobuf schema definition
- `src/planner.rs` - Protobuf → DataFusion conversion

**Build Configuration**:
```rust
// build.rs
fn main() {
    tonic_build::configure()
        .build_server(false)
        .compile(&["proto/auron.proto"], &["proto"])
        .unwrap();
}
```

#### native-engine/auron
**Location**: `native-engine/auron/`

**Purpose**: Native execution runtime

**Key Files**:
- `src/rt.rs` - JNI entry points
- `src/lib.rs` - Library exports

**Cargo Configuration**:
```toml
[lib]
name = "auron"
crate-type = ["cdylib"]  # C dynamic library for JNI
```

---

## Key Files and Locations

### Java/Flink Files

| File | Location | Purpose |
|------|----------|---------|
| `FlinkAuronAdaptor.java` | `auron-flink-runtime/src/main/java/org/apache/auron/jni/` | Flink adaptor implementation |
| `AuronCallNativeWrapper.java` | `auron-core/src/main/java/org/apache/auron/jni/` | Core JNI bridge |
| `JniBridge.java` | `auron-core/src/main/java/org/apache/auron/jni/` | Native method declarations |
| `AuronFlinkPlannerExtension.java` | `auron-flink-planner/src/main/java/org/apache/auron/flink/planner/` | Entry point and explicit API |
| `FlinkTypeConverter.java` | `auron-flink-planner/src/main/java/org/apache/auron/flink/planner/` | Type conversions (Flink → Arrow) |
| `FlinkExpressionConverter.java` | `auron-flink-planner/src/main/java/org/apache/auron/flink/planner/` | Expression conversions (RexNode → Protobuf) |
| `AuronFlinkConverters.java` | `auron-flink-planner/src/main/java/org/apache/auron/flink/planner/` | Plan node conversions |
| `AuronBatchExecutionWrapperOperator.java` | `auron-flink-planner/src/main/java/org/apache/auron/flink/planner/execution/` | Flink operator for native execution |

### Rust/Native Files

| File | Location | Purpose |
|------|----------|---------|
| `auron.proto` | `native-engine/auron-planner/proto/` | Protobuf schema definition |
| `planner.rs` | `native-engine/auron-planner/src/` | Protobuf → DataFusion conversion |
| `rt.rs` | `native-engine/auron/src/` | JNI entry points and runtime |
| `build.rs` | `native-engine/auron-planner/` | Protobuf code generation |

### Configuration Files

| File | Location | Purpose |
|------|----------|---------|
| `pom.xml` | `auron-flink-runtime/` | Runtime module Maven config |
| `pom.xml` | `auron-flink-planner/` | Planner module Maven config |
| `Cargo.toml` | `native-engine/auron/` | Rust library configuration |
| `org.apache.auron.jni.AuronEngineAdaptorProvider` | `auron-flink-runtime/src/main/resources/META-INF/services/` | SPI registration |

### Test Files

| File | Location | Purpose |
|------|----------|---------|
| `FlinkAuronAdaptorTest.java` | `auron-flink-runtime/src/test/java/org/apache/auron/jni/` | Adaptor unit tests (13 tests) |
| `FlinkTypeConverterTest.java` | `auron-flink-planner/src/test/java/org/apache/auron/flink/planner/` | Type conversion tests |
| `FlinkExpressionConverterTest.java` | `auron-flink-planner/src/test/java/org/apache/auron/flink/planner/` | Expression conversion tests |
| `AuronFlinkParquetScanITCase.java` | `auron-flink-planner/src/test/java/org/apache/auron/flink/table/runtime/` | End-to-end integration tests (7 tests) |
| `AuronFlinkCalcITCase.java` | `auron-flink-planner/src/test/java/org/apache/auron/flink/table/runtime/` | Calc node tests (1 test) |

---

## Memory Management

### Arrow Allocators

Auron uses Apache Arrow's memory management for efficient buffer allocation:

**Allocator Hierarchy**:
```
RootAllocator (JVM)
  └─ ChildAllocator (per query/task)
      └─ ChildAllocator (per operator)
```

**Initialization**:
```java
// In AuronBatchExecutionWrapperOperator
private void open() throws Exception {
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    this.arrowAllocator = rootAllocator.newChildAllocator(
        "auron-task-" + taskId,
        0,
        nativeMemoryLimit
    );
}
```

**Memory Limits**:
- Configured via `table.exec.auron.memory-fraction`
- Default: 70% of available memory for native operations
- Tracked via allocator limits

### Native Memory Management

**Rust Side**:
```rust
// DataFusion manages memory via Arc<dyn ExecutionPlan>
let execution_plan: Arc<dyn ExecutionPlan> = ...;

// RecordBatch uses Arc for shared ownership
let batch: RecordBatch = ...;
```

**FFI Transfer**:
- Arrow C Data Interface transfers ownership semantics
- `release` callback handles deallocation
- JVM side must call `close()` to trigger cleanup

**Resource Cleanup**:
```java
// Proper cleanup pattern
try (VectorSchemaRoot root = ...) {
    // Process data
} // Automatically releases memory

// Or explicit cleanup
adaptor.close();  // Calls native freeBatch()
```

### Memory Configuration

```java
Configuration config = new Configuration();

// Memory fraction for native operations
config.setDouble("table.exec.auron.memory-fraction", 0.7);

// Batch size (affects memory per batch)
config.setInteger("table.exec.auron.batch-size", 8192);
```

---

## Configuration and Extension Points

### Flink Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `table.exec.auron.enable` | boolean | false | Master switch for Auron execution |
| `table.exec.auron.enable.scan` | boolean | true | Enable Parquet scan optimization |
| `table.exec.auron.enable.project` | boolean | true | Enable projection optimization |
| `table.exec.auron.enable.filter` | boolean | true | Enable filter optimization |
| `table.exec.auron.batch-size` | int | 8192 | Batch size for vectorized operations |
| `table.exec.auron.memory-fraction` | double | 0.7 | Memory fraction for native operations |
| `table.exec.auron.log-level` | string | INFO | Logging level (TRACE, DEBUG, INFO, WARN, ERROR) |

### Extension Points

#### 1. Custom Adaptors via SPI

**Interface**: `org.apache.auron.jni.AuronEngineAdaptorProvider`

**Implementation**:
```java
public class FlinkAuronAdaptorProvider implements AuronEngineAdaptorProvider {
    @Override
    public String getEngineName() {
        return "flink";
    }

    @Override
    public AuronCallNativeWrapper createAdaptor(
        BufferAllocator allocator,
        PhysicalPlanNode plan,
        MetricNode metrics,
        int partitionId,
        int stageId,
        int taskId,
        long nativeMemory,
        AuronConfiguration config
    ) {
        return new FlinkAuronAdaptor(
            allocator, plan, metrics,
            partitionId, stageId, taskId,
            nativeMemory, (FlinkAuronConfiguration) config
        );
    }
}
```

**Registration**: Create file `META-INF/services/org.apache.auron.jni.AuronEngineAdaptorProvider`

#### 2. Custom Operators

Add new operator support by:

1. **Define protobuf message** in `auron.proto`:
```protobuf
message CustomOpExecNode {
  PhysicalPlanNode input = 1;
  CustomConfig config = 2;
}
```

2. **Add converter** in `AuronFlinkConverters`:
```java
public static PhysicalPlanNode convertCustomOp(CustomOp op) {
    // Convert Flink op to protobuf
}
```

3. **Implement planner** in Rust `planner.rs`:
```rust
fn create_custom_op_exec(&self, node: CustomOpExecNode)
    -> Result<Arc<dyn ExecutionPlan>> {
    // Convert protobuf to DataFusion plan
}
```

#### 3. Custom Types

Add type support in both `FlinkTypeConverter.java` and Rust planner:

**Java**:
```java
private static ArrowType convertLogicalType(LogicalType logicalType) {
    if (logicalType instanceof CustomType) {
        return ArrowType.newBuilder()
            .setCustom(/* conversion */)
            .build();
    }
    // ...
}
```

**Rust**:
```rust
fn from_proto_arrow_type(arrow_type: &pb::ArrowType) -> Result<DataType> {
    match &arrow_type.arrow_type_enum {
        Some(ArrowTypeEnum::Custom(c)) => {
            // Convert protobuf to DataFusion DataType
        }
        // ...
    }
}
```

---

## Performance Considerations

### Batch Size

- Default: 8192 rows per batch
- Larger batches: Better vectorization, more memory
- Smaller batches: Lower latency, less memory
- Configure via `table.exec.auron.batch-size`

### Memory Management

- Use memory fraction to limit native memory usage
- Monitor Arrow allocator statistics
- Ensure proper resource cleanup (use try-with-resources)

### Parallelism

- Flink parallelism applies to Auron operators
- Each parallel instance runs independent native execution
- No shared state across parallel tasks

### Zero-Copy Optimizations

- Arrow FFI enables zero-copy data transfer
- Minimize data copying between JVM and native
- Use columnar format throughout pipeline

---

## Future Enhancements

### Planned Features

1. **Automatic SQL Interception**
   - Calcite optimizer rules for automatic plan conversion
   - Transparent Auron execution for compatible queries

2. **Additional Operators**
   - Join operations (hash join, merge join)
   - Aggregations (hash aggregate, stream aggregate)
   - Window functions
   - Sort operations

3. **Streaming Support**
   - Extend to Flink streaming mode
   - Micro-batch execution
   - Stateful operations

4. **Advanced Optimizations**
   - Adaptive execution
   - Runtime filter pushdown
   - Dynamic partition pruning

### Extension Opportunities

- Custom UDFs (User-Defined Functions)
- Additional file formats (ORC, Avro, JSON)
- Object store integrations (S3, HDFS, Azure Blob)
- Custom shuffle implementations
- GPU acceleration via DataFusion GPU extensions

---

## Conclusion

Apache Auron provides high-performance native execution for Apache Flink through a carefully designed architecture that leverages:

- **Protobuf** for structured plan serialization
- **Apache Arrow** for efficient columnar data handling
- **JNI/FFI** for seamless JVM-native interop
- **DataFusion** for vectorized query execution

The modular design allows for easy extension and adaptation to other big data engines beyond Flink and Spark.

For more information, see:
- [Flink Integration README](auron-flink-extension/README.md)
- [Main README](README.md)
- [Build Status](auron-flink-extension/BUILD_STATUS.md)
