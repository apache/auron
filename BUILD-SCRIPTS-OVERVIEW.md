# Build Scripts Overview

This document provides a quick reference to all build scripts and documentation for the Flink-Auron integration.

## Quick Reference

| Task | Command | Where to Run |
|------|---------|--------------|
| **Build everything** | `./build-all.sh` | `/Users/vsowrira/git/auron` |
| **Build only Flink** | `./build-all.sh --flink-only` | `/Users/vsowrira/git/auron` |
| **Build only Auron** | `./build-all.sh --auron-only` | `/Users/vsowrira/git/auron` |
| **Clean build both** | `./build-all.sh --clean` | `/Users/vsowrira/git/auron` |
| **Fast build (no tests)** | `./build-all.sh --skip-tests` | `/Users/vsowrira/git/auron` |
| **Run examples** | `./run-example.sh groupby` | `/Users/vsowrira/git/auron/auron-flink-extension/auron-flink-planner` |
| **Run tests** | `./run-e2e-test-final.sh` | `/Users/vsowrira/git/auron/auron-flink-extension/auron-flink-planner` |

## Build Scripts

### 1. `/Users/vsowrira/git/auron/build-all.sh` (NEW - Unified Build)

**Purpose**: Build both Flink and Auron with intelligent dependency management

**Features**:
- ✅ Checks if Flink needs to be built (smart detection)
- ✅ Builds Flink 1.18-SNAPSHOT with Auron integration
- ✅ Builds Auron against those Flink JARs
- ✅ Verifies integration classes are present
- ✅ Supports granular control (Flink-only, Auron-only, clean builds)

**Usage**:
```bash
cd /Users/vsowrira/git/auron

# Most common usage
./build-all.sh                  # Build both (smart - skips Flink if up-to-date)
./build-all.sh --skip-tests     # Fast build
./build-all.sh --clean          # Force clean rebuild

# Granular control
./build-all.sh --flink-only     # Build only Flink
./build-all.sh --auron-only     # Build only Auron

# Help
./build-all.sh --help
```

**When to use**:
- **First time setup**: Always use this
- **After modifying Flink code**: `./build-all.sh --flink-only`
- **After modifying Auron code**: `./build-all.sh --auron-only`
- **Things are broken**: `./build-all.sh --clean`

### 2. `/Users/vsowrira/git/auron/build-flink.sh` (Auron-Only Build)

**Purpose**: Build only Auron with Flink support (assumes Flink already built)

**Usage**:
```bash
cd /Users/vsowrira/git/auron

./build-flink.sh          # Build and install (skip tests)
./build-flink.sh clean    # Clean build from scratch
./build-flink.sh test     # Build and run tests
```

**When to use**: When you only need to rebuild Auron and Flink is already built

### 3. `/Users/vsowrira/git/auron/auron-flink-extension/auron-flink-planner/run-example.sh`

**Purpose**: Run Auron-Flink integration examples

**Usage**:
```bash
cd /Users/vsowrira/git/auron/auron-flink-extension/auron-flink-planner

./run-example.sh groupby    # GROUP BY hybrid execution (recommended)
./run-example.sh parallel   # Parallel execution (50K rows)
./run-example.sh mvp        # Basic MVP example
```

**Features**:
- ✅ Automatically handles dependencies
- ✅ Compiles test classes if needed
- ✅ Fast execution (< 10 seconds after first run)

### 4. `/Users/vsowrira/git/auron/auron-flink-extension/auron-flink-planner/run-e2e-test-final.sh`

**Purpose**: Run comprehensive integration tests

**Usage**:
```bash
cd /Users/vsowrira/git/auron/auron-flink-extension/auron-flink-planner

./run-e2e-test-final.sh              # Default: Execution test
./run-e2e-test-final.sh Execution    # Shows Auron conversion logs
./run-e2e-test-final.sh Simple       # Basic infrastructure test
./run-e2e-test-final.sh Manual       # Manual end-to-end test
```

## Documentation

### 1. BUILD-GUIDE.md (Comprehensive)

**Location**: `/Users/vsowrira/git/auron/BUILD-GUIDE.md`

**Contents**:
- Complete build process for Flink + Auron
- Manual build instructions
- Verification steps
- Troubleshooting guide
- Development workflow
- Performance tips

**When to read**: Detailed reference for build system

### 2. QUICKSTART-FLINK.md (Quick Reference)

**Location**: `/Users/vsowrira/git/auron/QUICKSTART-FLINK.md`

**Contents**:
- Prerequisites
- Quick build commands
- Running examples
- Expected output
- Troubleshooting

**When to read**: Getting started quickly

### 3. BUILD-SCRIPTS-OVERVIEW.md (This File)

**Location**: `/Users/vsowrira/git/auron/BUILD-SCRIPTS-OVERVIEW.md`

**Contents**:
- Quick reference table
- All build scripts explained
- Documentation index

**When to read**: Finding the right script or documentation

### 4. README.md (Main Documentation)

**Location**: `/Users/vsowrira/git/auron/README.md`

**Contents**:
- Project overview
- Spark integration
- Flink integration (section)
- Performance benchmarks

**When to read**: Understanding the project

### 5. CLAUDE.md (Development Guidelines)

**Location**: `/Users/vsowrira/git/auron/CLAUDE.md`

**Contents**:
- Java version requirements
- Build commands for development
- Test execution
- Key integration points

**When to read**: Development and troubleshooting

### 6. BUILD-AURON-INTEGRATION.md (Flink Side)

**Location**: `/Users/vsowrira/git/flink/BUILD-AURON-INTEGRATION.md`

**Contents**:
- Building Flink with Auron integration
- What gets installed
- Integration changes overview
- Verification steps

**When to read**: Working on Flink integration code

### 7. AURON_INTEGRATION_1.18.md (Flink Side)

**Location**: `/Users/vsowrira/git/flink/AURON_INTEGRATION_1.18.md`

**Contents**:
- Complete integration details
- API compatibility notes
- End-to-end flow explanation

**When to read**: Understanding the integration architecture

## Directory Structure

```
/Users/vsowrira/git/
│
├── flink/  (Flink with Auron integration)
│   ├── auron-flink-1.18-integration (branch)
│   ├── BUILD-AURON-INTEGRATION.md (NEW)
│   ├── AURON_INTEGRATION_1.18.md
│   ├── NEXT_STEPS.md
│   └── flink-table/
│       ├── flink-table-api-java/
│       │   └── OptimizerConfigOptions.java (Auron config)
│       └── flink-table-planner/
│           ├── AuronExecNodeGraphProcessor.java (pattern detection)
│           └── AuronBatchExecNode.java (wrapper node)
│
└── auron/  (Auron native execution engine)
    ├── build-all.sh (NEW - unified build)
    ├── build-flink.sh (Auron-only build)
    ├── BUILD-GUIDE.md (NEW - comprehensive guide)
    ├── BUILD-SCRIPTS-OVERVIEW.md (NEW - this file)
    ├── QUICKSTART-FLINK.md (updated)
    ├── README.md (updated)
    ├── CLAUDE.md
    └── auron-flink-extension/auron-flink-planner/
        ├── run-example.sh (test runner)
        ├── run-e2e-test-final.sh (integration tests)
        ├── AuronExecNodeConverter.java (converts exec nodes)
        ├── AuronTransformationFactory.java (creates transformations)
        └── AuronBatchExecutionWrapperOperator.java (executes native code)
```

## Typical Workflows

### First Time Setup

```bash
# Clone repositories (already done)
cd /Users/vsowrira/git/flink && git checkout auron-flink-1.18-integration
cd /Users/vsowrira/git/auron

# Build everything
./build-all.sh

# Run tests
cd auron-flink-extension/auron-flink-planner
./run-example.sh groupby
```

**Time**: 15-20 minutes

### Daily Development - Modifying Auron

```bash
cd /Users/vsowrira/git/auron

# Make changes to Auron code
vim auron-flink-extension/auron-flink-planner/src/main/java/.../SomeClass.java

# Rebuild Auron only
./build-all.sh --auron-only

# Test
cd auron-flink-extension/auron-flink-planner
./run-example.sh groupby
```

**Time**: 3-5 minutes

### Daily Development - Modifying Flink Integration

```bash
cd /Users/vsowrira/git/flink

# Make changes to Flink integration code
vim flink-table/flink-table-planner/src/main/java/.../AuronExecNodeGraphProcessor.java

# Build Flink and Auron
cd /Users/vsowrira/git/auron
./build-all.sh --flink-only
./build-all.sh --auron-only

# Test
cd auron-flink-extension/auron-flink-planner
./run-example.sh groupby
```

**Time**: 5-10 minutes (Flink incremental: 2-3 min, Auron: 3-5 min)

### Clean Rebuild (When Things Break)

```bash
cd /Users/vsowrira/git/auron
./build-all.sh --clean
```

**Time**: 20-25 minutes

### Fast Iteration (Skip Tests)

```bash
cd /Users/vsowrira/git/auron
./build-all.sh --skip-tests --auron-only
```

**Time**: 2-3 minutes

## Decision Tree

**Which script should I use?**

```
┌─────────────────────────────────────┐
│   First time building everything?   │
└─────────────────┬───────────────────┘
                  │ YES
                  ▼
      ┌───────────────────────┐
      │  ./build-all.sh       │ ──────► Done! (~15-20 min)
      └───────────────────────┘
                  │ NO
                  ▼
┌─────────────────────────────────────┐
│  Did you modify Flink integration?  │
└─────────────────┬───────────────────┘
                  │ YES
                  ▼
      ┌───────────────────────────────┐
      │  ./build-all.sh --flink-only  │ ──────► Then --auron-only (~10 min)
      └───────────────────────────────┘
                  │ NO
                  ▼
┌─────────────────────────────────────┐
│   Did you modify Auron converter?   │
└─────────────────┬───────────────────┘
                  │ YES
                  ▼
      ┌───────────────────────────────┐
      │  ./build-all.sh --auron-only  │ ──────► Done! (~3-5 min)
      └───────────────────────────────┘
                  │ NO
                  ▼
┌─────────────────────────────────────┐
│      Just want to run tests?        │
└─────────────────┬───────────────────┘
                  │ YES
                  ▼
      ┌───────────────────────────┐
      │  ./run-example.sh groupby │ ──────► Done! (~10 sec)
      └───────────────────────────┘
                  │ NO
                  ▼
┌─────────────────────────────────────┐
│         Something broken?           │
└─────────────────┬───────────────────┘
                  │ YES
                  ▼
      ┌───────────────────────┐
      │  ./build-all.sh --clean│ ──────► Fresh start (~20 min)
      └───────────────────────┘
```

## Summary

| Script | Purpose | When to Use | Time |
|--------|---------|-------------|------|
| `build-all.sh` | Build both Flink and Auron | First time, or comprehensive rebuild | 15-20 min (first), 3-5 min (incremental) |
| `build-all.sh --flink-only` | Build only Flink | After modifying Flink integration code | 10-15 min (first), 2-3 min (incremental) |
| `build-all.sh --auron-only` | Build only Auron | After modifying Auron code | 3-5 min |
| `build-all.sh --clean` | Clean rebuild both | When things break | 20-25 min |
| `build-all.sh --skip-tests` | Fast build | Quick iteration | 5-8 min |
| `build-flink.sh` | Build Auron (Flink assumed ready) | Legacy/simple Auron rebuild | 3-5 min |
| `run-example.sh` | Run examples | Testing integration | < 10 sec |
| `run-e2e-test-final.sh` | Run integration tests | Comprehensive verification | < 10 sec |

## Getting Help

- **Build issues**: See [BUILD-GUIDE.md](BUILD-GUIDE.md) Troubleshooting section
- **Quick reference**: See [QUICKSTART-FLINK.md](QUICKSTART-FLINK.md)
- **Integration details**: See `/Users/vsowrira/git/flink/AURON_INTEGRATION_1.18.md`
- **Script help**: Run `./build-all.sh --help`
