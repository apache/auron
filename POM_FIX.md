# Fixing the Auron POM Spotless Issue

## Problem

When running Maven commands, you encountered:

```
[ERROR] 'build.plugins.plugin.version' for com.diffplug.spotless:spotless-maven-plugin
must be a valid version but is '${spotless.plugin.version}'.
```

This was caused by two issues:

## Issue 1: Property Defined in Profile

The `spotless.plugin.version` property was defined **inside a Maven profile** (`java-17-plus`) but referenced **outside the profile** in `<build><plugins>`.

**Location of Issue:**
- `/Users/vsowrira/git/auron/pom.xml` line ~494: References `${spotless.plugin.version}`
- `/Users/vsowrira/git/auron/pom.xml` line ~755: Defines it inside `<profile>`

**Problem:**
Maven evaluates the POM before activating profiles, so it couldn't resolve the property.

## Issue 2: Wrong Java Version

Spotless 2.45.0 requires Java 11+ but the system was using Java 8.

```
[ERROR] com/diffplug/spotless/maven/SpotlessApplyMojo has been compiled by a more
recent version of the Java Runtime (class file version 55.0), this version of the
Java Runtime only recognizes class file versions up to 52.0
```

## Solution

### Fix 1: Move Property to Main `<properties>` Section

Added `spotless.plugin.version` to the main `<properties>` block (not inside any profile):

```xml
<properties>
    <!-- ... existing properties ... -->
    <spotless.plugin.version>2.45.0</spotless.plugin.version>
    <!-- ... more properties ... -->
</properties>
```

**File:** `/Users/vsowrira/git/auron/pom.xml`
**Line:** Added after line ~75

This ensures the property is always available, regardless of which profiles are active.

### Fix 2: Use Java 17

Set `JAVA_HOME` to Java 17 before building:

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home
```

Then run the build:

```bash
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --flinkver 1.18 -Dmaven.test.skip=true
```

## Full Build Command

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home
cd /Users/vsowrira/git/auron
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --flinkver 1.18 -Dmaven.test.skip=true
```

**This command:**
- Uses Java 17 (required for Spotless and Auron)
- Builds Auron with Flink 1.18 profile
- Skips all tests (both compilation and execution) for faster build
- Includes the `auron-flink-extension` module

## What Gets Built

After successful build, you'll have:

```
auron-flink-extension/
├── auron-flink-planner/target/
│   └── auron-flink-planner-7.0.0-SNAPSHOT.jar
├── auron-flink-runtime/target/
│   └── auron-flink-runtime-7.0.0-SNAPSHOT.jar
└── auron-flink-assembly/target/
    └── auron-flink-assembly-7.0.0-SNAPSHOT.jar
```

These JARs will automatically be installed to:
```
~/.m2/repository/org/apache/auron/
├── auron-flink-planner/7.0.0-SNAPSHOT/
├── auron-flink-runtime/7.0.0-SNAPSHOT/
└── auron-flink-assembly/7.0.0-SNAPSHOT/
```

## Running the Verification Test

Once build completes, run the verification test:

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home
cd /Users/vsowrira/git/auron/auron-flink-extension/auron-flink-planner
/Users/vsowrira/git/auron/build/apache-maven-3.9.12/bin/mvn test -Dtest=AuronAutoConversionVerificationTest -Pflink-1.18 -Pspark-3.5 -Pscala-2.12
```

## Alternative: Permanent Fix in Shell

Add to your `~/.zshrc` or `~/.bashrc`:

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH
```

Then restart your terminal or run:
```bash
source ~/.zshrc  # or source ~/.bashrc
```

## Why This Happened

The Auron POM was designed to support multiple Java versions through profiles:
- Profile `java-17-plus` is activated for Java 17-20
- Profile `java-21-plus` is activated for Java 21+

However, the plugin version was defined inside these profiles but used outside, creating a dependency ordering issue.

## Changes Made

**File:** `/Users/vsowrira/git/auron/pom.xml`

1. **Line ~46:** Added `<module>auron-flink-extension</module>` to properly include Flink modules
2. **Line ~76:** Added `<spotless.plugin.version>2.45.0</spotless.plugin.version>` to main properties

These changes allow the build to proceed without Maven property resolution errors.

## Summary

✅ **Fixed spotless property issue** by moving it to main properties
✅ **Fixed Java version issue** by using Java 17
✅ **Fixed module structure** by adding parent module instead of sub-modules
✅ **Build now works** with proper profiles

Now Auron can build against Flink 1.18 with Auron integration classes!
