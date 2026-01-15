<!--
- Licensed to the Apache Software Foundation (ASF) under one or more
- contributor license agreements.  See the NOTICE file distributed with
- this work for additional information regarding copyright ownership.
- The ASF licenses this file to You under the Apache License, Version 2.0
- (the "License"); you may not use this file except in compliance with
- the License.  You may obtain a copy of the License at
-
-   http://www.apache.org/licenses/LICENSE-2.0
-
- Unless required by applicable law or agreed to in writing, software
- distributed under the License is distributed on an "AS IS" BASIS,
- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
- See the License for the specific language governing permissions and
- limitations under the License.
-->

# Contributing to Apache Auron (Incubating)

Welcome! We're excited that you're interested in contributing to Apache Auron. This document provides guidelines and information to help you contribute effectively to the project.

## Table of Contents

- [Ways to Contribute](#ways-to-contribute)
- [Getting Started](#getting-started)
- [Development Environment Setup](#development-environment-setup)
- [Building the Project](#building-the-project)
- [Before Submitting a Pull Request](#before-submitting-a-pull-request)
- [Pull Request Guidelines](#pull-request-guidelines)
- [Code Style and Formatting](#code-style-and-formatting)
- [Testing](#testing)
- [Documentation](#documentation)
- [Communication](#communication)
- [License](#license)

## Ways to Contribute

Contributions to Auron are not limited to code! Here are various ways you can help:

- **Report bugs**: File detailed bug reports with reproducible examples
- **Suggest features**: Propose new features or improvements via GitHub issues
- **Write code**: Fix bugs, implement features, or improve performance
- **Review pull requests**: Help review and test PRs from other contributors
- **Improve documentation**: Enhance README, API docs, or add examples
- **Answer questions**: Help other users on the mailing list or GitHub discussions
- **Benchmark and test**: Run benchmarks and report performance results

## Getting Started

### Prerequisites

Before contributing, ensure you have:

1. **Rust (nightly)**: Install via [rustup](https://rustup.rs/)
2. **JDK**: Version 8, 11, or 17 (set `JAVA_HOME` appropriately)
3. **Maven**: Version 3.9.11 or higher
4. **Git**: For version control

### Fork and Clone

1. Fork the [Apache Auron repository](https://github.com/apache/auron) on GitHub
2. Clone your fork locally:
   ```bash
   git clone git@github.com:<your-username>/auron.git
   cd auron
   ```
3. Add the upstream repository:
   ```bash
   git remote add upstream git@github.com:apache/auron.git
   ```

### Stay Synchronized

Keep your fork up to date with upstream:

```bash
git fetch upstream
git checkout master
git merge upstream/master
```

## Development Environment Setup

### Rust Setup

Auron uses Rust nightly. The project includes a `rust-toolchain.toml` file that specifies the required version:

```bash
rustup show  # Verify the correct toolchain is installed
```

### IDE Setup

For the best development experience:

- **IntelliJ IDEA** 
- **RustRover** or **VS Code with rust-analyzer**

## Building the Project

Auron provides a unified build script `auron-build.sh` that supports both local and Docker-based builds.

### Quick Start Build

```bash
# Local build with Spark 3.5 and Scala 2.12
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12

# Skip native build (useful for Java/Scala-only changes)
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 -DskipBuildNative
```

### Docker Build

```bash
# Build inside Docker container
./auron-build.sh --docker true --image centos7 --release \
  --sparkver 3.5 --scalaver 2.12
```

### Build Options

Run `./auron-build.sh --help` to see all available options, including:

- `--pre` or `--release`: Build profile
- `--sparkver`: Spark version (3.0, 3.1, 3.2, 3.3, 3.4, 3.5)
- `--scalaver`: Scala version (2.12, 2.13)
- `--celeborn`, `--uniffle`, `--paimon`, `--iceberg`: Optional integrations
- `--skiptests`: Skip unit tests (default: true)
- `--sparktests`: Run Spark integration tests

### Running Tests

By default, the build script skips unit tests (`--skiptests true`). To run them, you must explicitly set `--skiptests false`, as shown in the examples below:

```bash
# Run all tests
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --skiptests false

# Run Spark unit tests
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --sparktests true
```

## Before Submitting a Pull Request

Before creating a PR, please:

1. **Search for existing issues**: Check if someone else is already working on this

2. **Create a GitHub issue**: All contributions (including major changes) require a GitHub issue to be created first. This allows the community to discuss the approach and provide feedback before you invest time in implementation.

3. **For major changes, also create an AIP**: If your change is substantial (new features, significant architectural changes, or changes affecting the public API), you must, in addition to the GitHub issue from step 2:
   - Use the GitHub issue to describe the problem and proposed solution
   - Create an **Auron Improvement Plan (AIP)** document and link it from that GitHub issue
   - Discuss the AIP and high-level approach with the community in the GitHub issue
   - Wait for community feedback and consensus before starting implementation
   
   Example AIP: [AIP-3: Introduce auron-it for Enhanced CI Integration Testing](https://docs.google.com/document/d/1jR5VZ_uBd6Oe2x2UT_wGFzdX_xVLhORl6ghDs4f3FIw/edit?tab=t.0)

4. **Keep changes focused**: Split large changes into multiple smaller PRs when possible

5. **Write tests**: Add unit tests for bug fixes and new features

6. **Update documentation**: Update relevant docs if your change affects user-facing behavior

7. **Format your code**: Run the code formatter (see [Code Style](#code-style-and-formatting))

## Pull Request Guidelines

### PR Title Format

Use a clear, descriptive title that follows this format:

```
[AURON #<issue-number>] Brief description of the change
```

Examples:
- `[AURON #1805] Add contributing guidelines`
- `[AURON #123] Fix memory leak in shuffle manager`
- `[AURON #456] Add support for new aggregate function`

Note: Use the hash format `[AURON #<issue-number>]` consistently throughout your PR title and description.

### PR Size

We strongly prefer smaller, focused PRs over large ones because:

- Smaller PRs are reviewed more quickly
- Discussions remain focused and actionable
- Feedback is easier to incorporate early in the process

If you're working on a large feature, consider:
- Breaking it into multiple PRs
- Creating a draft PR to show the overall design
- Discussing the approach in a GitHub issue first

### Merging PRs

- PRs are merged using **squash and merge**
- At least one committer approval is required
- For significant changes, two committer approvals may be required
- Committers will ensure at least 24 hours pass for major changes to allow community review

## Code Style and Formatting

Auron enforces consistent code style across Java, Scala, and Rust code.

### Automatic Formatting

Use the `dev/reformat` script to format all code:

```bash
# Format all code
./dev/reformat

# Check formatting without making changes
./dev/reformat --check
```

### License Headers

All source files must include the Apache License 2.0 header. The build will fail if headers are missing or incorrect.

## Testing

### Unit Tests

- **Java/Scala tests**: Use ScalaTest framework
- **Rust tests**: Use standard Rust test framework

```bash
# Run Scala tests
./build/mvn test -Pspark-3.5 -Pscala-2.12

# Run Rust tests
cargo test
```

### Test Coverage

When adding new features or fixing bugs:

1. Add unit tests to cover the new code paths
2. If the feature is not covered by existing Spark tests, add integration tests
3. Ensure all tests pass before submitting your PR

## Documentation

### When to Update Documentation

Update documentation when your changes:

- Add new features or APIs
- Change existing behavior
- Add new configuration options
- Affect how users build or deploy Auron

### Documentation Locations

- **README.md**: High-level project overview and quick start
- **Code comments**: Inline documentation for complex logic
- **Configuration**: Update if adding new config properties

## Communication

### Mailing Lists

The primary communication channel for the Apache Auron community:

- **dev@auron.apache.org**: Development discussions
  - Subscribe: [dev-subscribe@auron.apache.org](mailto:dev-subscribe@auron.apache.org)
  - Unsubscribe: [dev-unsubscribe@auron.apache.org](mailto:dev-unsubscribe@auron.apache.org)

### GitHub Issues

- Search existing issues before creating a new one
- Provide clear reproduction steps for bugs
- Include environment details (Spark version, Scala version, OS, etc.)
- Use issue templates when available

### GitHub Discussions

Use GitHub Discussions for:
- Questions about using Auron
- Feature proposals and design discussions
- General community discussions

## License

By contributing to Apache Auron, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
Thank you for contributing to Apache Auron! Your efforts help make this project better for everyone. 🚀
