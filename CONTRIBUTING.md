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
- [Code Review Process](#code-review-process)
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
   git clone https://github.com/<your-username>/auron.git
   cd auron
   ```
3. Add the upstream repository:
   ```bash
   git remote add upstream https://github.com/apache/auron.git
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

- **IntelliJ IDEA** or **VS Code** for Scala/Java development
- **RustRover** or **VS Code with rust-analyzer** for Rust development

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

```bash
# Run all tests
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --skiptests false

# Run Spark integration tests
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --sparktests true

# Run specific test suite
./build/mvn test -Pspark-3.5 -Pscala-2.12 -Dtest=YourTestSuite
```

## Before Submitting a Pull Request

Before creating a PR, please:

1. **Search for existing issues**: Check if someone else is already working on this
2. **File a GitHub issue**: For non-trivial changes, create an issue first to discuss the approach
3. **Keep changes focused**: Split large changes into multiple smaller PRs when possible
4. **Write tests**: Add unit tests for bug fixes and new features
5. **Update documentation**: Update relevant docs if your change affects user-facing behavior
6. **Format your code**: Run the code formatter (see [Code Style](#code-style-and-formatting))

## Pull Request Guidelines

### PR Title Format

Use a clear, descriptive title that follows this format:

```
[AURON-<issue-number>] Brief description of the change
```

Examples:
- `[AURON-1805] Add contributing guidelines`
- `[AURON-123] Fix memory leak in shuffle manager`
- `[AURON-456] Add support for new aggregate function`

For minor changes without an issue:
- `[MINOR] Fix typo in README`
- `[DOC] Update build instructions`

### PR Description

Include in your PR description:

1. **Which issue does this PR close?**: Reference the issue number
2. **Rationale for this change**: Explain why this change is needed
3. **What changes are included?**: Summarize the key changes
4. **Are there any user-facing changes?**: Note any API or behavior changes
5. **How was this tested?**: Describe your testing approach

Use the provided PR template as a guide.

### PR Size

We strongly prefer smaller, focused PRs over large ones because:

- Smaller PRs are reviewed more quickly
- Discussions remain focused and actionable
- Feedback is easier to incorporate early in the process

If you're working on a large feature, consider:
- Breaking it into multiple PRs
- Creating a draft PR to show the overall design
- Discussing the approach in a GitHub issue first

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

This script will:
- Format Java code using Spotless (Palantir Java Format)
- Format Scala code using Scalafmt
- Format Rust code using `cargo fmt`
- Apply Scalafix rules

### Java/Scala Style

- **Line length**: Maximum 98 characters
- **Imports**: Automatically sorted and organized
- **License header**: Required on all source files

Configuration files:
- `scalafmt.conf`: Scala formatting rules
- `scalafix.conf`: Scala linting rules
- `dev/license-header`: Apache license header template

### Rust Style

- Follow standard Rust conventions
- Use `cargo fmt` for formatting
- Use `cargo clippy` for linting

```bash
# Format Rust code
cargo fmt

# Check formatting
cargo fmt --check

# Run clippy
cargo clippy --all-targets --all-features
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

### Integration Tests

Auron includes extensive Spark integration tests:

```bash
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --sparktests true
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

## Code Review Process

### For Contributors

1. **Ensure CI passes**: All GitHub Actions checks must pass
2. **Address feedback**: Respond to review comments promptly
3. **Keep PR updated**: Rebase on master if needed
4. **Be patient**: Reviews may take time; reviewers are volunteers

### For Reviewers

When reviewing PRs:

1. **Be constructive**: Provide specific, actionable feedback
2. **Explain rationale**: Don't just say "don't do this" – explain why and suggest alternatives
3. **Check thoroughly**:
   - Are tests sufficient?
   - Is the code clear and maintainable?
   - Does it follow project conventions?
   - Are there potential performance implications?

### Merging PRs

- PRs are merged using **squash and merge**
- At least one committer approval is required
- For significant changes, two committer approvals may be required
- Committers will ensure at least 24 hours pass for major changes to allow community review

## License

By contributing to Apache Auron, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).

When you contribute code, you affirm that:
- The contribution is your original work
- You license the work to the project under Apache License 2.0
- You have the legal authority to do so

---

Thank you for contributing to Apache Auron! Your efforts help make this project better for everyone. 🚀
