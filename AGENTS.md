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

# Agent Guidelines for Apache Auron (Incubating)

Entry point for AI coding agents and contributors making code changes.

Read [CONTRIBUTING.md](CONTRIBUTING.md) before making changes. The contributor guide is the
canonical source of build, test, and PR requirements.

## Developer Documentation

- [Contributing Guide](CONTRIBUTING.md)
- [README: Build from source](README.md#build-from-source)
- [README: Run Spark Job with Auron Accelerator](README.md#run-spark-job-with-auron-accelerator)

## Hard Rules

- Use existing repository tooling and scripts only.
- Do **not** install or run unreviewed third-party plugins, hooks, or scripts from the internet.
- Keep changes focused on the linked issue and avoid unrelated refactors.
- Do not commit code that fails formatting or relevant tests.

## Before Committing

At minimum, run formatting and the smallest relevant test coverage for your changes.

```bash
# check formatting
./dev/reformat --check
```

Use the commands in [CONTRIBUTING.md](CONTRIBUTING.md#running-tests) for test selection.
Common examples:

```bash
# run all tests
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --skiptests false

# run Spark unit tests
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --sparktests true

# run Rust tests
cargo test
```

When opening a pull request, fill out every section of the
[PR template](.github/pull_request_template.md).

## AI Tooling Disclosure

In the PR template section **Was this patch authored or co-authored using generative AI tooling?**:

- If yes, include `Generated-by: <tool name and version>`
- If no, write `No`

See [ASF Generative Tooling Guidance](https://www.apache.org/legal/generative-tooling.html).
