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

module.exports = {
    parserPreset: {
        parserOpts: {
            headerPattern: /^\[AURON #(\d+)\] (.+)$/,
            headerCorrespondence: ['issue', 'subject']
        }
    },
    plugins: [
        {
            rules: {
                'auron-format': (parsed) => {
                    const {issue, subject} = parsed;

                    if (!issue || !subject) {
                        return [false, 'PR title must follow format: [AURON #<issueNumber>]: <description>'];
                    }

                    // Verify that the issue number is numeric
                    if (!/^\d+$/.test(issue)) {
                        return [false, 'Issue number must be a valid number'];
                    }

                    // Verify that the description has appropriate length
                    if (subject.length < 3) {
                        return [false, 'Description must be at least 3 characters'];
                    }

                    if (subject.length > 100) {
                        return [false, 'Description must be less than 100 characters'];
                    }

                    return [true];
                }
            }
        }
    ],
    rules: {
        'auron-format': [2, 'always']
    }
};