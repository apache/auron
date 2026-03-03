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