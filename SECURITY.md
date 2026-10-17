# Security Policy

## Reporting a Security Issue

At Aggify, we take security seriously. We appreciate your help in identifying and addressing security issues. To report a security issue, please follow the guidelines below:

**DO NOT** create a public GitHub issue for security-related problems.

Instead, please email us directly at [support@aggify.org](mailto:support@aggify.org) with the following information:

1. A detailed description of the security issue, including any potential impact.
2. Steps to reproduce the issue or a proof-of-concept if applicable.
3. Any additional details that can help us understand and address the problem.

We will respond to your report as soon as possible and, if necessary, provide a timeline for a fix. We greatly appreciate responsible disclosure.

## Supported Versions

Aggify's security policy applies to the most recent stable release and the previous release. We encourage users to always keep their installations up to date with the latest version.

| Version | Supported          |
| ------- | ------------------ |
| 1.0.x   | :white_check_mark: |
| 0.x     | :x: (superseded by the 1.0 rewrite - please upgrade) |

## Security Updates

We are committed to addressing security issues promptly. In the event of a confirmed security vulnerability, we will:

- Acknowledge receipt of the report within 48 hours.
- Investigate and verify the reported issue.
- Develop and test a fix.
- Release a security update.
- Credit the individual or organization that reported the vulnerability.

## Contact

If you have any questions or concerns regarding the security of Aggify, please contact us at [support@aggify.org](mailto:support@aggify.org). Our team will be happy to assist you.

Thank you for helping us make Aggify more secure.


## Threat model and built-in protections

Aggify's protections (operator-injection prevention, regex escaping, JavaScript blocking, validated field names, resource
limits, credential masking) and what remains the application's responsibility are documented in
[docs/security.md](docs/security.md). Reports that bypass any of them - for example a value that reaches a query with
a `$` operator without going through `Raw()` - are treated as vulnerabilities.
