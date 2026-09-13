# Security Policy

<p><a href="SECURITY.md">English</a> | <a href="SECURITY.zh-CN.md">简体中文</a></p>

Last materially modified: 2026-08-29

Last materially synchronized: 2026-08-29

## Reporting a Vulnerability

Report a suspected vulnerability through the repository's
[GitHub private vulnerability reporting form](https://github.com/xiao-nanbei/NarrowGateMaker/security/advisories/new).
This is the preferred channel because the report and follow-up remain private while
the issue is assessed.

If GitHub does not offer that form for your account, open a
[minimal public issue](https://github.com/xiao-nanbei/NarrowGateMaker/issues/new/choose)
that asks the maintainer to establish a private reporting channel. Do not include
the vulnerability, exploit, proof of concept, affected host, credentials, private
data, or security-sensitive logs in that issue.

No dedicated public security email is published in this repository. Do not guess
or construct one from a contributor name or GitHub account.

## What to Include Privately

- a concise description of the vulnerability and likely impact;
- the affected version, tag, or commit, when known;
- minimal reproduction steps using synthetic or public data;
- relevant configuration assumptions with all secrets and private locators removed;
- any suggested mitigation or disclosure constraints.

Never send exchange credentials, signing keys, raw account state, private host
details, owner-side evidence, or restricted datasets. A maintainer may ask for a
smaller sanitized reproduction.

## Scope and Disclosure

Security reports cover vulnerabilities in the repository's maintained public code
and documented interfaces. Ordinary correctness bugs, research disagreements,
performance issues, and unsupported deployment questions belong in the normal
issue tracker unless they create a concrete security impact.

Please allow maintainers to triage and coordinate remediation before public
disclosure. This policy does not promise a response deadline, bounty, embargo
length, or support window. Historical research and execution tags are immutable
provenance; their existence is not a claim that every historical revision receives
security updates.

## Commercial Authorization

Security reporting is not a commercial licensing channel. NarrowGateMaker uses the
[PolyForm Noncommercial License 1.0.0](LICENSE) as a source-available license. It
permits the noncommercial purposes stated in its terms and restricts commercial
use, which requires separate written permission. The repository publishes no dedicated licensing email; use the
[commercial-license issue form](https://github.com/xiao-nanbei/NarrowGateMaker/issues/new?template=commercial_license.yml)
for a non-confidential inquiry. A public issue, discussion, or pull request is not
itself authorization.
