# Security policy

## Supported versions

Security fixes are provided for the latest release line only.

| Version | Supported |
|---|---|
| 0.4.x | Yes |
| Earlier versions | No |

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability.

Use the hosting platform's private vulnerability reporting feature. On GitHub, open the repository's **Security** tab and select **Report a vulnerability**. If private reporting is unavailable, contact the maintainer through the private contact method listed on the repository owner's profile.

Include:

- affected version or commit;
- impact and expected threat model;
- reproduction steps or a minimal proof of concept;
- suggested mitigation, if known;
- whether the report may be credited publicly.

Do not include production credentials, private keys, customer data, or third-party personal data.

The project aims to acknowledge reports within seven days and provide an initial assessment within fourteen days. Timelines may vary with severity and maintainer availability.

## Scope

Reports are especially useful for malformed P2P input, unsafe fork or finality handling, unauthorized publisher delivery, secret exposure, path traversal, resource exhaustion, and persistence corruption.

Operational weaknesses that require control of a configured trusted peer or Solidity endpoint should still be reported, but describe that prerequisite clearly.
