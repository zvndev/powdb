# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in PowDB, please report it responsibly.

**Do not open a public issue.** Instead, email security concerns to:

**78920650+zvndev@users.noreply.github.com**

Include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

You should receive an acknowledgment within 48 hours. We aim to provide a fix or mitigation within 7 days for critical issues.

## Scope

PowDB is a storage engine and query executor. Security-relevant areas include:

- **Wire protocol** (`crates/server/`) — binary framing, authentication, connection limits
- **Query parser** (`crates/query/src/parser.rs`) — input validation, nesting depth limits
- **Storage engine** (`crates/storage/`) — WAL integrity, mmap safety, file I/O bounds
- **Network binding** — server binds to `127.0.0.1` by default (not `0.0.0.0`)

## Known Limitations

- Authentication is single-password (`POWDB_PASSWORD` env var). There is no per-user auth or RBAC.
- TLS is not implemented. Use a reverse proxy or SSH tunnel for encrypted connections.
- The query parser has a nesting depth limit but no query timeout mechanism yet.
