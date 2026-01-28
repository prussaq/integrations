# Integrations

**Integrations** is a Python library providing functions to interact with cryptocurrency exchanges and other data providers.

It aims to make integrations **easier and slightly unified**, while staying **close to official API documentation**. Abstractions are intentionally minimal, and domain-specific behavior is preserved.

The library is intended to be **used as code**, not as a standalone service.

All imports are rooted at `integrations`, so the directory must be placed appropriately (e.g. at the application’s source root) to ensure imports resolve correctly.

---

### ⚠️ Caution on MEXC Web Module

Although the MEXC web module is included, **its use is strongly not recommended**.  
MEXC has recently introduced a new security system that likely detects automated requests from such modules.  
Using it may **lead to account restrictions or bans**. Proceed at your own risk.

---

## Overview

- Python-based
- Uses `requests` for HTTP
- Designed for direct integration into applications
- Interfaces closely follow official API documentation
- Includes simple, configurable rate limiting
- Provides configurable retry logic for idempotent operations

---

## Requirements

- Python 3.x
- `requests`

---

## Rate Limiting

Includes a **simple built-in rate limiter** to prevent accidental API abuse.

⚠️ **Caution:**  
The default rate limiter is conservative. For **latency-sensitive operations**, especially **placing orders**, consider to **tune, replace, or disable** the rate limiter according to their execution requirements and the exchange’s limits.

---

## Retry Logic

Provides **configurable retry logic for idempotent operations only** (e.g. data retrieval).

Retries apply **only** to:
- Network failures
- Transport / protocol-level errors

Retries are **explicitly disabled** for:
- All **non-idempotent operations** (e.g. placing orders, modifying margin)
- HTTP `4xx` responses
- API-level validation or business logic errors

---

## Contribution

- Submit pull requests to the **`main`** branch
- The **`master`** branch is personal

---

## License

MIT License
