"""
Periodically detect OpenAI accounts in Sub2Api that require reauthorization
and complete the OAuth refresh flow automatically.

Flow:
1. Fetch active OpenAI accounts
2. Call /api/v1/admin/accounts/:id/test and parse the SSE stream
3. Generate a new auth_url only for authentication-related failures
4. Query the password from D1, fetch or recreate the mailbox via DuckMail,
   and complete the OAuth flow automatically
5. Exchange the code and write refreshed credentials back to Sub2Api
6. Run /test again to verify the updated account

Output:
- logs/update_auth.json
"""

import argparse
import base64
import json
import os
import random
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, quote, unquote, urlparse

from curl_cffi import requests as curl_requests

from simple_register import (
    _extract_code_from_url,
    _make_trace_headers,
    _random_chrome_version,
    build_sentinel_token,
)


REAUTH_KEYWORDS = (
    "401",
    "unauthorized",
    "token invalidated",
    "invalidated",
    "sign in again",
    "re-auth",
    "reauth",
    "重新授权",
    "重新登录",
)

_PRINT_LOCK = Lock()


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _log(message: str, *, account_id: Optional[int] = None, email: str = "", stage: str = "") -> None:
    parts = [f"[update_auth {_now_iso()}]"]
    if stage:
        parts.append(f"[{stage}]")
    if account_id is not None:
        label = str(account_id)
        if email:
            label = f"{label} {email}"
        parts.append(f"[{label}]")
    elif email:
        parts.append(f"[{email}]")
    with _PRINT_LOCK:
        print(" ".join(parts), message, flush=True)


class Sub2ApiAuthError(RuntimeError):
    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


def _normalize_email(value: str) -> str:
    return str(value or "").strip().lower()


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_config(config_path: str) -> Dict[str, Any]:
    config: Dict[str, Any] = {}
    path = Path(config_path)
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            config.update(json.load(f))

    env_mapping = {
        "SUB2API_BASE_URL": "sub2api_base_url",
        "SUB2API_BEARER": "sub2api_bearer",
        "SUB2API_EMAIL": "sub2api_email",
        "SUB2API_PASSWORD": "sub2api_password",
        "DUCKMAIL_API_BASE": "duckmail_api_base",
        "DUCKMAIL_BEARER": "duckmail_bearer",
        "D1_API_BASE_URL": "d1_api_base_url",
        "D1_API_KEY": "d1_api_key",
        "OAUTH_REDIRECT_URI": "oauth_redirect_uri",
        "SUB2API_PROXY_ID": "sub2api_proxy_id",
    }
    for env_name, key in env_mapping.items():
        value = os.getenv(env_name)
        if value is not None and str(value).strip() != "":
            config[key] = value

    if "oauth_redirect_uri" not in config or not str(config.get("oauth_redirect_uri") or "").strip():
        config["oauth_redirect_uri"] = "http://localhost:1455/auth/callback"

    return config


def _resolve_sub2api_runtime_config(config: Dict[str, Any]) -> Dict[str, Any]:
    resolved = dict(config)
    static_bearer = str(resolved.get("sub2api_bearer", "")).strip()
    if static_bearer:
        _log("Using Sub2Api auth mode: static bearer", stage="main")
        return resolved

    _log("Using Sub2Api auth mode: one-time login", stage="main")
    client = Sub2ApiAdminClient(resolved)
    last_error: Optional[Exception] = None
    base_delay = 1.5

    for attempt in range(1, 4):
        try:
            token = client.bootstrap_login()
            resolved["sub2api_bearer"] = token
            _log(f"Established shared Sub2Api bearer on attempt {attempt}", stage="main")
            return resolved
        except Sub2ApiAuthError as exc:
            last_error = exc
            retryable = exc.status_code in {429, 500, 502, 503, 504} or exc.status_code is None
        except Exception as exc:
            last_error = exc
            retryable = True

        if attempt >= 3 or not retryable:
            break

        delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0.0, 0.5)
        _log(
            f"Shared Sub2Api login attempt {attempt} failed, retrying in {delay:.1f}s: {last_error}",
            stage="main",
        )
        time.sleep(delay)

    _log(f"Failed to establish shared Sub2Api bearer: {last_error}", stage="main")
    raise RuntimeError(f"Failed to establish shared Sub2Api bearer: {last_error}") from last_error


def _mail_api_url(api_base: str, path: str) -> str:
    base = str(api_base or "").rstrip("/")
    rel = "/" + str(path or "").lstrip("/")
    parsed = urlparse(base if "://" in base else f"https://{base}")
    base_path = parsed.path.rstrip("/")
    if base_path.endswith("/api") and rel == "/api":
        rel = ""
    elif base_path.endswith("/api") and rel.startswith("/api/"):
        rel = rel[4:]
    return f"{base}{rel}"


def _extract_verification_code(content: str) -> Optional[str]:
    if not content:
        return None
    patterns = [
        r"Verification code:?\s*(\d{6})",
        r"code is\s*(\d{6})",
        r"代码为[:：]?\s*(\d{6})",
        r"验证码[:：]?\s*(\d{6})",
        r">\s*(\d{6})\s*<",
        r"(?<![#&])\b(\d{6})\b",
    ]
    for pattern in patterns:
        matches = re.findall(pattern, content, re.IGNORECASE)
        for code in matches:
            if code == "177010":
                continue
            return code
    return None


class D1PasswordClient:
    def __init__(self, config: Dict[str, Any]):
        self.base_url = str(config.get("d1_api_base_url", "")).strip().rstrip("/")
        self.api_key = str(config.get("d1_api_key", "")).strip()
        self.session = curl_requests.Session(impersonate="chrome131")
        if not self.base_url or not self.api_key:
            raise ValueError("D1 is not configured: d1_api_base_url and d1_api_key are required")

    def get_password(self, email: str) -> str:
        normalized_email = _normalize_email(email)
        resp = self.session.get(
            f"{self.base_url}/v1/accounts/{quote(normalized_email, safe='')}",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Accept": "application/json",
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        account = data.get("account") or {}
        password = str(account.get("password") or "").strip()
        if not password:
            raise RuntimeError(f"No password found in D1 for {normalized_email}")
        return password


class Sub2ApiAdminClient:
    def __init__(self, config: Dict[str, Any]):
        self.base_url = str(config.get("sub2api_base_url", "")).strip().rstrip("/")
        self.admin_email = str(config.get("sub2api_email", "")).strip()
        self.admin_password = str(config.get("sub2api_password", "")).strip()
        self.admin_bearer = str(config.get("sub2api_bearer", "")).strip()
        self.session = curl_requests.Session(impersonate="chrome131")
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        })
        self._token = ""

        if not self.base_url:
            raise ValueError("sub2api_base_url is not configured")
        if not self.admin_bearer and (not self.admin_email or not self.admin_password):
            raise ValueError("Missing Sub2Api credentials: provide sub2api_bearer or sub2api_email + sub2api_password")

    def _request_login_token(self) -> str:
        resp = self.session.post(
            f"{self.base_url}/api/v1/auth/login",
            json={"email": self.admin_email, "password": self.admin_password},
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        if resp.status_code >= 400:
            raise Sub2ApiAuthError(
                f"Sub2Api login failed: {resp.status_code} - {resp.text[:300]}",
                status_code=resp.status_code,
            )
        data = resp.json()
        token = str((data.get("data") or {}).get("access_token") or "").strip()
        if not token:
            raise Sub2ApiAuthError("Sub2Api login succeeded but no access_token was returned")
        return token

    def _login(self) -> str:
        if self.admin_bearer:
            return self.admin_bearer
        if self._token:
            return self._token
        self._token = self._request_login_token()
        return self._token

    def bootstrap_login(self) -> str:
        token = self._request_login_token()
        self._token = token
        return token

    def _headers(self, accept: str = "application/json") -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._login()}",
            "Accept": accept,
            "Content-Type": "application/json",
        }

    def list_accounts(self, platform: str = "openai", status: str = "active", page_size: int = 100) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        page = 1
        safe_page_size = max(1, page_size)

        while True:
            params: Dict[str, Any] = {
                "platform": platform,
                "page": page,
                "page_size": safe_page_size,
            }
            if status:
                params["status"] = status

            resp = self.session.get(
                f"{self.base_url}/api/v1/admin/accounts",
                params=params,
                headers=self._headers(),
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json().get("data") or {}
            page_items = payload.get("items") or []
            if not page_items:
                break

            for item in page_items:
                items.append({
                    "id": item.get("id"),
                    "email": _normalize_email(item.get("name") or item.get("email") or ""),
                    "proxy_id": item.get("proxy_id"),
                })

            if len(page_items) < safe_page_size:
                break
            page += 1

        return [item for item in items if item.get("id") and item.get("email")]

    def test_account(self, account_id: int) -> Dict[str, Any]:
        resp = self.session.post(
            f"{self.base_url}/api/v1/admin/accounts/{account_id}/test",
            headers=self._headers(accept="text/event-stream"),
            timeout=90,
            stream=True,
        )
        if resp.status_code >= 400:
            body = resp.text[:300]
            try:
                resp.close()
            except Exception:
                pass
            raise RuntimeError(f"Sub2Api test failed: {resp.status_code} - {body}")

        text_parts: List[str] = []
        error_text = ""
        saw_complete = False
        try:
            for raw_line in resp.iter_lines():
                if not raw_line:
                    continue
                line = raw_line.decode("utf-8", errors="ignore").strip()
                if not line.startswith("data: "):
                    continue
                payload = line[6:]
                try:
                    event = json.loads(payload)
                except Exception:
                    continue

                event_type = str(event.get("type") or "").strip()
                if event_type == "content":
                    text = str(event.get("text") or "").strip()
                    if text:
                        text_parts.append(text)
                elif event_type == "error":
                    error_text = str(event.get("error") or "").strip()
                elif event_type == "test_complete":
                    saw_complete = True
        finally:
            try:
                resp.close()
            except Exception:
                pass

        success = saw_complete and not error_text
        return {
            "success": success,
            "error": error_text,
            "text": " ".join(part for part in text_parts if part).strip(),
        }

    @staticmethod
    def is_reauth_error(error_text: str) -> bool:
        normalized = str(error_text or "").strip().lower()
        if not normalized:
            return False
        return any(keyword in normalized for keyword in REAUTH_KEYWORDS)

    def generate_auth_url(self, account: Dict[str, Any], redirect_uri: str, default_proxy_id: Optional[int]) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        proxy_id = account.get("proxy_id")
        if proxy_id is None:
            proxy_id = default_proxy_id
        if proxy_id is not None:
            payload["proxy_id"] = proxy_id
        if redirect_uri:
            payload["redirect_uri"] = redirect_uri

        resp = self.session.post(
            f"{self.base_url}/api/v1/admin/openai/generate-auth-url",
            json=payload,
            headers=self._headers(),
            timeout=30,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"generate-auth-url failed: {resp.status_code} - {resp.text[:500]}")
        data = (resp.json().get("data") or {})
        return {
            "session_id": data.get("session_id"),
            "auth_url": data.get("auth_url"),
            "redirect_uri": payload.get("redirect_uri") or "",
            "proxy_id": proxy_id,
        }

    def get_account(self, account_id: int) -> Dict[str, Any]:
        resp = self.session.get(
            f"{self.base_url}/api/v1/admin/accounts/{account_id}",
            headers=self._headers(),
            timeout=30,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"Failed to fetch account: {resp.status_code} - {resp.text[:300]}")
        return resp.json().get("data") or {}

    def exchange_code(
        self,
        session_id: str,
        state: str,
        code: str,
        redirect_uri: str,
        proxy_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "session_id": session_id,
            "state": state,
            "code": code,
        }
        if redirect_uri:
            payload["redirect_uri"] = redirect_uri
        if proxy_id is not None:
            payload["proxy_id"] = proxy_id

        resp = self.session.post(
            f"{self.base_url}/api/v1/admin/openai/exchange-code",
            json=payload,
            headers=self._headers(),
            timeout=60,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"exchange-code failed: {resp.status_code} - {resp.text[:500]}")
        return resp.json().get("data") or {}

    @staticmethod
    def build_credentials(token_info: Dict[str, Any], existing: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        existing_creds = dict(existing or {})
        creds = dict(existing_creds)

        access_token = str(token_info.get("access_token") or "").strip()
        refresh_token = str(token_info.get("refresh_token") or "").strip()
        id_token = str(token_info.get("id_token") or "").strip()
        email = str(token_info.get("email") or "").strip()
        chatgpt_account_id = str(token_info.get("chatgpt_account_id") or "").strip()
        chatgpt_user_id = str(token_info.get("chatgpt_user_id") or "").strip()
        organization_id = str(token_info.get("organization_id") or "").strip()
        plan_type = str(token_info.get("plan_type") or "").strip()
        client_id = str(token_info.get("client_id") or "").strip()
        expires_at_raw = token_info.get("expires_at")

        if not access_token:
            raise RuntimeError("exchange-code did not return access_token")

        creds["access_token"] = access_token
        if expires_at_raw:
            expires_at_ts = int(expires_at_raw)
            creds["expires_at"] = datetime.fromtimestamp(expires_at_ts, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        if refresh_token:
            creds["refresh_token"] = refresh_token
        if id_token:
            creds["id_token"] = id_token
        if email:
            creds["email"] = email
        if chatgpt_account_id:
            creds["chatgpt_account_id"] = chatgpt_account_id
        if chatgpt_user_id:
            creds["chatgpt_user_id"] = chatgpt_user_id
        if organization_id:
            creds["organization_id"] = organization_id
        if plan_type:
            creds["plan_type"] = plan_type
        if client_id:
            creds["client_id"] = client_id

        return creds

    def update_account_credentials(self, account_id: int, credentials: Dict[str, Any]) -> Dict[str, Any]:
        resp = self.session.put(
            f"{self.base_url}/api/v1/admin/accounts/{account_id}",
            json={"credentials": credentials},
            headers=self._headers(),
            timeout=60,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"Failed to update account: {resp.status_code} - {resp.text[:500]}")
        return resp.json().get("data") or {}


class MoeMailClient:
    def __init__(self, config: Dict[str, Any]):
        self.api_base = str(config.get("duckmail_api_base", "")).strip().rstrip("/")
        self.api_key = str(config.get("duckmail_bearer", "")).strip()
        self.session = curl_requests.Session(impersonate="chrome131")
        if not self.api_base or not self.api_key:
            raise ValueError("DuckMail/MoeMail is not configured: duckmail_api_base and duckmail_bearer are required")

    def _headers(self) -> Dict[str, str]:
        return {"X-API-Key": self.api_key, "Accept": "application/json"}

    def _list_emails_page(self, cursor: str = "") -> Dict[str, Any]:
        params = {"cursor": cursor} if cursor else None
        resp = self.session.get(
            _mail_api_url(self.api_base, "/api/emails"),
            params=params,
            headers=self._headers(),
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    def list_emails(self) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        cursor = ""
        seen_cursors: Set[str] = set()
        while True:
            data = self._list_emails_page(cursor)
            page_items = data.get("emails") or data.get("data") or []
            if page_items:
                items.extend(page_items)
            next_cursor = str(data.get("nextCursor") or "").strip()
            if not next_cursor or next_cursor in seen_cursors:
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        return items

    def find_email(self, email: str) -> Optional[Dict[str, Any]]:
        target = _normalize_email(email)
        cursor = ""
        seen_cursors: Set[str] = set()
        while True:
            data = self._list_emails_page(cursor)
            for item in data.get("emails") or data.get("data") or []:
                address = _normalize_email(item.get("address") or item.get("email"))
                if address == target:
                    return item
            next_cursor = str(data.get("nextCursor") or "").strip()
            if not next_cursor or next_cursor in seen_cursors:
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        return None

    def create_same_email(self, email: str) -> Dict[str, Any]:
        local_part, domain = _normalize_email(email).split("@", 1)
        resp = self.session.post(
            _mail_api_url(self.api_base, "/api/emails/generate"),
            json={"name": local_part, "expiryTime": 86400000, "domain": domain},
            headers=self._headers(),
            timeout=15,
        )
        if resp.status_code == 409:
            matched = self.find_email(email)
            if matched:
                return matched
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"Failed to recreate mailbox: {resp.status_code} - {resp.text[:200]}")

        data = resp.json()
        inner = data.get("data") or {}
        email_id = data.get("id") or data.get("emailId") or inner.get("id") or inner.get("emailId")
        address = data.get("email") or data.get("address") or inner.get("email") or inner.get("address")
        if email_id and _normalize_email(address) == _normalize_email(email):
            return {"id": email_id, "address": address}

        matched = self.find_email(email)
        if matched:
            return matched
        raise RuntimeError("Mailbox was recreated but email_id could not be confirmed")

    def ensure_email(self, email: str) -> Dict[str, Any]:
        item = self.find_email(email)
        if item:
            item["_recreated"] = False
            return item
        item = self.create_same_email(email)
        item["_recreated"] = True
        return item

    def fetch_email_detail(self, email_id: str) -> Dict[str, Any]:
        resp = self.session.get(
            _mail_api_url(self.api_base, f"/api/emails/{email_id}"),
            headers=self._headers(),
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    def get_message_ids(self, email_id: str) -> Set[str]:
        detail = self.fetch_email_detail(email_id)
        messages = detail.get("messages") or detail.get("items") or []
        ids: Set[str] = set()
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            msg_id = str(msg.get("id") or "").strip()
            if msg_id:
                ids.add(msg_id)
        return ids

    def wait_for_openai_otp(
        self,
        email_id: str,
        baseline_ids: Set[str],
        timeout: int = 480,
        account_id: Optional[int] = None,
        email: str = "",
    ) -> str:
        deadline = time.time() + timeout
        seen = set(baseline_ids)
        start_time = time.time()
        next_report_after = 30
        _log(
            f"Waiting for mailbox OTP (timeout={timeout}s, baseline_messages={len(baseline_ids)})",
            account_id=account_id,
            email=email,
            stage="oauth",
        )

        while time.time() < deadline:
            detail = self.fetch_email_detail(email_id)
            messages = detail.get("messages") or detail.get("items") or []
            for msg in messages:
                if not isinstance(msg, dict):
                    continue
                msg_id = str(msg.get("id") or "").strip()
                if msg_id and msg_id in seen:
                    continue
                if msg_id:
                    seen.add(msg_id)
                content = "\n".join([
                    str(msg.get("subject") or ""),
                    str(msg.get("content") or ""),
                    str(msg.get("text") or ""),
                    str(msg.get("html") or ""),
                ])
                code = _extract_verification_code(content)
                if code:
                    _log("Mailbox OTP received", account_id=account_id, email=email, stage="oauth")
                    return code
            elapsed = int(time.time() - start_time)
            if elapsed >= next_report_after:
                _log(
                    f"Still waiting for OTP ({elapsed}s/{timeout}s)",
                    account_id=account_id,
                    email=email,
                    stage="oauth",
                )
                next_report_after += 30
            time.sleep(5)
        raise TimeoutError(f"Timed out waiting for mailbox verification code ({timeout}s)")


class OAuthRunner:
    def __init__(self, auth_url: str, redirect_uri: str, email: str, password: str, mail_client: MoeMailClient):
        self.auth_url = auth_url.strip()
        self.redirect_uri = redirect_uri.strip()
        self.email = _normalize_email(email)
        self.password = password
        self.mail_client = mail_client
        self.device_id = str(uuid.uuid4())
        self.impersonate, _, self.chrome_full, self.ua, self.sec_ch_ua = _random_chrome_version()
        self.session = curl_requests.Session(impersonate=self.impersonate)
        self.session.headers.update({
            "User-Agent": self.ua,
            "Accept-Language": random.choice(["en-US,en;q=0.9", "en-US,en;q=0.9,zh-CN;q=0.8"]),
            "sec-ch-ua": self.sec_ch_ua,
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-ch-ua-arch": '"x86"',
            "sec-ch-ua-bitness": '"64"',
            "sec-ch-ua-full-version": f'"{self.chrome_full}"',
            "sec-ch-ua-platform-version": f'"{random.randint(10, 15)}.0.0"',
        })
        self.session.cookies.set("oai-did", self.device_id, domain=".auth.openai.com")
        self.session.cookies.set("oai-did", self.device_id, domain="auth.openai.com")
        self.state = parse_qs(urlparse(self.auth_url).query).get("state", [""])[0]
        parsed = urlparse(self.auth_url)
        self.oauth_issuer = f"{parsed.scheme}://{parsed.netloc}"

    def _callback_matches(self, url: str) -> bool:
        if not url:
            return False
        if self.redirect_uri and url.startswith(self.redirect_uri):
            return True
        return bool(_extract_code_from_url(url))

    def _oauth_json_headers(self, referer: str) -> Dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": self.oauth_issuer,
            "Referer": referer,
            "User-Agent": self.ua,
            "oai-device-id": self.device_id,
        }
        headers.update(_make_trace_headers())
        return headers

    def _decode_oauth_session_cookie(self) -> Optional[Dict[str, Any]]:
        jar = getattr(self.session.cookies, "jar", None)
        cookie_items = list(jar) if jar is not None else []
        for cookie in cookie_items:
            name = getattr(cookie, "name", "") or ""
            if "oai-client-auth-session" not in name:
                continue
            raw_value = (getattr(cookie, "value", "") or "").strip()
            if not raw_value:
                continue
            candidates = [raw_value]
            try:
                decoded = unquote(raw_value)
                if decoded != raw_value:
                    candidates.append(decoded)
            except Exception:
                pass
            for value in candidates:
                try:
                    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
                        value = value[1:-1]
                    part = value.split(".")[0] if "." in value else value
                    padding = 4 - len(part) % 4
                    if padding != 4:
                        part += "=" * padding
                    decoded = base64.urlsafe_b64decode(part)
                    data = json.loads(decoded.decode("utf-8"))
                    if isinstance(data, dict):
                        return data
                except Exception:
                    continue
        return None

    def _oauth_allow_redirect_extract_callback(self, url: str, referer: Optional[str] = None) -> Optional[str]:
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": self.ua,
        }
        if referer:
            headers["Referer"] = referer
        try:
            resp = self.session.get(
                url,
                headers=headers,
                allow_redirects=True,
                timeout=30,
                impersonate=self.impersonate,
            )
            final_url = str(resp.url)
            if self._callback_matches(final_url):
                return final_url
            for item in getattr(resp, "history", []) or []:
                location = item.headers.get("Location", "")
                if self._callback_matches(location):
                    return location
                history_url = str(item.url)
                if self._callback_matches(history_url):
                    return history_url
        except Exception as exc:
            maybe_localhost = re.search(r"(https?://localhost[^\s'\"]+)", str(exc))
            if maybe_localhost and self._callback_matches(maybe_localhost.group(1)):
                return maybe_localhost.group(1)
        return None

    def _oauth_follow_for_callback(self, start_url: str, referer: Optional[str] = None, max_hops: int = 16) -> Tuple[Optional[str], str]:
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": self.ua,
        }
        if referer:
            headers["Referer"] = referer

        current_url = start_url
        last_url = start_url

        for _ in range(max_hops):
            try:
                resp = self.session.get(
                    current_url,
                    headers=headers,
                    allow_redirects=False,
                    timeout=30,
                    impersonate=self.impersonate,
                )
            except Exception as exc:
                maybe_localhost = re.search(r"(https?://localhost[^\s'\"]+)", str(exc))
                if maybe_localhost and self._callback_matches(maybe_localhost.group(1)):
                    return maybe_localhost.group(1), maybe_localhost.group(1)
                return None, last_url

            last_url = str(resp.url)
            if self._callback_matches(last_url):
                return last_url, last_url

            if resp.status_code not in (301, 302, 303, 307, 308):
                return None, last_url

            location = resp.headers.get("Location", "")
            if not location:
                return None, last_url
            if location.startswith("/"):
                location = f"{self.oauth_issuer}{location}"
            if self._callback_matches(location):
                return location, location
            current_url = location
            headers["Referer"] = last_url

        return None, last_url

    def _oauth_submit_workspace_and_org(self, consent_url: str) -> Optional[str]:
        session_data = self._decode_oauth_session_cookie()
        if not session_data:
            return None
        workspaces = session_data.get("workspaces", [])
        if not workspaces:
            return None
        workspace_id = (workspaces[0] or {}).get("id")
        if not workspace_id:
            return None

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": self.oauth_issuer,
            "Referer": consent_url,
            "User-Agent": self.ua,
            "oai-device-id": self.device_id,
        }
        headers.update(_make_trace_headers())

        resp = self.session.post(
            f"{self.oauth_issuer}/api/accounts/workspace/select",
            json={"workspace_id": workspace_id},
            headers=headers,
            allow_redirects=False,
            timeout=30,
            impersonate=self.impersonate,
        )

        if resp.status_code in (301, 302, 303, 307, 308):
            location = resp.headers.get("Location", "")
            if location.startswith("/"):
                location = f"{self.oauth_issuer}{location}"
            if self._callback_matches(location):
                return location
            callback_url, _ = self._oauth_follow_for_callback(location, referer=consent_url)
            if callback_url:
                return callback_url
            return self._oauth_allow_redirect_extract_callback(location, referer=consent_url)

        if resp.status_code != 200:
            return None

        try:
            ws_data = resp.json()
        except Exception:
            return None

        ws_next = ws_data.get("continue_url", "")
        orgs = (ws_data.get("data") or {}).get("orgs", [])
        org_id = None
        project_id = None
        if orgs:
            org_id = (orgs[0] or {}).get("id")
            projects = (orgs[0] or {}).get("projects", [])
            if projects:
                project_id = (projects[0] or {}).get("id")

        if org_id:
            org_body = {"org_id": org_id}
            if project_id:
                org_body["project_id"] = project_id
            headers_org = dict(headers)
            if ws_next:
                headers_org["Referer"] = ws_next if ws_next.startswith("http") else f"{self.oauth_issuer}{ws_next}"
            resp_org = self.session.post(
                f"{self.oauth_issuer}/api/accounts/organization/select",
                json=org_body,
                headers=headers_org,
                allow_redirects=False,
                timeout=30,
                impersonate=self.impersonate,
            )
            if resp_org.status_code in (301, 302, 303, 307, 308):
                location = resp_org.headers.get("Location", "")
                if location.startswith("/"):
                    location = f"{self.oauth_issuer}{location}"
                if self._callback_matches(location):
                    return location
                callback_url, _ = self._oauth_follow_for_callback(location, referer=headers_org.get("Referer"))
                if callback_url:
                    return callback_url
                return self._oauth_allow_redirect_extract_callback(location, referer=headers_org.get("Referer"))
            if resp_org.status_code == 200:
                try:
                    org_data = resp_org.json()
                except Exception:
                    org_data = {}
                org_next = org_data.get("continue_url", "")
                if org_next:
                    if org_next.startswith("/"):
                        org_next = f"{self.oauth_issuer}{org_next}"
                    callback_url, _ = self._oauth_follow_for_callback(org_next, referer=headers_org.get("Referer"))
                    if callback_url:
                        return callback_url
                    return self._oauth_allow_redirect_extract_callback(org_next, referer=headers_org.get("Referer"))

        if ws_next:
            if ws_next.startswith("/"):
                ws_next = f"{self.oauth_issuer}{ws_next}"
            callback_url, _ = self._oauth_follow_for_callback(ws_next, referer=consent_url)
            if callback_url:
                return callback_url
            return self._oauth_allow_redirect_extract_callback(ws_next, referer=consent_url)

        return None

    def _bootstrap_oauth_session(self) -> Tuple[bool, str]:
        resp = self.session.get(
            self.auth_url,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": "https://chatgpt.com/",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": self.ua,
            },
            allow_redirects=True,
            timeout=30,
            impersonate=self.impersonate,
        )

        final_url = str(resp.url)
        has_login_session = any(getattr(cookie, "name", "") == "login_session" for cookie in self.session.cookies)
        if has_login_session:
            return True, final_url

        parsed = urlparse(self.auth_url)
        query = {key: values[0] for key, values in parse_qs(parsed.query).items()}
        resp2 = self.session.get(
            f"{self.oauth_issuer}/api/oauth/oauth2/auth",
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": self.auth_url,
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": self.ua,
            },
            params=query,
            allow_redirects=True,
            timeout=30,
            impersonate=self.impersonate,
        )
        final_url = str(resp2.url)
        has_login_session = any(getattr(cookie, "name", "") == "login_session" for cookie in self.session.cookies)
        return has_login_session, final_url

    def run(self, mail_timeout: int, account_id: Optional[int] = None) -> Dict[str, Any]:
        _log("Ensuring mailbox", account_id=account_id, email=self.email, stage="oauth")
        mailbox = self.mail_client.ensure_email(self.email)
        email_id = str(mailbox.get("id") or "").strip()
        if not email_id:
            raise RuntimeError("DuckMail mailbox is missing email_id")
        _log(
            f"Mailbox ready (recreated={'yes' if mailbox.get('_recreated') else 'no'})",
            account_id=account_id,
            email=self.email,
            stage="oauth",
        )
        baseline_ids = self.mail_client.get_message_ids(email_id)

        _log("Bootstrapping OAuth session", account_id=account_id, email=self.email, stage="oauth")
        has_login_session, authorize_final_url = self._bootstrap_oauth_session()
        if not authorize_final_url:
            raise RuntimeError("OAuth bootstrap did not return final_url")

        continue_referer = authorize_final_url if authorize_final_url.startswith(self.oauth_issuer) else f"{self.oauth_issuer}/log-in"
        sentinel_authorize = build_sentinel_token(
            self.session,
            self.device_id,
            flow="authorize_continue",
            user_agent=self.ua,
            sec_ch_ua=self.sec_ch_ua,
            impersonate=self.impersonate,
        )
        if not sentinel_authorize:
            raise RuntimeError("Failed to obtain sentinel token for authorize_continue")

        _log("Submitting email to OAuth flow", account_id=account_id, email=self.email, stage="oauth")
        headers_continue = self._oauth_json_headers(continue_referer)
        headers_continue["openai-sentinel-token"] = sentinel_authorize
        resp_continue = self.session.post(
            f"{self.oauth_issuer}/api/accounts/authorize/continue",
            json={"username": {"kind": "email", "value": self.email}},
            headers=headers_continue,
            timeout=30,
            allow_redirects=False,
            impersonate=self.impersonate,
        )
        if resp_continue.status_code == 400 and "invalid_auth_step" in (resp_continue.text or ""):
            has_login_session, authorize_final_url = self._bootstrap_oauth_session()
            continue_referer = authorize_final_url if authorize_final_url.startswith(self.oauth_issuer) else f"{self.oauth_issuer}/log-in"
            headers_continue = self._oauth_json_headers(continue_referer)
            headers_continue["openai-sentinel-token"] = sentinel_authorize
            resp_continue = self.session.post(
                f"{self.oauth_issuer}/api/accounts/authorize/continue",
                json={"username": {"kind": "email", "value": self.email}},
                headers=headers_continue,
                timeout=30,
                allow_redirects=False,
                impersonate=self.impersonate,
            )
        if resp_continue.status_code != 200:
            raise RuntimeError(f"Email submission failed: {resp_continue.status_code} - {resp_continue.text[:200]}")

        continue_data = resp_continue.json()
        continue_url = continue_data.get("continue_url", "")
        page_type = (continue_data.get("page") or {}).get("type", "")
        _log(
            f"Email accepted, next page={page_type or '-'}",
            account_id=account_id,
            email=self.email,
            stage="oauth",
        )
        need_oauth_otp = page_type == "email_otp_verification" or "email-verification" in continue_url or "email-otp" in continue_url
        skip_password = need_oauth_otp or page_type in ("create_account_password", "email_otp_verification", "consent", "organization_select")

        if not skip_password:
            sentinel_pwd = build_sentinel_token(
                self.session,
                self.device_id,
                flow="password_verify",
                user_agent=self.ua,
                sec_ch_ua=self.sec_ch_ua,
                impersonate=self.impersonate,
            )
            if not sentinel_pwd:
                raise RuntimeError("Failed to obtain sentinel token for password_verify")
            _log("Submitting password to OAuth flow", account_id=account_id, email=self.email, stage="oauth")
            headers_verify = self._oauth_json_headers(f"{self.oauth_issuer}/log-in/password")
            headers_verify["openai-sentinel-token"] = sentinel_pwd
            resp_verify = self.session.post(
                f"{self.oauth_issuer}/api/accounts/password/verify",
                json={"password": self.password},
                headers=headers_verify,
                timeout=30,
                allow_redirects=False,
                impersonate=self.impersonate,
            )
            if resp_verify.status_code != 200:
                raise RuntimeError(f"Password verification failed: {resp_verify.status_code} - {resp_verify.text[:200]}")
            verify_data = resp_verify.json()
            continue_url = verify_data.get("continue_url", "") or continue_url
            page_type = (verify_data.get("page") or {}).get("type", "") or page_type
            _log(
                f"Password accepted, next page={page_type or '-'}",
                account_id=account_id,
                email=self.email,
                stage="oauth",
            )
            need_oauth_otp = page_type == "email_otp_verification" or "email-verification" in continue_url or "email-otp" in continue_url

        if need_oauth_otp:
            _log("OpenAI requested email OTP", account_id=account_id, email=self.email, stage="oauth")
            otp_code = self.mail_client.wait_for_openai_otp(
                email_id,
                baseline_ids,
                timeout=mail_timeout,
                account_id=account_id,
                email=self.email,
            )
            headers_otp = self._oauth_json_headers(f"{self.oauth_issuer}/email-verification")
            _log("Submitting email OTP", account_id=account_id, email=self.email, stage="oauth")
            resp_otp = self.session.post(
                f"{self.oauth_issuer}/api/accounts/email-otp/validate",
                json={"code": otp_code},
                headers=headers_otp,
                timeout=30,
                allow_redirects=False,
                impersonate=self.impersonate,
            )
            if resp_otp.status_code != 200:
                raise RuntimeError(f"OTP verification failed: {resp_otp.status_code} - {resp_otp.text[:200]}")
            otp_data = resp_otp.json()
            continue_url = otp_data.get("continue_url", "") or continue_url
            page_type = (otp_data.get("page") or {}).get("type", "") or page_type
            _log(
                f"OTP accepted, next page={page_type or '-'}",
                account_id=account_id,
                email=self.email,
                stage="oauth",
            )

        if page_type == "add_phone" or "add-phone" in (continue_url or ""):
            _log("OpenAI requires phone verification", account_id=account_id, email=self.email, stage="oauth")
            raise RuntimeError("OpenAI requires phone verification for this account")

        callback_url = None
        consent_url = continue_url
        if consent_url and consent_url.startswith("/"):
            consent_url = f"{self.oauth_issuer}{consent_url}"

        if consent_url and self._callback_matches(consent_url):
            callback_url = consent_url

        if not callback_url and consent_url:
            callback_url, _ = self._oauth_follow_for_callback(consent_url, referer=f"{self.oauth_issuer}/log-in/password")
            if not callback_url:
                callback_url = self._oauth_allow_redirect_extract_callback(consent_url, referer=f"{self.oauth_issuer}/log-in/password")

        consent_hint = (
            ("consent" in (consent_url or ""))
            or ("sign-in-with-chatgpt" in (consent_url or ""))
            or ("workspace" in (consent_url or ""))
            or ("organization" in (consent_url or ""))
            or ("consent" in page_type)
            or ("organization" in page_type)
        )
        if not callback_url and consent_hint:
            if not consent_url:
                consent_url = f"{self.oauth_issuer}/sign-in-with-chatgpt/codex/consent"
            callback_url = self._oauth_submit_workspace_and_org(consent_url)

        if not callback_url:
            fallback_consent = f"{self.oauth_issuer}/sign-in-with-chatgpt/codex/consent"
            callback_url = self._oauth_submit_workspace_and_org(fallback_consent)
            if not callback_url:
                callback_url, _ = self._oauth_follow_for_callback(fallback_consent, referer=f"{self.oauth_issuer}/log-in/password")

        if not callback_url:
            raise RuntimeError("callback_url was not obtained")

        callback_state = parse_qs(urlparse(callback_url).query).get("state", [""])[0]
        if self.state and callback_state and callback_state != self.state:
            raise RuntimeError("State returned by callback does not match the state from auth_url")

        _log("Captured OAuth callback", account_id=account_id, email=self.email, stage="oauth")

        return {
            "callback_url": callback_url,
            "code": _extract_code_from_url(callback_url),
            "state": callback_state or self.state,
            "redirect_uri": self.redirect_uri,
            "mailbox_email_id": email_id,
            "mailbox_address": mailbox.get("address"),
            "mailbox_recreated": bool(mailbox.get("_recreated")),
            "login_session_ready": bool(has_login_session),
        }


def _process_account(
    account: Dict[str, Any],
    config: Dict[str, Any],
    redirect_uri: str,
    default_proxy_id: Optional[int],
    dry_run: bool,
    mail_timeout: int,
    verify_after_update: bool,
) -> Dict[str, Any]:
    raw_account_id = account.get("id")
    email = _normalize_email(account.get("email"))
    item: Dict[str, Any] = {
        "candidate": True,
        "account_id": raw_account_id,
        "email": email,
        "status": "skipped" if dry_run else "failed",
        "test_error": "",
        "session_id": None,
        "auth_url": None,
        "callback_url": None,
        "redirect_uri": redirect_uri,
        "post_update_verified": None,
        "error": "",
    }

    try:
        account_id = int(raw_account_id)
        item["account_id"] = account_id
        _log("Starting account test", account_id=account_id, email=email, stage="account")
        admin = Sub2ApiAdminClient(config)
        test_result = admin.test_account(account_id)
        item["test_error"] = test_result.get("error") or ""

        if test_result["success"] or not admin.is_reauth_error(test_result["error"]):
            item["candidate"] = False
            if test_result["success"]:
                _log("Account test passed, no reauth needed", account_id=account_id, email=email, stage="account")
            else:
                _log(
                    f"Skipping non-reauth failure: {test_result['error'] or test_result.get('text') or 'unknown error'}",
                    account_id=account_id,
                    email=email,
                    stage="account",
                )
            return {
                "candidate": False,
                "account_id": account_id,
                "email": email,
                "test_error": test_result["error"],
            }

        _log(f"Detected reauth candidate: {test_result['error']}", account_id=account_id, email=email, stage="account")

        if dry_run:
            _log("Dry run enabled, skipping OAuth/update", account_id=account_id, email=email, stage="account")
            item["status"] = "skipped"
            return item

        _log("Generating auth URL", account_id=account_id, email=email, stage="account")
        generated = admin.generate_auth_url(account, redirect_uri=redirect_uri, default_proxy_id=default_proxy_id)
        item["session_id"] = generated.get("session_id")
        item["auth_url"] = generated.get("auth_url")
        item["redirect_uri"] = generated.get("redirect_uri") or item["redirect_uri"]

        _log("Fetching password from D1", account_id=account_id, email=email, stage="account")
        password = D1PasswordClient(config).get_password(email)
        _log("Password fetched from D1", account_id=account_id, email=email, stage="account")
        mail_client = MoeMailClient(config)
        oauth_result = OAuthRunner(
            auth_url=item["auth_url"],
            redirect_uri=item["redirect_uri"],
            email=email,
            password=password,
            mail_client=mail_client,
        ).run(mail_timeout=max(30, mail_timeout), account_id=account_id)
        item["callback_url"] = oauth_result.get("callback_url")

        _log("Exchanging OAuth code", account_id=account_id, email=email, stage="account")
        token_info = admin.exchange_code(
            session_id=item["session_id"],
            state=oauth_result.get("state") or "",
            code=oauth_result.get("code") or "",
            redirect_uri=item["redirect_uri"],
            proxy_id=generated.get("proxy_id"),
        )
        _log("Fetching current account credentials", account_id=account_id, email=email, stage="account")
        current_account = admin.get_account(account_id)
        current_credentials = current_account.get("credentials") if isinstance(current_account.get("credentials"), dict) else {}
        merged_credentials = admin.build_credentials(token_info, current_credentials)
        _log("Updating account credentials in Sub2Api", account_id=account_id, email=email, stage="account")
        admin.update_account_credentials(account_id, merged_credentials)

        if verify_after_update:
            _log("Running post-update verification", account_id=account_id, email=email, stage="account")
            post_test = admin.test_account(account_id)
            item["post_update_verified"] = bool(post_test.get("success"))
            if not post_test.get("success"):
                raise RuntimeError(f"Post-update verification failed: {post_test.get('error') or post_test.get('text') or 'unknown error'}")
        else:
            item["post_update_verified"] = None

        item["status"] = "updated"
        _log("Reauth completed successfully", account_id=account_id, email=email, stage="account")
        return item
    except Exception as exc:
        if not item["test_error"]:
            item["test_error"] = str(exc)
        item["error"] = str(exc)
        item["status"] = "failed"
        log_account_id = item["account_id"] if isinstance(item.get("account_id"), int) else None
        _log(f"Reauth failed: {exc}", account_id=log_account_id, email=email, stage="account")
        return item


def main() -> int:
    parser = argparse.ArgumentParser(description="Detect Sub2Api accounts with 401-style auth failures and refresh them automatically")
    parser.add_argument("--config", default="config.json", help="Path to the config file")
    parser.add_argument("--platform", default="openai", help="Account platform, default is openai")
    parser.add_argument("--status", default="active", help="Only process accounts with this status, default is active")
    parser.add_argument("--page-size", type=int, default=100, help="Account page size")
    parser.add_argument("--max-workers", type=int, default=5, help="Full pipeline concurrency, default is 5")
    parser.add_argument("--redirect-uri", default="", help="redirect_uri used when generating auth_url")
    parser.add_argument("--default-proxy-id", type=int, help="Fallback proxy_id when the account does not have one")
    parser.add_argument("--output", default="logs/update_auth.json", help="Output file path")
    parser.add_argument("--mail-timeout", type=int, default=480, help="Mailbox OTP timeout in seconds")
    parser.add_argument("--skip-post-verify", action="store_true", help="Skip the final /test verification after updating the account")
    parser.add_argument("--dry-run", action="store_true", help="Detect candidates only, do not run OAuth or update accounts")
    args = parser.parse_args()

    config = _load_config(args.config)
    redirect_uri = str(args.redirect_uri or config.get("oauth_redirect_uri") or "").strip()
    default_proxy_id: Optional[int] = args.default_proxy_id
    if default_proxy_id is None:
        raw_proxy_id = config.get("sub2api_proxy_id")
        if str(raw_proxy_id or "").strip() not in {"", "0", "None", "null"}:
            default_proxy_id = int(raw_proxy_id)

    resolved_config = _resolve_sub2api_runtime_config(config)
    admin = Sub2ApiAdminClient(resolved_config)
    _log(
        f"Starting run platform={args.platform} status={args.status} page_size={max(1, args.page_size)} max_workers={max(1, args.max_workers)} dry_run={args.dry_run}",
        stage="main",
    )
    accounts = admin.list_accounts(
        platform=args.platform,
        status=args.status,
        page_size=max(1, args.page_size),
    )
    _log(f"Loaded {len(accounts)} account(s) from Sub2Api", stage="main")

    items: List[Dict[str, Any]] = []
    tested_count = len(accounts)
    candidate_count = 0
    success_count = 0
    failed_count = 0
    skipped_count = 0
    completed_count = 0

    with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as executor:
        futures = {
            executor.submit(
                _process_account,
                account,
                resolved_config,
                redirect_uri,
                default_proxy_id,
                args.dry_run,
                args.mail_timeout,
                not args.skip_post_verify,
            ): account
            for account in accounts
        }
        for future in as_completed(futures):
            account = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                result = {
                    "candidate": True,
                    "account_id": account["id"],
                    "email": account["email"],
                    "status": "failed",
                    "test_error": str(exc),
                    "session_id": None,
                    "auth_url": None,
                    "callback_url": None,
                    "redirect_uri": redirect_uri,
                    "post_update_verified": None,
                    "error": str(exc),
                }
                _log(
                    f"Worker exception converted to failed result: {exc}",
                    account_id=int(account["id"]),
                    email=account["email"],
                    stage="main",
                )
            completed_count += 1
            if not result.get("candidate"):
                skipped_count += 1
                _log(
                    f"Progress {completed_count}/{tested_count} | candidates={candidate_count} success={success_count} failed={failed_count} skipped={skipped_count}",
                    stage="main",
                )
                continue

            candidate_count += 1
            item = {
                "account_id": result["account_id"],
                "email": result["email"],
                "status": result["status"],
                "test_error": result.get("test_error") or "",
                "session_id": result.get("session_id"),
                "auth_url": result.get("auth_url"),
                "callback_url": result.get("callback_url"),
                "redirect_uri": result.get("redirect_uri"),
                "post_update_verified": result.get("post_update_verified"),
                "error": result.get("error") or "",
            }
            items.append(item)

            if item["status"] == "updated":
                success_count += 1
            elif item["status"] == "failed":
                failed_count += 1
            else:
                skipped_count += 1

            _log(
                f"Progress {completed_count}/{tested_count} | candidates={candidate_count} success={success_count} failed={failed_count} skipped={skipped_count}",
                stage="main",
            )

    output = {
        "generated_at": _now_iso(),
        "source": admin.base_url,
        "platform": args.platform,
        "tested_count": tested_count,
        "reauth_candidate_count": candidate_count,
        "success_count": success_count,
        "failed_count": failed_count,
        "skipped_count": skipped_count,
        "items": sorted(items, key=lambda item: (item.get("status") != "updated", item.get("account_id") or 0)),
    }

    _write_json(Path(args.output), output)

    _log(f"Wrote results to {args.output}", stage="main")
    print(f"[update_auth] tested={tested_count}")
    print(f"[update_auth] candidates={candidate_count}")
    print(f"[update_auth] success={success_count}")
    print(f"[update_auth] failed={failed_count}")
    print(f"[update_auth] skipped={skipped_count}")
    print(f"[update_auth] wrote={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
