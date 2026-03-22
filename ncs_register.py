"""
ChatGPT 批量自动注册工具 (并发版) - 支持 DuckMail 与 CF 自建邮箱
依赖: pip install curl_cffi
"""

import os
import re
import uuid
import json
import random
import string
import time
import sys
import math
import threading
import traceback
import secrets
import hashlib
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs, urlencode, quote
from dataclasses import dataclass
from typing import Any, Dict, Optional

from curl_cffi import requests as curl_requests


def _mode_key(value) -> str:
    mode = str(value or "").strip().lower()
    return mode or "default"


def _proxy_runtime_enabled(mode) -> bool:
    return _mode_key(mode) != "github"


def _proxy_runtime_enabled_from_config(cfg: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(cfg, dict):
        return True
    return _proxy_runtime_enabled(cfg.get("mode", "default"))


def _apply_mode_overrides(config: Dict[str, Any]) -> Dict[str, Any]:
    cfg = dict(config or {})
    mode = _mode_key(cfg.get("mode", "default"))
    cfg["mode"] = mode
    if not _proxy_runtime_enabled(mode):
        cfg["proxy"] = ""
        cfg["proxy_list_url"] = ""
        cfg["proxy_list_bearer"] = ""
        cfg["stable_proxy"] = ""
        cfg["prefer_stable_proxy"] = False
    return cfg


# ================= 加载配置 =================
def _load_config():
    def _normalize_cpa_root(raw_url: str) -> str:
        value = str(raw_url or "").strip()
        if not value:
            return ""
        parsed = urlparse(value)
        path = (parsed.path or "").rstrip("/")
        if path.endswith("/management.html"):
            path = path[:-len("/management.html")] + "/v0/management"
        for suffix in ("/auth-files", "/api-call"):
            if path.endswith(suffix):
                path = path[:-len(suffix)]
        return parsed._replace(path=path.rstrip("/"), query="", fragment="").geturl().rstrip("/")

    config = {
        "mode": "default",
        "total_accounts": 3,
        "mail_provider": "duckmail",
        "cfmail_config_path": "zhuce5_cfmail_accounts.json",
        "cfmail_profile": "auto",
        "duckmail_api_base": "https://api.duckmail.sbs",
        "duckmail_bearer": "",
        "proxy": "",
        "proxy_list_url": "",
        "proxy_list_bearer": "",
        "proxy_validate_enabled": True,
        "proxy_validate_timeout_seconds": 6,
        "proxy_validate_workers": 40,
        "proxy_validate_test_url": "https://auth.openai.com/",
        "proxy_max_retries_per_request": 30,
        "proxy_bad_ttl_seconds": 180,
        "proxy_retry_attempts_per_account": 20,
        "stable_proxy_file": "stable_proxy.txt",
        "stable_proxy": "",
        "prefer_stable_proxy": True,
        "output_file": "registered_accounts.txt",
        "enable_oauth": True,
        "oauth_required": True,
        "oauth_issuer": "https://auth.openai.com",
        "oauth_client_id": "app_EMoamEEZ73f0CkXaXp7hrann",
        "oauth_redirect_uri": "http://localhost:1455/auth/callback",
        "ak_file": "ak.txt",
        "rk_file": "rk.txt",
        "token_json_dir": "codex_tokens",
        "upload_api_url": "",
        "upload_api_token": "",
        "cpa_base_url": "",
        "cpa_management_key": "",
        "auto_upload_cpa": True,
        "cpa_min_candidates": 200,
        "cpa_cleanup_enabled": True,
        "sub2api_base_url": "",
        "sub2api_bearer": "",
        "sub2api_email": "",
        "sub2api_password": "",
        "auto_upload_sub2api": False,
        "sub2api_group_ids": [2],
        "sub2api_min_candidates": 200,
        "sub2api_proxy_id": 0,
        "sub2api_proxy_name": "",
        "sub2api_auto_assign_proxy": False,
    }

    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                file_config = json.load(f)
                config.update(file_config)
        except Exception as e:
            print(f"⚠️ 加载 config.json 失败: {e}")

    config["duckmail_api_base"] = os.environ.get("DUCKMAIL_API_BASE", config["duckmail_api_base"])
    config["duckmail_bearer"] = os.environ.get("DUCKMAIL_BEARER", config["duckmail_bearer"])
    config["mode"] = os.environ.get("MODE", config.get("mode", "default"))
    config["proxy"] = os.environ.get("PROXY", config["proxy"])
    config["proxy_list_url"] = os.environ.get("PROXY_LIST_URL", config["proxy_list_url"])
    config["proxy_list_bearer"] = os.environ.get("PROXY_LIST_BEARER", config.get("proxy_list_bearer", ""))
    config["proxy_validate_enabled"] = os.environ.get("PROXY_VALIDATE_ENABLED", config["proxy_validate_enabled"])
    config["proxy_validate_timeout_seconds"] = float(os.environ.get(
        "PROXY_VALIDATE_TIMEOUT_SECONDS", config["proxy_validate_timeout_seconds"]
    ))
    config["proxy_validate_workers"] = int(os.environ.get("PROXY_VALIDATE_WORKERS", config["proxy_validate_workers"]))
    config["proxy_validate_test_url"] = os.environ.get("PROXY_VALIDATE_TEST_URL", config["proxy_validate_test_url"])
    config["proxy_max_retries_per_request"] = int(os.environ.get(
        "PROXY_MAX_RETRIES_PER_REQUEST", config["proxy_max_retries_per_request"]
    ))
    config["proxy_bad_ttl_seconds"] = int(os.environ.get("PROXY_BAD_TTL_SECONDS", config["proxy_bad_ttl_seconds"]))
    config["proxy_retry_attempts_per_account"] = int(os.environ.get(
        "PROXY_RETRY_ATTEMPTS_PER_ACCOUNT", config["proxy_retry_attempts_per_account"]
    ))
    config["stable_proxy_file"] = os.environ.get("STABLE_PROXY_FILE", config["stable_proxy_file"])
    config["stable_proxy"] = os.environ.get("STABLE_PROXY", config["stable_proxy"])
    config["prefer_stable_proxy"] = os.environ.get("PREFER_STABLE_PROXY", config["prefer_stable_proxy"])
    config["total_accounts"] = int(os.environ.get("TOTAL_ACCOUNTS", config["total_accounts"]))
    config["enable_oauth"] = os.environ.get("ENABLE_OAUTH", config["enable_oauth"])
    config["oauth_required"] = os.environ.get("OAUTH_REQUIRED", config["oauth_required"])
    config["oauth_issuer"] = os.environ.get("OAUTH_ISSUER", config["oauth_issuer"])
    config["oauth_client_id"] = os.environ.get("OAUTH_CLIENT_ID", config["oauth_client_id"])
    config["oauth_redirect_uri"] = os.environ.get("OAUTH_REDIRECT_URI", config["oauth_redirect_uri"])
    config["ak_file"] = os.environ.get("AK_FILE", config["ak_file"])
    config["rk_file"] = os.environ.get("RK_FILE", config["rk_file"])
    config["token_json_dir"] = os.environ.get("TOKEN_JSON_DIR", config["token_json_dir"])
    config["upload_api_url"] = os.environ.get("UPLOAD_API_URL", config["upload_api_url"])
    config["upload_api_token"] = os.environ.get("UPLOAD_API_TOKEN", config["upload_api_token"])
    config["mail_provider"] = os.environ.get("MAIL_PROVIDER", config["mail_provider"])
    config["cfmail_config_path"] = os.environ.get("CFMAIL_CONFIG_PATH", config["cfmail_config_path"])
    config["cfmail_profile"] = os.environ.get("CFMAIL_PROFILE", config["cfmail_profile"])
    config["cpa_base_url"] = os.environ.get("CPA_BASE_URL", config.get("cpa_base_url", ""))
    config["cpa_management_key"] = os.environ.get("CPA_MANAGEMENT_KEY", config.get("cpa_management_key", ""))
    config["auto_upload_cpa"] = os.environ.get("AUTO_UPLOAD_CPA", config.get("auto_upload_cpa", True))
    config["cpa_min_candidates"] = int(os.environ.get("CPA_MIN_CANDIDATES", config.get("cpa_min_candidates", 200)))
    config["sub2api_base_url"] = os.environ.get("SUB2API_BASE_URL", config["sub2api_base_url"])
    config["sub2api_bearer"] = os.environ.get("SUB2API_BEARER", config["sub2api_bearer"])
    config["sub2api_email"] = os.environ.get("SUB2API_EMAIL", config["sub2api_email"])
    config["sub2api_password"] = os.environ.get("SUB2API_PASSWORD", config["sub2api_password"])
    config["auto_upload_sub2api"] = os.environ.get("AUTO_UPLOAD_SUB2API", config["auto_upload_sub2api"])
    config["sub2api_min_candidates"] = int(os.environ.get(
        "SUB2API_MIN_CANDIDATES", config.get("sub2api_min_candidates", 200)
    ))
    config["sub2api_proxy_id"] = int(os.environ.get(
        "SUB2API_PROXY_ID", config.get("sub2api_proxy_id", 0) or 0
    ))
    config["sub2api_proxy_name"] = os.environ.get(
        "SUB2API_PROXY_NAME", config.get("sub2api_proxy_name", "")
    )
    config["sub2api_auto_assign_proxy"] = os.environ.get(
        "SUB2API_AUTO_ASSIGN_PROXY", config.get("sub2api_auto_assign_proxy", False)
    )
    raw_group_ids = os.environ.get("SUB2API_GROUP_IDS")
    if raw_group_ids:
        try:
            config["sub2api_group_ids"] = [
                int(x.strip()) for x in raw_group_ids.split(",") if x.strip().lstrip("-").isdigit()
            ]
        except Exception:
            pass

    cpa_root = _normalize_cpa_root(config.get("cpa_base_url") or config.get("upload_api_url"))
    if cpa_root:
        config["cpa_base_url"] = cpa_root
    if not config.get("upload_api_url") and cpa_root:
        config["upload_api_url"] = cpa_root.rstrip("/") + "/auth-files"
    if not config.get("cpa_management_key") and config.get("upload_api_token"):
        config["cpa_management_key"] = config["upload_api_token"]
    if not config.get("upload_api_token") and config.get("cpa_management_key"):
        config["upload_api_token"] = config["cpa_management_key"]

    return _apply_mode_overrides(config)


def _as_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


_CONFIG = _load_config()
MODE = _CONFIG.get("mode", "default")
DUCKMAIL_API_BASE = _CONFIG["duckmail_api_base"]
DUCKMAIL_BEARER = _CONFIG["duckmail_bearer"]
DEFAULT_TOTAL_ACCOUNTS = _CONFIG["total_accounts"]
DEFAULT_PROXY = _CONFIG["proxy"]
PROXY_LIST_URL = _CONFIG.get("proxy_list_url", "")
PROXY_LIST_BEARER = str(_CONFIG.get("proxy_list_bearer", "") or "").strip()
PROXY_VALIDATE_ENABLED = _as_bool(_CONFIG.get("proxy_validate_enabled", True))
PROXY_VALIDATE_TIMEOUT_SECONDS = max(1.0, float(_CONFIG.get("proxy_validate_timeout_seconds", 6)))
PROXY_VALIDATE_WORKERS = max(1, int(_CONFIG.get("proxy_validate_workers", 40)))
PROXY_VALIDATE_TEST_URL = str(_CONFIG.get("proxy_validate_test_url", "https://auth.openai.com/")).strip() or "https://auth.openai.com/"
PROXY_MAX_RETRIES_PER_REQUEST = max(1, int(_CONFIG.get("proxy_max_retries_per_request", 30)))
PROXY_BAD_TTL_SECONDS = max(10, int(_CONFIG.get("proxy_bad_ttl_seconds", 180)))
PROXY_RETRY_ATTEMPTS_PER_ACCOUNT = max(1, int(_CONFIG.get("proxy_retry_attempts_per_account", 20)))
STABLE_PROXY_FILE = _CONFIG.get("stable_proxy_file", "stable_proxy.txt")
STABLE_PROXY_RAW = _CONFIG.get("stable_proxy", "")
PREFER_STABLE_PROXY = _as_bool(_CONFIG.get("prefer_stable_proxy", True))
DEFAULT_OUTPUT_FILE = _CONFIG["output_file"]
ENABLE_OAUTH = _as_bool(_CONFIG.get("enable_oauth", True))
OAUTH_REQUIRED = _as_bool(_CONFIG.get("oauth_required", True))
OAUTH_ISSUER = _CONFIG["oauth_issuer"].rstrip("/")
OAUTH_CLIENT_ID = _CONFIG["oauth_client_id"]
OAUTH_REDIRECT_URI = _CONFIG["oauth_redirect_uri"]
AK_FILE = _CONFIG["ak_file"]
RK_FILE = _CONFIG["rk_file"]
TOKEN_JSON_DIR = _CONFIG["token_json_dir"]
UPLOAD_API_URL = _CONFIG["upload_api_url"]
UPLOAD_API_TOKEN = _CONFIG["upload_api_token"]
CPA_BASE_URL = str(_CONFIG.get("cpa_base_url", "") or "").strip().rstrip("/")
CPA_MANAGEMENT_KEY = str(_CONFIG.get("cpa_management_key", "") or "").strip()
AUTO_UPLOAD_CPA = _as_bool(_CONFIG.get("auto_upload_cpa", True))
CPA_MIN_CANDIDATES = max(1, int(_CONFIG.get("cpa_min_candidates", 200) or 200))
CPA_CLEANUP_ENABLED = _as_bool(_CONFIG.get("cpa_cleanup_enabled", True))
MAIL_PROVIDER = str(_CONFIG.get("mail_provider", "duckmail")).strip().lower()
SUB2API_BASE_URL = str(_CONFIG.get("sub2api_base_url", "") or "").strip().rstrip("/")
SUB2API_BEARER = str(_CONFIG.get("sub2api_bearer", "") or "").strip()
SUB2API_EMAIL = str(_CONFIG.get("sub2api_email", "") or "").strip()
SUB2API_PASSWORD = str(_CONFIG.get("sub2api_password", "") or "").strip()
AUTO_UPLOAD_SUB2API = _as_bool(_CONFIG.get("auto_upload_sub2api", False))
_raw_group_ids = _CONFIG.get("sub2api_group_ids", [2])
SUB2API_GROUP_IDS = [
    int(x) for x in (_raw_group_ids if isinstance(_raw_group_ids, list) else [_raw_group_ids])
    if str(x).strip().lstrip("-").isdigit()
]
SUB2API_MIN_CANDIDATES = max(1, int(_CONFIG.get("sub2api_min_candidates", 200) or 200))
SUB2API_PROXY_ID = max(0, int(_CONFIG.get("sub2api_proxy_id", 0) or 0))
SUB2API_PROXY_NAME = str(_CONFIG.get("sub2api_proxy_name", "") or "").strip()
SUB2API_AUTO_ASSIGN_PROXY = _as_bool(_CONFIG.get("sub2api_auto_assign_proxy", False))
DEFAULT_HTTP_TIMEOUT_SECONDS = max(10, int(_CONFIG.get("http_timeout_seconds", 30) or 30))

_sub2api_bearer_holder = [SUB2API_BEARER]
_sub2api_auth_lock = threading.Lock()
_sub2api_proxy_cache = {"ts": 0.0, "items": None, "signature": ""}
_sub2api_proxy_cache_lock = threading.Lock()

# 全局线程锁
_print_lock = threading.Lock()
_file_lock = threading.Lock()
_stop_event = threading.Event()


# ================= 代理池 / MoeMail 支持 =================

def _normalize_proxy(proxy: str):
    if not proxy:
        return None
    value = str(proxy).strip().strip("\"'")
    if not value:
        return None
    if re.match(r"^(?:http|https|socks4|socks5|socks5h)://", value, re.IGNORECASE):
        return value
    if re.fullmatch(r"[^:@/\s]+:\d+:[^:\s]+:[^:\s]+", value):
        host, port, username, password = value.split(":", 3)
        return f"http://{quote(username, safe='')}:{quote(password, safe='')}@{host}:{port}"
    if re.fullmatch(r"[^@\s]+@[^:\s]+:\d+", value):
        return f"http://{value}"
    return f"http://{value}"


def _normalize_single_proxy_source(value: str):
    if not isinstance(value, str):
        return None
    candidate = value.strip().strip("\"'")
    if not candidate:
        return None

    if re.match(r"^(?:http|https|socks4|socks5|socks5h)://", candidate, re.IGNORECASE):
        parsed = urlparse(candidate)
        if parsed.hostname and parsed.port and parsed.path in {"", "/"} and not parsed.query and not parsed.fragment:
            return _normalize_proxy(candidate)
        return None

    if re.fullmatch(r"[^:@/\s]+:\d+:[^:\s]+:[^:\s]+", candidate):
        return _normalize_proxy(candidate)
    if re.fullmatch(r"[^@\s]+@[^:\s]+:\d+", candidate):
        return _normalize_proxy(candidate)
    if re.fullmatch(r"[^:@/\s]+:\d+", candidate):
        return _normalize_proxy(candidate)
    return None


def _normalize_proxy_list_url(url: str):
    value = (url or "").strip()
    if value.lower() in {"", "0", "off", "none", "false", "disabled"}:
        return ""

    m = re.match(r"^https?://github\.com/([^/]+)/([^/]+)/blob/([^/]+)/(.+)$", value)
    if m:
        owner, repo, branch, path = m.groups()
        return f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"
    parsed = urlparse(value)
    host = parsed.netloc.lower()
    path = parsed.path.rstrip("/")
    if host == "resin.xspace.icu" and path in {"", "/ui"}:
        suffix = f"?{parsed.query}" if parsed.query else ""
        return f"{parsed.scheme}://{parsed.netloc}/api/v1/nodes{suffix}"
    return value


def _extract_proxy_list_bearer(url: str, configured_bearer: str = ""):
    value = (url or "").strip()
    bearer = str(configured_bearer or "").strip()
    if not value:
        return value, bearer

    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"}:
        return value, bearer

    query = parse_qs(parsed.query, keep_blank_values=True)
    if not bearer:
        for key in ("token", "bearer", "admin_token", "auth"):
            candidate = str((query.get(key) or [""])[0]).strip()
            if candidate:
                bearer = candidate
                query.pop(key, None)
                break

    clean_query = urlencode(query, doseq=True)
    if clean_query != parsed.query:
        value = parsed._replace(query=clean_query).geturl()
    return value, bearer


def _is_resin_proxy_pool_url(url: str):
    parsed = urlparse((url or "").strip())
    return parsed.scheme in {"http", "https"} and parsed.netloc.lower() == "resin.xspace.icu"


def _mail_provider_key(api_base: str):
    if MAIL_PROVIDER in {"moemail", "duckmail"}:
        return MAIL_PROVIDER
    raw = str(api_base or "").strip()
    if not raw:
        return "duckmail"
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    host = parsed.netloc.lower()
    path = parsed.path.rstrip("/").lower()
    if "duckmail" in host:
        return "duckmail"
    if host in {"ai4mind.site", "moemail.app"} or "moemail" in host or path.endswith("/api"):
        return "moemail"
    return "duckmail"


def _mail_provider_label(api_base: str):
    return "MoeMail" if _mail_provider_key(api_base) == "moemail" else "DuckMail"


MAIL_PROVIDER_LABEL = _mail_provider_label(DUCKMAIL_API_BASE)
STABLE_PROXY = _normalize_proxy(STABLE_PROXY_RAW)


class ProxyPool:
    """线程安全代理池：支持普通代理列表与 Resin 代理池。"""

    def __init__(self, list_url: str, fallback_proxy: str = None,
                 max_retries_per_request: int = 30, bad_ttl_seconds: int = 180,
                 validate_enabled: bool = True, validate_timeout_seconds: float = 6,
                 validate_workers: int = 40, validate_test_url: str = "https://auth.openai.com/",
                 prefer_stable_proxy: bool = True, list_bearer: str = ""):
        normalized_url, normalized_bearer = _extract_proxy_list_bearer(list_url, list_bearer)
        self.list_url = _normalize_proxy_list_url(normalized_url)
        self.list_bearer = str(normalized_bearer or "").strip()
        self.fallback_proxy = _normalize_proxy(fallback_proxy)
        self.max_retries_per_request = max(1, int(max_retries_per_request))
        self.bad_ttl_seconds = max(10, int(bad_ttl_seconds))
        self.validate_enabled = bool(validate_enabled)
        self.validate_timeout_seconds = max(1.0, float(validate_timeout_seconds))
        self.validate_workers = max(1, int(validate_workers))
        self.validate_test_url = str(validate_test_url).strip() or "https://auth.openai.com/"
        self.prefer_stable_proxy = bool(prefer_stable_proxy)
        self._lock = threading.Lock()
        self._loaded = False
        self._proxies = []
        self._index = 0
        self._bad_until = {}
        self._last_fetched_count = 0
        self._last_valid_count = 0
        self._stable_proxy = None
        self._last_error = ""

    def set_fallback(self, proxy: str):
        normalized = _normalize_proxy(proxy)
        with self._lock:
            self.fallback_proxy = normalized

    def set_stable_proxy(self, proxy: str):
        normalized = _normalize_proxy(proxy)
        if not normalized:
            return
        with self._lock:
            self._stable_proxy = normalized
            self._bad_until.pop(normalized, None)

    def set_prefer_stable_proxy(self, enabled: bool):
        with self._lock:
            self.prefer_stable_proxy = bool(enabled)

    def get_stable_proxy(self):
        with self._lock:
            return self._stable_proxy

    def _proxy_list_headers(self):
        headers = {"Accept": "application/json, text/plain;q=0.9,*/*;q=0.8"}
        if self.list_bearer:
            headers["Authorization"] = f"Bearer {self.list_bearer}"
        return headers

    def _dedupe_proxies(self, proxies):
        normalized_list = []
        seen = set()
        for proxy in proxies or []:
            normalized = _normalize_proxy(proxy)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            normalized_list.append(normalized)
        return normalized_list

    def _normalize_proxy_candidate(self, value: str, allow_url: bool = True):
        if not isinstance(value, str):
            return None
        candidate = value.strip().strip("\"'")
        if not candidate:
            return None

        if allow_url and re.match(r"^(?:http|https|socks4|socks5|socks5h)://", candidate, re.IGNORECASE):
            parsed = urlparse(candidate)
            if parsed.hostname and parsed.port:
                return _normalize_proxy(candidate)
            return None

        if re.fullmatch(r"[^:@/\s]+:\d+:[^:\s]+:[^:\s]+", candidate):
            return _normalize_proxy(candidate)
        if re.fullmatch(r"[^@\s]+@[^:\s]+:\d+", candidate):
            return _normalize_proxy(candidate)
        if re.fullmatch(r"[^:@/\s]+:\d+", candidate):
            return _normalize_proxy(candidate)
        return None

    def _build_proxy_from_mapping(self, data):
        if not isinstance(data, dict):
            return None

        host = None
        for key in ("proxy_host", "host", "hostname", "server", "address"):
            raw = data.get(key)
            if raw:
                host = str(raw).strip()
                break

        port = None
        for key in ("proxy_port", "port", "server_port"):
            raw = data.get(key)
            if raw not in (None, ""):
                port = str(raw).strip()
                break

        if not host or not port or not port.isdigit():
            return None

        scheme = "http"
        for key in ("scheme", "protocol", "proxy_type", "type"):
            raw = data.get(key)
            if not raw:
                continue
            lowered = str(raw).strip().lower()
            if "socks5h" in lowered:
                scheme = "socks5h"
                break
            if "socks5" in lowered:
                scheme = "socks5"
                break
            if "socks4" in lowered:
                scheme = "socks4"
                break
            if "https" in lowered:
                scheme = "https"
                break

        username = None
        for key in ("username", "user", "proxy_user"):
            raw = data.get(key)
            if raw not in (None, ""):
                username = str(raw)
                break

        password = None
        for key in ("password", "pass", "proxy_password"):
            raw = data.get(key)
            if raw not in (None, ""):
                password = str(raw)
                break

        if username is not None:
            auth = quote(username, safe="")
            if password is not None:
                auth = f"{auth}:{quote(password, safe='')}"
            return f"{scheme}://{auth}@{host}:{port}"
        return f"{scheme}://{host}:{port}"

    def _extract_proxies_from_text(self, text: str):
        proxies = []
        for raw_line in (text or "").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            candidate = self._normalize_proxy_candidate(line, allow_url=True)
            if candidate:
                proxies.append(candidate)
        return self._dedupe_proxies(proxies)

    def _extract_proxies_from_data(self, data, key_hint: str = ""):
        proxies = []
        lowered_hint = str(key_hint or "").lower()
        allow_url = lowered_hint in {"", "[]"} or any(
            token in lowered_hint for token in ("proxy", "outbound", "uri", "url", "endpoint", "content")
        )

        if isinstance(data, str):
            if "\n" in data:
                return self._extract_proxies_from_text(data)
            candidate = self._normalize_proxy_candidate(data, allow_url=allow_url)
            return [candidate] if candidate else []

        if isinstance(data, (list, tuple)):
            for item in data:
                proxies.extend(self._extract_proxies_from_data(item, "[]"))
            return self._dedupe_proxies(proxies)

        if isinstance(data, dict):
            candidate = self._build_proxy_from_mapping(data)
            if candidate:
                proxies.append(candidate)
            for key, value in data.items():
                proxies.extend(self._extract_proxies_from_data(value, str(key)))
            return self._dedupe_proxies(proxies)

        return []

    def _fetch_generic_proxies_from_url(self, request_url: str):
        res = curl_requests.get(
            request_url,
            timeout=20,
            headers=self._proxy_list_headers(),
            impersonate="chrome131",
        )
        if res.status_code != 200:
            raise Exception(f"HTTP {res.status_code}")

        body = res.text or ""
        content_type = str(res.headers.get("Content-Type", "") or "").lower()
        if "json" in content_type or body.lstrip().startswith(("{", "[")):
            try:
                return self._dedupe_proxies(self._extract_proxies_from_data(res.json()))
            except Exception:
                pass
        return self._extract_proxies_from_text(body)

    def _fetch_resin_proxies(self):
        if not self.list_bearer:
            raise Exception("Resin 代理池需要 Bearer Token，请设置 proxy_list_bearer 或在 URL 中追加 ?token=")

        parsed = urlparse(self.list_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        request_urls = [self.list_url]

        default_nodes = f"{base_url}/api/v1/nodes?limit=2000&offset=0&enabled=true&has_outbound=true"
        default_subscriptions = f"{base_url}/api/v1/subscriptions?limit=500&offset=0&enabled=true"
        for candidate in (default_nodes, default_subscriptions):
            if candidate not in request_urls:
                request_urls.append(candidate)

        proxies = []
        last_error = ""
        for request_url in request_urls:
            try:
                proxies.extend(self._fetch_generic_proxies_from_url(request_url))
            except Exception as e:
                last_error = str(e)

        proxies = self._dedupe_proxies(proxies)
        if proxies:
            return proxies
        if last_error:
            raise Exception(last_error)
        raise Exception("Resin 代理池未返回可解析的代理")

    def _fetch_proxies(self):
        if not self.list_url:
            return []
        direct_proxy = _normalize_single_proxy_source(self.list_url)
        if direct_proxy:
            return [direct_proxy]
        if _is_resin_proxy_pool_url(self.list_url):
            return self._fetch_resin_proxies()
        return self._fetch_generic_proxies_from_url(self.list_url)

    def _validate_single_proxy(self, proxy: str):
        try:
            res = curl_requests.get(
                self.validate_test_url,
                timeout=self.validate_timeout_seconds,
                allow_redirects=False,
                proxies={"http": proxy, "https": proxy},
                impersonate="chrome131",
            )
            return 200 <= res.status_code < 500
        except Exception:
            return False

    def _filter_valid_proxies(self, proxies):
        if not self.validate_enabled or not proxies:
            return list(proxies)

        workers = min(self.validate_workers, len(proxies))
        valid = []
        total = len(proxies)
        done = 0
        started_at = time.time()
        last_log_at = started_at

        with _print_lock:
            print(
                f"[ProxyCheck] 开始校验代理: 总数 {total}, 并发 {workers}, "
                f"超时 {self.validate_timeout_seconds}s, 测试URL {self.validate_test_url}"
            )

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(self._validate_single_proxy, proxy): proxy for proxy in proxies}
            for future in as_completed(futures):
                done += 1
                try:
                    if future.result():
                        valid.append(futures[future])
                except Exception:
                    pass
                now = time.time()
                if done == total or (now - last_log_at) >= 1.5:
                    with _print_lock:
                        print(f"[ProxyCheck] 进度 {done}/{total}, 可用 {len(valid)}")
                    last_log_at = now

        elapsed = time.time() - started_at
        with _print_lock:
            print(f"[ProxyCheck] 校验完成: 可用 {len(valid)}/{total}, 耗时 {elapsed:.1f}s")
        return valid

    def refresh(self, force=False):
        with self._lock:
            if self._loaded and not force:
                return

        proxies = []
        fetched_proxies = []
        last_error = ""
        try:
            fetched_proxies = self._fetch_proxies()
            proxies = self._filter_valid_proxies(fetched_proxies)
            if self.validate_enabled and fetched_proxies and not proxies:
                last_error = "代理校验后无可用代理，降级使用未校验代理"
                proxies = list(fetched_proxies)
        except Exception as e:
            last_error = str(e)

        with self._lock:
            self._last_fetched_count = len(fetched_proxies)
            self._last_valid_count = len(proxies) if self.validate_enabled else len(fetched_proxies)
            if proxies:
                random.shuffle(proxies)
                self._proxies = proxies
                self._index = 0
                self._bad_until = {}
                self._last_error = ""
            elif not self._proxies:
                self._proxies = []
                self._index = 0
                self._last_error = last_error
            else:
                self._last_error = last_error
            self._loaded = True

    def next_proxy(self):
        self.refresh()
        with self._lock:
            now = time.time()
            stable = self._stable_proxy if self.prefer_stable_proxy else None
            if stable:
                stable_bad_until = self._bad_until.get(stable, 0)
                if stable_bad_until and stable_bad_until <= now:
                    self._bad_until.pop(stable, None)
                    stable_bad_until = 0
                if stable_bad_until > now:
                    self._stable_proxy = None
                else:
                    return stable

            total = len(self._proxies)
            for _ in range(total):
                proxy = self._proxies[self._index]
                self._index = (self._index + 1) % total
                bad_until = self._bad_until.get(proxy, 0)
                if bad_until and bad_until <= now:
                    self._bad_until.pop(proxy, None)
                    bad_until = 0
                if bad_until > now:
                    continue
                return proxy

            fallback = self.fallback_proxy
            if fallback:
                bad_until = self._bad_until.get(fallback, 0)
                if bad_until and bad_until <= now:
                    self._bad_until.pop(fallback, None)
                    bad_until = 0
                if bad_until <= now:
                    return fallback
            if total > 0:
                proxy = self._proxies[self._index]
                self._index = (self._index + 1) % total
                return proxy
            return None

    def report_bad(self, proxy: str, error=None):
        normalized = _normalize_proxy(proxy)
        if not normalized:
            return
        until = time.time() + self.bad_ttl_seconds
        with self._lock:
            self._bad_until[normalized] = until
            if self._stable_proxy == normalized:
                self._stable_proxy = None
            if error:
                self._last_error = f"{normalized} -> {str(error)[:160]}"

    def report_success(self, proxy: str):
        normalized = _normalize_proxy(proxy)
        if not normalized:
            return
        with self._lock:
            self._stable_proxy = normalized
            self._bad_until.pop(normalized, None)

    def request_retry_limit(self):
        self.refresh()
        with self._lock:
            pool_size = len(self._proxies)
            if self.fallback_proxy and self.fallback_proxy not in self._proxies:
                pool_size += 1
            if self._stable_proxy and self._stable_proxy not in self._proxies and self._stable_proxy != self.fallback_proxy:
                pool_size += 1
            max_retries = self.max_retries_per_request
        return max(1, min(max_retries, max(1, pool_size)))

    def info(self):
        with self._lock:
            now = time.time()
            bad_count = sum(1 for until in self._bad_until.values() if until > now)
            return {
                "list_url": self.list_url or "已关闭",
                "list_enabled": bool(self.list_url),
                "count": len(self._proxies),
                "fetched_count": self._last_fetched_count,
                "validated_count": self._last_valid_count,
                "validate_enabled": self.validate_enabled,
                "validate_test_url": self.validate_test_url,
                "validate_timeout_seconds": self.validate_timeout_seconds,
                "validate_workers": self.validate_workers,
                "bad_count": bad_count,
                "fallback_proxy": self.fallback_proxy,
                "stable_proxy": self._stable_proxy,
                "prefer_stable_proxy": self.prefer_stable_proxy,
                "max_retries_per_request": self.max_retries_per_request,
                "bad_ttl_seconds": self.bad_ttl_seconds,
                "last_error": self._last_error,
            }


_proxy_pool = ProxyPool(
    PROXY_LIST_URL,
    fallback_proxy=DEFAULT_PROXY,
    max_retries_per_request=PROXY_MAX_RETRIES_PER_REQUEST,
    bad_ttl_seconds=PROXY_BAD_TTL_SECONDS,
    validate_enabled=PROXY_VALIDATE_ENABLED,
    validate_timeout_seconds=PROXY_VALIDATE_TIMEOUT_SECONDS,
    validate_workers=PROXY_VALIDATE_WORKERS,
    validate_test_url=PROXY_VALIDATE_TEST_URL,
    prefer_stable_proxy=PREFER_STABLE_PROXY,
    list_bearer=PROXY_LIST_BEARER,
)
_stable_proxy_loaded = False


def _stable_proxy_path():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return STABLE_PROXY_FILE if os.path.isabs(STABLE_PROXY_FILE) else os.path.join(base_dir, STABLE_PROXY_FILE)


def _load_stable_proxy_from_file():
    path = _stable_proxy_path()
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            line = f.readline().strip()
        return _normalize_proxy(line)
    except Exception:
        return None


def _save_stable_proxy_to_config(proxy: str):
    normalized = _normalize_proxy(proxy)
    if not normalized:
        return

    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    if not os.path.exists(config_path):
        return

    try:
        with _file_lock:
            with open(config_path, "r", encoding="utf-8") as f:
                config = json.load(f)
            config["stable_proxy"] = normalized
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
                f.write("\n")
    except Exception:
        return


def _save_stable_proxy_to_file(proxy: str):
    normalized = _normalize_proxy(proxy)
    if not normalized:
        return
    path = _stable_proxy_path()
    with _file_lock:
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"{normalized}\n")


def _get_proxy_pool(fallback_proxy=None):
    global _stable_proxy_loaded
    _proxy_pool.set_prefer_stable_proxy(PREFER_STABLE_PROXY)
    if not _stable_proxy_loaded:
        stable = STABLE_PROXY or _load_stable_proxy_from_file()
        if stable:
            _proxy_pool.set_stable_proxy(stable)
        _stable_proxy_loaded = True
    _proxy_pool.set_fallback(fallback_proxy)
    return _proxy_pool


def _is_proxy_related_error(exc: Exception):
    class_name = exc.__class__.__name__.lower()
    if "proxy" in class_name:
        return True

    curl_code = getattr(exc, "code", None)
    if curl_code in {5, 6, 7, 28, 35, 47, 52, 55, 56, 97}:
        return True

    msg = str(exc).lower()
    for word in ("proxy", "connect tunnel failed", "could not connect", "connection refused", "timed out"):
        if word in msg:
            return True
    return False


def _enable_proxy_rotation(session, fallback_proxy=None, fixed_proxy=None, default_timeout=None):
    pool = _get_proxy_pool(fallback_proxy)
    fixed_proxy = _normalize_proxy(fixed_proxy)
    if default_timeout is not None:
        try:
            default_timeout = max(1, int(default_timeout))
        except Exception:
            default_timeout = None
    original_request = session.request
    if getattr(original_request, "_proxy_rotation_wrapped", False):
        return session

    def _request_with_rotating_proxy(method, url, **kwargs):
        base_kwargs = dict(kwargs)
        if default_timeout is not None:
            base_kwargs.setdefault("timeout", default_timeout)

        if base_kwargs.get("proxies"):
            return original_request(method, url, **base_kwargs)

        if fixed_proxy:
            req_kwargs = dict(base_kwargs)
            req_kwargs["proxies"] = {"http": fixed_proxy, "https": fixed_proxy}
            try:
                return original_request(method, url, **req_kwargs)
            except Exception as e:
                if _is_proxy_related_error(e):
                    pool.report_bad(fixed_proxy, error=e)
                raise

        retry_limit = pool.request_retry_limit()
        last_error = None
        for _ in range(retry_limit):
            proxy = pool.next_proxy()
            req_kwargs = dict(base_kwargs)
            if proxy:
                req_kwargs["proxies"] = {"http": proxy, "https": proxy}
            try:
                return original_request(method, url, **req_kwargs)
            except Exception as e:
                last_error = e
                if not proxy:
                    raise
                if not _is_proxy_related_error(e):
                    raise
                pool.report_bad(proxy, error=e)

        if last_error:
            raise last_error
        return original_request(method, url, **base_kwargs)

    _request_with_rotating_proxy._proxy_rotation_wrapped = True
    session.request = _request_with_rotating_proxy
    return session


def _close_session_quietly(session):
    if session is None:
        return
    try:
        session.close()
    except Exception:
        pass


# ================= CF 自建邮箱 (cfmail) 支持 =================

@dataclass(frozen=True)
class CfmailAccount:
    name: str
    worker_domain: str
    email_domain: str
    admin_password: str


def _normalize_host(value: str) -> str:
    value = str(value or "").strip()
    if value.startswith("https://"):
        value = value[len("https://"):]
    elif value.startswith("http://"):
        value = value[len("http://"):]
    return value.strip().strip("/")


def _normalize_cfmail_account(raw: Dict[str, Any]) -> Optional[CfmailAccount]:
    if not isinstance(raw, dict):
        return None
    if not raw.get("enabled", True):
        return None
    name = str(raw.get("name") or "").strip()
    worker_domain = _normalize_host(raw.get("worker_domain") or raw.get("WORKER_DOMAIN") or "")
    email_domain = _normalize_host(raw.get("email_domain") or raw.get("EMAIL_DOMAIN") or "")
    admin_password = str(raw.get("admin_password") or raw.get("ADMIN_PASSWORD") or "").strip()
    if not name or not worker_domain or not email_domain or not admin_password:
        return None
    return CfmailAccount(name=name, worker_domain=worker_domain,
                         email_domain=email_domain, admin_password=admin_password)


def _load_cfmail_accounts_from_file(config_path: str, *, silent: bool = False) -> list:
    path = str(config_path or "").strip()
    if not path or not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        if not silent:
            print(f"[警告] 读取 cfmail 配置文件失败: {path}，错误: {e}")
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        accounts = data.get("accounts")
        if isinstance(accounts, list):
            return accounts
    if not silent:
        print(f"[警告] cfmail 配置文件格式无效: {path}")
    return []


def _build_cfmail_accounts(raw_accounts: list) -> list:
    accounts = []
    seen_names = set()
    for item in raw_accounts:
        account = _normalize_cfmail_account(item)
        if not account:
            continue
        key = account.name.lower()
        if key in seen_names:
            continue
        seen_names.add(key)
        accounts.append(account)

    env_worker_domain = _normalize_host(os.getenv("CFMAIL_WORKER_DOMAIN", ""))
    env_email_domain = _normalize_host(os.getenv("CFMAIL_EMAIL_DOMAIN", ""))
    env_admin_password = str(os.getenv("CFMAIL_ADMIN_PASSWORD", "")).strip()
    env_profile_name = str(os.getenv("CFMAIL_PROFILE_NAME", "default")).strip() or "default"

    if env_worker_domain and env_email_domain and env_admin_password:
        env_account = CfmailAccount(
            name=env_profile_name,
            worker_domain=env_worker_domain,
            email_domain=env_email_domain,
            admin_password=env_admin_password,
        )
        env_key = env_account.name.lower()
        accounts = [acc for acc in accounts if acc.name.lower() != env_key]
        accounts.insert(0, env_account)

    return accounts


# cfmail 全局状态
_cfmail_account_lock = threading.Lock()
_cfmail_account_index = 0
_cfmail_reload_lock = threading.Lock()
_cfmail_failure_lock = threading.Lock()

_CFMAIL_CONFIG_PATH = str(_CONFIG.get("cfmail_config_path", "zhuce5_cfmail_accounts.json")).strip()
CFMAIL_PROFILE_MODE = str(_CONFIG.get("cfmail_profile", "auto")).strip() or "auto"
CFMAIL_ACCOUNTS: list = _build_cfmail_accounts(
    _load_cfmail_accounts_from_file(_CFMAIL_CONFIG_PATH, silent=True)
)
CFMAIL_HOT_RELOAD_ENABLED = True
CFMAIL_CONFIG_MTIME = (
    os.path.getmtime(_CFMAIL_CONFIG_PATH) if os.path.exists(_CFMAIL_CONFIG_PATH) else None
)
CFMAIL_FAIL_THRESHOLD = 3
CFMAIL_COOLDOWN_SECONDS = 1800
CFMAIL_FAILURE_STATE: Dict[str, Dict[str, Any]] = {}


def _cfmail_skip_remaining_seconds(account_name: str) -> int:
    key = str(account_name or "").strip().lower()
    if not key:
        return 0
    with _cfmail_failure_lock:
        state = CFMAIL_FAILURE_STATE.get(key) or {}
        cooldown_until = float(state.get("cooldown_until") or 0)
    remaining = int(math.ceil(cooldown_until - time.time()))
    return max(0, remaining)


def _record_cfmail_success(account_name: str) -> None:
    key = str(account_name or "").strip().lower()
    if not key:
        return
    with _cfmail_failure_lock:
        state = CFMAIL_FAILURE_STATE.setdefault(key, {"name": account_name})
        state["consecutive_failures"] = 0
        state["cooldown_until"] = 0
        state["last_error"] = ""
        state["last_success_at"] = time.time()


def _record_cfmail_failure(account_name: str, reason: str = "") -> None:
    key = str(account_name or "").strip().lower()
    if not key:
        return
    now = time.time()
    with _cfmail_failure_lock:
        state = CFMAIL_FAILURE_STATE.setdefault(key, {"name": account_name})
        state["consecutive_failures"] = int(state.get("consecutive_failures") or 0) + 1
        state["last_error"] = str(reason or "")[:300]
        state["last_failed_at"] = now
        if state["consecutive_failures"] >= CFMAIL_FAIL_THRESHOLD:
            state["cooldown_until"] = max(
                float(state.get("cooldown_until") or 0),
                now + CFMAIL_COOLDOWN_SECONDS,
            )
            state["consecutive_failures"] = 0
            remaining = int(math.ceil(state["cooldown_until"] - now))
            print(f"[警告] cfmail 配置 {account_name} 连续失败，已跳过 {remaining} 秒")


def _reload_cfmail_accounts_if_needed(force: bool = False) -> bool:
    global CFMAIL_CONFIG_MTIME
    if not CFMAIL_HOT_RELOAD_ENABLED:
        return False
    config_path = _CFMAIL_CONFIG_PATH
    if not config_path:
        return False
    try:
        mtime = os.path.getmtime(config_path)
    except OSError:
        return False
    with _cfmail_reload_lock:
        if not force and CFMAIL_CONFIG_MTIME == mtime:
            return False
        raw_accounts = _load_cfmail_accounts_from_file(config_path)
        new_accounts = _build_cfmail_accounts(raw_accounts)
        if not new_accounts:
            CFMAIL_CONFIG_MTIME = mtime
            return False
        global CFMAIL_ACCOUNTS, _cfmail_account_index
        CFMAIL_ACCOUNTS = new_accounts
        _cfmail_account_index = 0
        CFMAIL_CONFIG_MTIME = mtime
        return True


def _select_cfmail_account(profile_name: str = "auto") -> Optional[CfmailAccount]:
    global _cfmail_account_index
    accounts = CFMAIL_ACCOUNTS
    if not accounts:
        return None

    selected_name = str(profile_name or "auto").strip()
    if selected_name and selected_name.lower() != "auto":
        for account in accounts:
            if account.name.lower() == selected_name.lower():
                return account
        return None

    with _cfmail_account_lock:
        start_index = _cfmail_account_index % len(accounts)
        for offset in range(len(accounts)):
            index = (start_index + offset) % len(accounts)
            account = accounts[index]
            if _cfmail_skip_remaining_seconds(account.name) > 0:
                continue
            _cfmail_account_index = (index + 1) % len(accounts)
            return account
    return None


def _cfmail_headers(*, jwt: str = "", use_json: bool = False) -> Dict[str, str]:
    headers = {"Accept": "application/json"}
    if use_json:
        headers["Content-Type"] = "application/json"
    if jwt:
        headers["Authorization"] = f"Bearer {jwt}"
    return headers


# ================= Chrome 指纹配置 =================

_CHROME_PROFILES = [
    {
        "major": 131, "impersonate": "chrome131",
        "build": 6778, "patch_range": (69, 205),
        "sec_ch_ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    },
    {
        "major": 133, "impersonate": "chrome133a",
        "build": 6943, "patch_range": (33, 153),
        "sec_ch_ua": '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    },
    {
        "major": 136, "impersonate": "chrome136",
        "build": 7103, "patch_range": (48, 175),
        "sec_ch_ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
    },
    {
        "major": 142, "impersonate": "chrome142",
        "build": 7540, "patch_range": (30, 150),
        "sec_ch_ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    },
]


def _random_chrome_version():
    profile = random.choice(_CHROME_PROFILES)
    major = profile["major"]
    build = profile["build"]
    patch = random.randint(*profile["patch_range"])
    full_ver = f"{major}.0.{build}.{patch}"
    ua = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{full_ver} Safari/537.36"
    return profile["impersonate"], major, full_ver, ua, profile["sec_ch_ua"]


def _random_delay(low=0.3, high=1.0):
    time.sleep(random.uniform(low, high))


def _make_trace_headers():
    trace_id = random.randint(10**17, 10**18 - 1)
    parent_id = random.randint(10**17, 10**18 - 1)
    tp = f"00-{uuid.uuid4().hex}-{format(parent_id, '016x')}-01"
    return {
        "traceparent": tp, "tracestate": "dd=s:1;o:rum",
        "x-datadog-origin": "rum", "x-datadog-sampling-priority": "1",
        "x-datadog-trace-id": str(trace_id), "x-datadog-parent-id": str(parent_id),
    }


def _generate_pkce():
    code_verifier = base64.urlsafe_b64encode(secrets.token_bytes(64)).rstrip(b"=").decode("ascii")
    digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
    code_challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return code_verifier, code_challenge


# ================= Sentinel Token =================

class SentinelTokenGenerator:
    MAX_ATTEMPTS = 500000
    ERROR_PREFIX = "wQ8Lk5FbGpA2NcR9dShT6gYjU7VxZ4D"

    def __init__(self, device_id=None, user_agent=None):
        self.device_id = device_id or str(uuid.uuid4())
        self.user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        )
        self.requirements_seed = str(random.random())
        self.sid = str(uuid.uuid4())

    @staticmethod
    def _fnv1a_32(text: str):
        h = 2166136261
        for ch in text:
            h ^= ord(ch)
            h = (h * 16777619) & 0xFFFFFFFF
        h ^= (h >> 16)
        h = (h * 2246822507) & 0xFFFFFFFF
        h ^= (h >> 13)
        h = (h * 3266489909) & 0xFFFFFFFF
        h ^= (h >> 16)
        h &= 0xFFFFFFFF
        return format(h, "08x")

    def _get_config(self):
        now_str = time.strftime(
            "%a %b %d %Y %H:%M:%S GMT+0000 (Coordinated Universal Time)",
            time.gmtime(),
        )
        perf_now = random.uniform(1000, 50000)
        time_origin = time.time() * 1000 - perf_now
        nav_prop = random.choice([
            "vendorSub", "productSub", "vendor", "maxTouchPoints",
            "scheduling", "userActivation", "doNotTrack", "geolocation",
            "connection", "plugins", "mimeTypes", "pdfViewerEnabled",
            "webkitTemporaryStorage", "webkitPersistentStorage",
            "hardwareConcurrency", "cookieEnabled", "credentials",
            "mediaDevices", "permissions", "locks", "ink",
        ])
        nav_val = f"{nav_prop}-undefined"
        return [
            "1920x1080", now_str, 4294705152, random.random(),
            self.user_agent,
            "https://sentinel.openai.com/sentinel/20260124ceb8/sdk.js",
            None, None, "en-US", "en-US,en", random.random(), nav_val,
            random.choice(["location", "implementation", "URL", "documentURI", "compatMode"]),
            random.choice(["Object", "Function", "Array", "Number", "parseFloat", "undefined"]),
            perf_now, self.sid, "", random.choice([4, 8, 12, 16]), time_origin,
        ]

    @staticmethod
    def _base64_encode(data):
        raw = json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        return base64.b64encode(raw).decode("ascii")

    def _run_check(self, start_time, seed, difficulty, config, nonce):
        config[3] = nonce
        config[9] = round((time.time() - start_time) * 1000)
        data = self._base64_encode(config)
        hash_hex = self._fnv1a_32(seed + data)
        diff_len = len(difficulty)
        if hash_hex[:diff_len] <= difficulty:
            return data + "~S"
        return None

    def generate_token(self, seed=None, difficulty=None):
        seed = seed if seed is not None else self.requirements_seed
        difficulty = str(difficulty or "0")
        start_time = time.time()
        config = self._get_config()
        for i in range(self.MAX_ATTEMPTS):
            result = self._run_check(start_time, seed, difficulty, config, i)
            if result:
                return "gAAAAAB" + result
        return "gAAAAAB" + self.ERROR_PREFIX + self._base64_encode(str(None))

    def generate_requirements_token(self):
        config = self._get_config()
        config[3] = 1
        config[9] = round(random.uniform(5, 50))
        data = self._base64_encode(config)
        return "gAAAAAC" + data


def fetch_sentinel_challenge(session, device_id, flow="authorize_continue", user_agent=None,
                             sec_ch_ua=None, impersonate=None):
    generator = SentinelTokenGenerator(device_id=device_id, user_agent=user_agent)
    req_body = {"p": generator.generate_requirements_token(), "id": device_id, "flow": flow}
    headers = {
        "Content-Type": "text/plain;charset=UTF-8",
        "Referer": "https://sentinel.openai.com/backend-api/sentinel/frame.html",
        "Origin": "https://sentinel.openai.com",
        "User-Agent": user_agent or "Mozilla/5.0",
        "sec-ch-ua": sec_ch_ua or '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }
    kwargs = {"data": json.dumps(req_body), "headers": headers, "timeout": 20}
    if impersonate:
        kwargs["impersonate"] = impersonate
    try:
        resp = session.post("https://sentinel.openai.com/backend-api/sentinel/req", **kwargs)
    except Exception:
        return None
    if resp.status_code != 200:
        return None
    try:
        return resp.json()
    except Exception:
        return None


def build_sentinel_token(session, device_id, flow="authorize_continue", user_agent=None,
                         sec_ch_ua=None, impersonate=None):
    challenge = fetch_sentinel_challenge(session, device_id, flow=flow,
                                         user_agent=user_agent, sec_ch_ua=sec_ch_ua,
                                         impersonate=impersonate)
    if not challenge:
        return None
    c_value = challenge.get("token", "")
    if not c_value:
        return None
    pow_data = challenge.get("proofofwork") or {}
    generator = SentinelTokenGenerator(device_id=device_id, user_agent=user_agent)
    if pow_data.get("required") and pow_data.get("seed"):
        p_value = generator.generate_token(seed=pow_data.get("seed"),
                                            difficulty=pow_data.get("difficulty", "0"))
    else:
        p_value = generator.generate_requirements_token()
    return json.dumps({"p": p_value, "t": "", "c": c_value, "id": device_id, "flow": flow},
                      separators=(",", ":"))


def _extract_code_from_url(url: str):
    if not url or "code=" not in url:
        return None
    try:
        return parse_qs(urlparse(url).query).get("code", [None])[0]
    except Exception:
        return None


def _decode_jwt_payload(token: str):
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return {}
        payload = parts[1]
        padding = 4 - len(payload) % 4
        if padding != 4:
            payload += "=" * padding
        decoded = base64.urlsafe_b64decode(payload)
        return json.loads(decoded)
    except Exception:
        return {}


def _build_codex_token_data(email: str, tokens: dict) -> dict:
    access_token = tokens.get("access_token", "")
    refresh_token = tokens.get("refresh_token", "")
    id_token = tokens.get("id_token", "")

    payload = _decode_jwt_payload(access_token) if access_token else {}
    auth_info = payload.get("https://api.openai.com/auth", {})
    account_id = auth_info.get("chatgpt_account_id", "")
    exp_timestamp = payload.get("exp")
    expired_str = ""
    if isinstance(exp_timestamp, int) and exp_timestamp > 0:
        from datetime import datetime, timezone, timedelta
        exp_dt = datetime.fromtimestamp(exp_timestamp, tz=timezone(timedelta(hours=8)))
        expired_str = exp_dt.strftime("%Y-%m-%dT%H:%M:%S+08:00")

    from datetime import datetime, timezone, timedelta
    now = datetime.now(tz=timezone(timedelta(hours=8)))
    return {
        "type": "codex",
        "email": email,
        "expired": expired_str,
        "id_token": id_token,
        "account_id": account_id,
        "access_token": access_token,
        "last_refresh": now.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
        "refresh_token": refresh_token,
    }


def _save_codex_tokens(email: str, tokens: dict):
    access_token = tokens.get("access_token", "")
    refresh_token = tokens.get("refresh_token", "")

    if access_token:
        with _file_lock:
            with open(AK_FILE, "a", encoding="utf-8") as f:
                f.write(f"{access_token}\n")

    if refresh_token:
        with _file_lock:
            with open(RK_FILE, "a", encoding="utf-8") as f:
                f.write(f"{refresh_token}\n")

    if not access_token:
        return

    token_data = _build_codex_token_data(email, tokens)
    existing = {}

    base_dir = os.path.dirname(os.path.abspath(__file__))
    token_dir = TOKEN_JSON_DIR if os.path.isabs(TOKEN_JSON_DIR) else os.path.join(base_dir, TOKEN_JSON_DIR)
    os.makedirs(token_dir, exist_ok=True)
    token_path = os.path.join(token_dir, f"{email}.json")
    with _file_lock:
        if os.path.exists(token_path):
            try:
                with open(token_path, "r", encoding="utf-8") as f:
                    existing = json.load(f)
            except Exception:
                existing = {}
        uploaded_platforms = existing.get("uploaded_platforms", [])
        sync_status = existing.get("sync_status", {})
        if isinstance(uploaded_platforms, list):
            token_data["uploaded_platforms"] = uploaded_platforms
        if isinstance(sync_status, dict):
            token_data["sync_status"] = sync_status
        with open(token_path, "w", encoding="utf-8") as f:
            json.dump(token_data, f, ensure_ascii=False)


def _mark_token_upload_result(email: str, platform: str, ok: bool, detail: str = ""):
    if not email or not platform:
        return

    base_dir = os.path.dirname(os.path.abspath(__file__))
    token_dir = TOKEN_JSON_DIR if os.path.isabs(TOKEN_JSON_DIR) else os.path.join(base_dir, TOKEN_JSON_DIR)
    token_path = os.path.join(token_dir, f"{email}.json")
    if not os.path.exists(token_path):
        return

    try:
        from datetime import datetime, timezone, timedelta
        ts = datetime.now(tz=timezone(timedelta(hours=8))).strftime("%Y-%m-%dT%H:%M:%S+08:00")
    except Exception:
        ts = str(int(time.time()))

    platform_key = str(platform).strip().lower()
    clean_detail = str(detail or "").strip()[:300]

    with _file_lock:
        try:
            with open(token_path, "r", encoding="utf-8") as f:
                token_data = json.load(f)
        except Exception:
            return

        uploaded = token_data.get("uploaded_platforms", [])
        if not isinstance(uploaded, list):
            uploaded = []
        uploaded = [str(item).strip().lower() for item in uploaded if str(item).strip()]

        sync_status = token_data.get("sync_status", {})
        if not isinstance(sync_status, dict):
            sync_status = {}

        if ok and platform_key not in uploaded:
            uploaded.append(platform_key)

        sync_status[platform_key] = {
            "ok": bool(ok),
            "updated_at": ts,
            "detail": clean_detail,
        }

        token_data["uploaded_platforms"] = uploaded
        token_data["sync_status"] = sync_status

        with open(token_path, "w", encoding="utf-8") as f:
            json.dump(token_data, f, ensure_ascii=False)


def _should_upload_token_json(filepath, platform: str = "cpa") -> bool:
    platform_key = str(platform or "").strip().lower()
    if not platform_key:
        return True
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            token_data = json.load(f)
    except Exception:
        return True

    if not isinstance(token_data, dict):
        return True

    uploaded = token_data.get("uploaded_platforms", [])
    if not isinstance(uploaded, list):
        uploaded = []
    uploaded_set = {
        str(item).strip().lower()
        for item in uploaded
        if str(item).strip()
    }

    sync_status = token_data.get("sync_status", {})
    if not isinstance(sync_status, dict):
        sync_status = {}
    status = sync_status.get(platform_key)

    if isinstance(status, dict) and status.get("ok") is True:
        return False
    if platform_key in uploaded_set:
        return False
    return True


def _upload_token_json(filepath):
    mp = None
    session = None
    try:
        from curl_cffi import CurlMime
        filename = os.path.basename(filepath)
        email = os.path.splitext(filename)[0]
        mp = CurlMime()
        mp.addpart(name="file", content_type="application/json",
                   filename=filename, local_path=filepath)
        session = curl_requests.Session()
        _enable_proxy_rotation(
            session,
            fallback_proxy=DEFAULT_PROXY,
            default_timeout=DEFAULT_HTTP_TIMEOUT_SECONDS,
        )
        resp = session.post(UPLOAD_API_URL, multipart=mp,
                            headers={"Authorization": f"Bearer {UPLOAD_API_TOKEN}"},
                            verify=False, timeout=30)
        if resp.status_code in (200, 201):
            _mark_token_upload_result(email, "cpa", True, f"HTTP {resp.status_code}")
            print(f"  [CPA] 上传成功: {filename} (HTTP {resp.status_code})")
            return True
        else:
            _mark_token_upload_result(email, "cpa", False, f"HTTP {resp.status_code}: {resp.text[:200]}")
            print(f"  [CPA] 上传失败: {filename} -> {resp.status_code} - {resp.text[:200]}")
            return False
    except Exception as e:
        filename = os.path.basename(filepath)
        _mark_token_upload_result(os.path.splitext(filename)[0], "cpa", False, str(e))
        print(f"  [CPA] 上传异常: {os.path.basename(filepath)} -> {e}")
        return False
    finally:
        if mp:
            mp.close()
        _close_session_quietly(session)


def _upload_all_tokens_to_cpa():
    if not UPLOAD_API_URL:
        print("\n[CPA] ⚠️ 未配置 upload_api_url，跳过 CPA 上传")
        return
    base_dir = os.path.dirname(os.path.abspath(__file__))
    token_dir = TOKEN_JSON_DIR if os.path.isabs(TOKEN_JSON_DIR) else os.path.join(base_dir, TOKEN_JSON_DIR)
    if not os.path.isdir(token_dir):
        return
    json_files = [f for f in os.listdir(token_dir) if f.endswith(".json")]
    if not json_files:
        return
    pending_files = [
        f for f in json_files
        if _should_upload_token_json(os.path.join(token_dir, f), platform="cpa")
    ]
    skipped = len(json_files) - len(pending_files)
    if not pending_files:
        print(f"\n[CPA] 无待上传账号，跳过本轮 (已同步 {skipped} 个)")
        return
    print(
        f"\n{'='*60}\n"
        f"  [CPA] 开始上传 {len(pending_files)} 个账号到 CPA 管理平台"
        f" (跳过已同步 {skipped} 个)\n"
        f"{'='*60}"
    )
    uploaded = 0
    failed = 0
    for filename in pending_files:
        filepath = os.path.join(token_dir, filename)
        if _upload_token_json(filepath):
            uploaded += 1
        else:
            failed += 1
    print(f"\n  [CPA] 上传完成: 成功 {uploaded} 个, 失败 {failed} 个\n{'='*60}")


def _sub2api_proxy_candidates(preferred_proxy: str = None):
    candidates = []
    seen = set()

    def _add(value):
        normalized = _normalize_proxy(value)
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        candidates.append(normalized)

    stable_from_file = _load_stable_proxy_from_file()
    direct_proxy = _normalize_single_proxy_source(PROXY_LIST_URL)
    pool_proxy = None
    try:
        pool_proxy = _get_proxy_pool(fallback_proxy=preferred_proxy or DEFAULT_PROXY).next_proxy()
    except Exception:
        pool_proxy = None

    _add(preferred_proxy)
    _add(DEFAULT_PROXY)
    _add(STABLE_PROXY)
    _add(stable_from_file)
    _add(direct_proxy)
    _add(pool_proxy)
    return candidates


def _sub2api_request(method: str, url: str, preferred_proxy: str = None, **kwargs):
    req_kwargs = dict(kwargs)
    req_kwargs.setdefault("impersonate", "chrome131")
    req_kwargs.setdefault("timeout", 20)

    candidates = _sub2api_proxy_candidates(preferred_proxy)
    if not candidates:
        return curl_requests.request(method, url, **req_kwargs)

    last_error = None
    error_texts = []
    for proxy in candidates:
        call_kwargs = dict(req_kwargs)
        call_kwargs["proxies"] = {"http": proxy, "https": proxy}
        try:
            return curl_requests.request(method, url, **call_kwargs)
        except Exception as e:
            last_error = e
            error_texts.append(f"{proxy} -> {str(e)[:120]}")
            if not _is_proxy_related_error(e):
                raise

    if last_error:
        raise Exception("Sub2Api 代理全部失败: " + " | ".join(error_texts[:3]))
    return curl_requests.request(method, url, **req_kwargs)


def _sub2api_cfg_signature():
    return "|".join([
        SUB2API_BASE_URL,
        SUB2API_EMAIL.lower(),
        SUB2API_BEARER[:24],
        str(SUB2API_PROXY_ID),
        SUB2API_PROXY_NAME.lower(),
        "1" if SUB2API_AUTO_ASSIGN_PROXY else "0",
    ])


def _sub2api_auth_request(method: str, path: str, preferred_proxy: str = None, **kwargs):
    url = path if str(path).startswith("http") else f"{SUB2API_BASE_URL}{path}"
    bearer = _sub2api_bearer_holder[0]
    if not bearer and SUB2API_EMAIL and SUB2API_PASSWORD:
        with _sub2api_auth_lock:
            if not _sub2api_bearer_holder[0]:
                token = _sub2api_login(preferred_proxy=preferred_proxy)
                if token:
                    _sub2api_bearer_holder[0] = token
        bearer = _sub2api_bearer_holder[0]

    headers = dict(kwargs.pop("headers", {}) or {})
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    resp = _sub2api_request(method, url, preferred_proxy=preferred_proxy, headers=headers, **kwargs)

    if resp.status_code == 401 and SUB2API_EMAIL and SUB2API_PASSWORD:
        with _sub2api_auth_lock:
            new_token = _sub2api_login(preferred_proxy=preferred_proxy)
            if new_token:
                _sub2api_bearer_holder[0] = new_token
        bearer = _sub2api_bearer_holder[0]
        headers = dict(headers)
        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"
        resp = _sub2api_request(method, url, preferred_proxy=preferred_proxy, headers=headers, **kwargs)
    return resp


def _sub2api_proxy_record_to_url(item: dict):
    if not isinstance(item, dict):
        return None
    host = str(item.get("host", "") or "").strip()
    port = str(item.get("port", "") or "").strip()
    if not host or not port:
        return None
    protocol = str(item.get("protocol", "") or "http").strip().lower() or "http"
    username = item.get("username")
    password = item.get("password")
    if username not in (None, ""):
        auth = quote(str(username), safe="")
        if password not in (None, ""):
            auth = f"{auth}:{quote(str(password), safe='')}"
        return f"{protocol}://{auth}@{host}:{port}"
    return f"{protocol}://{host}:{port}"


def _sub2api_proxy_matches(item: dict, proxy: str):
    normalized_proxy = _normalize_proxy(proxy)
    if not normalized_proxy:
        return False

    item_url = _sub2api_proxy_record_to_url(item)
    normalized_item_url = _normalize_proxy(item_url) if item_url else None
    if normalized_item_url and normalized_item_url == normalized_proxy:
        return True

    try:
        proxy_parsed = urlparse(normalized_proxy)
    except Exception:
        return False
    host = str(item.get("host", "") or "").strip().lower()
    port = str(item.get("port", "") or "").strip()
    username = str(item.get("username", "") or "").strip()
    if proxy_parsed.hostname and host and proxy_parsed.hostname.lower() != host:
        return False
    if proxy_parsed.port and port and str(proxy_parsed.port) != port:
        return False
    if username:
        proxy_user = ""
        if proxy_parsed.username:
            try:
                proxy_user = re.sub(r"%([0-9A-Fa-f]{2})", lambda m: chr(int(m.group(1), 16)), proxy_parsed.username)
            except Exception:
                proxy_user = proxy_parsed.username
        if proxy_user and proxy_user != username:
            return False
    return bool(proxy_parsed.hostname and proxy_parsed.port)


def _sub2api_choose_proxy_record(proxies: list, preferred_proxy: str = None):
    candidates = [p for p in proxies if isinstance(p, dict)]
    active = [p for p in candidates if str(p.get("status", "") or "").strip().lower() == "active"]
    if active:
        candidates = active

    if SUB2API_PROXY_NAME:
        target = SUB2API_PROXY_NAME.strip().lower()
        exact = [p for p in candidates if str(p.get("name", "") or "").strip().lower() == target]
        if exact:
            return exact[0]
        contains = [p for p in candidates if target in str(p.get("name", "") or "").strip().lower()]
        if contains:
            return contains[0]

    if preferred_proxy:
        matched = [p for p in candidates if _sub2api_proxy_matches(p, preferred_proxy)]
        if matched:
            return matched[0]

    if SUB2API_AUTO_ASSIGN_PROXY and candidates:
        def _sort_key(item):
            try:
                account_count = int(item.get("account_count", 0) or 0)
            except Exception:
                account_count = 0
            latency_status = str(item.get("latency_status", "") or "").strip().lower()
            quality_status = str(item.get("quality_status", "") or "").strip().lower()
            latency_rank = 0 if latency_status == "success" else 1
            quality_rank = 1 if quality_status in {"failed", "inactive"} else 0
            try:
                item_id = int(item.get("id", 0) or 0)
            except Exception:
                item_id = 0
            return (latency_rank, quality_rank, account_count, item_id)
        return sorted(candidates, key=_sort_key)[0]

    return None


def _sub2api_resolve_proxy_binding(preferred_proxy: str = None):
    if SUB2API_PROXY_ID > 0:
        return SUB2API_PROXY_ID, {"id": SUB2API_PROXY_ID, "name": f"id:{SUB2API_PROXY_ID}"}

    signature = _sub2api_cfg_signature()
    now = time.time()
    with _sub2api_proxy_cache_lock:
        cached_items = _sub2api_proxy_cache.get("items")
        cached_sig = _sub2api_proxy_cache.get("signature", "")
        cached_ts = float(_sub2api_proxy_cache.get("ts", 0.0) or 0.0)
    proxies = None
    if cached_items and cached_sig == signature and (now - cached_ts) < 60:
        proxies = list(cached_items)

    if proxies is None:
        try:
            resp = _sub2api_auth_request(
                "GET",
                "/api/v1/admin/proxies",
                preferred_proxy=preferred_proxy,
                params={"page": 1, "page_size": 200},
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
            payload = data.get("data") if isinstance(data, dict) else data
            items = payload.get("items") if isinstance(payload, dict) else []
            proxies = [item for item in items if isinstance(item, dict)]
            with _sub2api_proxy_cache_lock:
                _sub2api_proxy_cache.update({"ts": now, "items": list(proxies), "signature": signature})
        except Exception as e:
            with _print_lock:
                print(f"[Sub2Api] 拉取代理列表失败: {e}")
            return None, None

    selected = _sub2api_choose_proxy_record(proxies or [], preferred_proxy=preferred_proxy)
    if not selected:
        return None, None
    try:
        proxy_id = int(selected.get("id", 0) or 0)
    except Exception:
        proxy_id = 0
    if proxy_id <= 0:
        return None, None
    return proxy_id, selected


def _sub2api_login(preferred_proxy: str = None) -> str:
    if not SUB2API_BASE_URL or not SUB2API_EMAIL or not SUB2API_PASSWORD:
        return ""
    url = f"{SUB2API_BASE_URL}/api/v1/auth/login"
    try:
        resp = _sub2api_request(
            "POST",
            url,
            preferred_proxy=preferred_proxy,
            json={"email": SUB2API_EMAIL, "password": SUB2API_PASSWORD},
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            timeout=15,
        )
        data = resp.json()
        token = (
            data.get("token")
            or data.get("access_token")
            or (data.get("data") or {}).get("token")
            or (data.get("data") or {}).get("access_token")
            or ""
        )
        return str(token).strip()
    except Exception as e:
        with _print_lock:
            print(f"[Sub2Api] 登录失败: {e}")
        return ""


def _build_sub2api_account_payload(email: str, tokens: dict, proxy_id: int = 0) -> dict:
    access_token = tokens.get("access_token", "")
    refresh_token = tokens.get("refresh_token", "")
    id_token = tokens.get("id_token", "")

    at_payload = _decode_jwt_payload(access_token) if access_token else {}
    at_auth = at_payload.get("https://api.openai.com/auth") or {}
    chatgpt_account_id = at_auth.get("chatgpt_account_id", "") or tokens.get("account_id", "")
    chatgpt_user_id = at_auth.get("chatgpt_user_id", "")
    exp_timestamp = at_payload.get("exp", 0)
    expires_at = exp_timestamp if isinstance(exp_timestamp, int) and exp_timestamp > 0 else int(time.time()) + 863999

    it_payload = _decode_jwt_payload(id_token) if id_token else {}
    it_auth = it_payload.get("https://api.openai.com/auth") or {}
    organization_id = it_auth.get("organization_id", "")
    if not organization_id:
        orgs = it_auth.get("organizations") or []
        if orgs:
            organization_id = (orgs[0] or {}).get("id", "")

    payload = {
        "name": email,
        "notes": "",
        "platform": "openai",
        "type": "oauth",
        "credentials": {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_in": 863999,
            "expires_at": expires_at,
            "chatgpt_account_id": chatgpt_account_id,
            "chatgpt_user_id": chatgpt_user_id,
            "organization_id": organization_id,
        },
        "extra": {"email": email},
        "group_ids": SUB2API_GROUP_IDS or [2],
        "concurrency": 10,
        "priority": 1,
        "auto_pause_on_expired": True,
    }
    if proxy_id and int(proxy_id) > 0:
        payload["proxy_id"] = int(proxy_id)
    return payload


def _push_account_to_sub2api(email: str, tokens: dict, preferred_proxy: str = None) -> bool:
    if not SUB2API_BASE_URL or not tokens.get("refresh_token"):
        return False

    url = f"{SUB2API_BASE_URL}/api/v1/admin/accounts"
    proxy_id, proxy_meta = _sub2api_resolve_proxy_binding(preferred_proxy=preferred_proxy)
    payload = _build_sub2api_account_payload(email, tokens, proxy_id=proxy_id or 0)
    if proxy_id:
        with _print_lock:
            proxy_name = str((proxy_meta or {}).get("name", "") or f"id:{proxy_id}")
            print(f"[Sub2Api] 自动绑定代理: {proxy_name} (#{proxy_id})")

    def _do_request(bearer: str):
        try:
            resp = _sub2api_request(
                "POST",
                url,
                preferred_proxy=preferred_proxy,
                json=payload,
                headers={
                    "Authorization": f"Bearer {bearer}",
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/plain, */*",
                    "Referer": f"{SUB2API_BASE_URL}/admin/accounts",
                },
                timeout=20,
            )
            return resp.status_code, resp.text
        except Exception as e:
            return 0, str(e)

    bearer = _sub2api_bearer_holder[0]
    status, body = _do_request(bearer)
    if status == 401 and SUB2API_EMAIL and SUB2API_PASSWORD:
        with _sub2api_auth_lock:
            if _sub2api_bearer_holder[0] == bearer:
                new_token = _sub2api_login(preferred_proxy=preferred_proxy)
                if new_token:
                    _sub2api_bearer_holder[0] = new_token
        bearer = _sub2api_bearer_holder[0]
        status, body = _do_request(bearer)

    ok = status in (200, 201)
    detail = f"HTTP {status}" if ok else f"HTTP {status}: {body[:200]}"
    _mark_token_upload_result(email, "sub2api", ok, detail)
    with _print_lock:
        if ok:
            print(f"[Sub2Api] 上传成功 (HTTP {status})")
        else:
            print(f"[Sub2Api] 上传失败 (HTTP {status}): {body[:500]}")
    return ok


# ================= CPA Codex 清理引擎 =================

from datetime import datetime as _cpa_datetime
from urllib.parse import urlparse as _cpa_urlparse, urlunparse as _cpa_urlunparse

_CPA_STATUS_KEYWORDS = {"token_invalidated", "token_revoked", "usage_limit_reached"}
_CPA_MESSAGE_KEYWORDS = [
    "额度获取失败：401", '"status":401', '"status": 401',
    "your authentication token has been invalidated.",
    "encountered invalidated oauth token for user",
    "token_invalidated", "token_revoked", "usage_limit_reached",
]
_CPA_PROBE_TARGET_URL = "https://chatgpt.com/backend-api/codex/responses/compact"
_CPA_PROBE_MODEL = "gpt-5.1-codex"


def _cpa_normalize_api_root(raw_url):
    value = (raw_url or "").strip()
    if not value:
        return ""
    parsed = _cpa_urlparse(value)
    path = parsed.path or ""
    if path.endswith("/management.html"):
        path = path[:-len("/management.html")] + "/v0/management"
    for suffix in ("/api-call", "/auth-files"):
        if path.endswith(suffix):
            path = path[:-len(suffix)]
    normalized = _cpa_urlunparse((parsed.scheme, parsed.netloc, path.rstrip("/"), "", "", ""))
    return normalized.rstrip("/")


class _CpaCleanupConfig:
    def __init__(self, management_url, management_token, management_timeout=15,
                 active_probe=True, probe_timeout=8, probe_workers=12,
                 delete_workers=8, max_active_probes=120):
        self.management_url = management_url
        self.management_token = management_token
        self.management_timeout = management_timeout
        self.active_probe = active_probe
        self.probe_timeout = probe_timeout
        self.probe_workers = probe_workers
        self.delete_workers = delete_workers
        self.max_active_probes = max_active_probes

    @classmethod
    def from_mapping(cls, data):
        def to_int(name, default, minimum):
            try:
                parsed = int(data.get(name, default))
            except Exception:
                parsed = default
            return max(minimum, parsed)

        def to_bool(name, default):
            raw = data.get(name, default)
            if isinstance(raw, bool):
                return raw
            text = str(raw).strip().lower()
            if text in {"1", "true", "yes", "on"}:
                return True
            if text in {"0", "false", "no", "off"}:
                return False
            return default

        return cls(
            management_url=_cpa_normalize_api_root(str(data.get("management_url", "") or "")),
            management_token=str(data.get("management_token", "") or "").strip(),
            management_timeout=to_int("management_timeout", 15, 1),
            active_probe=to_bool("active_probe", True),
            probe_timeout=to_int("probe_timeout", 8, 1),
            probe_workers=to_int("probe_workers", 12, 1),
            delete_workers=to_int("delete_workers", 8, 1),
            max_active_probes=to_int("max_active_probes", 120, 0),
        )

    def validate(self):
        if not self.management_url:
            return False, "management_url 不能为空"
        if not self.management_token:
            return False, "management_token 不能为空"
        if not self.management_url.startswith(("http://", "https://")):
            return False, "management_url 必须以 http:// 或 https:// 开头"
        return True, ""


class _CpaManagementGateway:
    def __init__(self, config):
        self.config = config

    @property
    def _headers(self):
        return {"Authorization": f"Bearer {self.config.management_token}"}

    @property
    def auth_files_endpoint(self):
        return self.config.management_url.rstrip("/") + "/auth-files"

    @property
    def api_call_endpoint(self):
        return self.config.management_url.rstrip("/") + "/api-call"

    def list_auth_files(self):
        resp = curl_requests.get(self.auth_files_endpoint, headers=self._headers,
                                 timeout=self.config.management_timeout)
        if resp.status_code == 404:
            raise RuntimeError(f"auth-files 接口不存在: {self.auth_files_endpoint}")
        resp.raise_for_status()
        payload = resp.json()
        files = payload.get("files", []) if isinstance(payload, dict) else []
        return files if isinstance(files, list) else []

    def delete_auth_file(self, name):
        resp = curl_requests.delete(
            self.auth_files_endpoint, params={"name": name},
            headers=self._headers, timeout=self.config.management_timeout,
        )
        if 200 <= resp.status_code < 300:
            return True, ""
        detail = ""
        try:
            detail = json.dumps(resp.json(), ensure_ascii=False)
        except Exception:
            detail = resp.text
        return False, f"HTTP {resp.status_code}: {detail}"

    def probe_auth_index(self, auth_index):
        payload = {
            "auth_index": auth_index, "method": "POST",
            "url": _CPA_PROBE_TARGET_URL,
            "header": {
                "Authorization": "Bearer $TOKEN$",
                "Content-Type": "application/json",
                "User-Agent": "codex_cli_rs/0.101.0",
            },
            "data": json.dumps(
                {"model": _CPA_PROBE_MODEL, "input": [{"role": "user", "content": "ping"}]},
                ensure_ascii=False,
            ),
        }
        resp = curl_requests.post(self.api_call_endpoint, headers=self._headers,
                                  json=payload, timeout=self.config.probe_timeout)
        resp.raise_for_status()
        body = resp.json()
        if not isinstance(body, dict):
            return 0, ""
        return int(body.get("status_code", 0) or 0), str(body.get("body", "") or "")


def _cpa_safe_status_message(file_obj):
    return str(file_obj.get("status_message", "") or "")


def _cpa_reason_from_status(file_obj):
    status_message = _cpa_safe_status_message(file_obj)
    if not status_message:
        return ""
    lower_msg = status_message.lower()
    for keyword in _CPA_MESSAGE_KEYWORDS:
        if keyword in lower_msg:
            return keyword
    try:
        parsed = json.loads(status_message)
    except Exception:
        parsed = None
    if isinstance(parsed, dict):
        if int(parsed.get("status", 0) or 0) == 401:
            return "status_401"
        err = parsed.get("error", {})
        if isinstance(err, dict):
            code = str(err.get("code", "") or "")
            if code in _CPA_STATUS_KEYWORDS:
                return code
    return ""


def _cpa_looks_401(file_obj):
    try:
        if int(file_obj.get("status", 0) or 0) == 401:
            return True
    except Exception:
        pass
    text = _cpa_safe_status_message(file_obj).lower()
    return "401" in text or "unauthorized" in text


class _CpaCleanupOrchestrator:
    def __init__(self, config, log=None):
        self.config = config
        self.gateway = _CpaManagementGateway(config)
        self.log = log or (lambda msg: None)

    def _log(self, message):
        self.log(message)

    def _probe_one(self, file_obj):
        name = str(file_obj.get("name", "") or "")
        auth_index = str(file_obj.get("auth_index", "") or "").strip()
        if not auth_index:
            return name, ""
        try:
            status_code, body = self.gateway.probe_auth_index(auth_index)
        except Exception as exc:
            return name, f"probe_error:{exc}"
        body_lower = body.lower()
        if status_code == 401:
            return name, "probe_status_401"
        if "401" in body_lower or "unauthorized" in body_lower:
            return name, "probe_body_401"
        for keyword in _CPA_MESSAGE_KEYWORDS:
            if keyword in body_lower:
                return name, f"probe_{keyword}"
        return name, ""

    def _delete_batch(self, hits):
        deleted = 0
        failures = []
        total = len(hits)

        def task(item):
            ok, err = self.gateway.delete_auth_file(item["name"])
            return item["name"], ok, err

        with ThreadPoolExecutor(max_workers=self.config.delete_workers) as pool:
            future_map = {pool.submit(task, item): item for item in hits}
            done = 0
            for future in as_completed(future_map):
                done += 1
                name, ok, err = future.result()
                if ok:
                    deleted += 1
                    self._log(f"[CPA清理] 删除成功: {name} ({done}/{total})")
                else:
                    failures.append({"name": name, "error": err})
                    self._log(f"[CPA清理] 删除失败: {name} -> {err}")
        return deleted, failures

    def _cleanup_401_only(self, exclude_names):
        try:
            files = self.gateway.list_auth_files()
        except Exception as exc:
            self._log(f"[CPA清理] 401补删列表读取失败: {exc}")
            return 0, [{"name": "<list>", "error": str(exc)}]
        targets = []
        for file_obj in files:
            name = str(file_obj.get("name", "") or "")
            if not name or name in exclude_names:
                continue
            if _cpa_looks_401(file_obj):
                targets.append({"name": name, "keyword": "status_401",
                                "status_message": _cpa_safe_status_message(file_obj)})
        if not targets:
            return 0, []
        deleted, failures = self._delete_batch(targets)
        return deleted, failures

    def run(self):
        self._log("[CPA清理] 开始清理")
        files = self.gateway.list_auth_files()
        self._log(f"[CPA清理] 拉取 auth-files 成功，总数: {len(files)}")

        fixed_hits = []
        probe_candidates = []

        for file_obj in files:
            reason = _cpa_reason_from_status(file_obj)
            name = str(file_obj.get("name", "") or "")
            if not name:
                continue
            if reason:
                fixed_hits.append({"name": name, "keyword": reason,
                                   "status_message": _cpa_safe_status_message(file_obj)})
                continue
            provider = str(file_obj.get("provider", "") or "").strip().lower()
            auth_index = str(file_obj.get("auth_index", "") or "").strip()
            if self.config.active_probe and provider == "codex" and auth_index:
                probe_candidates.append(file_obj)

        if (self.config.active_probe and self.config.max_active_probes > 0
                and len(probe_candidates) > self.config.max_active_probes):
            probe_candidates = probe_candidates[:self.config.max_active_probes]

        probed_hits = []
        if self.config.active_probe and self.config.max_active_probes != 0 and probe_candidates:
            self._log(f"[CPA清理] 开始主动探测，候选 {len(probe_candidates)} 个")
            with ThreadPoolExecutor(max_workers=self.config.probe_workers) as pool:
                future_map = {pool.submit(self._probe_one, item): item for item in probe_candidates}
                done = 0
                total = len(probe_candidates)
                for future in as_completed(future_map):
                    done += 1
                    name, reason = future.result()
                    if reason and not reason.startswith("probe_error"):
                        status_message = _cpa_safe_status_message(future_map[future])
                        probed_hits.append({"name": name, "keyword": reason,
                                            "status_message": status_message})

        merged_by_name = {}
        for item in fixed_hits + probed_hits:
            if item["name"] not in merged_by_name:
                merged_by_name[item["name"]] = item

        matched = list(merged_by_name.values())
        deleted_main = 0
        failures = []
        if matched:
            deleted_main, failures = self._delete_batch(matched)

        deleted_401, failures_401 = self._cleanup_401_only(set(merged_by_name.keys()))
        failures.extend(failures_401)

        result = {
            "scanned_total": len(files), "matched_total": len(matched),
            "deleted_main": deleted_main, "deleted_401": deleted_401,
            "deleted_total": deleted_main + deleted_401, "failures": failures,
        }
        self._log(f"[CPA清理] 完成: scanned={result['scanned_total']}, "
                  f"deleted_total={result['deleted_total']}")
        return result


def _cpa_execute_cleanup(payload, log=None):
    config = _CpaCleanupConfig.from_mapping(payload)
    ok, msg = config.validate()
    if not ok:
        raise ValueError(msg)
    orchestrator = _CpaCleanupOrchestrator(config=config, log=log)
    return orchestrator.run()


def _run_cpa_cleanup_before_register():
    print(f"\n{'='*60}\n  [CPA清理] 注册前清理 CPA 无效号...\n{'='*60}")
    try:
        payload = {
            "management_url": UPLOAD_API_URL,
            "management_token": UPLOAD_API_TOKEN,
            "active_probe": True, "probe_workers": 12,
            "delete_workers": 8, "max_active_probes": 120,
        }
        result = _cpa_execute_cleanup(payload, log=lambda msg: print(f"  {msg}"))
        print(f"\n  [CPA清理] 清理完成: 扫描 {result['scanned_total']} 个, "
              f"删除 {result['deleted_total']} 个")
    except Exception as e:
        print(f"  [CPA清理] ⚠️ 清理失败 (不影响注册): {e}")
    print(f"{'='*60}\n")


def _generate_password(length=14):
    lower = string.ascii_lowercase
    upper = string.ascii_uppercase
    digits = string.digits
    special = "!@#$%&*"
    pwd = [random.choice(lower), random.choice(upper),
           random.choice(digits), random.choice(special)]
    all_chars = lower + upper + digits + special
    pwd += [random.choice(all_chars) for _ in range(length - 4)]
    random.shuffle(pwd)
    return "".join(pwd)


# ================= 临时邮箱函数 =================

def _create_duckmail_session():
    session = curl_requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Content-Type": "application/json",
    })
    return _enable_proxy_rotation(session, default_timeout=DEFAULT_HTTP_TIMEOUT_SECONDS)


def _mail_api_url(api_base: str, path: str):
    base = str(api_base or "").rstrip("/")
    rel = "/" + str(path or "").lstrip("/")
    parsed = urlparse(base if "://" in base else f"https://{base}")
    base_path = parsed.path.rstrip("/")
    if base_path.endswith("/api") and rel == "/api":
        rel = ""
    elif base_path.endswith("/api") and rel.startswith("/api/"):
        rel = rel[4:]
    return f"{base}{rel}"


def _mail_context_provider(mail_token):
    if isinstance(mail_token, dict):
        provider = str(mail_token.get("provider", "") or "").strip().lower()
        if provider:
            return provider
    return "duckmail"


def _mail_context_value(mail_token, key: str, default=None):
    if isinstance(mail_token, dict):
        return mail_token.get(key, default)
    if key == "token":
        return mail_token
    return default


_mail_domain_cache = {}
_mail_domain_cache_lock = threading.Lock()


def _load_moemail_domains(session, api_base: str, api_key: str, impersonate: str):
    cache_key = str(api_base or "").rstrip("/")
    with _mail_domain_cache_lock:
        cached = _mail_domain_cache.get(cache_key)
        if cached:
            return list(cached)

    res = session.get(
        _mail_api_url(api_base, "/api/config"),
        headers={"X-API-Key": api_key},
        timeout=15,
        impersonate=impersonate,
    )
    if res.status_code != 200:
        raise Exception(f"获取 MoeMail 域名失败: {res.status_code} - {res.text[:200]}")

    data = res.json()
    raw_domains = data.get("emailDomains") or data.get("email_domains") or []
    if isinstance(raw_domains, str):
        domains = [item.strip() for item in raw_domains.split(",") if item.strip()]
    elif isinstance(raw_domains, list):
        domains = [str(item).strip() for item in raw_domains if str(item).strip()]
    else:
        domains = []

    if not domains:
        raise Exception("MoeMail 未返回可用邮箱域名")

    with _mail_domain_cache_lock:
        _mail_domain_cache[cache_key] = list(domains)
    return domains


def _create_temp_email_with_session(session, api_base: str, api_key: str, impersonate: str = "chrome131"):
    provider = _mail_provider_key(api_base)
    chars = string.ascii_lowercase + string.digits
    length = random.randint(8, 13)
    email_local = "".join(random.choice(chars) for _ in range(length))
    password = _generate_password()
    api_base = api_base.rstrip("/")

    if provider == "moemail":
        domains = _load_moemail_domains(session, api_base, api_key, impersonate)
        domain = domains[0]
        payload = {"name": email_local, "expiryTime": 86400000, "domain": domain}
        res = session.post(
            _mail_api_url(api_base, "/api/emails/generate"),
            json=payload,
            headers={"X-API-Key": api_key},
            timeout=15,
            impersonate=impersonate,
        )
        if res.status_code not in [200, 201]:
            raise Exception(f"创建邮箱失败: {res.status_code} - {res.text[:200]}")

        data = res.json()
        inner = data.get("data") or {}
        email_id = data.get("id") or data.get("emailId") or inner.get("id") or inner.get("emailId")
        email = data.get("email") or data.get("address") or inner.get("email") or inner.get("address")
        if not email_id or not email:
            raise Exception("MoeMail 创建邮箱响应缺少 id 或 email")

        return email, "<api-only>", {
            "provider": "moemail",
            "email": email,
            "email_id": email_id,
            "api_key": api_key,
            "domain": domain,
        }

    email = f"{email_local}@duckmail.sbs"
    headers = {"Authorization": f"Bearer {api_key}"}
    res = session.post(
        f"{api_base}/accounts",
        json={"address": email, "password": password},
        headers=headers,
        timeout=15,
        impersonate=impersonate,
    )
    if res.status_code not in [200, 201]:
        raise Exception(f"创建邮箱失败: {res.status_code} - {res.text[:200]}")

    time.sleep(0.5)
    token_res = session.post(
        f"{api_base}/token",
        json={"address": email, "password": password},
        timeout=15,
        impersonate=impersonate,
    )
    if token_res.status_code == 200:
        token_data = token_res.json()
        mailbox_token = token_data.get("token")
        if mailbox_token:
            return email, password, {"provider": "duckmail", "email": email, "token": mailbox_token}

    raise Exception(f"获取邮件 Token 失败: {token_res.status_code}")


def _fetch_mail_messages_with_session(session, api_base: str, api_key: str, mail_token, impersonate: str = "chrome131"):
    provider = _mail_context_provider(mail_token)
    api_base = api_base.rstrip("/")

    if provider == "moemail":
        email_id = str(_mail_context_value(mail_token, "email_id", "") or "").strip()
        scoped_key = str(_mail_context_value(mail_token, "api_key", api_key) or "").strip()
        if not email_id or not scoped_key:
            return []
        res = session.get(
            _mail_api_url(api_base, f"/api/emails/{email_id}"),
            headers={"X-API-Key": scoped_key},
            timeout=15,
            impersonate=impersonate,
        )
        if res.status_code == 200:
            data = res.json()
            inner = data.get("data") or {}
            return data.get("messages") or inner.get("messages") or data.get("items") or inner.get("items") or []
        return []

    mailbox_token = str(_mail_context_value(mail_token, "token", mail_token) or "").strip()
    if not mailbox_token:
        return []

    res = session.get(
        f"{api_base}/messages",
        headers={"Authorization": f"Bearer {mailbox_token}"},
        timeout=15,
        impersonate=impersonate,
    )
    if res.status_code == 200:
        data = res.json()
        return data.get("hydra:member") or data.get("member") or data.get("data") or []
    return []


def _fetch_mail_message_detail_with_session(session, api_base: str, api_key: str, mail_token, msg_id: str,
                                            impersonate: str = "chrome131"):
    provider = _mail_context_provider(mail_token)
    api_base = api_base.rstrip("/")

    if provider == "moemail":
        email_id = str(_mail_context_value(mail_token, "email_id", "") or "").strip()
        scoped_key = str(_mail_context_value(mail_token, "api_key", api_key) or "").strip()
        if not email_id or not scoped_key:
            return None
        res = session.get(
            _mail_api_url(api_base, f"/api/emails/{email_id}/{msg_id}"),
            headers={"X-API-Key": scoped_key},
            timeout=15,
            impersonate=impersonate,
        )
        if res.status_code == 200:
            data = res.json()
            inner = data.get("data") or {}
            return data.get("message") or inner.get("message") or data
        return None

    mailbox_token = str(_mail_context_value(mail_token, "token", mail_token) or "").strip()
    if not mailbox_token:
        return None

    if isinstance(msg_id, str) and msg_id.startswith("/messages/"):
        msg_id = msg_id.split("/")[-1]

    res = session.get(
        f"{api_base}/messages/{msg_id}",
        headers={"Authorization": f"Bearer {mailbox_token}"},
        timeout=15,
        impersonate=impersonate,
    )
    if res.status_code == 200:
        return res.json()
    return None


def create_temp_email():
    if not DUCKMAIL_BEARER:
        raise Exception("DUCKMAIL_BEARER 未设置，无法创建临时邮箱")
    session = _create_duckmail_session()
    try:
        return _create_temp_email_with_session(session, DUCKMAIL_API_BASE, DUCKMAIL_BEARER, "chrome131")
    except Exception as e:
        raise Exception(f"{MAIL_PROVIDER_LABEL} 创建邮箱失败: {e}")
    finally:
        _close_session_quietly(session)


def _fetch_emails_duckmail(mail_token: str):
    session = None
    try:
        session = _create_duckmail_session()
        return _fetch_mail_messages_with_session(session, DUCKMAIL_API_BASE, DUCKMAIL_BEARER, mail_token, "chrome131")
    except Exception:
        return []
    finally:
        _close_session_quietly(session)


def _fetch_email_detail_duckmail(mail_token: str, msg_id: str):
    session = None
    try:
        session = _create_duckmail_session()
        return _fetch_mail_message_detail_with_session(
            session, DUCKMAIL_API_BASE, DUCKMAIL_BEARER, mail_token, msg_id, "chrome131"
        )
    except Exception:
        pass
    finally:
        _close_session_quietly(session)
    return None


def _extract_verification_code(email_content: str):
    if not email_content:
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
        matches = re.findall(pattern, email_content, re.IGNORECASE)
        for code in matches:
            if code == "177010":
                continue
            return code
    return None


def wait_for_verification_email(mail_token: str, timeout: int = 120):
    start_time = time.time()
    while time.time() - start_time < timeout:
        messages = _fetch_emails_duckmail(mail_token)
        if messages:
            first_msg = messages[0]
            msg_id = first_msg.get("id") or first_msg.get("@id")
            if msg_id:
                detail = _fetch_email_detail_duckmail(mail_token, msg_id)
                if detail:
                    content = detail.get("text") or detail.get("content") or detail.get("html") or ""
                    code = _extract_verification_code(content)
                    if code:
                        return code
        time.sleep(3)
    return None


def _random_name():
    first = random.choice([
        "James", "Emma", "Liam", "Olivia", "Noah", "Ava", "Ethan", "Sophia",
        "Lucas", "Mia", "Mason", "Isabella", "Logan", "Charlotte", "Alexander",
        "Amelia", "Benjamin", "Harper", "William", "Evelyn", "Henry", "Abigail",
        "Sebastian", "Emily", "Jack", "Elizabeth",
    ])
    last = random.choice([
        "Smith", "Johnson", "Brown", "Davis", "Wilson", "Moore", "Taylor",
        "Clark", "Hall", "Young", "Anderson", "Thomas", "Jackson", "White",
        "Harris", "Martin", "Thompson", "Garcia", "Robinson", "Lewis",
        "Walker", "Allen", "King", "Wright", "Scott", "Green",
    ])
    return f"{first} {last}"


def _random_birthdate():
    y = random.randint(1985, 2002)
    m = random.randint(1, 12)
    d = random.randint(1, 28)
    return f"{y}-{m:02d}-{d:02d}"


# ================= ChatGPTRegister 主类 =================

class ChatGPTRegister:
    BASE = "https://chatgpt.com"
    AUTH = "https://auth.openai.com"

    def __init__(self, proxy: str = None, tag: str = "", fixed_proxy: str = None):
        self.tag = tag
        self.device_id = str(uuid.uuid4())
        self.auth_session_logging_id = str(uuid.uuid4())
        self.impersonate, self.chrome_major, self.chrome_full, self.ua, self.sec_ch_ua = _random_chrome_version()

        self.session = curl_requests.Session(impersonate=self.impersonate)
        self.proxy = _normalize_proxy(proxy)
        self.fixed_proxy = _normalize_proxy(fixed_proxy)
        _enable_proxy_rotation(
            self.session,
            fallback_proxy=self.proxy,
            fixed_proxy=self.fixed_proxy,
            default_timeout=DEFAULT_HTTP_TIMEOUT_SECONDS,
        )

        self.session.headers.update({
            "User-Agent": self.ua,
            "Accept-Language": random.choice([
                "en-US,en;q=0.9", "en-US,en;q=0.9,zh-CN;q=0.8",
                "en,en-US;q=0.9", "en-US,en;q=0.8",
            ]),
            "sec-ch-ua": self.sec_ch_ua, "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"', "sec-ch-ua-arch": '"x86"',
            "sec-ch-ua-bitness": '"64"',
            "sec-ch-ua-full-version": f'"{self.chrome_full}"',
            "sec-ch-ua-platform-version": f'"{random.randint(10, 15)}.0.0"',
        })
        self.session.cookies.set("oai-did", self.device_id, domain="chatgpt.com")
        self._callback_url = None
        self._mail_session = None

        # cfmail 状态（仅在使用 cfmail 时填充）
        self._cfmail_api_base = ""
        self._cfmail_account_name = ""
        self._cfmail_mail_token = ""

    def close(self):
        _close_session_quietly(self._mail_session)
        self._mail_session = None
        _close_session_quietly(self.session)

    def _log(self, step, method, url, status, body=None):
        prefix = f"[{self.tag}] " if self.tag else ""
        lines = [f"\n{'='*60}", f"{prefix}[Step] {step}",
                 f"{prefix}[{method}] {url}", f"{prefix}[Status] {status}"]
        if body:
            try:
                lines.append(f"{prefix}[Response] {json.dumps(body, indent=2, ensure_ascii=False)[:1000]}")
            except Exception:
                lines.append(f"{prefix}[Response] {str(body)[:1000]}")
        lines.append(f"{'='*60}")
        with _print_lock:
            print("\n".join(lines))

    def _print(self, msg):
        prefix = f"[{self.tag}] " if self.tag else ""
        with _print_lock:
            print(f"{prefix}{msg}")

    def _absolute_auth_url(self, url: str) -> str:
        value = str(url or "").strip()
        if not value:
            return ""
        if value.startswith("/"):
            return f"{self.AUTH}{value}"
        return value

    def _rewrite_signup_authorize_url(self, url: str) -> str:
        value = self._absolute_auth_url(url)
        if not value:
            return value
        parsed = urlparse(value)
        if parsed.netloc.lower() != "auth.openai.com":
            return value
        path = parsed.path.rstrip("/")
        if path in {"/oauth/authorize", "/authorize"}:
            rewritten = parsed._replace(path="/api/accounts/authorize").geturl()
            self._print(f"[AuthFlow] 重写注册入口: {path} -> /api/accounts/authorize")
            return rewritten
        return value

    def open_auth_page(self, url: str, step_name: str = "Open Auth Page"):
        target_url = self._absolute_auth_url(url)
        if not target_url:
            return None, None
        r = self.session.get(target_url, headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": f"{self.AUTH}/",
            "Upgrade-Insecure-Requests": "1",
        }, allow_redirects=True)
        final_url = str(r.url)
        self._log(step_name, "GET", target_url, r.status_code, {"final_url": final_url})
        return r.status_code, final_url

    def _auth_json_headers(self, referer: str):
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": self.AUTH,
            "Referer": referer,
            "User-Agent": self.ua,
            "oai-device-id": self.device_id,
        }
        headers.update(_make_trace_headers())
        return headers

    def _build_auth_sentinel_token(self, flow: str, step_name: str, fallback_flows=()):
        tried = []
        for candidate_flow in (flow, *tuple(fallback_flows or ())):
            candidate_flow = str(candidate_flow or "").strip()
            if not candidate_flow or candidate_flow in tried:
                continue
            tried.append(candidate_flow)
            token = build_sentinel_token(
                self.session,
                self.device_id,
                flow=candidate_flow,
                user_agent=self.ua,
                sec_ch_ua=self.sec_ch_ua,
                impersonate=self.impersonate,
            )
            if token:
                if candidate_flow != flow:
                    self._print(f"[Sentinel] {step_name} flow 回退到 {candidate_flow}")
                return token
        raise Exception(f"{step_name} 的 sentinel token 获取失败")

    # ==================== DuckMail ====================

    def _create_duckmail_session(self):
        if self._mail_session is None:
            session = curl_requests.Session()
            session.headers.update({
                "User-Agent": self.ua,
                "Accept": "application/json",
                "Content-Type": "application/json",
            })
            self._mail_session = _enable_proxy_rotation(
                session,
                fallback_proxy=self.proxy,
                fixed_proxy=self.fixed_proxy,
                default_timeout=DEFAULT_HTTP_TIMEOUT_SECONDS,
            )
        return self._mail_session

    def create_temp_email(self):
        if not DUCKMAIL_BEARER:
            raise Exception("DUCKMAIL_BEARER 未设置，无法创建临时邮箱")
        session = self._create_duckmail_session()
        try:
            return _create_temp_email_with_session(session, DUCKMAIL_API_BASE, DUCKMAIL_BEARER, self.impersonate)
        except Exception as e:
            raise Exception(f"{MAIL_PROVIDER_LABEL} 创建邮箱失败: {e}")

    def _fetch_emails_duckmail(self, mail_token: str):
        try:
            session = self._create_duckmail_session()
            return _fetch_mail_messages_with_session(
                session, DUCKMAIL_API_BASE, DUCKMAIL_BEARER, mail_token, self.impersonate
            )
        except Exception:
            return []

    def _fetch_email_detail_duckmail(self, mail_token: str, msg_id: str):
        try:
            session = self._create_duckmail_session()
            return _fetch_mail_message_detail_with_session(
                session, DUCKMAIL_API_BASE, DUCKMAIL_BEARER, mail_token, msg_id, self.impersonate
            )
        except Exception:
            pass
        return None

    def _extract_verification_code(self, email_content: str):
        if not email_content:
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
            matches = re.findall(pattern, email_content, re.IGNORECASE)
            for code in matches:
                if code == "177010":
                    continue
                return code
        return None

    # ==================== CF 自建邮箱 (cfmail) ====================

    def create_cfmail_email(self):
        """创建 CF 自建邮箱，返回 (email, password, mail_token)"""
        _reload_cfmail_accounts_if_needed()
        account = _select_cfmail_account(CFMAIL_PROFILE_MODE)
        if not account:
            raise Exception(
                f"没有可用的 cfmail 配置，请检查 {_CFMAIL_CONFIG_PATH}；"
                f"当前已加载配置数: {len(CFMAIL_ACCOUNTS)}"
            )

        proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None
        local = f"oc{secrets.token_hex(5)}"

        try:
            resp = curl_requests.post(
                f"https://{account.worker_domain}/admin/new_address",
                headers={
                    "x-admin-auth": account.admin_password,
                    **_cfmail_headers(use_json=True),
                },
                json={"enablePrefix": True, "name": local, "domain": account.email_domain},
                proxies=proxies,
                impersonate=self.impersonate,
                timeout=15,
            )
        except Exception as e:
            _record_cfmail_failure(account.name, f"new_address exception: {e}")
            raise Exception(f"cfmail 请求异常: {e}")

        if resp.status_code != 200:
            _record_cfmail_failure(account.name, f"new_address status={resp.status_code}")
            raise Exception(f"cfmail 创建失败: {resp.status_code} - {resp.text[:200]}")

        data = resp.json()
        email = str(data.get("address") or "").strip()
        jwt = str(data.get("jwt") or "").strip()

        if not email or not jwt:
            _record_cfmail_failure(account.name, "new_address incomplete data")
            raise Exception("cfmail 返回数据不完整（address 或 jwt 为空）")

        # 保存 cfmail 状态供后续轮询使用
        self._cfmail_api_base = f"https://{account.worker_domain}"
        self._cfmail_account_name = account.name
        self._cfmail_mail_token = jwt

        self._print(f"[cfmail] 创建邮箱成功: {email} (配置: {account.name})")
        # cfmail 没有独立密码概念，返回空串占位
        return email, "", jwt

    def _fetch_emails_cfmail(self, mail_token: str):
        """从 cfmail 拉取邮件列表"""
        if not self._cfmail_api_base:
            return []
        proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None
        try:
            resp = curl_requests.get(
                f"{self._cfmail_api_base}/api/mails",
                params={"limit": 10, "offset": 0},
                headers=_cfmail_headers(jwt=mail_token, use_json=True),
                proxies=proxies,
                impersonate=self.impersonate,
                timeout=15,
            )
            if resp.status_code != 200:
                return []
            data = resp.json() if resp.content else {}
            messages = data.get("results", []) if isinstance(data, dict) else []
            return messages if isinstance(messages, list) else []
        except Exception:
            return []

    def _extract_cfmail_code(self, messages: list, email: str) -> Optional[str]:
        """从 cfmail 邮件列表中提取验证码"""
        patterns = [
            r"Subject:\s*Your ChatGPT code is\s*(\d{6})",
            r"Your ChatGPT code is\s*(\d{6})",
            r"temporary verification code to continue:\s*(\d{6})",
            r"(?<![#&])\b(\d{6})\b",
        ]
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            recipient = str(msg.get("address") or "").strip().lower()
            if recipient and recipient != email.strip().lower():
                continue
            raw = str(msg.get("raw") or "")
            metadata = msg.get("metadata") or {}
            metadata_text = json.dumps(metadata, ensure_ascii=False)
            content = "\n".join([recipient, raw, metadata_text])
            if "openai" not in content.lower():
                continue
            for pattern in patterns:
                m = re.search(pattern, content, re.I | re.S)
                if m:
                    return m.group(1)
        return None

    def _collect_temp_mail_codes(self, mail_token, email: str = "",
                                 tried_codes: Optional[set] = None,
                                 seen_ids: Optional[set] = None) -> list:
        tried_codes = tried_codes or set()
        provider_key = _mail_context_provider(mail_token)
        messages = self._fetch_emails_duckmail(mail_token) or []
        candidate_codes = []

        for msg in messages[:12]:
            msg_id = str(
                msg.get("id") or msg.get("@id") or msg.get("messageId")
                or msg.get("_id") or msg.get("createdAt") or ""
            ).strip()
            if seen_ids is not None and msg_id:
                if msg_id in seen_ids:
                    continue
                seen_ids.add(msg_id)

            detail = msg
            if msg_id:
                fetched = self._fetch_email_detail_duckmail(mail_token, msg_id)
                if fetched:
                    detail = fetched

            if provider_key == "moemail":
                content = "\n".join([
                    str(detail.get("subject") or msg.get("subject") or ""),
                    str(detail.get("text") or detail.get("content") or detail.get("html") or detail.get("body") or ""),
                    json.dumps(detail, ensure_ascii=False),
                ])
                code = self._extract_verification_code(content)
                if code and code not in tried_codes:
                    candidate_codes.append(code)
                continue

            if not msg_id:
                continue
            content = detail.get("text") or detail.get("content") or detail.get("html") or ""
            code = self._extract_verification_code(content)
            if code and code not in tried_codes:
                candidate_codes.append(code)

        return candidate_codes

    # ==================== 统一等待验证码接口 ====================

    def wait_for_verification_email(self, mail_token: str, timeout: int = 120,
                                     email: str = "", provider: str = ""):
        """等待并提取 OpenAI 验证码，自动根据 provider 选择轮询方式"""
        effective_provider = provider or MAIL_PROVIDER
        self._print(f"[OTP] 等待验证码邮件 (最多 {timeout}s, provider={effective_provider})...")
        start_time = time.time()
        seen_ids: set = set()

        while time.time() - start_time < timeout:
            code = None

            if effective_provider == "cfmail":
                messages = self._fetch_emails_cfmail(mail_token)
                # 过滤已见过的消息
                new_messages = []
                for msg in messages:
                    msg_id = str(msg.get("id") or msg.get("createdAt") or "").strip()
                    if msg_id and msg_id not in seen_ids:
                        seen_ids.add(msg_id)
                        new_messages.append(msg)
                if new_messages:
                    code = self._extract_cfmail_code(new_messages, email)
            else:
                candidate_codes = self._collect_temp_mail_codes(
                    mail_token, email=email, tried_codes=set(), seen_ids=seen_ids
                )
                if candidate_codes:
                    code = candidate_codes[0]

            if code:
                self._print(f"[OTP] 验证码: {code}")
                return code

            elapsed = int(time.time() - start_time)
            self._print(f"[OTP] 等待中... ({elapsed}s/{timeout}s)")
            time.sleep(3)

        self._print(f"[OTP] 超时 ({timeout}s)")
        return None

    # ==================== 注册流程 ====================

    def visit_homepage(self):
        url = f"{self.BASE}/"
        r = self.session.get(url, headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Upgrade-Insecure-Requests": "1",
        }, allow_redirects=True)
        self._log("0. Visit homepage", "GET", url, r.status_code,
                   {"cookies_count": len(self.session.cookies)})

    def get_csrf(self) -> str:
        url = f"{self.BASE}/api/auth/csrf"
        r = self.session.get(url, headers={"Accept": "application/json", "Referer": f"{self.BASE}/"})
        data = r.json()
        token = data.get("csrfToken", "")
        self._log("1. Get CSRF", "GET", url, r.status_code, data)
        if not token:
            raise Exception("Failed to get CSRF token")
        return token

    def signin(self, email: str, csrf: str) -> str:
        url = f"{self.BASE}/api/auth/signin/openai"
        params = {
            "prompt": "login", "ext-oai-did": self.device_id,
            "auth_session_logging_id": self.auth_session_logging_id,
            "screen_hint": "login_or_signup", "login_hint": email,
        }
        form_data = {"callbackUrl": f"{self.BASE}/", "csrfToken": csrf, "json": "true"}
        r = self.session.post(url, params=params, data=form_data, headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json", "Referer": f"{self.BASE}/", "Origin": self.BASE,
        })
        data = r.json()
        authorize_url = data.get("url", "")
        self._log("2. Signin", "POST", url, r.status_code, data)
        if not authorize_url:
            raise Exception("Failed to get authorize URL")
        return self._rewrite_signup_authorize_url(authorize_url)

    def authorize(self, url: str) -> str:
        target_url = self._rewrite_signup_authorize_url(url)
        r = self.session.get(target_url, headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": f"{self.BASE}/", "Upgrade-Insecure-Requests": "1",
        }, allow_redirects=True)
        final_url = str(r.url)
        self._log("3. Authorize", "GET", target_url, r.status_code, {"final_url": final_url})
        return final_url

    def register(self, email: str, password: str):
        url = f"{self.AUTH}/api/accounts/user/register"
        headers = {"Content-Type": "application/json", "Accept": "application/json",
                    "Referer": f"{self.AUTH}/create-account/password", "Origin": self.AUTH}
        headers.update(_make_trace_headers())
        r = self.session.post(url, json={"username": email, "password": password}, headers=headers)
        try:
            data = r.json()
        except Exception:
            data = {"text": r.text[:500]}
        self._log("4. Register", "POST", url, r.status_code, data)
        return r.status_code, data

    def send_otp(self):
        url = f"{self.AUTH}/api/accounts/email-otp/send"
        r = self.session.get(url, headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": f"{self.AUTH}/create-account/password", "Upgrade-Insecure-Requests": "1",
        }, allow_redirects=True)
        try:
            data = r.json()
        except Exception:
            data = {"final_url": str(r.url), "status": r.status_code}
        self._log("5. Send OTP", "GET", url, r.status_code, data)
        return r.status_code, data

    def validate_otp(self, code: str):
        url = f"{self.AUTH}/api/accounts/email-otp/validate"
        headers = {"Content-Type": "application/json", "Accept": "application/json",
                    "Referer": f"{self.AUTH}/email-verification", "Origin": self.AUTH}
        headers.update(_make_trace_headers())
        r = self.session.post(url, json={"code": code}, headers=headers)
        try:
            data = r.json()
        except Exception:
            data = {"text": r.text[:500]}
        self._log("6. Validate OTP", "POST", url, r.status_code, data)
        return r.status_code, data

    def create_account(self, name: str, birthdate: str):
        url = f"{self.AUTH}/api/accounts/create_account"
        headers = self._auth_json_headers(f"{self.AUTH}/about-you")
        headers["openai-sentinel-token"] = self._build_auth_sentinel_token(
            "create_account",
            "create_account",
            fallback_flows=("authorize_continue",),
        )
        r = self.session.post(url, json={"name": name, "birthdate": birthdate}, headers=headers)
        try:
            data = r.json()
        except Exception:
            data = {"text": r.text[:500]}
        self._log("7. Create Account", "POST", url, r.status_code, data)
        if isinstance(data, dict):
            cb = data.get("continue_url") or data.get("url") or data.get("redirect_url")
            if cb:
                self._callback_url = cb
        return r.status_code, data

    def callback(self, url: str = None):
        if not url:
            url = self._callback_url
        if not url:
            self._print("[!] No callback URL, skipping.")
            return None, None
        r = self.session.get(url, headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Upgrade-Insecure-Requests": "1",
        }, allow_redirects=True)
        self._log("8. Callback", "GET", url, r.status_code, {"final_url": str(r.url)})
        return r.status_code, {"final_url": str(r.url)}

    # ==================== 自动注册主流程 ====================

    def run_register(self, email, password, name, birthdate, mail_token, provider="duckmail"):
        """注册流程，provider 决定验证码收取方式"""
        self.visit_homepage()
        _random_delay(0.3, 0.8)
        csrf = self.get_csrf()
        _random_delay(0.2, 0.5)
        auth_url = self.signin(email, csrf)
        _random_delay(0.3, 0.8)
        final_url = self.authorize(auth_url)
        final_path = urlparse(final_url).path
        _random_delay(0.3, 0.8)
        self._print(f"Authorize → {final_path}")

        need_otp = False

        if "create-account/password" in final_path:
            self._print("全新注册流程")
            _random_delay(0.5, 1.0)
            status, data = self.register(email, password)
            if status != 200:
                raise Exception(f"Register 失败 ({status}): {data}")
            _random_delay(0.3, 0.8)
            self.send_otp()
            need_otp = True
        elif "email-verification" in final_path or "email-otp" in final_path:
            self._print("跳到 OTP 验证阶段")
            need_otp = True
        elif "about-you" in final_path:
            self._print("跳到填写信息阶段")
            _random_delay(0.5, 1.0)
            self.create_account(name, birthdate)
            _random_delay(0.3, 0.5)
            self.callback()
            return True
        elif "callback" in final_path or "chatgpt.com" in final_url:
            self._print("账号已完成注册")
            return True
        else:
            self._print(f"未知跳转: {final_url}")
            self.register(email, password)
            self.send_otp()
            need_otp = True

        if need_otp:
            otp_code = self.wait_for_verification_email(
                mail_token, timeout=120, email=email, provider=provider
            )
            if not otp_code:
                raise Exception("未能获取验证码")

            _random_delay(0.3, 0.8)
            status, data = self.validate_otp(otp_code)
            if status != 200:
                self._print("验证码失败，重试...")
                self.send_otp()
                _random_delay(1.0, 2.0)
                otp_code = self.wait_for_verification_email(
                    mail_token, timeout=60, email=email, provider=provider
                )
                if not otp_code:
                    raise Exception("重试后仍未获取验证码")
                _random_delay(0.3, 0.8)
                status, data = self.validate_otp(otp_code)
                if status != 200:
                    raise Exception(f"验证码失败 ({status}): {data}")

            # cfmail 成功标记
            if provider == "cfmail" and self._cfmail_account_name:
                _record_cfmail_success(self._cfmail_account_name)

            if isinstance(data, dict):
                next_url = data.get("continue_url") or data.get("url") or ""
                if next_url:
                    self._print("[AuthFlow] OTP 通过，先进入 about-you 页面再提交资料")
                    self.open_auth_page(next_url, step_name="6.5 Open About You")
                else:
                    self.open_auth_page(f"{self.AUTH}/about-you", step_name="6.5 Open About You")

        _random_delay(0.5, 1.5)
        status, data = self.create_account(name, birthdate)
        if status == 400 and isinstance(data, dict):
            err = data.get("error") or {}
            if str(err.get("code") or "").strip().lower() == "registration_disallowed":
                self._print("[AuthFlow] create_account 返回 registration_disallowed，补开 about-you 后重试一次")
                self.open_auth_page(f"{self.AUTH}/about-you", step_name="7.1 Reopen About You")
                _random_delay(0.4, 1.0)
                status, data = self.create_account(name, birthdate)
        if status != 200:
            raise Exception(f"Create account 失败 ({status}): {data}")
        _random_delay(0.2, 0.5)
        self.callback()
        return True

    def _decode_oauth_session_cookie(self):
        jar = getattr(self.session.cookies, "jar", None)
        if jar is not None:
            cookie_items = list(jar)
        else:
            cookie_items = []

        for c in cookie_items:
            name = getattr(c, "name", "") or ""
            if "oai-client-auth-session" not in name:
                continue
            raw_val = (getattr(c, "value", "") or "").strip()
            if not raw_val:
                continue
            candidates = [raw_val]
            try:
                from urllib.parse import unquote
                decoded = unquote(raw_val)
                if decoded != raw_val:
                    candidates.append(decoded)
            except Exception:
                pass
            for val in candidates:
                try:
                    if (val.startswith('"') and val.endswith('"')) or \
                       (val.startswith("'") and val.endswith("'")):
                        val = val[1:-1]
                    part = val.split(".")[0] if "." in val else val
                    pad = 4 - len(part) % 4
                    if pad != 4:
                        part += "=" * pad
                    raw = base64.urlsafe_b64decode(part)
                    data = json.loads(raw.decode("utf-8"))
                    if isinstance(data, dict):
                        return data
                except Exception:
                    continue
        return None

    def _oauth_allow_redirect_extract_code(self, url: str, referer: str = None):
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Upgrade-Insecure-Requests": "1", "User-Agent": self.ua,
        }
        if referer:
            headers["Referer"] = referer
        try:
            resp = self.session.get(url, headers=headers, allow_redirects=True,
                                    timeout=30, impersonate=self.impersonate)
            final_url = str(resp.url)
            code = _extract_code_from_url(final_url)
            if code:
                return code
            for r in getattr(resp, "history", []) or []:
                loc = r.headers.get("Location", "")
                code = _extract_code_from_url(loc)
                if code:
                    return code
                code = _extract_code_from_url(str(r.url))
                if code:
                    return code
        except Exception as e:
            maybe_localhost = re.search(r'(https?://localhost[^\s\'\"]+)', str(e))
            if maybe_localhost:
                code = _extract_code_from_url(maybe_localhost.group(1))
                if code:
                    return code
        return None

    def _oauth_follow_for_code(self, start_url: str, referer: str = None, max_hops: int = 16):
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Upgrade-Insecure-Requests": "1", "User-Agent": self.ua,
        }
        if referer:
            headers["Referer"] = referer
        current_url = start_url
        last_url = start_url
        for hop in range(max_hops):
            try:
                resp = self.session.get(current_url, headers=headers, allow_redirects=False,
                                        timeout=30, impersonate=self.impersonate)
            except Exception as e:
                maybe_localhost = re.search(r'(https?://localhost[^\s\'\"]+)', str(e))
                if maybe_localhost:
                    code = _extract_code_from_url(maybe_localhost.group(1))
                    if code:
                        return code, maybe_localhost.group(1)
                return None, last_url
            last_url = str(resp.url)
            code = _extract_code_from_url(last_url)
            if code:
                return code, last_url
            if resp.status_code in (301, 302, 303, 307, 308):
                loc = resp.headers.get("Location", "")
                if not loc:
                    return None, last_url
                if loc.startswith("/"):
                    loc = f"{OAUTH_ISSUER}{loc}"
                code = _extract_code_from_url(loc)
                if code:
                    return code, loc
                current_url = loc
                headers["Referer"] = last_url
                continue
            return None, last_url
        return None, last_url

    def _oauth_submit_workspace_and_org(self, consent_url: str):
        session_data = self._decode_oauth_session_cookie()
        if not session_data:
            return None
        workspaces = session_data.get("workspaces", [])
        if not workspaces:
            return None
        workspace_id = (workspaces[0] or {}).get("id")
        if not workspace_id:
            return None

        h = {
            "Accept": "application/json", "Content-Type": "application/json",
            "Origin": OAUTH_ISSUER, "Referer": consent_url,
            "User-Agent": self.ua, "oai-device-id": self.device_id,
        }
        h.update(_make_trace_headers())

        resp = self.session.post(
            f"{OAUTH_ISSUER}/api/accounts/workspace/select",
            json={"workspace_id": workspace_id}, headers=h,
            allow_redirects=False, timeout=30, impersonate=self.impersonate,
        )
        self._print(f"[OAuth] workspace/select -> {resp.status_code}")

        if resp.status_code in (301, 302, 303, 307, 308):
            loc = resp.headers.get("Location", "")
            if loc.startswith("/"):
                loc = f"{OAUTH_ISSUER}{loc}"
            code = _extract_code_from_url(loc)
            if code:
                return code
            code, _ = self._oauth_follow_for_code(loc, referer=consent_url)
            if not code:
                code = self._oauth_allow_redirect_extract_code(loc, referer=consent_url)
            return code

        if resp.status_code != 200:
            return None

        try:
            ws_data = resp.json()
        except Exception:
            return None

        ws_next = ws_data.get("continue_url", "")
        orgs = ws_data.get("data", {}).get("orgs", [])

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
            h_org = dict(h)
            if ws_next:
                h_org["Referer"] = ws_next if ws_next.startswith("http") else f"{OAUTH_ISSUER}{ws_next}"
            resp_org = self.session.post(
                f"{OAUTH_ISSUER}/api/accounts/organization/select",
                json=org_body, headers=h_org, allow_redirects=False,
                timeout=30, impersonate=self.impersonate,
            )
            self._print(f"[OAuth] organization/select -> {resp_org.status_code}")
            if resp_org.status_code in (301, 302, 303, 307, 308):
                loc = resp_org.headers.get("Location", "")
                if loc.startswith("/"):
                    loc = f"{OAUTH_ISSUER}{loc}"
                code = _extract_code_from_url(loc)
                if code:
                    return code
                code, _ = self._oauth_follow_for_code(loc, referer=h_org.get("Referer"))
                if not code:
                    code = self._oauth_allow_redirect_extract_code(loc, referer=h_org.get("Referer"))
                return code
            if resp_org.status_code == 200:
                try:
                    org_data = resp_org.json()
                except Exception:
                    return None
                org_next = org_data.get("continue_url", "")
                if org_next:
                    if org_next.startswith("/"):
                        org_next = f"{OAUTH_ISSUER}{org_next}"
                    code, _ = self._oauth_follow_for_code(org_next, referer=h_org.get("Referer"))
                    if not code:
                        code = self._oauth_allow_redirect_extract_code(org_next, referer=h_org.get("Referer"))
                    return code

        if ws_next:
            if ws_next.startswith("/"):
                ws_next = f"{OAUTH_ISSUER}{ws_next}"
            code, _ = self._oauth_follow_for_code(ws_next, referer=consent_url)
            if not code:
                code = self._oauth_allow_redirect_extract_code(ws_next, referer=consent_url)
            return code

        return None

    def perform_codex_oauth_login_http(self, email: str, password: str, mail_token: str = None,
                                        provider: str = "duckmail"):
        self._print("[OAuth] 开始执行 Codex OAuth 纯协议流程...")
        self.session.cookies.set("oai-did", self.device_id, domain=".auth.openai.com")
        self.session.cookies.set("oai-did", self.device_id, domain="auth.openai.com")

        code_verifier, code_challenge = _generate_pkce()
        state = secrets.token_urlsafe(24)

        authorize_params = {
            "response_type": "code", "client_id": OAUTH_CLIENT_ID,
            "redirect_uri": OAUTH_REDIRECT_URI,
            "scope": "openid profile email offline_access",
            "code_challenge": code_challenge, "code_challenge_method": "S256",
            "state": state,
        }
        authorize_url = f"{OAUTH_ISSUER}/oauth/authorize?{urlencode(authorize_params)}"

        def _oauth_json_headers(referer: str):
            h = {
                "Accept": "application/json", "Content-Type": "application/json",
                "Origin": OAUTH_ISSUER, "Referer": referer,
                "User-Agent": self.ua, "oai-device-id": self.device_id,
            }
            h.update(_make_trace_headers())
            return h

        def _bootstrap_oauth_session():
            self._print("[OAuth] 1/7 GET /oauth/authorize")
            try:
                r = self.session.get(authorize_url, headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Referer": f"{self.BASE}/", "Upgrade-Insecure-Requests": "1",
                    "User-Agent": self.ua,
                }, allow_redirects=True, timeout=30, impersonate=self.impersonate)
            except Exception as e:
                self._print(f"[OAuth] /oauth/authorize 异常: {e}")
                return False, ""

            final_url = str(r.url)
            has_login = any(getattr(c, "name", "") == "login_session" for c in self.session.cookies)

            if not has_login:
                oauth2_url = f"{OAUTH_ISSUER}/api/oauth/oauth2/auth"
                try:
                    r2 = self.session.get(oauth2_url, headers={
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Referer": authorize_url, "Upgrade-Insecure-Requests": "1",
                        "User-Agent": self.ua,
                    }, params=authorize_params, allow_redirects=True, timeout=30,
                        impersonate=self.impersonate)
                    final_url = str(r2.url)
                except Exception as e:
                    self._print(f"[OAuth] /api/oauth/oauth2/auth 异常: {e}")
                has_login = any(getattr(c, "name", "") == "login_session" for c in self.session.cookies)

            return has_login, final_url

        def _post_authorize_continue(referer_url: str):
            sentinel_authorize = build_sentinel_token(
                self.session, self.device_id, flow="authorize_continue",
                user_agent=self.ua, sec_ch_ua=self.sec_ch_ua, impersonate=self.impersonate,
            )
            if not sentinel_authorize:
                return None
            headers_continue = _oauth_json_headers(referer_url)
            headers_continue["openai-sentinel-token"] = sentinel_authorize
            try:
                return self.session.post(
                    f"{OAUTH_ISSUER}/api/accounts/authorize/continue",
                    json={"username": {"kind": "email", "value": email}},
                    headers=headers_continue, timeout=30,
                    allow_redirects=False, impersonate=self.impersonate,
                )
            except Exception as e:
                self._print(f"[OAuth] authorize/continue 异常: {e}")
                return None

        has_login_session, authorize_final_url = _bootstrap_oauth_session()
        if not authorize_final_url:
            return None

        continue_referer = (authorize_final_url if authorize_final_url.startswith(OAUTH_ISSUER)
                            else f"{OAUTH_ISSUER}/log-in")

        self._print("[OAuth] 2/7 POST /api/accounts/authorize/continue")
        resp_continue = _post_authorize_continue(continue_referer)
        if resp_continue is None:
            return None

        self._print(f"[OAuth] /authorize/continue -> {resp_continue.status_code}")
        if resp_continue.status_code == 400 and "invalid_auth_step" in (resp_continue.text or ""):
            has_login_session, authorize_final_url = _bootstrap_oauth_session()
            if not authorize_final_url:
                return None
            continue_referer = (authorize_final_url if authorize_final_url.startswith(OAUTH_ISSUER)
                                else f"{OAUTH_ISSUER}/log-in")
            resp_continue = _post_authorize_continue(continue_referer)
            if resp_continue is None:
                return None

        if resp_continue.status_code != 200:
            return None

        try:
            continue_data = resp_continue.json()
        except Exception:
            return None

        continue_url = continue_data.get("continue_url", "")
        page_type = (continue_data.get("page") or {}).get("type", "")

        self._print("[OAuth] 3/7 POST /api/accounts/password/verify")
        sentinel_pwd = build_sentinel_token(
            self.session, self.device_id, flow="password_verify",
            user_agent=self.ua, sec_ch_ua=self.sec_ch_ua, impersonate=self.impersonate,
        )
        if not sentinel_pwd:
            return None

        headers_verify = _oauth_json_headers(f"{OAUTH_ISSUER}/log-in/password")
        headers_verify["openai-sentinel-token"] = sentinel_pwd

        try:
            resp_verify = self.session.post(
                f"{OAUTH_ISSUER}/api/accounts/password/verify",
                json={"password": password}, headers=headers_verify,
                timeout=30, allow_redirects=False, impersonate=self.impersonate,
            )
        except Exception as e:
            self._print(f"[OAuth] password/verify 异常: {e}")
            return None

        if resp_verify.status_code != 200:
            return None

        try:
            verify_data = resp_verify.json()
        except Exception:
            return None

        continue_url = verify_data.get("continue_url", "") or continue_url
        page_type = (verify_data.get("page") or {}).get("type", "") or page_type

        need_oauth_otp = (
            page_type == "email_otp_verification"
            or "email-verification" in (continue_url or "")
            or "email-otp" in (continue_url or "")
        )

        if need_oauth_otp:
            self._print("[OAuth] 4/7 检测到邮箱 OTP 验证")
            if not mail_token:
                self._print("[OAuth] OAuth 阶段需要邮箱 OTP，但未提供 mail_token")
                return None

            headers_otp = _oauth_json_headers(f"{OAUTH_ISSUER}/email-verification")
            tried_codes: set = set()
            otp_success = False
            otp_deadline = time.time() + 120

            while time.time() < otp_deadline and not otp_success:
                # 根据 provider 选择邮件拉取方式
                candidate_codes = []
                if provider == "cfmail":
                    messages = self._fetch_emails_cfmail(mail_token)
                    for msg in messages[:12]:
                        msg_id = str(msg.get("id") or msg.get("createdAt") or "").strip()
                        if not msg_id:
                            continue
                        recipient = str(msg.get("address") or "").strip().lower()
                        if recipient and recipient != email.strip().lower():
                            continue
                        raw = str(msg.get("raw") or "")
                        metadata = msg.get("metadata") or {}
                        content = "\n".join([recipient, raw, json.dumps(metadata, ensure_ascii=False)])
                        if "openai" not in content.lower():
                            continue
                        for pattern in [
                            r"Subject:\s*Your ChatGPT code is\s*(\d{6})",
                            r"Your ChatGPT code is\s*(\d{6})",
                            r"temporary verification code to continue:\s*(\d{6})",
                            r"(?<![#&])\b(\d{6})\b",
                        ]:
                            m = re.search(pattern, content, re.I | re.S)
                            if m and m.group(1) not in tried_codes:
                                candidate_codes.append(m.group(1))
                                break
                else:
                    candidate_codes = self._collect_temp_mail_codes(
                        mail_token, email=email, tried_codes=tried_codes
                    )

                if not candidate_codes:
                    elapsed = int(120 - max(0, otp_deadline - time.time()))
                    self._print(f"[OAuth] OTP 等待中... ({elapsed}s/120s)")
                    time.sleep(2)
                    continue

                for otp_code in candidate_codes:
                    tried_codes.add(otp_code)
                    self._print(f"[OAuth] 尝试 OTP: {otp_code}")
                    try:
                        resp_otp = self.session.post(
                            f"{OAUTH_ISSUER}/api/accounts/email-otp/validate",
                            json={"code": otp_code}, headers=headers_otp,
                            timeout=30, allow_redirects=False, impersonate=self.impersonate,
                        )
                    except Exception as e:
                        self._print(f"[OAuth] email-otp/validate 异常: {e}")
                        continue
                    if resp_otp.status_code != 200:
                        continue
                    try:
                        otp_data = resp_otp.json()
                    except Exception:
                        continue
                    continue_url = otp_data.get("continue_url", "") or continue_url
                    page_type = (otp_data.get("page") or {}).get("type", "") or page_type
                    otp_success = True
                    break

                if not otp_success:
                    time.sleep(2)

            if not otp_success:
                self._print(f"[OAuth] OAuth 阶段 OTP 验证失败")
                return None

        code = None
        consent_url = continue_url
        if consent_url and consent_url.startswith("/"):
            consent_url = f"{OAUTH_ISSUER}{consent_url}"
        if not consent_url and "consent" in page_type:
            consent_url = f"{OAUTH_ISSUER}/sign-in-with-chatgpt/codex/consent"
        if consent_url:
            code = _extract_code_from_url(consent_url)

        if not code and consent_url:
            self._print("[OAuth] 5/7 跟随 continue_url 提取 code")
            code, _ = self._oauth_follow_for_code(consent_url, referer=f"{OAUTH_ISSUER}/log-in/password")

        consent_hint = (
                ("consent" in (consent_url or ""))
                or ("sign-in-with-chatgpt" in (consent_url or ""))
                or ("workspace" in (consent_url or ""))
                or ("organization" in (consent_url or ""))
                or ("consent" in page_type)
                or ("organization" in page_type)
        )

        if not code and consent_hint:
            if not consent_url:
                consent_url = f"{OAUTH_ISSUER}/sign-in-with-chatgpt/codex/consent"
            self._print("[OAuth] 6/7 执行 workspace/org 选择")
            code = self._oauth_submit_workspace_and_org(consent_url)

        if not code:
            fallback_consent = f"{OAUTH_ISSUER}/sign-in-with-chatgpt/codex/consent"
            self._print("[OAuth] 6/7 回退 consent 路径重试")
            code = self._oauth_submit_workspace_and_org(fallback_consent)
            if not code:
                code, _ = self._oauth_follow_for_code(fallback_consent, referer=f"{OAUTH_ISSUER}/log-in/password")

        if not code:
            self._print("[OAuth] 未获取到 authorization code")
            return None

        self._print("[OAuth] 7/7 POST /oauth/token")
        token_resp = self.session.post(
            f"{OAUTH_ISSUER}/oauth/token",
            headers={"Content-Type": "application/x-www-form-urlencoded", "User-Agent": self.ua},
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": OAUTH_REDIRECT_URI,
                "client_id": OAUTH_CLIENT_ID,
                "code_verifier": code_verifier,
            },
            timeout=60,
            impersonate=self.impersonate,
        )
        self._print(f"[OAuth] /oauth/token -> {token_resp.status_code}")

        if token_resp.status_code != 200:
            self._print(f"[OAuth] token 交换失败: {token_resp.status_code} {token_resp.text[:200]}")
            return None

        try:
            data = token_resp.json()
        except Exception:
            self._print("[OAuth] token 响应解析失败")
            return None

        if not data.get("access_token"):
            self._print("[OAuth] token 响应缺少 access_token")
            return None

        self._print("[OAuth] Codex Token 获取成功")
        return data

    # ==================== 并发批量注册 ====================

def _register_one(idx, total, proxy, output_file):
    pool = _get_proxy_pool(fallback_proxy=proxy)
    provider = MAIL_PROVIDER
    last_error = "unknown error"

    for attempt in range(1, PROXY_RETRY_ATTEMPTS_PER_ACCOUNT + 1):
        if _stop_event.is_set():
            return False, None, "已手动停止"

        reg = None
        current_proxy = pool.next_proxy()
        proxy_label = current_proxy or "direct"

        try:
            reg = ChatGPTRegister(
                proxy=current_proxy,
                fixed_proxy=current_proxy,
                tag=f"{idx}-try{attempt}",
            )
            reg._print(f"[Proxy] 尝试 {attempt}/{PROXY_RETRY_ATTEMPTS_PER_ACCOUNT}: {proxy_label}")

            if provider == "cfmail":
                reg._print("[cfmail] 创建 CF 自建邮箱...")
                email, email_pwd, mail_token = reg.create_cfmail_email()
            else:
                reg._print(f"[{MAIL_PROVIDER_LABEL}] 创建临时邮箱...")
                if not DUCKMAIL_BEARER:
                    raise Exception("DUCKMAIL_BEARER 未设置，请在 config.json 中配置或改用 cfmail")
                email, email_pwd, mail_token = reg.create_temp_email()

            tag = email.split("@")[0]
            reg.tag = tag

            chatgpt_password = _generate_password()
            name = _random_name()
            birthdate = _random_birthdate()

            with _print_lock:
                print(f"\n{'=' * 60}")
                print(f"  [{idx}/{total}] 注册: {email}")
                print(f"  邮箱服务: {provider}")
                print(f"  ChatGPT密码: {chatgpt_password}")
                if email_pwd:
                    print(f"  邮箱密码: {email_pwd}")
                print(f"  姓名: {name} | 生日: {birthdate}")
                print(f"  代理: {proxy_label}")
                print(f"{'=' * 60}")

            reg.run_register(email, chatgpt_password, name, birthdate, mail_token, provider=provider)

            oauth_ok = True
            if ENABLE_OAUTH:
                reg._print("[OAuth] 开始获取 Codex Token...")
                tokens = reg.perform_codex_oauth_login_http(
                    email, chatgpt_password, mail_token=mail_token, provider=provider
                )
                oauth_ok = bool(tokens and tokens.get("access_token"))
                if oauth_ok:
                    _save_codex_tokens(email, tokens)
                    reg._print("[OAuth] Token 已保存")
                    if AUTO_UPLOAD_SUB2API and SUB2API_BASE_URL and tokens.get("refresh_token"):
                        reg._print("[Sub2Api] 正在上传账号...")
                        _push_account_to_sub2api(email, tokens, preferred_proxy=current_proxy)
                else:
                    msg = "OAuth 获取失败"
                    if OAUTH_REQUIRED:
                        raise Exception(f"{msg}（oauth_required=true）")
                    reg._print(f"[OAuth] {msg}（按配置继续）")

            if current_proxy:
                pool.report_success(current_proxy)
                _save_stable_proxy_to_file(current_proxy)
                _save_stable_proxy_to_config(current_proxy)

            with _file_lock:
                with open(output_file, "a", encoding="utf-8") as out:
                    line = f"{email}----{chatgpt_password}"
                    if email_pwd:
                        line += f"----{email_pwd}"
                    line += f"----oauth={'ok' if oauth_ok else 'fail'}----proxy={proxy_label}\n"
                    out.write(line)

            with _print_lock:
                print(f"\n[OK] [{tag}] {email} 注册成功! 代理: {proxy_label}")
            return True, email, None

        except Exception as e:
            last_error = str(e)
            if current_proxy:
                pool.report_bad(current_proxy, error=e)
            with _print_lock:
                print(
                    f"\n[FAIL] [{idx}] 尝试 {attempt}/{PROXY_RETRY_ATTEMPTS_PER_ACCOUNT} "
                    f"失败: {last_error} | 代理: {proxy_label}"
                )
                traceback.print_exc()
            if attempt >= PROXY_RETRY_ATTEMPTS_PER_ACCOUNT:
                return False, None, last_error
            time.sleep(min(3 + attempt, 8))
        finally:
            if reg is not None:
                reg.close()

    return False, None, last_error

def run_batch(total_accounts: int = 3, output_file="registered_accounts.txt",
              max_workers=3, proxy=None, cpa_cleanup=None):
    """并发批量注册"""
    provider = MAIL_PROVIDER
    _stop_event.clear()

    # 检查邮箱服务配置
    if provider == "cfmail":
        if not CFMAIL_ACCOUNTS:
            print(f"❌ 错误: mail_provider=cfmail 但未找到可用的 cfmail 配置")
            print(f"   请检查配置文件: {_CFMAIL_CONFIG_PATH}")
            return
    else:
        if not DUCKMAIL_BEARER:
            print(f"❌ 错误: mail_provider={provider} 但未设置 DUCKMAIL_BEARER")
            print("   请在 config.json 中设置 duckmail_bearer，或将 mail_provider 改为 cfmail")
            return

    actual_workers = min(max_workers, total_accounts)
    print(f"\n{'#' * 60}")
    print(f"  ChatGPT 批量自动注册 ({'cfmail' if provider == 'cfmail' else MAIL_PROVIDER_LABEL})")
    print(f"  运行模式: {MODE}")
    print(f"  注册数量: {total_accounts} | 并发数: {actual_workers}")
    print(f"  邮箱服务: {provider}")
    if provider == "cfmail":
        cfmail_names = ", ".join(a.name for a in CFMAIL_ACCOUNTS)
        print(f"  cfmail 配置: {cfmail_names}")
        print(f"  cfmail 模式: {CFMAIL_PROFILE_MODE}")
    else:
        print(f"  {MAIL_PROVIDER_LABEL}: {DUCKMAIL_API_BASE}")
    print(f"  代理池: {PROXY_LIST_URL or '关闭'}")
    print(f"  账号级代理重试: {PROXY_RETRY_ATTEMPTS_PER_ACCOUNT} 次/账号")
    print(f"  OAuth: {'开启' if ENABLE_OAUTH else '关闭'} | required: {'是' if OAUTH_REQUIRED else '否'}")
    if ENABLE_OAUTH:
        print(f"  Token输出: {TOKEN_JSON_DIR}/, {AK_FILE}, {RK_FILE}")
    print(f"  输出文件: {output_file}")
    print(f"{'#' * 60}\n")

    # 注册前清理 CPA 无效号
    do_cleanup = cpa_cleanup if cpa_cleanup is not None else CPA_CLEANUP_ENABLED
    if do_cleanup and UPLOAD_API_URL:
        _run_cpa_cleanup_before_register()

    success_count = 0
    fail_count = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=actual_workers) as executor:
        futures = {}
        for idx in range(1, total_accounts + 1):
            future = executor.submit(_register_one, idx, total_accounts, proxy, output_file)
            futures[future] = idx

        for future in as_completed(futures):
            idx = futures[future]
            try:
                ok, email, err = future.result()
                if ok:
                    success_count += 1
                else:
                    fail_count += 1
                    print(f"  [账号 {idx}] 失败: {err}")
            except Exception as e:
                fail_count += 1
                with _print_lock:
                    print(f"[FAIL] 账号 {idx} 线程异常: {e}")

    elapsed = time.time() - start_time
    avg = elapsed / total_accounts if total_accounts else 0
    print(f"\n{'#' * 60}")
    print(f"  注册完成! 耗时 {elapsed:.1f} 秒")
    print(f"  总数: {total_accounts} | 成功: {success_count} | 失败: {fail_count}")
    print(f"  平均速度: {avg:.1f} 秒/个")
    if success_count > 0:
        print(f"  结果文件: {output_file}")
    print(f"{'#' * 60}")

    if success_count > 0 and AUTO_UPLOAD_CPA:
        _upload_all_tokens_to_cpa()

def main():
    print("=" * 60)
    print("  ChatGPT 批量自动注册工具")
    print(f"  运行模式: {MODE}")
    print(f"  邮箱服务: {MAIL_PROVIDER}")
    if MAIL_PROVIDER != "cfmail":
        print(f"  临时邮箱后端: {MAIL_PROVIDER_LABEL} ({DUCKMAIL_API_BASE})")
    print(f"  代理池: {PROXY_LIST_URL or '关闭'}")
    print("=" * 60)

    provider = MAIL_PROVIDER

    # 检查配置
    if provider == "cfmail":
        if not CFMAIL_ACCOUNTS:
            print(f"\n⚠️  警告: mail_provider=cfmail 但未找到可用配置")
            print(f"   配置文件: {_CFMAIL_CONFIG_PATH}")
            print(f"   请参考 zhuce5_cfmail_accounts.json 格式补充配置")
            print("\n   按 Enter 继续尝试运行 (可能会失败)...")
            input()
        else:
            cfmail_names = ", ".join(a.name for a in CFMAIL_ACCOUNTS)
            print(f"\n[Info] cfmail 配置已加载: {cfmail_names}")
    else:
        if not DUCKMAIL_BEARER:
            print("\n⚠️  警告: 未设置 DUCKMAIL_BEARER")
            print("   请编辑 config.json 设置 duckmail_bearer，或将 mail_provider 改为 cfmail")
            print("\n   按 Enter 继续尝试运行 (可能会失败)...")
            input()

    # 代理配置
    proxy = DEFAULT_PROXY
    if PROXY_LIST_URL:
        print(f"[Info] 已启用代理池: {PROXY_LIST_URL}")
        if PROXY_LIST_BEARER:
            print("[Info] 已配置代理池 Bearer Token")
        print(f"[Info] 账号失败自动换代理重试: {PROXY_RETRY_ATTEMPTS_PER_ACCOUNT} 次")
    if proxy:
        print(f"[Info] 检测到默认代理: {proxy}")
        use_default = input("使用此代理? (Y/n): ").strip().lower()
        if use_default == "n":
            proxy = input("输入代理地址 (留空=不使用代理): ").strip() or None
    else:
        env_proxy = (os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
                     or os.environ.get("ALL_PROXY") or os.environ.get("all_proxy"))
        if env_proxy:
            print(f"[Info] 检测到环境变量代理: {env_proxy}")
            use_env = input("使用此代理? (Y/n): ").strip().lower()
            proxy = None if use_env == "n" else env_proxy
            if use_env == "n":
                proxy = input("输入代理地址 (留空=不使用代理): ").strip() or None
        else:
            proxy = input("输入代理地址 (如 http://127.0.0.1:7890，留空=不使用代理): ").strip() or None

    print(f"[Info] {'使用代理: ' + proxy if proxy else '不使用代理'}")

    # CPA 清理
    cpa_cleanup = False
    if UPLOAD_API_URL:
        cleanup_input = input("\n注册前清理 CPA 无效号? (Y/n): ").strip().lower()
        cpa_cleanup = cleanup_input != "n"

    # 注册数量
    count_input = input(f"\n注册账号数量 (默认 {DEFAULT_TOTAL_ACCOUNTS}): ").strip()
    total_accounts = (int(count_input) if count_input.isdigit() and int(count_input) > 0
                      else DEFAULT_TOTAL_ACCOUNTS)

    workers_input = input("并发数 (默认 3): ").strip()
    max_workers = int(workers_input) if workers_input.isdigit() and int(workers_input) > 0 else 3

    run_batch(total_accounts=total_accounts, output_file=DEFAULT_OUTPUT_FILE,
              max_workers=max_workers, proxy=proxy, cpa_cleanup=cpa_cleanup)

if __name__ == "__main__":
    main()
