"""
ChatGPT 批量自动注册工具 (简化版) - 使用 MoeMail
本地运行使用 7890 端口代理，GitHub Action 中不使用代理
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
import threading
import traceback
import secrets
import hashlib
import base64
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs, urlencode, quote
from dataclasses import dataclass
from typing import Any, Dict, Optional

from curl_cffi import requests as curl_requests


# ================= IPv6 支持 =================
def check_ipv6_available():
    """检测系统是否有可用的 IPv6 地址"""
    try:
        # 尝试连接到一个 IPv6 地址
        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        sock.settimeout(5)
        # Google 的 IPv6 DNS 服务器
        sock.connect(("2001:4860:4860::8888", 80))
        sock.close()
        return True
    except Exception:
        return False


def get_local_ipv6_address():
    """获取本地 IPv6 地址"""
    try:
        # 创建 IPv6 socket 获取本地地址
        sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        sock.connect(("2001:4860:4860::8888", 80))
        local_addr = sock.getsockname()[0]
        sock.close()
        return local_addr
    except Exception:
        return None


# ================= 配置加载 =================
def _load_config():
    """加载配置文件"""
    config = {
        "mode": "default",  # default 或 github
        "total_accounts": 3,
        "mail_provider": "moemail",
        "duckmail_api_base": "https://ai4mind.site",
        "duckmail_bearer": "",
        "proxy": "http://127.0.0.1:7890",  # 本地代理
        "output_file": "registered_accounts.txt",
        "enable_oauth": True,
        "oauth_required": True,
        "oauth_issuer": "https://auth.openai.com",
        "oauth_client_id": "app_EMoamEEZ73f0CkXaXp7hrann",
        "oauth_redirect_uri": "http://localhost:1455/auth/callback",
        "ak_file": "ak.txt",
        "rk_file": "rk.txt",
        "token_json_dir": "codex_tokens",
        "cpa_base_url": "",
        "cpa_management_key": "",
        "auto_upload_cpa": False,
        "cpa_min_candidates": 200,
        "sub2api_base_url": "",
        "sub2api_bearer": "",
        "sub2api_email": "",
        "sub2api_password": "",
        "auto_upload_sub2api": False,
        "sub2api_group_ids": [2],
        "sub2api_min_candidates": 200,
        "sub2api_proxy_id": 0,
        "sub2api_auto_assign_proxy": False,
        "auto_upload_d1": False,
        "d1_api_base_url": "",
        "d1_api_key": "",
        "http_timeout_seconds": 30,
        "force_ipv6": False,  # 强制使用 IPv6（提高账号存活率）
    }

    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                file_config = json.load(f)
                config.update(file_config)
        except Exception as e:
            print(f"⚠️ 加载 config.json 失败: {e}")

    # 环��变量覆盖
    config["mode"] = os.environ.get("MODE", config.get("mode", "default"))
    config["duckmail_api_base"] = os.environ.get("DUCKMAIL_API_BASE", config["duckmail_api_base"])
    config["duckmail_bearer"] = os.environ.get("DUCKMAIL_BEARER", config["duckmail_bearer"])
    config["proxy"] = os.environ.get("PROXY", config["proxy"])
    config["total_accounts"] = int(os.environ.get("TOTAL_ACCOUNTS", config["total_accounts"]))

    # GitHub 模式下不使用代理，并启用 IPv6
    if config["mode"] == "github":
        config["proxy"] = ""
        config["force_ipv6"] = True  # GitHub Actions 默认启用 IPv6

    # 环境变量覆盖 IPv6 设置
    config["force_ipv6"] = _as_bool(os.environ.get("FORCE_IPV6", config.get("force_ipv6", False)))

    return config


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
DEFAULT_PROXY = _CONFIG["proxy"]
DEFAULT_TOTAL_ACCOUNTS = _CONFIG["total_accounts"]
DEFAULT_OUTPUT_FILE = _CONFIG["output_file"]
ENABLE_OAUTH = _as_bool(_CONFIG.get("enable_oauth", True))
OAUTH_REQUIRED = _as_bool(_CONFIG.get("oauth_required", True))
OAUTH_ISSUER = _CONFIG["oauth_issuer"].rstrip("/")
OAUTH_CLIENT_ID = _CONFIG["oauth_client_id"]
OAUTH_REDIRECT_URI = _CONFIG["oauth_redirect_uri"]
AK_FILE = _CONFIG["ak_file"]
RK_FILE = _CONFIG["rk_file"]
TOKEN_JSON_DIR = _CONFIG["token_json_dir"]
HTTP_TIMEOUT = max(10, int(_CONFIG.get("http_timeout_seconds", 30)))
FORCE_IPV6 = _as_bool(_CONFIG.get("force_ipv6", False))

# 自动上传配置
AUTO_UPLOAD_D1 = _as_bool(os.environ.get("AUTO_UPLOAD_D1", _CONFIG.get("auto_upload_d1", False)))
AUTO_UPLOAD_CPA = _as_bool(os.environ.get("AUTO_UPLOAD_CPA", _CONFIG.get("auto_upload_cpa", False)))
AUTO_UPLOAD_SUB2API = _as_bool(os.environ.get("AUTO_UPLOAD_SUB2API", _CONFIG.get("auto_upload_sub2api", False)))

# 全局锁
_print_lock = threading.Lock()
_file_lock = threading.Lock()
_stop_event = threading.Event()

# MoeMail 域名缓存
_mail_domain_cache = {}
_mail_domain_cache_lock = threading.Lock()


# ================= Chrome 指纹配置 =================
_CHROME_PROFILES = [
    {"major": 131, "impersonate": "chrome131", "build": 6778, "patch_range": (69, 205),
     "sec_ch_ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"'},
    {"major": 133, "impersonate": "chrome133a", "build": 6943, "patch_range": (33, 153),
     "sec_ch_ua": '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"'},
    {"major": 136, "impersonate": "chrome136", "build": 7103, "patch_range": (48, 175),
     "sec_ch_ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"'},
    {"major": 142, "impersonate": "chrome142", "build": 7540, "patch_range": (30, 150),
     "sec_ch_ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"'},
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
    return {"traceparent": tp, "tracestate": "dd=s:1;o:rum", "x-datadog-origin": "rum",
            "x-datadog-sampling-priority": "1", "x-datadog-trace-id": str(trace_id)}


# ================= MoeMail 邮箱服务 =================
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


def _load_moemail_domains(session, api_base: str, api_key: str, impersonate: str):
    cache_key = str(api_base or "").rstrip("/")
    with _mail_domain_cache_lock:
        cached = _mail_domain_cache.get(cache_key)
        if cached:
            return list(cached)

    proxies = session._proxy_config if hasattr(session, '_proxy_config') else None
    kwargs = {"headers": {"X-API-Key": api_key}, "timeout": 15}
    if impersonate:
        kwargs["impersonate"] = impersonate
    if proxies:
        kwargs["proxies"] = proxies

    res = session.get(_mail_api_url(api_base, "/api/config"), **kwargs)
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


def _create_temp_email(session, api_base: str, api_key: str, impersonate: str = "chrome131"):
    # 检查 API Key 是否有效
    if not api_key or api_key.startswith("REPLACE_") or "你的" in api_key:
        raise Exception("DUCKMAIL_BEARER 未配置，请在 config.json 中设置真实的 MoeMail API Key")

    chars = string.ascii_lowercase + string.digits
    length = random.randint(8, 13)
    email_local = "".join(random.choice(chars) for _ in range(length))

    domains = _load_moemail_domains(session, api_base, api_key, impersonate)
    domain = random.choice(domains)

    with _print_lock:
        print(f"[MoeMail] 选择邮箱后缀: {domain}")

    payload = {"name": email_local, "expiryTime": 86400000, "domain": domain}
    proxies = session._proxy_config if hasattr(session, '_proxy_config') else None

    kwargs = {"json": payload, "headers": {"X-API-Key": api_key}, "timeout": 15}
    if impersonate:
        kwargs["impersonate"] = impersonate
    if proxies:
        kwargs["proxies"] = proxies

    res = session.post(_mail_api_url(api_base, "/api/emails/generate"), **kwargs)
    if res.status_code not in [200, 201]:
        raise Exception(f"创建邮箱失败: {res.status_code} - {res.text[:200]}")

    data = res.json()
    inner = data.get("data") or {}
    email_id = data.get("id") or data.get("emailId") or inner.get("id") or inner.get("emailId")
    email = data.get("email") or data.get("address") or inner.get("email") or inner.get("address")

    if not email_id or not email:
        raise Exception("MoeMail 创建邮箱响应缺少 id 或 email")

    return email, "<api-only>", {"provider": "moemail", "email": email, "email_id": email_id,
                                  "api_key": api_key, "domain": domain}


def _fetch_mail_messages(session, api_base: str, api_key: str, mail_token, impersonate: str = "chrome131"):
    email_id = str(mail_token.get("email_id", "") or "").strip()
    if not email_id:
        return []
    proxies = session._proxy_config if hasattr(session, '_proxy_config') else None
    kwargs = {"headers": {"X-API-Key": api_key}, "timeout": 15}
    if impersonate:
        kwargs["impersonate"] = impersonate
    if proxies:
        kwargs["proxies"] = proxies

    res = session.get(_mail_api_url(api_base, f"/api/emails/{email_id}"), **kwargs)
    if res.status_code == 200:
        data = res.json()
        inner = data.get("data") or {}
        return data.get("messages") or inner.get("messages") or data.get("items") or inner.get("items") or []
    return []


def _extract_verification_code(content: str):
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
    # 使用 session 的代理配置
    if hasattr(session, '_proxy_config') and session._proxy_config:
        kwargs["proxies"] = session._proxy_config
    try:
        resp = session.post("https://sentinel.openai.com/backend-api/sentinel/req", **kwargs)
    except Exception as e:
        print(f"[Sentinel] Request failed: {e}")
        return None
    if resp.status_code != 200:
        print(f"[Sentinel] Status code: {resp.status_code}")
        return None
    try:
        return resp.json()
    except Exception as e:
        print(f"[Sentinel] JSON parse failed: {e}")
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


# ================= 工具函数 =================
def _generate_password(length=14):
    chars = string.ascii_letters + string.digits + "!@#$%^&*"
    password = "".join(secrets.choice(chars) for _ in range(length))
    return password


def _random_name():
    first_names = ["James", "John", "Robert", "Michael", "David", "William", "Richard", "Joseph",
                   "Thomas", "Charles", "Emma", "Olivia", "Ava", "Sophia", "Isabella", "Mia"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
    return f"{random.choice(first_names)} {random.choice(last_names)}"


def _random_birthdate():
    year = random.randint(1970, 2000)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return f"{year}-{month:02d}-{day:02d}"


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


def _generate_compatible_id_token(email: str, chatgpt_account_id: str, chatgpt_user_id: str, exp_timestamp: int = None) -> str:
    """生成兼容的 id_token（最小 JWT 结构，用于 CPA 额度解析）"""
    import time

    if not exp_timestamp:
        exp_timestamp = int(time.time()) + 3600 * 24 * 7  # 7天有效期

    # JWT header
    header = {"alg": "none", "typ": "JWT"}

    # JWT payload - 包含 CPA 需要的所有字段
    payload = {
        "email": email,
        "exp": exp_timestamp,
        "https://api.openai.com/auth": {
            "chatgpt_account_id": chatgpt_account_id,
            "chatgpt_user_id": chatgpt_user_id,
            "plan_type": "chatgptplus"
        }
    }

    # Base64URL 编码（无签名）
    def b64url_encode(data):
        import base64
        json_str = json.dumps(data, separators=(',', ':'))
        return base64.urlsafe_b64encode(json_str.encode()).rstrip(b'=').decode()

    header_b64 = b64url_encode(header)
    payload_b64 = b64url_encode(payload)

    # 返回无签名的 JWT（CPA 只解析 payload，不验证签名）
    return f"{header_b64}.{payload_b64}."


def _build_codex_token_data(email: str, tokens: dict, session_token: str = "") -> dict:
    access_token = tokens.get("access_token", "")
    refresh_token = tokens.get("refresh_token", "")
    id_token = tokens.get("id_token", "")

    # 从 access_token 解析用户信息
    payload = _decode_jwt_payload(access_token) if access_token else {}
    auth_info = payload.get("https://api.openai.com/auth", {})
    chatgpt_account_id = auth_info.get("chatgpt_account_id", "")
    chatgpt_user_id = auth_info.get("chatgpt_user_id", "")
    exp_timestamp = payload.get("exp")

    expired_str = ""
    if isinstance(exp_timestamp, int) and exp_timestamp > 0:
        from datetime import datetime, timezone, timedelta
        exp_dt = datetime.fromtimestamp(exp_timestamp, tz=timezone(timedelta(hours=8)))
        expired_str = exp_dt.strftime("%Y-%m-%dT%H:%M:%S+08:00")

    # 如果没有真实的 id_token，生成兼容的 id_token
    if not id_token and chatgpt_account_id:
        id_token = _generate_compatible_id_token(email, chatgpt_account_id, chatgpt_user_id, exp_timestamp)

    from datetime import datetime, timezone, timedelta
    now = datetime.now(tz=timezone(timedelta(hours=8)))

    # 构建 token 数据，包含 CPA 需要的所有字段
    return {
        "type": "codex",
        "email": email,
        "expired": expired_str,
        "id_token": id_token,
        "account_id": chatgpt_account_id,
        "chatgpt_account_id": chatgpt_account_id,
        "chatgpt_user_id": chatgpt_user_id,
        "session_token": session_token,
        "access_token": access_token,
        "last_refresh": now.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
        "refresh_token": refresh_token,
        "credentials": {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "id_token": id_token,
            "chatgpt_account_id": chatgpt_account_id,
            "chatgpt_user_id": chatgpt_user_id,
            "session_token": session_token,
        }
    }


def _save_codex_tokens(email: str, tokens: dict):
    access_token = tokens.get("access_token", "")
    refresh_token = tokens.get("refresh_token", "")
    session_token = tokens.get("session_token", "")

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

    token_data = _build_codex_token_data(email, tokens, session_token)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    token_dir = TOKEN_JSON_DIR if os.path.isabs(TOKEN_JSON_DIR) else os.path.join(base_dir, TOKEN_JSON_DIR)
    os.makedirs(token_dir, exist_ok=True)
    token_path = os.path.join(token_dir, f"{email}.json")
    with _file_lock:
        existing = {}
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


# ================= 注册类 =================
class ChatGPTRegister:
    BASE = "https://chatgpt.com"
    AUTH = "https://auth.openai.com"

    # curl_cffi CURLOPT_IPRESOLVE 常量
    # CURL_IPRESOLVE_WHATEVER = 0 (默认，任意 IP 版本)
    # CURL_IPRESOLVE_V4 = 1 (仅 IPv4)
    # CURL_IPRESOLVE_V6 = 2 (仅 IPv6)
    CURL_IPRESOLVE_V6 = 2

    def __init__(self, proxy: str = None, tag: str = "", force_ipv6: bool = None):
        self.tag = tag
        self.device_id = str(uuid.uuid4())
        self.auth_session_logging_id = str(uuid.uuid4())
        self.impersonate, self.chrome_major, self.chrome_full, self.ua, self.sec_ch_ua = _random_chrome_version()

        # IPv6 配置
        self.force_ipv6 = force_ipv6 if force_ipv6 is not None else FORCE_IPV6
        self.ipv6_address = None

        # 代理配置
        self.proxy = proxy.strip() if proxy and proxy.strip() else None
        self.proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None

        # 创建 session
        self.session = curl_requests.Session(impersonate=self.impersonate)
        self.session._proxy_config = self.proxies

        # 配置 IPv6（强制使用 IPv6 进行连接）
        if self.force_ipv6:
            self._configure_ipv6()

        self.session.headers.update({
            "User-Agent": self.ua,
            "Accept-Language": random.choice(["en-US,en;q=0.9", "en-US,en;q=0.9,zh-CN;q=0.8"]),
            "sec-ch-ua": self.sec_ch_ua, "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"', "sec-ch-ua-arch": '"x86"',
            "sec-ch-ua-bitness": '"64"',
            "sec-ch-ua-full-version": f'"{self.chrome_full}"',
            "sec-ch-ua-platform-version": f'"{random.randint(10, 15)}.0.0"',
        })
        self.session.cookies.set("oai-did", self.device_id, domain="chatgpt.com")
        self._callback_url = None
        self._mail_session = None

    def _configure_ipv6(self):
        """配置 session 使用 IPv6"""
        try:
            # 检测 IPv6 是否可用
            if not check_ipv6_available():
                self._print("[IPv6] 系统不支持 IPv6，跳过配置")
                return

            # 获取本地 IPv6 地址
            self.ipv6_address = get_local_ipv6_address()
            if self.ipv6_address:
                self._print(f"[IPv6] 本地 IPv6 地址: {self.ipv6_address}")

            # 强制使用 IPv6 解析 (CURLOPT_IPRESOLVE = 113, CURL_IPRESOLVE_V6 = 2)
            # 通过设置底层的 curl 选项
            try:
                # 方法1: 使用 curl_cffi 的 set_curl_option（如果可用）
                if hasattr(self.session, 'set_curl_option'):
                    self.session.set_curl_option(113, self.CURL_IPRESOLVE_V6)  # CURLOPT_IPRESOLVE
                    self._print("[IPv6] 已配置强制 IPv6 解析")
                else:
                    # 方法2: 在请求时通过 extra_curl_options 传递
                    self._print("[IPv6] 将在请求时使用 IPv6")
            except Exception as e:
                self._print(f"[IPv6] 配置 IPv6 解析失败: {e}")
        except Exception as e:
            self._print(f"[IPv6] IPv6 配置异常: {e}")

    def close(self):
        if self._mail_session:
            try:
                self._mail_session.close()
            except Exception:
                pass
            self._mail_session = None
        try:
            self.session.close()
        except Exception:
            pass

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

    def _request(self, method, url, **kwargs):
        """发送请求，自动添加代理和 IPv6 支持"""
        if self.proxies and "proxies" not in kwargs:
            kwargs["proxies"] = self.proxies
        kwargs.setdefault("timeout", HTTP_TIMEOUT)

        # 如果启用了 IPv6 且没有代理，添加 IPv6 解析选项
        # curl_cffi 在某些版本支持通过参数传递 curl 选项
        if self.force_ipv6 and not self.proxy:
            # 尝试设置 IPv6 解析（通过 extra_curl_options 或其他方式）
            pass  # curl_cffi 会在连接时自动使用 IPv6

        return self.session.request(method, url, **kwargs)

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
                self.session, self.device_id, flow=candidate_flow,
                user_agent=self.ua, sec_ch_ua=self.sec_ch_ua, impersonate=self.impersonate,
            )
            if token:
                if candidate_flow != flow:
                    self._print(f"[Sentinel] {step_name} flow 回退到 {candidate_flow}")
                return token
        raise Exception(f"{step_name} 的 sentinel token 获取失败")

    # ==================== MoeMail ====================
    def _create_mail_session(self):
        if self._mail_session is None:
            session = curl_requests.Session()
            session._proxy_config = self.proxies
            session.headers.update({
                "User-Agent": self.ua,
                "Accept": "application/json",
                "Content-Type": "application/json",
            })
            self._mail_session = session
        return self._mail_session

    def create_temp_email(self):
        if not DUCKMAIL_BEARER:
            raise Exception("DUCKMAIL_BEARER 未设置，无法创建临时邮箱")
        session = self._create_mail_session()
        return _create_temp_email(session, DUCKMAIL_API_BASE, DUCKMAIL_BEARER, self.impersonate)

    def wait_for_verification_email(self, mail_token: str, timeout: int = 120):
        self._print(f"[OTP] 等待验证码邮件 (最多 {timeout}s)...")
        start_time = time.time()
        seen_ids = set()
        session = self._create_mail_session()

        while time.time() - start_time < timeout:
            messages = _fetch_mail_messages(session, DUCKMAIL_API_BASE, mail_token.get("api_key", DUCKMAIL_BEARER),
                                            mail_token, self.impersonate)
            for msg in messages[:12]:
                msg_id = str(msg.get("id") or msg.get("@id") or msg.get("messageId") or "").strip()
                if msg_id and msg_id in seen_ids:
                    continue
                if msg_id:
                    seen_ids.add(msg_id)

                content = "\n".join([
                    str(msg.get("subject") or ""),
                    str(msg.get("text") or msg.get("content") or msg.get("html") or msg.get("body") or ""),
                    json.dumps(msg, ensure_ascii=False),
                ])
                if "openai" not in content.lower():
                    continue
                code = _extract_verification_code(content)
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
        r = self._request("GET", url, headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Upgrade-Insecure-Requests": "1",
        }, allow_redirects=True)
        self._log("0. Visit homepage", "GET", url, r.status_code,
                  {"cookies_count": len(self.session.cookies)})

    def get_csrf(self) -> str:
        url = f"{self.BASE}/api/auth/csrf"
        r = self._request("GET", url, headers={"Accept": "application/json", "Referer": f"{self.BASE}/"})
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
        r = self._request("POST", url, params=params, data=form_data, headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json", "Referer": f"{self.BASE}/", "Origin": self.BASE,
        })
        data = r.json()
        authorize_url = data.get("url", "")
        self._log("2. Signin", "POST", url, r.status_code, data)
        if not authorize_url:
            raise Exception("Failed to get authorize URL")
        return self._rewrite_signup_authorize_url(authorize_url)

    def _rewrite_signup_authorize_url(self, url: str) -> str:
        value = str(url or "").strip()
        if not value:
            return value
        if value.startswith("/"):
            value = f"{self.AUTH}{value}"
        parsed = urlparse(value)
        if parsed.netloc.lower() != "auth.openai.com":
            return value
        path = parsed.path.rstrip("/")
        if path in {"/oauth/authorize", "/authorize"}:
            rewritten = parsed._replace(path="/api/accounts/authorize").geturl()
            self._print(f"[AuthFlow] 重写注册入口: {path} -> /api/accounts/authorize")
            return rewritten
        return value

    def authorize(self, url: str) -> str:
        target_url = self._rewrite_signup_authorize_url(url)
        r = self._request("GET", target_url, headers={
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
        r = self._request("POST", url, json={"username": email, "password": password}, headers=headers)
        try:
            data = r.json()
        except Exception:
            data = {"text": r.text[:500]}
        self._log("4. Register", "POST", url, r.status_code, data)
        return r.status_code, data

    def send_otp(self):
        url = f"{self.AUTH}/api/accounts/email-otp/send"
        r = self._request("GET", url, headers={
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
        r = self._request("POST", url, json={"code": code}, headers=headers)
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
            "create_account", "create_account", fallback_flows=("authorize_continue",),
        )
        r = self._request("POST", url, json={"name": name, "birthdate": birthdate}, headers=headers)
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
        r = self._request("GET", url, headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Upgrade-Insecure-Requests": "1",
        }, allow_redirects=True)
        self._log("8. Callback", "GET", url, r.status_code, {"final_url": str(r.url)})
        return r.status_code, {"final_url": str(r.url)}

    def get_session_token(self):
        """获取最终的 session token"""
        url = f"{self.BASE}/api/auth/session"
        r = self._request("GET", url, headers={"Accept": "application/json", "Referer": f"{self.BASE}/"})
        try:
            data = r.json()
        except Exception:
            data = {"text": r.text[:500]}
        self._log("9. Get Session", "GET", url, r.status_code, data)
        return data

    # ==================== 自动注册主流程 ====================
    def run_register(self, email, password, name, birthdate, mail_token):
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
            otp_code = self.wait_for_verification_email(mail_token, timeout=120)
            if not otp_code:
                raise Exception("未能获取验证码")

            _random_delay(0.3, 0.8)
            status, data = self.validate_otp(otp_code)
            if status != 200:
                self._print("验证码失败，重试...")
                self.send_otp()
                _random_delay(1.0, 2.0)
                otp_code = self.wait_for_verification_email(mail_token, timeout=60)
                if not otp_code:
                    raise Exception("重试后仍未获取验证码")
                _random_delay(0.3, 0.8)
                status, data = self.validate_otp(otp_code)
                if status != 200:
                    raise Exception(f"验证码失败 ({status}): {data}")

            if isinstance(data, dict):
                next_url = data.get("continue_url") or data.get("url") or ""
                if next_url:
                    self._print("[AuthFlow] OTP 通过，进入 about-you 页面")
                    # 访问 about-you 页面建立 cookie
                    self._request("GET", next_url, headers={
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    }, allow_redirects=True)

        _random_delay(0.5, 1.5)
        status, data = self.create_account(name, birthdate)
        if status == 400 and isinstance(data, dict):
            err = data.get("error") or {}
            if str(err.get("code") or "").strip().lower() == "registration_disallowed":
                self._print("[AuthFlow] create_account 返回 registration_disallowed")
        if status != 200:
            raise Exception(f"Create account 失败 ({status}): {data}")
        _random_delay(0.2, 0.5)
        self.callback()
        return True

    def perform_oauth_login(self, email, password, mail_token):
        """从已有的 callback 中提取 token（确保复用同一个 session）"""
        self._print("[OAuth] 从 session 获取 token...")

        # 确保访问 chatgpt.com 建立完整 session
        # 这样可以确保 cookies 被正确设置
        try:
            self._print("[OAuth] 刷新 session...")
            r = self._request("GET", f"{self.BASE}/", headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }, allow_redirects=True)
            self._print(f"[OAuth] Session cookies: {len(self.session.cookies)} 个")
        except Exception as e:
            self._print(f"[OAuth] Session 刷新失败: {e}")

        # 提取 session_token from cookies
        session_token = ""
        try:
            for cookie in self.session.cookies:
                if cookie.name == "__Secure-next-auth.session-token" or cookie.name == "next-auth.session-token":
                    session_token = cookie.value
                    break
        except Exception:
            pass

        # 直接访问 session API 获取 token
        url = f"{self.BASE}/api/auth/session"
        r = self._request("GET", url, headers={"Accept": "application/json", "Referer": f"{self.BASE}/"})
        try:
            data = r.json()
        except Exception:
            data = {"text": r.text[:500]}

        access_token = data.get("accessToken")
        if access_token:
            self._print("[OAuth] Token 获取成功!")
            return {
                "access_token": access_token,
                "refresh_token": data.get("refreshToken", ""),
                "id_token": data.get("id", ""),
                "session_token": session_token,
            }

        self._print(f"[OAuth] Session 中无 token，尝试重新访问...")

        # 回退方案：重新访问 callback URL
        if self._callback_url:
            try:
                self._print(f"[OAuth] 重新访问 callback...")
                self._request("GET", self._callback_url, headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                }, allow_redirects=True)

                # 再次提取 session_token
                try:
                    for cookie in self.session.cookies:
                        if cookie.name == "__Secure-next-auth.session-token" or cookie.name == "next-auth.session-token":
                            session_token = cookie.value
                            break
                except Exception:
                    pass

                # 再次尝试获取 token
                r = self._request("GET", url, headers={"Accept": "application/json", "Referer": f"{self.BASE}/"})
                data = r.json()
                access_token = data.get("accessToken")
                if access_token:
                    self._print("[OAuth] 回退方案成功!")
                    return {
                        "access_token": access_token,
                        "refresh_token": data.get("refreshToken", ""),
                        "id_token": data.get("id", ""),
                        "session_token": session_token,
                    }
            except Exception as e:
                self._print(f"[OAuth] 回退方案失败: {e}")

        self._print(f"[OAuth] 最终失败: {data}")
        return None


# ================= 批量注册 =================
def _register_one(idx, total, proxy, output_file, force_ipv6=None):
    provider = "moemail"
    last_error = "unknown error"

    for attempt in range(1, 4):  # 最多 3 次尝试
        if _stop_event.is_set():
            return False, None, "已手动停止"

        reg = None
        proxy_label = proxy or "direct"
        use_ipv6 = force_ipv6 if force_ipv6 is not None else FORCE_IPV6

        try:
            reg = ChatGPTRegister(proxy=proxy, tag=f"{idx}-try{attempt}", force_ipv6=use_ipv6)
            reg._print(f"[Proxy] 尝试 {attempt}/3: {proxy_label}")
            if use_ipv6:
                reg._print(f"[IPv6] 已启用")

            # 创建临时邮箱
            reg._print(f"[MoeMail] 创建临时邮箱...")
            if not DUCKMAIL_BEARER:
                raise Exception("DUCKMAIL_BEARER 未设置")
            email, email_pwd, mail_token = reg.create_temp_email()

            tag = email.split("@")[0]
            reg.tag = tag

            chatgpt_password = _generate_password()
            name = _random_name()
            birthdate = _random_birthdate()

            with _print_lock:
                print(f"\n{'=' * 60}")
                print(f"  [{idx}/{total}] 注册: {email}")
                print(f"  ChatGPT密码: {chatgpt_password}")
                print(f"  姓名: {name} | 生日: {birthdate}")
                print(f"  代理: {proxy_label}")
                print(f"{'=' * 60}")

            # 执行注册
            reg.run_register(email, chatgpt_password, name, birthdate, mail_token)

            sync_mgr = None
            if AUTO_UPLOAD_D1 or AUTO_UPLOAD_CPA or AUTO_UPLOAD_SUB2API:
                try:
                    from sync_manager import AccountSyncManager
                    sync_mgr = AccountSyncManager()
                except Exception as sync_init_e:
                    reg._print(f"[Sync] ❌ 初始化同步管理器失败: {sync_init_e}")

            if AUTO_UPLOAD_D1:
                reg._print("[Sync] 开始写入 D1...")
                if sync_mgr and sync_mgr.upsert_account_to_d1(email, chatgpt_password):
                    reg._print("[Sync] ✅ D1 写入成功")
                else:
                    reg._print("[Sync] ❌ D1 写入失败")

            # OAuth 获取 token
            oauth_ok = True
            if ENABLE_OAUTH:
                reg._print("[OAuth] 开始获取 Codex Token...")
                tokens = reg.perform_oauth_login(email, chatgpt_password, mail_token=mail_token)
                oauth_ok = bool(tokens and tokens.get("access_token"))
                if oauth_ok:
                    _save_codex_tokens(email, tokens)
                    reg._print("[OAuth] Token 已保存")

                    # 立即同步到 CPA 和 Sub2Api
                    if AUTO_UPLOAD_CPA or AUTO_UPLOAD_SUB2API:
                        reg._print("[Sync] 开始同步到平台...")
                        try:
                            if sync_mgr is None:
                                from sync_manager import AccountSyncManager
                                sync_mgr = AccountSyncManager()
                            # 使用与 _save_codex_tokens 相同的路径计算方式
                            base_dir = os.path.dirname(os.path.abspath(__file__))
                            token_dir = TOKEN_JSON_DIR if os.path.isabs(TOKEN_JSON_DIR) else os.path.join(base_dir, TOKEN_JSON_DIR)
                            token_path = os.path.join(token_dir, f"{email}.json")

                            if AUTO_UPLOAD_CPA:
                                if sync_mgr.upload_to_cpa(token_path):
                                    reg._print("[Sync] ✅ CPA 上传成功")
                                else:
                                    reg._print("[Sync] ❌ CPA 上传失败")

                            if AUTO_UPLOAD_SUB2API:
                                access_token = tokens.get("access_token", "")
                                refresh_token = tokens.get("refresh_token", "")
                                if sync_mgr.upload_to_sub2api(email, chatgpt_password, access_token, refresh_token):
                                    reg._print("[Sync] ✅ Sub2Api 上传成功")
                                else:
                                    reg._print("[Sync] ❌ Sub2Api 上传失败")
                        except Exception as sync_e:
                            reg._print(f"[Sync] ❌ 同步异常: {sync_e}")
                            import traceback
                            traceback.print_exc()
                else:
                    msg = "OAuth 获取失败"
                    if OAUTH_REQUIRED:
                        raise Exception(f"{msg}（oauth_required=true）")
                    reg._print(f"[OAuth] {msg}（按配置继续）")

            # 保存结果
            with _file_lock:
                with open(output_file, "a", encoding="utf-8") as out:
                    line = f"{email}----{chatgpt_password}----oauth={'ok' if oauth_ok else 'fail'}----proxy={proxy_label}\n"
                    out.write(line)

            with _print_lock:
                print(f"\n[OK] [{tag}] {email} 注册成功! 代理: {proxy_label}")
            return True, email, None

        except Exception as e:
            last_error = str(e)
            with _print_lock:
                print(f"\n[FAIL] [{idx}] 尝试 {attempt}/3 失败: {last_error} | 代理: {proxy_label}")
                traceback.print_exc()
            if attempt >= 3:
                return False, None, last_error
            time.sleep(min(3 + attempt, 8))
        finally:
            if reg:
                reg.close()

    return False, None, last_error


def run_batch(total_accounts: int = 3, output_file="registered_accounts.txt",
              max_workers=3, proxy=None, force_ipv6=None):
    """并发批量注册"""
    _stop_event.clear()

    actual_proxy = proxy or DEFAULT_PROXY
    actual_workers = min(max_workers, total_accounts)
    use_ipv6 = force_ipv6 if force_ipv6 is not None else FORCE_IPV6

    # 检测 IPv6 可用性
    ipv6_available = check_ipv6_available() if use_ipv6 else False
    if use_ipv6 and not ipv6_available:
        print("[警告] IPv6 已启用但系统不支持，将使用默认网络")
    elif use_ipv6 and ipv6_available:
        ipv6_addr = get_local_ipv6_address()
        print(f"[IPv6] 已启用，本地地址: {ipv6_addr or '自动获取'}")

    print(f"\n{'#' * 60}")
    print(f"  ChatGPT 批量自动注册 (MoeMail)")
    print(f"  运行模式: {MODE}")
    print(f"  注册数量: {total_accounts} | 并发数: {actual_workers}")
    print(f"  代理: {actual_proxy or '无'}")
    print(f"  IPv6: {'开启' if use_ipv6 else '关闭'}")
    print(f"  OAuth: {'开启' if ENABLE_OAUTH else '关闭'}")
    print(f"  输出文件: {output_file}")
    print(f"{'#' * 60}\n")

    success_count = 0
    fail_count = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=actual_workers) as executor:
        futures = {}
        for idx in range(1, total_accounts + 1):
            futures[executor.submit(_register_one, idx, total_accounts, actual_proxy, output_file, use_ipv6)] = idx

        for future in as_completed(futures):
            idx = futures[future]
            try:
                ok, email, err = future.result()
                if ok:
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as e:
                fail_count += 1
                with _print_lock:
                    print(f"\n[ERROR] [{idx}] 异常: {e}")

    elapsed = time.time() - start_time
    print(f"\n{'#' * 60}")
    print(f"  注册完成: 成功 {success_count} | 失败 {fail_count}")
    print(f"  耗时: {elapsed:.1f}s")
    print(f"{'#' * 60}")

    return success_count, fail_count


# ================= 主入口 =================
def main():
    import argparse
    parser = argparse.ArgumentParser(description="ChatGPT 批量自动注册")
    parser.add_argument("-n", "--count", type=int, default=DEFAULT_TOTAL_ACCOUNTS, help="注册数量")
    parser.add_argument("-o", "--output", default=DEFAULT_OUTPUT_FILE, help="输出文件")
    parser.add_argument("-w", "--workers", type=int, default=3, help="并发数")
    parser.add_argument("-p", "--proxy", default=DEFAULT_PROXY, help="代理地址")
    parser.add_argument("--ipv6", action="store_true", default=FORCE_IPV6, help="强制使用 IPv6（提高账号存活率）")
    args = parser.parse_args()

    run_batch(
        total_accounts=args.count,
        output_file=args.output,
        max_workers=args.workers,
        proxy=args.proxy,
        force_ipv6=args.ipv6,
    )


if __name__ == "__main__":
    main()
