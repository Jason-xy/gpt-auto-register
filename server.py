"""
Auto CPA Register Web 管理服务
FastAPI backend — port 18421
"""
import asyncio
import copy
import json
import multiprocessing
import os
import socket
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

# macOS/Linux 用 fork 避免 spawn 重新导入 __main__ 引发端口冲突
_MP_CTX = multiprocessing.get_context("fork" if sys.platform != "win32" else "spawn")

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import ncs_register as register_module

BASE_DIR = Path(__file__).parent
WEB_DIR = BASE_DIR / "web"
CONFIG_FILE = BASE_DIR / "config.json"
TOKENS_DIR = BASE_DIR / "codex_tokens"
TOKENS_DIR.mkdir(exist_ok=True)

app = FastAPI(title="Auto CPA Register")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=str(WEB_DIR)), name="static")


# ============================================================
# SSE / Log infrastructure
# ============================================================
_log_lock = threading.Lock()
_log_entries: List[Dict] = []
_sse_lock = threading.Lock()
_sse_subscribers: List[Tuple] = []  # (loop, asyncio.Queue)


def _push_log(level: str, message: str, step: str = "") -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    entry: Dict[str, Any] = {"ts": ts, "level": level, "message": message}
    if step:
        entry["step"] = step
    with _log_lock:
        _log_entries.append(entry)
        if len(_log_entries) > 2000:
            _log_entries.pop(0)
    _broadcast_sse({"type": "log.appended", "log": entry})


def _broadcast_sse(payload: Dict) -> None:
    with _sse_lock:
        subs = list(_sse_subscribers)
    for loop, q in subs:
        def _enqueue(tq=q, d=payload):
            try:
                tq.put_nowait(copy.deepcopy(d))
            except asyncio.QueueFull:
                pass
        try:
            loop.call_soon_threadsafe(_enqueue)
        except RuntimeError:
            pass


class _QueueWriter:
    """Subprocess stdout → multiprocessing.Queue，每行一条消息。"""

    def __init__(self, q):
        self._q = q
        self._buf = ""
        self._dropped = 0

    def _push_line(self, line: str) -> None:
        if not line:
            return
        if self._dropped:
            summary = f"[WARN] 日志队列拥塞，已丢弃 {self._dropped} 条日志"
            try:
                self._q.put_nowait(summary)
                self._dropped = 0
            except Exception:
                pass
        try:
            self._q.put_nowait(line)
        except Exception:
            self._dropped += 1

    def write(self, text: str) -> int:
        self._buf += text
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            line = line.strip()
            if line:
                self._push_line(line)
        return len(text)

    def flush(self):
        line = self._buf.strip()
        self._buf = ""
        if line:
            self._push_line(line)

    def isatty(self):
        return False


def _worker_process_fn(total_accounts: int, max_workers: int, proxy: Optional[str],
                       output_file: str, log_queue) -> None:
    """在独立子进程中运行注册任务，stdout 重定向到队列。"""
    import sys
    sys.stdout = _QueueWriter(log_queue)
    sys.stderr = sys.stdout
    try:
        import ncs_register as reg
        reg.run_batch(
            total_accounts=total_accounts,
            output_file=output_file,
            max_workers=max_workers,
            proxy=proxy,
        )
    except Exception as e:
        try:
            log_queue.put(f"[ERROR] 任务异常: {e}")
        except Exception:
            pass
    finally:
        try:
            log_queue.put(None)  # sentinel
        except Exception:
            pass


def _log_level(line: str) -> str:
    ll = line.lower()
    if "[ok]" in ll or ("成功" in ll and "[fail]" not in ll):
        return "success"
    if "[fail]" in ll or "失败" in ll or "错误" in ll or "error" in ll:
        return "error"
    if "⚠" in line or "警告" in ll or "warn" in ll:
        return "warn"
    return "info"


def _log_reader_fn(log_queue, task_process) -> None:
    """主进程中读取子进程日志队列，推送到 SSE。"""
    while True:
        try:
            line = log_queue.get(timeout=1.0)
        except Exception:
            if task_process is None or not task_process.is_alive():
                break
            continue
        if line is None:
            break
        line_lower = line.lower()
        if line.startswith("[OK]") and "注册成功" in line:
            with _task_lock:
                _task["success"] = int(_task.get("success", 0) or 0) + 1
            _broadcast_sse({"type": "task.updated", **_get_snapshot()})
        elif line.strip().startswith("[账号 ") and "失败:" in line:
            with _task_lock:
                _task["fail"] = int(_task.get("fail", 0) or 0) + 1
            _broadcast_sse({"type": "task.updated", **_get_snapshot()})
        _push_log(_log_level(line), line)
    _push_log("info", "任务已结束", step="stopped")
    _set_task(status="idle", finished_at=datetime.now().isoformat(timespec="seconds"))


# ============================================================
# Task state
# ============================================================
_task_lock = threading.RLock()
_task: Dict[str, Any] = {
    "status": "idle",
    "run_id": None,
    "started_at": None,
    "finished_at": None,
    "worker_count": 1,
    "total_accounts": 0,
    "success": 0,
    "fail": 0,
}
_task_process: Optional[multiprocessing.Process] = None
_log_reader_thread: Optional[threading.Thread] = None
_task_log_queue = None


def _close_log_queue(log_queue) -> None:
    if log_queue is None:
        return
    try:
        log_queue.close()
    except Exception:
        pass
    try:
        log_queue.join_thread()
    except Exception:
        pass


def _cleanup_task_runtime(force: bool = False, kill: bool = False) -> None:
    global _task_process, _log_reader_thread, _task_log_queue
    proc = _task_process
    reader = _log_reader_thread
    log_queue = _task_log_queue

    if kill and proc and proc.is_alive():
        try:
            proc.kill()
        except Exception:
            pass

    if proc and (force or not proc.is_alive()):
        try:
            proc.join(timeout=2.0)
        except Exception:
            pass
        if proc.is_alive() and kill:
            try:
                proc.kill()
            except Exception:
                pass
            try:
                proc.join(timeout=2.0)
            except Exception:
                pass
        if not proc.is_alive():
            _task_process = None
            proc = None

    proc_finished = proc is None or not proc.is_alive()

    if log_queue and proc_finished:
        try:
            log_queue.put_nowait(None)
        except Exception:
            pass

    if reader and (force or not reader.is_alive() or proc_finished):
        try:
            reader.join(timeout=2.5)
        except Exception:
            pass
        if not reader.is_alive():
            _log_reader_thread = None
            reader = None

    reader_finished = reader is None or not reader.is_alive()
    if log_queue and proc_finished and reader_finished:
        _close_log_queue(log_queue)
        _task_log_queue = None


def _get_snapshot() -> Dict:
    with _task_lock:
        t = copy.deepcopy(_task)
    return {
        "task": {
            "run_id": t["run_id"],
            "status": t["status"],
            "revision": 0,
            "started_at": t["started_at"],
            "finished_at": t["finished_at"],
        },
        "runtime": {"run_id": t["run_id"], "revision": 0, "workers": []},
        "stats": {
            "success": t["success"],
            "fail": t["fail"],
            "total": t["success"] + t["fail"],
        },
        "server_time": datetime.now().isoformat(timespec="seconds"),
    }


def _set_task(**kwargs) -> None:
    with _task_lock:
        _task.update(kwargs)
    snap = _get_snapshot()
    _broadcast_sse({"type": "task.updated", **snap})


# ============================================================
# Config
# ============================================================
def _load_config() -> Dict:
    try:
        return register_module._load_config()
    except Exception:
        if CONFIG_FILE.exists():
            try:
                return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {}


def _save_config(cfg: Dict) -> None:
    raw = {}
    if CONFIG_FILE.exists():
        try:
            raw = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        except Exception:
            raw = {}

    raw.update(cfg)

    cpa_base_url = str(raw.get("cpa_base_url", "") or "").strip().rstrip("/")
    if cpa_base_url:
        try:
            cpa_base_url = register_module._cpa_normalize_api_root(cpa_base_url)
        except Exception:
            cpa_base_url = cpa_base_url.rstrip("/")
        raw["cpa_base_url"] = cpa_base_url
    cpa_management_key = str(raw.get("cpa_management_key", "") or "").strip()
    if cpa_base_url:
        raw["upload_api_url"] = cpa_base_url + "/auth-files"
    if cpa_management_key:
        raw["upload_api_token"] = cpa_management_key
    if not raw.get("cpa_base_url") and raw.get("upload_api_url"):
        try:
            raw["cpa_base_url"] = register_module._cpa_normalize_api_root(str(raw["upload_api_url"]))
        except Exception:
            pass
    if not raw.get("cpa_management_key") and raw.get("upload_api_token"):
        raw["cpa_management_key"] = raw["upload_api_token"]

    CONFIG_FILE.write_text(
        json.dumps(raw, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


# ============================================================
# Sub2Api Client (curl_cffi based)
# ============================================================
from curl_cffi import requests as cffi_req  # noqa: E402

_sub2api_bearer_lock = threading.Lock()
_sub2api_bearer_cache: List[str] = [""]
_sub2api_pool_status_lock = threading.Lock()
_sub2api_pool_status_cache: Dict[str, Any] = {"ts": 0.0, "signature": "", "data": None}
_SUB2API_POOL_STATUS_TTL_SECONDS = 90


def _cfg_proxy_candidates(cfg: Optional[Dict]) -> List[str]:
    if not isinstance(cfg, dict):
        return []
    try:
        if not register_module._proxy_runtime_enabled_from_config(cfg):
            return []
    except Exception:
        pass

    candidates: List[str] = []
    seen = set()

    def _add(value: Any) -> None:
        try:
            normalized = register_module._normalize_proxy(value)
        except Exception:
            normalized = None
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        candidates.append(normalized)

    _add(cfg.get("proxy"))
    _add(cfg.get("stable_proxy"))

    try:
        direct_proxy = register_module._normalize_single_proxy_source(
            str(cfg.get("proxy_list_url", "") or "").strip()
        )
    except Exception:
        direct_proxy = None
    _add(direct_proxy)

    if not direct_proxy and str(cfg.get("proxy_list_url", "") or "").strip():
        try:
            pool = register_module.ProxyPool(
                str(cfg.get("proxy_list_url", "") or "").strip(),
                fallback_proxy=str(cfg.get("proxy", "") or cfg.get("stable_proxy", "") or "").strip() or None,
                max_retries_per_request=int(cfg.get("proxy_max_retries_per_request", 30) or 30),
                bad_ttl_seconds=int(cfg.get("proxy_bad_ttl_seconds", 180) or 180),
                validate_enabled=bool(cfg.get("proxy_validate_enabled", True)),
                validate_timeout_seconds=float(cfg.get("proxy_validate_timeout_seconds", 6) or 6),
                validate_workers=int(cfg.get("proxy_validate_workers", 40) or 40),
                validate_test_url=str(cfg.get("proxy_validate_test_url", "https://auth.openai.com/") or "").strip()
                or "https://auth.openai.com/",
                prefer_stable_proxy=bool(cfg.get("prefer_stable_proxy", True)),
                list_bearer=str(cfg.get("proxy_list_bearer", "") or "").strip(),
            )
            _add(pool.next_proxy())
        except Exception:
            pass
    return candidates


def _cffi_request_with_proxy_candidates(method: str, url: str, cfg: Optional[Dict], **kwargs):
    req_kwargs = dict(kwargs)
    req_kwargs.setdefault("timeout", 20)
    req_kwargs.setdefault("impersonate", "chrome131")

    candidates = _cfg_proxy_candidates(cfg)
    if not candidates:
        return cffi_req.request(method, url, **req_kwargs)

    last_error = None
    for proxy in candidates:
        call_kwargs = dict(req_kwargs)
        call_kwargs["proxies"] = {"http": proxy, "https": proxy}
        try:
            return cffi_req.request(method, url, **call_kwargs)
        except Exception as e:
            last_error = e
            if not register_module._is_proxy_related_error(e):
                raise

    if last_error:
        raise last_error
    return cffi_req.request(method, url, **req_kwargs)


def _cffi_sub2api_login(base_url: str, email: str, password: str, cfg: Optional[Dict] = None) -> str:
    try:
        resp = _cffi_request_with_proxy_candidates(
            "POST",
            f"{base_url}/api/v1/auth/login",
            cfg,
            json={"email": email, "password": password},
            timeout=15,
        )
        data = resp.json()
        return str(
            data.get("token") or data.get("access_token")
            or (data.get("data") or {}).get("token")
            or (data.get("data") or {}).get("access_token")
            or ""
        ).strip()
    except Exception:
        return ""


def _cffi_sub2api_headers(token: str) -> Dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _cffi_sub2api_req(method: str, path: str, cfg: Dict, **kwargs):
    base_url = str(cfg.get("sub2api_base_url", "") or "").rstrip("/")
    if not base_url:
        raise ValueError("Sub2Api 未配置地址")

    bearer = str(cfg.get("sub2api_bearer", "") or "").strip()
    if bearer:
        _sub2api_bearer_cache[0] = bearer

    if not _sub2api_bearer_cache[0]:
        email = str(cfg.get("sub2api_email", "") or "").strip()
        password = str(cfg.get("sub2api_password", "") or "").strip()
        if email and password:
            with _sub2api_bearer_lock:
                if not _sub2api_bearer_cache[0]:
                    token = _cffi_sub2api_login(base_url, email, password, cfg)
                    if token:
                        _sub2api_bearer_cache[0] = token

    token = _sub2api_bearer_cache[0]
    kwargs.setdefault("timeout", 20)
    kwargs.setdefault("impersonate", "chrome131")
    resp = _cffi_request_with_proxy_candidates(
        method,
        f"{base_url}{path}",
        cfg,
        headers=_cffi_sub2api_headers(token),
        **kwargs,
    )
    if resp.status_code == 401:
        email = str(cfg.get("sub2api_email", "") or "").strip()
        password = str(cfg.get("sub2api_password", "") or "").strip()
        if email and password:
            with _sub2api_bearer_lock:
                new = _cffi_sub2api_login(base_url, email, password, cfg)
                if new:
                    _sub2api_bearer_cache[0] = new
            resp = _cffi_request_with_proxy_candidates(
                method,
                f"{base_url}{path}",
                cfg,
                headers=_cffi_sub2api_headers(_sub2api_bearer_cache[0]),
                **kwargs,
            )
    return resp


def _sub2api_cfg_signature(cfg: Dict) -> str:
    return "|".join([
        str(cfg.get("sub2api_base_url", "") or "").strip().rstrip("/"),
        str(cfg.get("sub2api_email", "") or "").strip().lower(),
        str(cfg.get("sub2api_bearer", "") or "").strip()[:24],
        str(cfg.get("sub2api_min_candidates", "") or "").strip(),
    ])


def _sub2api_fetch_page(cfg: Dict, page: int = 1, page_size: int = 20, timeout: int = 20) -> Dict[str, Any]:
    pg = max(1, int(page or 1))
    ps = max(1, min(int(page_size or 20), 200))
    resp = _cffi_sub2api_req(
        "GET", "/api/v1/admin/accounts", cfg,
        params={"page": pg, "page_size": ps, "platform": "openai", "type": "oauth"},
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    payload = data.get("data") if isinstance(data.get("data"), dict) else data
    if not isinstance(payload, dict):
        payload = {}
    items = payload.get("items") or []
    if not isinstance(items, list):
        items = []
    try:
        total = int(payload.get("total", 0) or 0)
    except Exception:
        total = 0
    try:
        total_pages = int(payload.get("total_pages", 0) or 0)
    except Exception:
        total_pages = 0
    if total_pages <= 0:
        total_pages = max(1, (total + ps - 1) // ps) if total > 0 else (1 if items else pg)
    return {
        "items": [i for i in items if isinstance(i, dict)],
        "total": total,
        "page": pg,
        "page_size": ps,
        "total_pages": max(1, total_pages),
    }


def _sub2api_list_all(cfg: Dict, page_size: int = 100, timeout: int = 20) -> List[Dict]:
    all_items: List[Dict] = []
    page = 1
    while True:
        page_data = _sub2api_fetch_page(cfg, page=page, page_size=page_size, timeout=timeout)
        items = page_data["items"]
        all_items.extend(items)
        total = page_data["total"]
        if not items or len(items) < page_data["page_size"]:
            break
        if isinstance(total, int) and total > 0 and len(all_items) >= total:
            break
        page += 1
    return all_items


def _sub2api_refresh_account(cfg: Dict, account_id: int) -> bool:
    try:
        resp = _cffi_sub2api_req("POST", f"/api/v1/admin/accounts/{account_id}/refresh", cfg)
        return resp.status_code in (200, 201)
    except Exception:
        return False


def _sub2api_delete_account(cfg: Dict, account_id: int) -> bool:
    try:
        resp = _cffi_sub2api_req("DELETE", f"/api/v1/admin/accounts/{account_id}", cfg)
        return resp.status_code in (200, 204)
    except Exception:
        return False


def _is_abnormal(status: Any) -> bool:
    return str(status or "").strip().lower() in ("error", "disabled")


# ============================================================
# CPA Client (curl_cffi based)
# ============================================================

def _cffi_cpa_headers(management_key: str) -> Dict:
    return {
        "Authorization": f"Bearer {management_key}",
        "X-Management-Key": management_key,
        "Accept": "application/json",
    }


def _cffi_cpa_req(method: str, path: str, cfg: Dict, **kwargs):
    base_url = str(cfg.get("cpa_base_url", "") or "").rstrip("/")
    if not base_url:
        raise ValueError("CPA 未配置地址")
    management_key = str(cfg.get("cpa_management_key", "") or "").strip()
    if not management_key:
        raise ValueError("CPA 未配置 Management Key")
    kwargs.setdefault("timeout", 20)
    kwargs.setdefault("impersonate", "chrome131")
    return cffi_req.request(
        method, f"{base_url}/v0/management{path}",
        headers=_cffi_cpa_headers(management_key),
        **kwargs,
    )


def _cpa_list_all(cfg: Dict) -> List[Dict]:
    resp = _cffi_cpa_req("GET", "/auth-files", cfg)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        files = data.get("files") or data.get("items") or data.get("data") or []
        return files if isinstance(files, list) else []
    return []


def _cpa_is_abnormal(auth_file: Dict) -> bool:
    if auth_file.get("disabled"):
        return True
    if auth_file.get("unavailable"):
        return True
    status = str(auth_file.get("status", "")).lower()
    return status in ("error", "disabled", "unavailable")


def _cpa_delete_auth_file(cfg: Dict, filename: str) -> bool:
    try:
        resp = _cffi_cpa_req("DELETE", "/auth-files", cfg, params={"name": filename})
        return resp.status_code in (200, 204)
    except Exception:
        return False


def _account_identity(item: Dict) -> Tuple[str, str]:
    email = ""
    rt = ""
    extra = item.get("extra")
    if isinstance(extra, dict):
        email = str(extra.get("email") or "").strip().lower()
    if not email:
        name = str(item.get("name") or "").strip().lower()
        if "@" in name:
            email = name
    creds = item.get("credentials")
    if isinstance(creds, dict):
        rt = str(creds.get("refresh_token") or "").strip()
    return email, rt


def _build_dedupe_plan(all_accounts: List[Dict]) -> Dict:
    def sort_key(item):
        raw = item.get("updated_at") or item.get("updatedAt") or ""
        try:
            ts = datetime.fromisoformat(str(raw).replace("Z", "+00:00")).timestamp() if raw else 0.0
        except Exception:
            ts = 0.0
        try:
            item_id = int(item.get("id") or 0)
        except Exception:
            item_id = 0
        return (ts, item_id)

    id_to_item: Dict[int, Dict] = {}
    parent: Dict[int, int] = {}
    key_to_ids: Dict[str, List[int]] = {}

    for item in all_accounts:
        try:
            acc_id = int(item.get("id") or 0)
        except Exception:
            continue
        if acc_id <= 0:
            continue
        id_to_item[acc_id] = item
        parent[acc_id] = acc_id
        email, rt = _account_identity(item)
        if email:
            key_to_ids.setdefault(f"email:{email}", []).append(acc_id)
        if rt:
            key_to_ids.setdefault(f"rt:{rt}", []).append(acc_id)

    def find(x):
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != x:
            nxt = parent[x]
            parent[x] = root
            x = nxt
        return root

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for ids in key_to_ids.values():
        if len(ids) > 1:
            for acc_id in ids[1:]:
                union(ids[0], acc_id)

    components: Dict[int, List[int]] = {}
    for acc_id in id_to_item:
        components.setdefault(find(acc_id), []).append(acc_id)

    dup_groups = [ids for ids in components.values() if len(ids) > 1]
    delete_ids: List[int] = []
    for group_ids in dup_groups:
        group_items = [id_to_item[i] for i in group_ids]
        keep = max(group_items, key=sort_key)
        try:
            keep_id = int(keep.get("id") or 0)
        except Exception:
            keep_id = 0
        delete_ids.extend(i for i in group_ids if i != keep_id)

    return {
        "duplicate_groups": len(dup_groups),
        "duplicate_accounts": sum(len(g) for g in dup_groups),
        "delete_ids": delete_ids,
    }


def _parallel_run(fn, items: List, workers: int = 8) -> Dict:
    ok_ids, fail_ids = [], []
    if not items:
        return {"ok": ok_ids, "fail": fail_ids}
    w = min(workers, len(items))
    with ThreadPoolExecutor(max_workers=w) as ex:
        futs = {ex.submit(fn, i): i for i in items}
        for fut in as_completed(futs):
            i = futs[fut]
            try:
                if fut.result():
                    ok_ids.append(i)
                else:
                    fail_ids.append(i)
            except Exception:
                fail_ids.append(i)
    return {"ok": ok_ids, "fail": fail_ids}


# ============================================================
# Token helpers
# ============================================================
def _list_tokens() -> List[Dict]:
    tokens = []
    for fpath in sorted(TOKENS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        try:
            data = json.loads(fpath.read_text(encoding="utf-8"))
        except Exception:
            continue
        tokens.append({
            "filename": fpath.name,
            "email": data.get("email", fpath.stem),
            "expired": data.get("expired", ""),
            "uploaded_platforms": data.get("uploaded_platforms", []),
            "sync_status": data.get("sync_status", {}),
            "content": data,
        })
    return tokens


# ============================================================
# Task runner (subprocess-based)
# ============================================================


# ============================================================
# FastAPI routes
# ============================================================
@app.get("/", response_class=HTMLResponse)
async def serve_index():
    index_file = WEB_DIR / "index.html"
    if not index_file.exists():
        raise HTTPException(status_code=404, detail="index.html not found")
    return index_file.read_text(encoding="utf-8")


@app.get("/api/logs")
async def sse_logs():
    loop = asyncio.get_running_loop()
    q: asyncio.Queue = asyncio.Queue(maxsize=500)
    with _sse_lock:
        _sse_subscribers.append((loop, q))

    async def event_generator() -> AsyncGenerator[str, None]:
        with _log_lock:
            backlog = list(_log_entries[-100:])
        for entry in backlog:
            yield f"event: log.appended\ndata: {json.dumps({'type': 'log.appended', 'log': entry}, ensure_ascii=False)}\n\n"
        yield f"event: connected\ndata: {json.dumps({'type': 'connected', 'snapshot': _get_snapshot()}, ensure_ascii=False)}\n\n"
        try:
            while True:
                try:
                    payload = await asyncio.wait_for(q.get(), timeout=25)
                    event_type = payload.get("type", "message")
                    yield f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            with _sse_lock:
                try:
                    _sse_subscribers.remove((loop, q))
                except ValueError:
                    pass

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/status")
async def get_status():
    return _get_snapshot()


class StartRequest(BaseModel):
    total_accounts: int = 3
    worker_count: int = 1


@app.post("/api/start")
async def start_task(req: StartRequest):
    global _task_process, _log_reader_thread, _task_log_queue
    _cleanup_task_runtime()
    with _task_lock:
        if _task["status"] not in ("idle",):
            raise HTTPException(status_code=400, detail="任务已在运行")
    run_id = uuid.uuid4().hex[:12]
    cfg = _load_config()
    proxy_enabled = True
    try:
        proxy_enabled = register_module._proxy_runtime_enabled_from_config(cfg)
    except Exception:
        proxy_enabled = True
    proxy = None
    if proxy_enabled:
        proxy = str(cfg.get("proxy", "") or cfg.get("stable_proxy", "") or "").strip() or None
    output_file = str(BASE_DIR / str(cfg.get("output_file", "registered_accounts.txt") or "registered_accounts.txt"))

    _set_task(
        status="running",
        run_id=run_id,
        started_at=datetime.now().isoformat(timespec="seconds"),
        finished_at=None,
        total_accounts=req.total_accounts,
        worker_count=req.worker_count,
        success=0,
        fail=0,
    )
    _push_log("info", f"任务启动: 注册 {req.total_accounts} 个账号, {req.worker_count} 线程", step="start")

    log_queue = _MP_CTX.Queue(maxsize=2000)
    _task_log_queue = log_queue
    _task_process = _MP_CTX.Process(
        target=_worker_process_fn,
        args=(req.total_accounts, req.worker_count, proxy, output_file, log_queue),
        daemon=True,
    )
    _task_process.start()

    _log_reader_thread = threading.Thread(
        target=_log_reader_fn,
        args=(log_queue, _task_process),
        daemon=True,
    )
    _log_reader_thread.start()
    return _get_snapshot()


@app.post("/api/stop")
async def stop_task():
    global _task_process
    with _task_lock:
        if _task["status"] == "idle":
            raise HTTPException(status_code=400, detail="没有运行中的任务")
    _cleanup_task_runtime(force=True, kill=True)
    _push_log("warn", "任务已强制终止", step="stopped")
    _set_task(status="idle", finished_at=datetime.now().isoformat(timespec="seconds"))
    return _get_snapshot()


@app.get("/api/tokens")
async def list_tokens_api():
    tokens = await run_in_threadpool(_list_tokens)
    return {"tokens": tokens}


@app.delete("/api/tokens/{filename}")
async def delete_token(filename: str):
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="无效文件名")
    fpath = TOKENS_DIR / filename
    if not fpath.exists():
        raise HTTPException(status_code=404, detail="文件不存在")
    fpath.unlink()
    return {"ok": True}


@app.get("/api/config")
async def get_config():
    cfg = _load_config()
    safe = dict(cfg)
    if safe.get("sub2api_password"):
        safe["sub2api_password"] = "**masked**"
    if safe.get("cpa_management_key"):
        safe["cpa_management_key"] = "**masked**"
    if safe.get("duckmail_bearer"):
        safe["duckmail_bearer_preview"] = safe["duckmail_bearer"][:20] + "..."
        safe["duckmail_bearer"] = "**masked**"
    if safe.get("proxy_list_bearer"):
        safe["proxy_list_bearer_preview"] = safe["proxy_list_bearer"][:20] + "..."
        safe["proxy_list_bearer"] = "**masked**"
    return safe


@app.post("/api/config")
async def save_config_api(body: Dict[str, Any]):
    cfg = _load_config()
    sub2api_keys = {
        "sub2api_base_url", "sub2api_bearer", "sub2api_email",
        "sub2api_password", "sub2api_min_candidates",
        "sub2api_proxy_id", "sub2api_proxy_name", "sub2api_auto_assign_proxy",
    }
    if body.get("sub2api_password") == "**masked**":
        body.pop("sub2api_password")
    if body.get("cpa_management_key") == "**masked**":
        body.pop("cpa_management_key")
    if body.get("duckmail_bearer") == "**masked**":
        body.pop("duckmail_bearer")
    if body.get("proxy_list_bearer") == "**masked**":
        body.pop("proxy_list_bearer")
    body.pop("duckmail_bearer_preview", None)
    body.pop("proxy_list_bearer_preview", None)
    cfg.update(body)
    await run_in_threadpool(_save_config, cfg)
    # Invalidate bearer cache if credentials changed
    new_bearer = str(body.get("sub2api_bearer", "") or "").strip()
    if new_bearer:
        _sub2api_bearer_cache[0] = new_bearer
    elif "sub2api_email" in body or "sub2api_password" in body:
        _sub2api_bearer_cache[0] = ""
    if any(key in body for key in sub2api_keys):
        with _sub2api_pool_status_lock:
            _sub2api_pool_status_cache.update({"ts": 0.0, "signature": "", "data": None})
    return {"ok": True}


# Sub2Api pool status
@app.get("/api/sub2api/pool/status")
async def sub2api_pool_status():
    cfg = _load_config()
    if not str(cfg.get("sub2api_base_url", "") or "").strip():
        return {"configured": False}
    signature = _sub2api_cfg_signature(cfg)
    now = time.time()
    with _sub2api_pool_status_lock:
        cached = copy.deepcopy(_sub2api_pool_status_cache.get("data"))
        cached_sig = _sub2api_pool_status_cache.get("signature", "")
        cached_ts = float(_sub2api_pool_status_cache.get("ts", 0.0) or 0.0)
    if cached and cached_sig == signature and (now - cached_ts) < _SUB2API_POOL_STATUS_TTL_SECONDS:
        return cached
    try:
        page_data = await run_in_threadpool(_sub2api_fetch_page, cfg, 1, 20, 12)
        items = page_data.get("items") or []
        total = int(page_data.get("total", 0) or 0)
        sample_count = len(items)
        sample_error = sum(1 for a in items if _is_abnormal(a.get("status")))
        if total <= 0:
            total = sample_count
        if sample_count > 0 and total > sample_count:
            error = int(round(sample_error / sample_count * total))
        else:
            error = sample_error
        error = max(0, min(total, error))
        normal = max(0, total - error)
        threshold = int(cfg.get("sub2api_min_candidates", 200) or 200)
        pct = round(normal / threshold * 100, 1) if threshold > 0 else 100.0
        data = {
            "configured": True,
            "total": total,
            "candidates": normal,
            "error_count": error,
            "threshold": threshold,
            "healthy": normal >= threshold,
            "percent": pct,
            "error": None,
            "estimated": total > sample_count,
            "sample_size": sample_count,
        }
        with _sub2api_pool_status_lock:
            _sub2api_pool_status_cache.update({"ts": now, "signature": signature, "data": copy.deepcopy(data)})
        return data
    except Exception as e:
        if cached and cached_sig == signature:
            stale = copy.deepcopy(cached)
            stale["stale"] = True
            stale["error"] = stale.get("error") or str(e)
            return stale
        return {
            "configured": True,
            "total": 0, "candidates": 0, "error_count": 0,
            "threshold": int(cfg.get("sub2api_min_candidates", 200) or 200),
            "healthy": False, "percent": 0, "error": str(e),
        }


@app.post("/api/sub2api/pool/check")
async def sub2api_pool_check():
    cfg = _load_config()
    if not str(cfg.get("sub2api_base_url", "") or "").strip():
        return {"ok": False, "message": "Sub2Api 未配置地址"}
    try:
        page_data = await run_in_threadpool(_sub2api_fetch_page, cfg, 1, 1, 12)
        total = int(page_data.get("total", 0) or 0)
        sample = len(page_data.get("items") or [])
        return {
            "ok": True,
            "total": total,
            "normal": total if total > 0 else sample,
            "error": 0,
            "message": f"连接成功，接口可访问，账号总数 {total if total > 0 else '已返回数据'}",
        }
    except Exception as e:
        return {"ok": False, "message": f"连接失败: {e}"}


@app.post("/api/sub2api/pool/maintain")
async def sub2api_pool_maintain():
    cfg = _load_config()
    if not str(cfg.get("sub2api_base_url", "") or "").strip():
        raise HTTPException(status_code=400, detail="Sub2Api 未配置")

    def _maintain():
        t0 = time.time()
        all_accounts = _sub2api_list_all(cfg)
        error_ids = [
            int(a.get("id") or 0) for a in all_accounts
            if _is_abnormal(a.get("status")) and int(a.get("id") or 0) > 0
        ]
        refreshed = _parallel_run(lambda i: _sub2api_refresh_account(cfg, i), error_ids, 8)
        if refreshed["ok"]:
            time.sleep(2)
        all_after = _sub2api_list_all(cfg)
        still_error_ids = [
            int(a.get("id") or 0) for a in all_after
            if _is_abnormal(a.get("status")) and int(a.get("id") or 0) > 0
        ]
        plan = _build_dedupe_plan(all_after)
        del_ids = list(set(still_error_ids + plan["delete_ids"]))
        deleted = _parallel_run(lambda i: _sub2api_delete_account(cfg, i), del_ids, 12)
        return {
            "total": len(all_after),
            "error_count": len(still_error_ids),
            "refreshed": len(refreshed["ok"]),
            "duplicate_groups": plan["duplicate_groups"],
            "duplicate_accounts": plan["duplicate_accounts"],
            "deleted_ok": len(deleted["ok"]),
            "deleted_fail": len(deleted["fail"]),
            "duration_ms": int((time.time() - t0) * 1000),
        }

    return await run_in_threadpool(_maintain)


class DedupRequest(BaseModel):
    dry_run: bool = True
    timeout: int = 20


@app.post("/api/sub2api/pool/dedupe")
async def sub2api_pool_dedupe(req: DedupRequest):
    cfg = _load_config()
    if not str(cfg.get("sub2api_base_url", "") or "").strip():
        raise HTTPException(status_code=400, detail="Sub2Api 未配置")

    def _dedupe():
        all_accounts = _sub2api_list_all(cfg)
        plan = _build_dedupe_plan(all_accounts)
        deleted_ok = deleted_fail = 0
        if not req.dry_run and plan["delete_ids"]:
            result = _parallel_run(lambda i: _sub2api_delete_account(cfg, i), plan["delete_ids"], 12)
            deleted_ok = len(result["ok"])
            deleted_fail = len(result["fail"])
        return {
            "dry_run": req.dry_run,
            "total": len(all_accounts),
            "duplicate_groups": plan["duplicate_groups"],
            "duplicate_accounts": plan["duplicate_accounts"],
            "to_delete": len(plan["delete_ids"]),
            "deleted_ok": deleted_ok,
            "deleted_fail": deleted_fail,
        }

    return await run_in_threadpool(_dedupe)


@app.get("/api/sub2api/accounts")
async def sub2api_accounts(
    page: int = 1,
    page_size: int = 20,
    status: str = "all",
    keyword: str = "",
):
    cfg = _load_config()
    if not str(cfg.get("sub2api_base_url", "") or "").strip():
        return {
            "configured": False, "items": [], "total": 0,
            "filtered_total": 0, "page": 1, "page_size": page_size, "total_pages": 1,
        }
    try:
        if status == "all" and not keyword.strip():
            page_data = await run_in_threadpool(_sub2api_fetch_page, cfg, page, page_size, 20)
            result = []
            for a in page_data["items"]:
                email, _ = _account_identity(a)
                try:
                    acc_id = int(a.get("id") or 0)
                except Exception:
                    acc_id = 0
                result.append({
                    "id": acc_id,
                    "email": email or str(a.get("name", "")),
                    "name": str(a.get("name", "")),
                    "status": str(a.get("status", "unknown")).lower(),
                    "updated_at": a.get("updated_at") or a.get("updatedAt") or "",
                    "created_at": a.get("created_at") or a.get("createdAt") or "",
                    "is_duplicate": False,
                })
            return {
                "configured": True,
                "items": result,
                "total": page_data["total"],
                "filtered_total": page_data["total"],
                "page": page_data["page"],
                "page_size": page_data["page_size"],
                "total_pages": page_data["total_pages"],
            }

        def _fetch():
            all_accounts = _sub2api_list_all(cfg)
            kw = keyword.strip().lower()
            filtered = []
            for a in all_accounts:
                a_status = str(a.get("status", "")).lower()
                if status == "normal" and _is_abnormal(a_status):
                    continue
                if status == "abnormal" and not _is_abnormal(a_status):
                    continue
                if status == "error" and a_status != "error":
                    continue
                if status == "disabled" and a_status != "disabled":
                    continue
                if kw:
                    email, _ = _account_identity(a)
                    name = str(a.get("name", "")).lower()
                    a_id = str(a.get("id", ""))
                    if kw not in email and kw not in name and kw not in a_id:
                        continue
                filtered.append(a)

            filtered.sort(
                key=lambda a: str(a.get("updated_at") or a.get("updatedAt") or ""),
                reverse=True,
            )
            filtered_total = len(filtered)
            pg = max(1, page)
            ps = max(1, min(page_size, 200))
            start = (pg - 1) * ps
            page_items = filtered[start:start + ps]
            total_pages = max(1, (filtered_total + ps - 1) // ps)

            result = []
            for a in page_items:
                email, _ = _account_identity(a)
                try:
                    acc_id = int(a.get("id") or 0)
                except Exception:
                    acc_id = 0
                result.append({
                    "id": acc_id,
                    "email": email or str(a.get("name", "")),
                    "name": str(a.get("name", "")),
                    "status": str(a.get("status", "unknown")).lower(),
                    "updated_at": a.get("updated_at") or a.get("updatedAt") or "",
                    "created_at": a.get("created_at") or a.get("createdAt") or "",
                    "is_duplicate": False,
                })
            return {
                "configured": True,
                "items": result,
                "total": len(all_accounts),
                "filtered_total": filtered_total,
                "page": pg,
                "page_size": ps,
                "total_pages": total_pages,
            }

        return await run_in_threadpool(_fetch)
    except Exception as e:
        return {
            "configured": True, "error": str(e),
            "items": [], "total": 0, "filtered_total": 0,
            "page": 1, "page_size": page_size, "total_pages": 1,
        }


class ProbeRequest(BaseModel):
    account_ids: List[int] = []
    timeout: int = 30


@app.post("/api/sub2api/accounts/probe")
async def sub2api_accounts_probe(req: ProbeRequest):
    cfg = _load_config()
    if not str(cfg.get("sub2api_base_url", "") or "").strip():
        raise HTTPException(status_code=400, detail="Sub2Api 未配置")
    ids = [i for i in req.account_ids if isinstance(i, int) and i > 0]
    if not ids:
        raise HTTPException(status_code=400, detail="请提供账号 ID 列表")

    def _probe():
        result = _parallel_run(lambda i: _sub2api_refresh_account(cfg, i), ids, 8)
        if result["ok"]:
            time.sleep(2)
        return {
            "requested": len(ids),
            "refreshed_ok": len(result["ok"]),
            "refreshed_fail": len(result["fail"]),
            "recovered": len(result["ok"]),
            "still_abnormal": len(result["fail"]),
        }

    return await run_in_threadpool(_probe)


class HandleExceptionRequest(BaseModel):
    account_ids: List[int] = []
    timeout: int = 30
    delete_unresolved: bool = True


@app.post("/api/sub2api/accounts/handle-exception")
async def sub2api_handle_exception(req: HandleExceptionRequest):
    cfg = _load_config()
    if not str(cfg.get("sub2api_base_url", "") or "").strip():
        raise HTTPException(status_code=400, detail="Sub2Api 未配置")

    def _handle():
        ids = [i for i in req.account_ids if isinstance(i, int) and i > 0]
        if not ids:
            all_acc = _sub2api_list_all(cfg)
            ids = [
                int(a.get("id") or 0) for a in all_acc
                if _is_abnormal(a.get("status")) and int(a.get("id") or 0) > 0
            ]
        targeted = len(ids)
        refreshed = _parallel_run(lambda i: _sub2api_refresh_account(cfg, i), ids, 8)
        if refreshed["ok"]:
            time.sleep(2)
        deleted_ok = deleted_fail = 0
        if req.delete_unresolved and refreshed["fail"]:
            deleted = _parallel_run(lambda i: _sub2api_delete_account(cfg, i), refreshed["fail"], 12)
            deleted_ok = len(deleted["ok"])
            deleted_fail = len(deleted["fail"])
        return {
            "targeted": targeted,
            "refreshed_ok": len(refreshed["ok"]),
            "refreshed_fail": len(refreshed["fail"]),
            "recovered": len(refreshed["ok"]),
            "remaining_abnormal": max(0, len(refreshed["fail"]) - deleted_ok),
            "deleted_ok": deleted_ok,
            "deleted_fail": deleted_fail,
        }

    return await run_in_threadpool(_handle)


class DeleteRequest(BaseModel):
    account_ids: List[int] = []
    timeout: int = 20


@app.post("/api/sub2api/accounts/delete")
async def sub2api_accounts_delete(req: DeleteRequest):
    cfg = _load_config()
    if not str(cfg.get("sub2api_base_url", "") or "").strip():
        raise HTTPException(status_code=400, detail="Sub2Api 未配置")
    ids = [i for i in req.account_ids if isinstance(i, int) and i > 0]
    if not ids:
        raise HTTPException(status_code=400, detail="请提供账号 ID 列表")

    result = await run_in_threadpool(
        lambda: _parallel_run(lambda i: _sub2api_delete_account(cfg, i), ids, 12)
    )
    return {
        "requested": len(ids),
        "deleted_ok": len(result["ok"]),
        "deleted_fail": len(result["fail"]),
        "deleted_ok_ids": result["ok"],
        "failed_ids": result["fail"],
    }


# ============================================================
# CPA (CLIProxyAPI) routes
# ============================================================

@app.get("/api/cpa/pool/status")
async def cpa_pool_status():
    cfg = _load_config()
    if not str(cfg.get("cpa_base_url", "") or "").strip():
        return {"configured": False}
    try:
        all_files = await run_in_threadpool(_cpa_list_all, cfg)
        total = len(all_files)
        error = sum(1 for f in all_files if _cpa_is_abnormal(f))
        normal = total - error
        threshold = int(cfg.get("cpa_min_candidates", 200) or 200)
        pct = round(normal / threshold * 100, 1) if threshold > 0 else 100.0
        return {
            "configured": True,
            "total": total,
            "candidates": normal,
            "error_count": error,
            "threshold": threshold,
            "healthy": normal >= threshold,
            "percent": pct,
            "error": None,
        }
    except Exception as e:
        return {
            "configured": True,
            "total": 0, "candidates": 0, "error_count": 0,
            "threshold": int(cfg.get("cpa_min_candidates", 200) or 200),
            "healthy": False, "percent": 0, "error": str(e),
        }


@app.post("/api/cpa/pool/check")
async def cpa_pool_check():
    cfg = _load_config()
    if not str(cfg.get("cpa_base_url", "") or "").strip():
        return {"ok": False, "message": "CPA 未配置地址"}
    try:
        all_files = await run_in_threadpool(_cpa_list_all, cfg)
        total = len(all_files)
        error = sum(1 for f in all_files if _cpa_is_abnormal(f))
        normal = total - error
        return {
            "ok": True,
            "total": total, "normal": normal, "error": error,
            "message": f"连接成功，共 {total} 个 auth file，{normal} 正常，{error} 异常",
        }
    except Exception as e:
        return {"ok": False, "message": f"连接失败: {e}"}


@app.get("/api/cpa/accounts")
async def cpa_accounts(
    page: int = 1,
    page_size: int = 20,
    status: str = "all",
    keyword: str = "",
):
    cfg = _load_config()
    if not str(cfg.get("cpa_base_url", "") or "").strip():
        return {
            "configured": False, "items": [], "total": 0,
            "filtered_total": 0, "page": 1, "page_size": page_size, "total_pages": 1,
        }
    try:
        def _fetch():
            all_files = _cpa_list_all(cfg)
            kw = keyword.strip().lower()
            filtered = []
            for f in all_files:
                is_bad = _cpa_is_abnormal(f)
                if status == "normal" and is_bad:
                    continue
                if status == "abnormal" and not is_bad:
                    continue
                if status == "disabled" and not f.get("disabled"):
                    continue
                if status == "unavailable" and not f.get("unavailable"):
                    continue
                if kw:
                    name = str(f.get("name", "")).lower()
                    email = str(f.get("email", "")).lower()
                    if kw not in name and kw not in email:
                        continue
                filtered.append(f)

            filtered.sort(
                key=lambda x: str(x.get("updated_at") or x.get("modtime") or ""),
                reverse=True,
            )
            filtered_total = len(filtered)
            pg = max(1, page)
            ps = max(1, min(page_size, 200))
            start = (pg - 1) * ps
            page_items = filtered[start:start + ps]
            total_pages = max(1, (filtered_total + ps - 1) // ps)

            result = []
            for item in page_items:
                result.append({
                    "name": str(item.get("name", "")),
                    "email": str(item.get("email", "") or str(item.get("name", "")).replace(".json", "")),
                    "status": str(item.get("status", "unknown")).lower(),
                    "disabled": bool(item.get("disabled")),
                    "unavailable": bool(item.get("unavailable")),
                    "provider": str(item.get("provider", "")),
                    "account_type": str(item.get("account_type", "")),
                    "updated_at": item.get("updated_at") or item.get("modtime") or "",
                    "created_at": item.get("created_at") or "",
                    "last_refresh": item.get("last_refresh") or "",
                    "status_message": str(item.get("status_message", "")),
                })
            return {
                "configured": True,
                "items": result,
                "total": len(all_files),
                "filtered_total": filtered_total,
                "page": pg,
                "page_size": ps,
                "total_pages": total_pages,
            }

        return await run_in_threadpool(_fetch)
    except Exception as e:
        return {
            "configured": True, "error": str(e),
            "items": [], "total": 0, "filtered_total": 0,
            "page": 1, "page_size": page_size, "total_pages": 1,
        }


class CpaDeleteRequest(BaseModel):
    filenames: List[str] = []


@app.post("/api/cpa/accounts/delete")
async def cpa_accounts_delete(req: CpaDeleteRequest):
    cfg = _load_config()
    if not str(cfg.get("cpa_base_url", "") or "").strip():
        raise HTTPException(status_code=400, detail="CPA 未配置")
    names = [n for n in req.filenames if n and "/" not in n and "\\" not in n]
    if not names:
        raise HTTPException(status_code=400, detail="请提供文件名列表")

    result = await run_in_threadpool(
        lambda: _parallel_run(lambda n: _cpa_delete_auth_file(cfg, n), names, 12)
    )
    return {
        "requested": len(names),
        "deleted_ok": len(result["ok"]),
        "deleted_fail": len(result["fail"]),
        "deleted_ok_names": result["ok"],
        "failed_names": result["fail"],
    }


# ============================================================
# Entrypoint
# ============================================================
if __name__ == "__main__":
    print("=" * 50)
    print("  Auto CPA Register 管理界面")
    print("  访问: http://localhost:18421")
    print("  按 Ctrl+C 退出")
    print("=" * 50)
    try:
        uvicorn.run(app, host="0.0.0.0", port=18421, log_level="warning")
    except KeyboardInterrupt:
        pass
    finally:
        _cleanup_task_runtime(force=True, kill=True)
    sys.exit(0)
