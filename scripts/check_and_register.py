import os
import sys
import json
from pathlib import Path

from curl_cffi import requests as cffi

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import simple_register as reg
from sync_manager import AccountSyncManager


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except Exception as exc:
        raise ValueError(f"{name} 必须是整数: {raw}") from exc
    if value <= 0:
        raise ValueError(f"{name} 必须大于 0: {raw}")
    return value


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _write_github_output(key: str, value) -> None:
    path = str(os.getenv("GITHUB_OUTPUT", "") or "").strip()
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"{key}={value}\n")
    except Exception:
        pass


def _append_step_summary(lines) -> None:
    path = str(os.getenv("GITHUB_STEP_SUMMARY", "") or "").strip()
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write("\n".join(str(line) for line in lines))
            f.write("\n")
    except Exception:
        pass


def _get_sub2api_token() -> str:
    bearer = str(os.getenv("SUB2API_BEARER", "") or "").strip()
    if bearer:
        return bearer
    token = reg._sub2api_login()
    return str(token or "").strip()


def _fetch_sub2api_total(token: str) -> int:
    base_url = str(os.getenv("SUB2API_BASE_URL", "") or "").strip().rstrip("/")
    if not base_url:
        raise ValueError("SUB2API_BASE_URL 未配置")

    headers = {"Accept": "application/json"}
    if token:
        # Sub2Api uses X-API-Key header (not Bearer)
        headers["X-API-Key"] = token

    resp = cffi.get(
        f"{base_url}/api/v1/admin/accounts",
        params={"page": 1, "page_size": 1, "platform": "openai", "type": "oauth"},
        headers=headers,
        timeout=20,
        impersonate="chrome131",
    )
    resp.raise_for_status()

    data = resp.json()
    payload = data.get("data") if isinstance(data, dict) and isinstance(data.get("data"), dict) else data
    if not isinstance(payload, dict):
        payload = {}

    try:
        return int(payload.get("total", 0) or 0)
    except Exception:
        return 0


def main() -> int:
    min_count = _env_int("SUB2API_MIN_COUNT", 2000)
    batch_size = _env_int("TOPUP_BATCH_SIZE", 10)
    max_topup = _env_int("TOPUP_MAX_COUNT", 100)
    max_workers = _env_int("REGISTER_MAX_WORKERS", 3)
    force_run = _env_bool("FORCE_REGISTER", False)

    output_file = str(
        os.getenv("REGISTER_OUTPUT_FILE")
        or os.getenv("OUTPUT_FILE")
        or "registered_accounts.txt"
    ).strip() or "registered_accounts.txt"

    proxy = str(
        os.getenv("REGISTER_PROXY")
        or os.getenv("PROXY")
        or os.getenv("STABLE_PROXY")
        or ""
    ).strip() or None

    token = _get_sub2api_token()
    total = _fetch_sub2api_total(token)
    manual_total_accounts = 0
    raw_manual_total = str(os.getenv("MANUAL_TOTAL_ACCOUNTS", "") or "").strip()
    if raw_manual_total:
        try:
            manual_total_accounts = int(raw_manual_total)
        except Exception as exc:
            raise ValueError(f"MANUAL_TOTAL_ACCOUNTS 必须是整数: {raw_manual_total}") from exc
        if manual_total_accounts <= 0:
            raise ValueError(f"MANUAL_TOTAL_ACCOUNTS 必须大于 0: {raw_manual_total}")

    print(
        "[Action] sub2api_status("
        f"total={total}, min_count={min_count}, "
        f"force_run={force_run}, manual_total_accounts={manual_total_accounts})"
    )

    if not force_run and total >= min_count:
        print("[Action] 账号池充足，跳过注册")
        _write_github_output("sub2api_total", total)
        _write_github_output("sub2api_min_count", min_count)
        _write_github_output("need_register", "false")
        _write_github_output("planned_total_accounts", 0)
        _write_github_output("result", "skipped")
        _append_step_summary([
            "## Register Automation",
            "",
            f"- Result: skipped",
            f"- Sub2Api total: {total}",
            f"- Threshold: {min_count}",
            f"- Force run: {force_run}",
            f"- Manual total accounts: {manual_total_accounts or 0}",
        ])
        return 0

    if manual_total_accounts > 0:
        need = max(0, min_count - total)
        total_accounts = manual_total_accounts
    else:
        need = max(0, min_count - total)
        total_accounts = min(max(batch_size, need), max_topup)

    print(
        "[Action] run_batch("
        f"total_accounts={total_accounts}, "
        f"need={need}, "
        f"max_workers={max_workers}, "
        f"proxy={'set' if proxy else 'unset'}, "
        f"output_file={output_file})"
    )

    _write_github_output("sub2api_total", total)
    _write_github_output("sub2api_min_count", min_count)
    _write_github_output("need_register", "true")
    _write_github_output("planned_total_accounts", total_accounts)
    _write_github_output("result", "triggered")
    _append_step_summary([
        "## Register Automation",
        "",
        f"- Result: triggered",
        f"- Sub2Api total: {total}",
        f"- Threshold: {min_count}",
        f"- Need gap: {need}",
        f"- Planned register count: {total_accounts}",
        f"- Max workers: {max_workers}",
        f"- Force run: {force_run}",
        f"- Manual total accounts: {manual_total_accounts or 0}",
    ])

    reg.run_batch(
        total_accounts=total_accounts,
        output_file=output_file,
        max_workers=max_workers,
        proxy=proxy,
    )

    # 注意: 实时同步已在 simple_register.py 的 _register_one() 中完成
    # 这里不再重复同步

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        _write_github_output("result", "failed")
        _append_step_summary([
            "## Register Automation",
            "",
            f"- Result: failed",
            f"- Error: {exc}",
        ])
        print(f"[Action] 执行失败: {exc}", file=sys.stderr)
        raise
