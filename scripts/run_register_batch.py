import os
import sys

import ncs_register as reg


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


def main() -> int:
    total_accounts = _env_int("REGISTER_TOTAL_ACCOUNTS", 3)
    max_workers = _env_int("REGISTER_MAX_WORKERS", 3)
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

    cpa_cleanup = _env_bool("REGISTER_CPA_CLEANUP", False)

    print(
        "[Action] run_batch("
        f"total_accounts={total_accounts}, "
        f"max_workers={max_workers}, "
        f"proxy={'set' if proxy else 'unset'}, "
        f"output_file={output_file}, "
        f"cpa_cleanup={cpa_cleanup})"
    )

    reg.run_batch(
        total_accounts=total_accounts,
        output_file=output_file,
        max_workers=max_workers,
        proxy=proxy,
        cpa_cleanup=cpa_cleanup,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[Action] 执行失败: {exc}", file=sys.stderr)
        raise
