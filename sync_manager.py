"""
统一账号同步管理器 - 支持 D1、CPA 和 Sub2Api 平台
依赖: pip install curl_cffi
"""

import os
import json
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.parse import quote

from curl_cffi import requests as curl_requests


def _load_config() -> dict:
    """从 config.json 加载配置"""
    config_path = Path(__file__).parent / "config.json"
    if config_path.exists():
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _as_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _normalize_email(email: str) -> str:
    return str(email or "").strip().lower()


def _chunked(items: List[Dict[str, str]], chunk_size: int) -> Iterable[List[Dict[str, str]]]:
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def _load_registered_accounts(accounts_file: str) -> Dict[str, str]:
    accounts: Dict[str, str] = {}
    path = Path(accounts_file)
    if not path.exists():
        return accounts

    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line:
                    continue
                parts = line.split("----")
                if len(parts) < 2:
                    continue
                email = _normalize_email(parts[0])
                password = parts[1].strip()
                if email and password:
                    accounts[email] = password
    except Exception:
        return {}
    return accounts


class AccountSyncManager:
    """统一账号同步管理器"""

    def __init__(self):
        self.session = curl_requests.Session(impersonate="chrome131")
        config = _load_config()

        # D1 配置 (环境变量优先，config.json 作为备选)
        self.auto_upload_d1 = _as_bool(os.getenv("AUTO_UPLOAD_D1", config.get("auto_upload_d1", False)))
        self.d1_api_base_url = (
            os.getenv("D1_API_BASE_URL")
            or config.get("d1_api_base_url", "")
        ).strip().rstrip("/")
        self.d1_api_key = (
            os.getenv("D1_API_KEY")
            or config.get("d1_api_key", "")
        ).strip()

        # CPA 配置 (环境变量优先，config.json 作为备选)
        self.cpa_base_url = os.getenv("CPA_BASE_URL") or config.get("cpa_base_url", "https://cpa.xspace.icu")
        self.cpa_management_key = os.getenv("CPA_MANAGEMENT_KEY") or config.get("cpa_management_key", "")

        # Sub2Api 配置 (环境变量优先，config.json 作为备选)
        self.sub2api_base_url = os.getenv("SUB2API_BASE_URL") or config.get("sub2api_base_url", "https://sub2api.xspace.icu")
        # 支持三种认证方式:
        # 1. SUB2API_BEARER / SUB2API_ADMIN_KEY - 静态 Bearer token
        # 2. SUB2API_EMAIL + SUB2API_PASSWORD - 动态登录获取 token
        self.sub2api_bearer = os.getenv("SUB2API_BEARER") or os.getenv("SUB2API_ADMIN_KEY") or config.get("sub2api_bearer", "")
        self.sub2api_email = os.getenv("SUB2API_EMAIL") or config.get("sub2api_email", "")
        self.sub2api_password = os.getenv("SUB2API_PASSWORD") or config.get("sub2api_password", "")
        self.sub2api_token = ""  # 动态获取的 JWT token
        group_ids_str = os.getenv("SUB2API_GROUP_IDS", "")
        if group_ids_str:
            self.sub2api_group_ids = [int(x) for x in group_ids_str.split(",") if x.strip()]
        else:
            self.sub2api_group_ids = config.get("sub2api_group_ids", [2])

        # 配置是否启用
        self.enable_d1 = self.auto_upload_d1 and bool(self.d1_api_base_url) and bool(self.d1_api_key)
        self.enable_cpa = bool(self.cpa_management_key)
        self.enable_sub2api = bool(self.sub2api_bearer) or (bool(self.sub2api_email) and bool(self.sub2api_password))

        # 调试信息
        if self.enable_d1:
            print(f"[Sync] D1 配置: {self.d1_api_base_url}")
        elif self.auto_upload_d1:
            print("[Sync] D1 已启用但缺少 D1_API_BASE_URL 或 D1_API_KEY")
        else:
            print("[Sync] D1 未启用")

        if self.sub2api_bearer:
            print(f"[Sync] Sub2Api 配置: {self.sub2api_base_url} | Key: {self.sub2api_bearer[:15]}...")
        elif self.sub2api_email:
            print(f"[Sync] Sub2Api 配置: {self.sub2api_base_url} | Email: {self.sub2api_email}")
        else:
            print("[Sync] Sub2Api 未配置或 Key 为空")

    def _print(self, msg: str):
        print(f"[Sync] {msg}")

    # ==================== D1 相关 ====================

    def _d1_request(self, method: str, path: str, **kwargs) -> dict:
        """发送请求到 D1 Worker API"""
        if not self.enable_d1:
            raise RuntimeError("D1 未配置或未启用")

        url = f"{self.d1_api_base_url}{path}"
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self.d1_api_key}"
        headers["Accept"] = "application/json"
        if "json" in kwargs:
            headers.setdefault("Content-Type", "application/json")

        resp = self.session.request(method, url, headers=headers, timeout=30, **kwargs)
        if 200 <= resp.status_code < 300:
            try:
                return resp.json()
            except Exception:
                return {}

        body = ""
        try:
            body = resp.text[:300]
        except Exception:
            body = "无法读取响应"
        raise RuntimeError(f"D1 请求失败: {resp.status_code} - {body}")

    def upsert_account_to_d1(self, email: str, password: str) -> bool:
        """写入单个账号到 D1"""
        if not self.enable_d1:
            return False

        normalized_email = _normalize_email(email)
        if not normalized_email or password is None or str(password) == "":
            self._print("D1 写入跳过: 邮箱或密码为空")
            return False

        try:
            self._d1_request(
                "PUT",
                "/v1/accounts",
                json={"email": normalized_email, "password": str(password)},
            )
            self._print(f"D1 写入成功: {normalized_email}")
            return True
        except Exception as e:
            self._print(f"D1 写入失败 {normalized_email}: {e}")
            return False

    def batch_upsert_accounts_to_d1(self, accounts: List[Dict[str, str]], batch_size: int = 50) -> Dict:
        """批量写入账号到 D1，批量失败时回退单条写入"""
        results = {"success": 0, "failed": 0, "accounts": [], "failed_accounts": []}
        if not self.enable_d1:
            return results

        deduped: Dict[str, str] = {}
        for item in accounts:
            email = _normalize_email(item.get("email", ""))
            password = item.get("password")
            if not email or password is None or str(password) == "":
                results["failed"] += 1
                results["failed_accounts"].append(email or item.get("email", ""))
                continue
            deduped[email] = str(password)

        items = [{"email": email, "password": password} for email, password in deduped.items()]
        if not items:
            return results

        for chunk in _chunked(items, batch_size):
            try:
                self._d1_request("POST", "/v1/accounts/batch", json={"accounts": chunk})
                results["success"] += len(chunk)
                results["accounts"].extend(item["email"] for item in chunk)
                self._print(f"D1 批量写入成功: {len(chunk)} 条")
            except Exception as e:
                self._print(f"D1 批量写入失败，回退单条重试: {e}")
                for item in chunk:
                    if self.upsert_account_to_d1(item["email"], item["password"]):
                        results["success"] += 1
                        results["accounts"].append(item["email"])
                    else:
                        results["failed"] += 1
                        results["failed_accounts"].append(item["email"])

        return results

    def get_d1_account(self, email: str) -> Optional[Dict]:
        """按邮箱查询 D1 账号"""
        if not self.enable_d1:
            return None

        normalized_email = _normalize_email(email)
        if not normalized_email:
            return None

        try:
            result = self._d1_request("GET", f"/v1/accounts/{quote(normalized_email, safe='')}")
        except Exception as e:
            self._print(f"D1 查询失败 {normalized_email}: {e}")
            return None
        return result.get("account")

    def list_d1_accounts(self, limit: int = 20) -> List[Dict]:
        """列出最近写入的 D1 账号"""
        if not self.enable_d1:
            return []

        safe_limit = max(1, min(int(limit), 100))
        try:
            result = self._d1_request("GET", "/v1/accounts", params={"limit": safe_limit})
        except Exception as e:
            self._print(f"D1 列表查询失败: {e}")
            return []
        accounts = result.get("accounts")
        return accounts if isinstance(accounts, list) else []

    # ==================== Sub2Api 认证 ====================

    def _sub2api_login(self) -> Optional[str]:
        """使用邮箱密码登录获取 JWT token"""
        if not self.sub2api_email or not self.sub2api_password:
            return None
        try:
            login_url = f"{self.sub2api_base_url.rstrip('/')}/api/v1/auth/login"
            resp = self.session.post(
                login_url,
                json={"email": self.sub2api_email, "password": self.sub2api_password},
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            if resp.status_code == 200:
                data = resp.json()
                token = data.get("data", {}).get("access_token", "")
                if token:
                    self._print("Sub2Api 登录成功，获取到 token")
                    return token
            self._print(f"Sub2Api 登录失败: {resp.status_code} - {resp.text[:100]}")
            return None
        except Exception as e:
            self._print(f"Sub2Api 登录异常: {e}")
            return None

    def _get_sub2api_token(self) -> Optional[str]:
        """获取 Sub2Api 认证 token (优先使用静态 Bearer，否则动态登录)"""
        if self.sub2api_bearer:
            return self.sub2api_bearer
        if self.sub2api_token:
            return self.sub2api_token
        if self.sub2api_email and self.sub2api_password:
            self.sub2api_token = self._sub2api_login()
            return self.sub2api_token
        return None

    def _get_sub2api_headers(self) -> Dict:
        """获取 Sub2Api 请求头"""
        token = self._get_sub2api_token()
        if token:
            return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        return {}

    # ==================== CPA 相关 ====================

    def upload_to_cpa(self, token_path: str) -> bool:
        """上传 token JSON 到 CPA"""
        if not self.enable_cpa:
            self._print("CPA 未配置，跳过上传")
            return False

        if not Path(token_path).exists():
            self._print(f"CPA 文件不存在: {token_path}")
            return False

        try:
            from curl_cffi import CurlMime

            upload_url = f"{self.cpa_base_url.rstrip('/')}/v0/management/auth-files"

            mp = CurlMime()
            mp.addpart(
                name="file",
                content_type="application/json",
                filename=Path(token_path).name,
                local_path=token_path,
            )

            resp = self.session.post(
                upload_url,
                multipart=mp,
                headers={"Authorization": f"Bearer {self.cpa_management_key}"},
                verify=False,
                timeout=30
            )

            if resp.status_code == 200:
                self._print(f"CPA 上传成功: {Path(token_path).name}")
                return True
            else:
                self._print(f"CPA 上传失败: {resp.status_code}")
                return False
        except Exception as e:
            self._print(f"CPA 上传异常: {e}")
            return False
        finally:
            if "mp" in locals():
                mp.close()

    # ==================== Sub2Api 相关 ====================

    def upload_to_sub2api(self, email: str, password: str, access_token: str = None,
                          refresh_token: str = None) -> bool:
        """上传账号到 Sub2Api"""
        if not self.enable_sub2api:
            self._print("Sub2Api 未配置，跳过上传")
            return False

        try:
            url = f"{self.sub2api_base_url.rstrip('/')}/api/v1/admin/accounts"

            credentials = {}
            if access_token:
                credentials["access_token"] = access_token
            if refresh_token:
                credentials["refresh_token"] = refresh_token

            payload = {
                "name": _normalize_email(email),
                "platform": "openai",
                "type": "oauth",
                "credentials": credentials if credentials else None,
            }

            payload = {k: v for k, v in payload.items() if v is not None}

            if self.sub2api_group_ids:
                payload["group_ids"] = self.sub2api_group_ids

            resp = self.session.post(
                url,
                json=payload,
                headers=self._get_sub2api_headers(),
                timeout=30
            )

            if resp.status_code in [200, 201]:
                self._print(f"Sub2Api 上传成功: {email}")
                return True

            try:
                error_body = resp.text[:200]
            except Exception:
                error_body = "无法读取响应"
            self._print(f"Sub2Api 上传失败: {resp.status_code} - {error_body}")
            self._print(f"  URL: {url}")
            return False
        except Exception as e:
            self._print(f"Sub2Api 上传异常: {e}")
            return False

    # ==================== 统一同步 ====================

    def sync_account(self, email: str, password: str, token_path: str = None, write_d1: bool = True) -> Dict:
        """同步单个账号到所有平台"""
        normalized_email = _normalize_email(email)
        results = {"email": normalized_email or email, "d1": False, "cpa": False, "sub2api": False}

        if write_d1 and self.enable_d1 and normalized_email and password:
            results["d1"] = self.upsert_account_to_d1(normalized_email, password)
            time.sleep(0.1)

        access_token = ""
        refresh_token = ""
        if token_path and Path(token_path).exists():
            try:
                with open(token_path, "r", encoding="utf-8") as f:
                    token_data = json.load(f)
                access_token = token_data.get("access_token", "")
                refresh_token = token_data.get("refresh_token", "")
            except Exception:
                pass

        if self.enable_cpa and token_path:
            results["cpa"] = self.upload_to_cpa(token_path)
            time.sleep(0.2)

        if self.enable_sub2api and access_token:
            results["sub2api"] = self.upload_to_sub2api(
                normalized_email, password, access_token, refresh_token
            )
            time.sleep(0.2)

        return results

    def sync_all_tokens(self, token_dir: str = "codex_tokens",
                        accounts_file: str = "registered_accounts.txt") -> Dict:
        """批量同步所有账号"""
        results = {
            "total": 0,
            "d1_success": 0,
            "d1_failed": 0,
            "cpa_success": 0,
            "cpa_failed": 0,
            "sub2api_success": 0,
            "sub2api_failed": 0,
            "accounts": []
        }

        accounts = _load_registered_accounts(accounts_file)
        results["total"] = len(accounts)

        d1_success_emails = set()
        d1_failed_emails = set()
        if self.enable_d1 and accounts:
            d1_result = self.batch_upsert_accounts_to_d1(
                [{"email": email, "password": password} for email, password in accounts.items()],
                batch_size=50,
            )
            results["d1_success"] += d1_result["success"]
            results["d1_failed"] += d1_result["failed"]
            d1_success_emails.update(d1_result["accounts"])
            d1_failed_emails.update(d1_result["failed_accounts"])

        token_path = Path(token_dir)
        if not token_path.exists():
            if self.enable_d1 and accounts:
                self._print(f"Token 目录不存在: {token_dir}，已完成 D1 回填，跳过 CPA/Sub2Api")
            else:
                self._print(f"Token 目录不存在: {token_dir}")
            return results

        json_files = list(token_path.glob("*.json"))
        results["total"] = max(results["total"], len(json_files))

        if not json_files:
            self._print("没有找到 token 文件")
            return results

        self._print(f"开始同步 {len(json_files)} 个账号...")

        for token_file in json_files:
            email = _normalize_email(token_file.stem)
            password = accounts.get(email, "")
            write_d1 = self.enable_d1 and email not in d1_success_emails and email not in d1_failed_emails and bool(password)

            result = self.sync_account(email, password, str(token_file), write_d1=write_d1)
            if not write_d1:
                result["d1"] = email in d1_success_emails
            else:
                if result["d1"]:
                    results["d1_success"] += 1
                    d1_success_emails.add(email)
                elif self.enable_d1 and password:
                    results["d1_failed"] += 1
                    d1_failed_emails.add(email)

            results["accounts"].append(result)

            if result["cpa"]:
                results["cpa_success"] += 1
            elif self.enable_cpa:
                results["cpa_failed"] += 1

            if result["sub2api"]:
                results["sub2api_success"] += 1
            elif self.enable_sub2api:
                results["sub2api_failed"] += 1

        return results

    def check_sub2api_health(self, min_healthy: int = 2000) -> Dict:
        """检查 Sub2Api 账号池健康度"""
        if not self.enable_sub2api:
            return {"healthy": False, "total": 0, "error": "Sub2Api 未配置"}

        try:
            url = f"{self.sub2api_base_url.rstrip('/')}/api/v1/admin/accounts"
            resp = self.session.get(
                url,
                params={"page": 1, "page_size": 1, "platform": "openai", "type": "oauth"},
                headers=self._get_sub2api_headers(),
                timeout=20
            )
            resp.raise_for_status()

            data = resp.json()
            total = data.get("data", {}).get("total", 0)

            return {
                "healthy": total >= min_healthy,
                "total": total,
                "threshold": min_healthy,
                "need_register": total < min_healthy
            }
        except Exception as e:
            return {"healthy": False, "total": 0, "error": str(e)}


def main():
    """命令行入口"""
    import argparse

    parser = argparse.ArgumentParser(description="统一账号同步管理器")
    parser.add_argument("action", choices=["sync", "check", "cpa", "sub2api", "d1"], help="操作类型")
    parser.add_argument("--token-dir", default="codex_tokens", help="Token 目录")
    parser.add_argument("--accounts-file", default="registered_accounts.txt", help="账号文件")
    parser.add_argument("--min-healthy", type=int, default=2000, help="最小健康账号数")
    parser.add_argument("--email", help="单个账号邮箱")
    parser.add_argument("--password", help="账号密码")
    parser.add_argument("--limit", type=int, default=20, help="D1 列表条数限制")

    args = parser.parse_args()

    manager = AccountSyncManager()

    print("\n[Sync] 平台状态:")
    print(f"  D1: {'✅ 已配置' if manager.enable_d1 else '❌ 未配置'} ({manager.d1_api_base_url or '未设置'})")
    print(f"  CPA: {'✅ 已配置' if manager.enable_cpa else '❌ 未配置'} ({manager.cpa_base_url})")
    print(f"  Sub2Api: {'✅ 已配置' if manager.enable_sub2api else '❌ 未配置'} ({manager.sub2api_base_url})")
    print()

    if args.action == "check":
        if manager.enable_sub2api:
            result = manager.check_sub2api_health(args.min_healthy)
            print("[Sync] Sub2Api 状态:")
            print(f"  总账号: {result.get('total', 'N/A')}")
            print(f"  阈值: {result.get('threshold', args.min_healthy)}")
            if result.get("error"):
                print(f"  错误: {result.get('error')}")
            print(f"  健康: {'✅' if result.get('healthy') else '❌'}")
            print(f"  需注册: {'是' if result.get('need_register') else '否'}")

            if result.get("need_register"):
                return 100
        else:
            print("[Sync] Sub2Api 未配置，无法检查健康度")
        return 0

    if args.action == "sync":
        results = manager.sync_all_tokens(args.token_dir, args.accounts_file)

        print("\n[Sync] 同步完成:")
        print(f"  总数: {results['total']}")
        print(f"  D1 成功: {results['d1_success']}")
        print(f"  D1 失败: {results['d1_failed']}")
        print(f"  CPA 成功: {results['cpa_success']}")
        print(f"  CPA 失败: {results['cpa_failed']}")
        print(f"  Sub2Api 成功: {results['sub2api_success']}")
        print(f"  Sub2Api 失败: {results['sub2api_failed']}")
        return 0

    if args.action == "cpa":
        if not args.email:
            results = manager.sync_all_tokens(args.token_dir, args.accounts_file)
            print(f"\n[CPA] 上传完成: 成功 {results['cpa_success']} / 失败 {results['cpa_failed']}")
        else:
            token_path = Path(args.token_dir) / f"{args.email}.json"
            success = manager.upload_to_cpa(str(token_path))
            print(f"[CPA] 上传 {'成功' if success else '失败'}")
        return 0

    if args.action == "sub2api":
        if not args.email or not args.password:
            results = manager.sync_all_tokens(args.token_dir, args.accounts_file)
            print(f"\n[Sub2Api] 上传完成: 成功 {results['sub2api_success']} / 失败 {results['sub2api_failed']}")
        else:
            token_path = Path(args.token_dir) / f"{args.email}.json"
            access_token = ""
            if token_path.exists():
                with open(token_path, "r", encoding="utf-8") as f:
                    access_token = json.load(f).get("access_token", "")
            success = manager.upload_to_sub2api(args.email, args.password, access_token)
            print(f"[Sub2Api] 上传 {'成功' if success else '失败'}")
        return 0

    if args.action == "d1":
        if not manager.enable_d1:
            print("[D1] 未配置或未启用")
            return 1

        if args.email:
            account = manager.get_d1_account(args.email)
            print(json.dumps(account, ensure_ascii=False, indent=2))
        else:
            accounts = manager.list_d1_accounts(args.limit)
            print(json.dumps(accounts, ensure_ascii=False, indent=2))
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
