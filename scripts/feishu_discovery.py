import argparse
import json
import os
from pathlib import Path
from typing import Dict, List

import requests

ROOT = Path(__file__).resolve().parents[1]

FEISHU_BASE = "https://open.feishu.cn/open-apis"


def _load_local_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def get_tenant_access_token(app_id: str, app_secret: str, timeout: int = 15) -> str:
    url = f"{FEISHU_BASE}/auth/v3/tenant_access_token/internal"
    resp = requests.post(url, json={"app_id": app_id, "app_secret": app_secret}, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"auth failed: {data}")
    return str(data["tenant_access_token"])


def list_chats(token: str, page_size: int = 50, timeout: int = 15) -> List[Dict]:
    headers = {"Authorization": f"Bearer {token}"}
    out: List[Dict] = []
    page_token = ""

    while True:
        params = {"page_size": page_size}
        if page_token:
            params["page_token"] = page_token

        resp = requests.get(f"{FEISHU_BASE}/im/v1/chats", headers=headers, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"list chats failed: {data}")

        items = data.get("data", {}).get("items", [])
        out.extend(items)

        has_more = data.get("data", {}).get("has_more", False)
        page_token = data.get("data", {}).get("page_token", "")
        if not has_more:
            break

    return out


def send_text_by_chat_id(token: str, chat_id: str, text: str, timeout: int = 15) -> Dict:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    body = {
        "receive_id": chat_id,
        "msg_type": "text",
        "content": json.dumps({"text": text}, ensure_ascii=False),
    }
    resp = requests.post(
        f"{FEISHU_BASE}/im/v1/messages",
        headers=headers,
        params={"receive_id_type": "chat_id"},
        json=body,
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"send failed: {data}")
    return data


def main():
    _load_local_env()

    parser = argparse.ArgumentParser(description="Feishu helper: list chats / send test")
    parser.add_argument("--app-id", default=os.getenv("FEISHU_APP_ID", ""))
    parser.add_argument("--app-secret", default=os.getenv("FEISHU_APP_SECRET", ""))

    sub = parser.add_subparsers(dest="cmd", required=True)

    p_list = sub.add_parser("list-chats", help="List available chat_id")
    p_list.add_argument("--keyword", default="", help="Filter by chat name keyword")

    p_send = sub.add_parser("send-test", help="Send test message to chat_id")
    p_send.add_argument("--chat-id", required=True)
    p_send.add_argument("--text", default="[AIQuant] Feishu App模式连通测试成功")

    args = parser.parse_args()

    if not args.app_id or not args.app_secret:
        raise SystemExit("missing --app-id/--app-secret (or FEISHU_APP_ID/FEISHU_APP_SECRET)")

    token = get_tenant_access_token(args.app_id, args.app_secret)

    if args.cmd == "list-chats":
        chats = list_chats(token)
        keyword = (args.keyword or "").strip().lower()
        if keyword:
            chats = [c for c in chats if keyword in str(c.get("name", "")).lower()]

        print(f"found chats: {len(chats)}")
        for c in chats:
            print("-" * 60)
            print(f"name: {c.get('name')}")
            print(f"chat_id: {c.get('chat_id')}")
            print(f"chat_mode: {c.get('chat_mode')}")
            print(f"chat_type: {c.get('chat_type')}")

    elif args.cmd == "send-test":
        data = send_text_by_chat_id(token, chat_id=args.chat_id, text=args.text)
        print("send ok")
        print(json.dumps(data, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
