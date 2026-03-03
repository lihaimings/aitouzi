from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from pathlib import Path
from typing import Dict, List, Optional

import requests


FEISHU_BASE = "https://open.feishu.cn/open-apis"
_ENV_LOADED = False


def _load_local_env_once() -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return

    env_path = Path(__file__).resolve().parents[2] / ".env"
    if not env_path.exists():
        _ENV_LOADED = True
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

    _ENV_LOADED = True


def _get_tenant_access_token(app_id: str, app_secret: str, timeout: int = 15) -> str:
    url = f"{FEISHU_BASE}/auth/v3/tenant_access_token/internal"
    payload = {"app_id": app_id, "app_secret": app_secret}
    resp = requests.post(url, json=payload, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"feishu auth failed: {data}")
    return str(data["tenant_access_token"])


def _send_text_message(
    tenant_access_token: str,
    receive_id: str,
    text: str,
    receive_id_type: str = "chat_id",
    timeout: int = 15,
):
    url = f"{FEISHU_BASE}/im/v1/messages"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json",
    }
    body = {
        "receive_id": receive_id,
        "msg_type": "text",
        "content": json.dumps({"text": text}, ensure_ascii=False),
    }
    resp = requests.post(url, headers=headers, params={"receive_id_type": receive_id_type}, json=body, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"feishu send failed: {data}")
    return data


def _list_chats(tenant_access_token: str, timeout: int = 15) -> List[Dict]:
    url = f"{FEISHU_BASE}/im/v1/chats"
    headers = {"Authorization": f"Bearer {tenant_access_token}"}

    items: List[Dict] = []
    page_token = ""

    while True:
        params: Dict[str, str | int] = {"page_size": 50}
        if page_token:
            params["page_token"] = page_token

        resp = requests.get(url, headers=headers, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"feishu list chats failed: {data}")

        data_block = data.get("data", {})
        items.extend(data_block.get("items", []))

        if not data_block.get("has_more", False):
            break
        page_token = data_block.get("page_token", "")

    return items


def _auto_discover_chat_id(tenant_access_token: str, name_keyword: str, timeout: int = 15) -> str:
    chats = _list_chats(tenant_access_token=tenant_access_token, timeout=timeout)
    if not chats:
        raise RuntimeError("feishu auto discover failed: no chats available for this app")

    keyword = name_keyword.strip().lower()
    matched = chats
    if keyword:
        matched = [c for c in chats if keyword in str(c.get("name", "")).strip().lower()]

    if len(matched) == 1:
        return str(matched[0].get("chat_id", "")).strip()

    if len(chats) == 1 and not keyword:
        return str(chats[0].get("chat_id", "")).strip()

    sample = [
        {
            "name": c.get("name"),
            "chat_id": c.get("chat_id"),
        }
        for c in matched[:5]
    ]
    raise RuntimeError(
        "feishu auto discover failed: receive_id missing and chat match is not unique; "
        f"set FEISHU_RECEIVE_ID or refine FEISHU_RECEIVE_NAME_KEYWORD. sample={sample}"
    )


def _webhook_sign(timestamp: str, secret: str) -> str:
    string_to_sign = f"{timestamp}\n{secret}"
    digest = hmac.new(string_to_sign.encode("utf-8"), b"", digestmod=hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def _send_webhook_text(webhook_url: str, text: str, webhook_secret: str = "", timeout: int = 15):
    body = {
        "msg_type": "text",
        "content": {"text": text},
    }

    if webhook_secret:
        ts = str(int(time.time()))
        body["timestamp"] = ts
        body["sign"] = _webhook_sign(timestamp=ts, secret=webhook_secret)

    resp = requests.post(webhook_url, json=body, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") not in (0, None):
        raise RuntimeError(f"feishu webhook send failed: {data}")
    return data


def push_dm(text: str, image_path: Optional[str] = None):
    """
    飞书推送（文本），支持两种模式：

    1) Webhook机器人（优先）
       - FEISHU_WEBHOOK_URL
       - 可选 FEISHU_WEBHOOK_SECRET（如果群机器人开启签名）

    2) 应用IM模式
        - FEISHU_APP_ID
        - FEISHU_APP_SECRET
        - FEISHU_RECEIVE_ID（可留空并开启自动发现）
        - 可选 FEISHU_RECEIVE_ID_TYPE（默认 chat_id）
        - 可选 FEISHU_RECEIVE_NAME_KEYWORD（自动发现 chat_id 时按群名过滤）

    若都未配置，自动降级本地print，不中断主流程。
    """
    _load_local_env_once()
    timeout = int(os.getenv("FEISHU_TIMEOUT", "15"))

    webhook_url = os.getenv("FEISHU_WEBHOOK_URL", "").strip()
    webhook_secret = os.getenv("FEISHU_WEBHOOK_SECRET", "").strip()

    app_id = os.getenv("FEISHU_APP_ID", "").strip()
    app_secret = os.getenv("FEISHU_APP_SECRET", "").strip()
    receive_id = os.getenv("FEISHU_RECEIVE_ID", "").strip()
    receive_id_type = os.getenv("FEISHU_RECEIVE_ID_TYPE", "chat_id").strip() or "chat_id"
    receive_name_keyword = os.getenv("FEISHU_RECEIVE_NAME_KEYWORD", "").strip()
    auto_discover = os.getenv("FEISHU_RECEIVE_ID_AUTO_DISCOVER", "1").strip().lower() not in {"0", "false", "no"}

    try:
        if webhook_url:
            _send_webhook_text(
                webhook_url=webhook_url,
                text=text,
                webhook_secret=webhook_secret,
                timeout=timeout,
            )
            print("[feishu] webhook push success")
            return

        if app_id and app_secret:
            token = _get_tenant_access_token(app_id=app_id, app_secret=app_secret, timeout=timeout)
            final_receive_id = receive_id

            if not final_receive_id and auto_discover and receive_id_type == "chat_id":
                final_receive_id = _auto_discover_chat_id(
                    tenant_access_token=token,
                    name_keyword=receive_name_keyword,
                    timeout=timeout,
                )
                print(f"[feishu] auto discovered chat_id: {final_receive_id}")

            if not final_receive_id:
                raise RuntimeError(
                    "missing FEISHU_RECEIVE_ID; set FEISHU_RECEIVE_ID or enable chat auto discover"
                )

            _send_text_message(
                tenant_access_token=token,
                receive_id=final_receive_id,
                text=text,
                receive_id_type=receive_id_type,
                timeout=timeout,
            )
            print("[feishu] app push success")
            return

        print("[feishu] skip push: missing config (webhook or app+receive_id)")
        print("[feishu] dm push:\n" + text)
    except Exception as e:
        print(f"[feishu] push failed: {e}")
        print("[feishu] fallback local print:\n" + text)

    if image_path:
        # 预留：后续可接入图片上传并发送image消息
        print(f"[feishu] image not sent yet (placeholder): {image_path}")
