from typing import Optional

# 占位：后续接入飞书机器人或当前会话推送

def push_dm(text: str, image_path: Optional[str] = None):
    print("[feishu] dm push:\n" + text)
    if image_path:
        print(f"[feishu] with image: {image_path}")
