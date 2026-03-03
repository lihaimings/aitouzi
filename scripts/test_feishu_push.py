import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.reporting.feishu_push import push_dm


if __name__ == "__main__":
    push_dm("[AIQuant] 飞书连通性测试：如果你看到这条消息，说明推送配置成功。")
