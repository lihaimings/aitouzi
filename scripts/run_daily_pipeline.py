import subprocess
import sys
from pathlib import Path

UNIVERSE_SIZE = 200

ROOT = Path(__file__).resolve().parents[1]


def _run_step(cmd):
    print(f"\n[step] {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"step failed: {' '.join(cmd)}")


def main():
    py = sys.executable

    # 1) 刷新ETF池（目标200）
    _run_step([py, "scripts/refresh_etf_universe.py", "--size", str(UNIVERSE_SIZE)])

    # 2) 更新ETF缓存
    _run_step(
        [
            py,
            "scripts/fetch_etf_cache.py",
            "--universe-size",
            str(UNIVERSE_SIZE),
            "--max-retries",
            "3",
            "--retry-sleep",
            "1",
            "--fresh-tolerance-days",
            "3",
            "--min-bootstrap-rows",
            "240",
            "--bootstrap-batch-size",
            "10",
        ]
    )

    # 3) 运行研究/纸盘主流程
    _run_step([py, "scripts/run_preflight_check.py", "--strict", "--push"])

    # 4) T+1 机制一致性校验
    _run_step([py, "scripts/run_tplus1_check.py"])

    # 5) 运行研究/纸盘主流程
    _run_step([py, "scripts/run_paper_rotation.py"])

    # 6) 系统健康报告
    _run_step([py, "scripts/run_system_health.py"])

    print("\n[done] daily pipeline finished")


if __name__ == "__main__":
    main()
