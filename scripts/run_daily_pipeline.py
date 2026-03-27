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

    # 1) 分层抓取（L0/L1/L2）：爬虫主、接口辅
    _run_step(
        [
            py,
            "scripts/run_layered_fetch.py",
            "--mode",
            "full",
            "--l1-size",
            str(UNIVERSE_SIZE),
        ]
    )

    # 2) 更新ETF元数据（上市时间/缓存深度）
    _run_step([py, "scripts/build_etf_metadata.py", "--scope", "universe"])

    # 2.5) 同步数据目录数据库（SQLite catalog）
    _run_step([py, "scripts/sync_data_catalog_db.py"])

    # 3) 同步宏观中台并生成宏观特征
    _run_step([py, "scripts/load_macro_brain_context.py"])
    _run_step([py, "scripts/build_macro_features.py"])

    # 4) 运行 Preflight 检查
    _run_step([py, "scripts/run_preflight_check.py", "--strict"])

    # 5) T+1 机制一致性校验
    _run_step([py, "scripts/run_tplus1_check.py"])

    # 6) 运行研究/纸盘主流程
    _run_step([py, "scripts/run_paper_rotation.py"])

    # 7) 系统健康报告
    _run_step([py, "scripts/run_system_health.py"])

    print("\n[done] daily pipeline finished")


if __name__ == "__main__":
    main()
