import subprocess
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]


def run(cmd):
    print('RUN:', ' '.join(cmd))
    p = subprocess.run(cmd, cwd=BASE)
    if p.returncode != 0:
        raise SystemExit(p.returncode)


def main():
    py = sys.executable
    run([py, 'src/data_pipeline/fetch_minute_history.py'])
    run([py, 'src/data_pipeline/fetch_macro_history_3y.py', '--years', '3'])
    run([py, 'src/data_pipeline/history_coverage_report.py'])
    print('[OK] quant history bootstrap done')


if __name__ == '__main__':
    main()
