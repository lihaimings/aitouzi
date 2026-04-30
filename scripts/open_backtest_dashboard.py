import argparse
import http.server
import socketserver
import webbrowser
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"


def main() -> int:
    parser = argparse.ArgumentParser(description="Open local backtest dashboard")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    dashboard = REPORTS_DIR / "paper_rotation_backtest_dashboard.html"
    if not dashboard.exists():
        print("dashboard not found, please run: python scripts/run_paper_rotation.py")
        return 1

    handler = lambda *h_args, **h_kwargs: http.server.SimpleHTTPRequestHandler(
        *h_args,
        directory=str(REPORTS_DIR),
        **h_kwargs,
    )
    url = f"http://127.0.0.1:{args.port}/paper_rotation_backtest_dashboard.html"
    print(f"serving reports at {url}")
    webbrowser.open(url)

    with socketserver.TCPServer(("127.0.0.1", args.port), handler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nserver stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
