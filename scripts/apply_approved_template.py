from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"


def main():
    template = REPORTS / "paper_rotation_approved_params_template.json"
    approved = REPORTS / "paper_rotation_approved_params.json"

    if not template.exists():
        print(f"[warn] template not found: {template}")
        return

    backup = None
    if approved.exists():
        backup = REPORTS / "paper_rotation_approved_params.backup.json"
        shutil.copyfile(approved, backup)

    shutil.copyfile(template, approved)
    print(f"applied: {approved}")
    if backup is not None:
        print(f"backup: {backup}")


if __name__ == "__main__":
    main()
