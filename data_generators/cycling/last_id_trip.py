import sys
import re
from pathlib import Path
from typing import Optional

TRIP_PATTERN = re.compile(r"^\s*trip_id:\s*(\d+)")
EXT = ".txt"

def extract_trip_id(path: Path) -> Optional[int]:
    """return trip id or none of file"""
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            first_line = f.readline()
    except Exception:
        return None
    m = TRIP_PATTERN.match(first_line)
    return int(m.group(1)) if m else None

def main() -> None:
    folder = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd()
    if not folder.is_dir():
        print(f"Error : {folder} is not a directory")
        sys.exit(1)

    max_id = None
    max_file = None

    for file in folder.glob(f"*{EXT}"):
        tid = extract_trip_id(file)
        if tid is not None and (max_id is None or tid > max_id):
            max_id, max_file = tid, file.name

    if max_id is None:
        print("No valid trip_id found")
    else:
        print(f"Last trip_id : {max_id} (file : {max_file})")

if __name__ == "__main__":
    main()