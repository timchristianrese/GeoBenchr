import sys
import argparse
from pathlib import Path

EXT = ".txt"



def count_points(path: Path) -> int:
    """Return the number of points lines in a file (excluding header)"""
    with path.open(encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()
    return max(0, len(lines) - 3)

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete files with exactly two points"
    )
    parser.add_argument(
        "folder",
        type=Path,
        help="all txt to process"
    )
    parser.add_argument(
        "-d", "--delete",
        action="store_true",
        help="to supress files (else dry‑run)"
    )
    args = parser.parse_args()

    folder = args.folder
    if not folder.is_dir():
        print(f"Error : {folder} is not a directory")
        sys.exit(1)

    txt_files = sorted(folder.glob(f"*{EXT}"))
    total_files = len(txt_files)
    to_remove = [p for p in txt_files if count_points(p) == 2]
    nb_two_points = len(to_remove)

    print(f"Analyse finished : {total_files} txt files found")
    print(f"{nb_two_points} files with exactly two points\n")

    if nb_two_points == 0:
        print("No file to process")
        return

    for p in to_remove:
        print(f"- {p.name}")
    print()

    if args.delete:
        for p in to_remove:
            try:
                p.unlink()
            except Exception as exc:
                print(f"can't delete : {p.name} : {exc}")
        print(f"Delete effective, {nb_two_points} supressed files")
    else:
        print("Dry‑run finished. restart with --d to succefully delete them")

    print(f"\nNb files : {nb_two_points}")


if __name__ == "__main__":
    main()