#!/usr/bin/env python3
import json
import argparse

def dict_values(d):
    return set(str(d.values()))

def compare_files(path_a, path_b):
    with open(path_a, "r", encoding="utf-8") as f:
        data_a = json.load(f)
    with open(path_b, "r", encoding="utf-8") as f:
        data_b = json.load(f)

    for i, (row_a, row_b) in enumerate(zip(data_a, data_b), start=1):
        if dict_values(row_a) != dict_values(row_b):
            return False
    return True

def main():
    ap = argparse.ArgumentParser(description="Compare two JSON objects (lists of dictionaries) by values only.")
    ap.add_argument("--a", required=True, help="JSON file A")
    ap.add_argument("--b", required=True, help="JSON file B")
    args = ap.parse_args()

    ok = compare_files(args.a, args.b)
    exit(0 if ok else 1)

if __name__ == "__main__":
    main()
