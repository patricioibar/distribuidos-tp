import os
import sys
from itertools import combinations

repo_root = os.path.dirname(os.path.dirname(__file__))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)
from tests.compare_results import compare_files


def compare_pair(dir_a, dir_b):
    """Compare two directories and return list of differences.

    Returns a list of tuples (filename, result) where result is the dict returned by compare_files
    and has keys: status, same_header (optional), diff_preview (optional).
    """
    filesA = {f for f in os.listdir(dir_a) if f.endswith('.csv')}
    filesB = {f for f in os.listdir(dir_b) if f.endswith('.csv')}
    common = sorted(filesA & filesB)
    diffs = []
    for fname in common:
        pathA = os.path.join(dir_a, fname)
        pathB = os.path.join(dir_b, fname)
        res = compare_files(pathA, pathB)
        if res.get('status') != 'equal':
            diffs.append((fname, res))
    return diffs


def main():
    if len(sys.argv) != 2:
        print("Usage: python tests/compare_all_results.py <results_root_folder>")
        sys.exit(2)

    root = sys.argv[1]
    if not os.path.isdir(root):
        print(f"ERROR: '{root}' is not a directory", file=sys.stderr)
        sys.exit(2)

    # find immediate subdirectories
    subdirs = sorted([os.path.join(root, d) for d in os.listdir(root) if os.path.isdir(os.path.join(root, d))])

    if len(subdirs) < 2:
        print("Need at least two subdirectories to compare.")
        sys.exit(0)

    overall_equal = True

    for a, b in combinations(subdirs, 2):
        name_a = os.path.basename(a)
        name_b = os.path.basename(b)
        diffs = compare_pair(a, b)
        if diffs:
            overall_equal = False
            print(f"\nDifferences between '{name_a}' and '{name_b}':")
            for fname, res in diffs:
                print(f"  File: {fname}")
                if res.get('same_header'):
                    print("    (Same header; comparing body only)")
                else:
                    print("    (Different header; comparing everything)")
                for line in res.get('diff_preview', []):
                    print("      ", line)

    if overall_equal:
        print("\nALL EQUAL: All compared pairs had identical CSV contents.")
        sys.exit(0)
    else:
        print("\nDIFFERENCES FOUND: See above for differing files.")
        sys.exit(1)


if __name__ == '__main__':
    main()
