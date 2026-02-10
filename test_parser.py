#!/usr/bin/env python3
import argparse
import os
import struct
import sys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse a binary token file (uint32 little-endian) and print summary."
    )
    parser.add_argument("path", help="Path to .bin file (e.g., /mnt/nvme/wiki_corpus.bin)")
    parser.add_argument("--limit", type=int, default=64, help="How many tokens to print from head/tail")
    return parser.parse_args()


def read_u32_le(path: str):
    with open(path, "rb") as f:
        data = f.read()
    size = len(data)
    if size == 0:
        return [], 0, 0
    if size % 4 != 0:
        print(f"[WARN] file size {size} not multiple of 4; trailing {size % 4} bytes ignored", file=sys.stderr)
        data = data[: size - (size % 4)]
    count = len(data) // 4
    fmt = "<" + ("I" * count)
    return list(struct.unpack(fmt, data)), size, count


def main() -> int:
    args = parse_args()
    if not os.path.exists(args.path):
        print(f"[ERR] file not found: {args.path}", file=sys.stderr)
        return 1

    tokens, size_bytes, count = read_u32_le(args.path)
    if count == 0:
        print("[INFO] no tokens found (empty or truncated file)")
        return 0

    nonzero = sum(1 for t in tokens if t != 0)
    zeros = count - nonzero
    tmin = min(tokens)
    tmax = max(tokens)

    print(f"[INFO] file={args.path}")
    print(f"[INFO] size_bytes={size_bytes} tokens={count}")
    print(f"[INFO] nonzero={nonzero} zeros={zeros} zero_ratio={zeros / count:.3f}")
    print(f"[INFO] min={tmin} max={tmax}")

    lim = max(0, args.limit)
    if lim > 0:
        head = tokens[:lim]
        tail = tokens[-lim:] if count > lim else []
        print(f"[HEAD] {head}")
        if tail:
            print(f"[TAIL] {tail}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
