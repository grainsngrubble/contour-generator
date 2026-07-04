from __future__ import print_function

import os
import re
import sys

TARGET = "/usr/lib/python3/dist-packages/phyghtmap/NASASRTMUtil.py"
MARKER = "NSV_FLAT_HGT_ON_SRTM_404_PATCH"

HELPER_LINES = [
    "# NSV_FLAT_HGT_ON_SRTM_404_PATCH",
    "def _nsv_flat_hgt_for_missing_area(area):",
    "    tile_name = str(area)",
    "    out_dir = os.path.join('hgt', 'VIEW3')",
    "    out_path = os.path.join(out_dir, tile_name + '.hgt')",
    "    samples = 1201",
    "    expected_size = samples * samples * 2",
    "",
    "    if not os.path.isdir(out_dir):",
    "        os.makedirs(out_dir)",
    "",
    "    if (not os.path.exists(out_path)) or os.path.getsize(out_path) != expected_size:",
    "        row = b'\\x00\\x00' * samples",
    "        with open(out_path, 'wb') as fh:",
    "            for _ in range(samples):",
    "                fh.write(row)",
    "",
    "    try:",
    "        sys.stderr.write('%s: SRTM3 HTTP 404 -> generated flat zero-height HGT %s\\\\n' % (tile_name, out_path))",
    "    except Exception:",
    "        pass",
    "",
    "    return out_path",
    "",
]
HELPER = "\n".join(HELPER_LINES) + "\n"


def main():
    if not os.path.exists(TARGET):
        print("ERROR: target file not found: %s" % TARGET, file=sys.stderr)
        return 1

    with open(TARGET, "r") as fh:
        text = fh.read()

    if MARKER in text:
        print("already patched: %s" % TARGET)
        return 0

    text, n = re.subn(
        r"(?m)^def downloadAndUnzip_Tif\(",
        HELPER + "\ndef downloadAndUnzip_Tif(",
        text,
        count=1,
    )
    if n != 1:
        print("ERROR: could not find def downloadAndUnzip_Tif(...) in %s" % TARGET, file=sys.stderr)
        return 1

    pattern = re.compile(r"(?m)^([ \t]*)downloadToFile\(url, saveFilename, source\)")
    match = pattern.search(text)
    if not match:
        print("ERROR: could not find downloadToFile(url, saveFilename, source) in %s" % TARGET, file=sys.stderr)
        return 1

    indent = match.group(1)
    replacement = (
        indent + "try:\n"
        + indent + "    downloadToFile(url, saveFilename, source)\n"
        + indent + "except Exception as exc:\n"
        + indent + "    if getattr(exc, 'code', None) == 404:\n"
        + indent + "        return _nsv_flat_hgt_for_missing_area(area)\n"
        + indent + "    raise"
    )
    text = pattern.sub(replacement, text, count=1)

    backup = TARGET + ".nsv-flat404.bak"
    with open(backup, "w") as fh:
        with open(TARGET, "r") as original:
            fh.write(original.read())

    with open(TARGET, "w") as fh:
        fh.write(text)

    print("patched: %s" % TARGET)
    print("backup : %s" % backup)
    return 0


if __name__ == "__main__":
    sys.exit(main())
