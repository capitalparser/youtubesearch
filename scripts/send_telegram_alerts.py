#!/usr/bin/env python3
"""Send Telegram alerts for all unsent enriched YouTube video files."""

import os
import re
import time
import glob
import requests
import yaml

TELEGRAM_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

DATA_DIR = "/home/user/youtubesearch/data"

FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---\n", re.DOTALL)


def parse_frontmatter(content):
    m = FRONTMATTER_RE.match(content)
    if not m:
        return None, content
    fm_str = m.group(1)
    try:
        fm = yaml.safe_load(fm_str)
    except Exception:
        return None, content
    return fm, content


def rebuild_content(content, fm):
    """Replace the YAML frontmatter in content with updated fm dict."""
    m = FRONTMATTER_RE.match(content)
    if not m:
        return content
    new_yaml = yaml.dump(fm, allow_unicode=True, default_flow_style=False, sort_keys=False)
    # Remove trailing newline from yaml.dump to keep consistent
    new_yaml = new_yaml.rstrip("\n")
    rest = content[m.end():]
    return f"---\n{new_yaml}\n---\n{rest}"


def format_message(fm):
    channel = fm.get("channel", "")
    title = fm.get("title", "")
    date = str(fm.get("date", ""))
    url = fm.get("url", "")
    sectors = fm.get("sectors", []) or []
    tickers = fm.get("tickers", []) or []
    themes = fm.get("themes", []) or []
    sentiment = fm.get("sentiment", "")

    sectors_str = ", ".join(str(s) for s in sectors) if sectors else "-"
    tickers_str = ", ".join(str(t) for t in tickers) if tickers else ""
    themes_str = ", ".join(str(t) for t in themes) if themes else "-"

    lines = [
        f"<b>[{channel}]</b> {title}",
        f"📅 {date}",
        f'🔗 <a href="{url}">영상 보기</a>',
        f"📊 섹터: {sectors_str}",
    ]
    if tickers_str:
        lines.append(f"💹 종목: {tickers_str}")
    lines.append(f"🎯 테마: {themes_str}")
    lines.append(f"📈 센티먼트: {sentiment}")

    return "\n".join(lines)


def send_message(text):
    payload = {
        "chat_id": CHAT_ID,
        "parse_mode": "HTML",
        "text": text,
    }
    resp = requests.post(API_URL, json=payload, timeout=15)
    return resp


def find_unsent_files():
    files = []
    for root, dirs, filenames in os.walk(DATA_DIR):
        # Skip analysis and links directories
        rel = os.path.relpath(root, DATA_DIR)
        if rel.startswith("analysis") or rel.startswith("links"):
            continue
        for fn in sorted(filenames):
            if fn.endswith(".md"):
                files.append(os.path.join(root, fn))
    files.sort()

    unsent = []
    for fp in files:
        with open(fp, "r", encoding="utf-8") as f:
            content = f.read()
        fm, _ = parse_frontmatter(content)
        if fm is None:
            continue
        sent = fm.get("telegram_sent")
        if sent is True:
            continue
        unsent.append((fp, fm, content))
    return unsent


def main():
    unsent = find_unsent_files()
    print(f"Found {len(unsent)} unsent files")

    sent_count = 0
    error_count = 0

    for i, (fp, fm, content) in enumerate(unsent):
        text = format_message(fm)
        resp = send_message(text)

        if resp.status_code == 200:
            data = resp.json()
            if data.get("ok"):
                # Update telegram_sent in file
                fm["telegram_sent"] = True
                new_content = rebuild_content(content, fm)
                with open(fp, "w", encoding="utf-8") as f:
                    f.write(new_content)
                sent_count += 1
                print(f"[{i+1}/{len(unsent)}] SENT: {os.path.basename(fp)}")
            else:
                error_count += 1
                print(f"[{i+1}/{len(unsent)}] FAIL (ok=false): {os.path.basename(fp)} — {data}")
        else:
            error_count += 1
            print(f"[{i+1}/{len(unsent)}] FAIL (HTTP {resp.status_code}): {os.path.basename(fp)} — {resp.text[:200]}")

        if i + 1 < len(unsent):
            time.sleep(1)

    print(f"\nDone: {sent_count} sent, {error_count} errors")


if __name__ == "__main__":
    main()
