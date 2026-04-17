#!/usr/bin/env python3
"""Send Telegram alerts for all unsent markdown files."""

import os
import re
import time
import glob
import requests
import yaml

BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
TELEGRAM_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
DATA_DIR = "/home/user/youtubesearch/data"

def parse_frontmatter(content):
    if not content.startswith("---"):
        return None, content
    end = content.find("\n---", 3)
    if end == -1:
        return None, content
    yaml_str = content[3:end].strip()
    try:
        fm = yaml.safe_load(yaml_str)
        return fm, content
    except Exception:
        return None, content

def set_telegram_sent(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
    new_content = re.sub(r"telegram_sent:\s*false", "telegram_sent: true", content, count=1)
    if new_content != content:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(new_content)
        return True
    return False

def build_message(fm, filepath):
    channel = fm.get("channel") or fm.get("source") or fm.get("author") or "Unknown"
    title = fm.get("title") or os.path.basename(filepath)
    date = str(fm.get("date", ""))
    url = fm.get("url", "")
    sectors = fm.get("sectors", []) or []
    tickers = fm.get("tickers", []) or []
    themes = fm.get("themes", []) or []
    sentiment = fm.get("sentiment", "")

    # Escape HTML special chars in title
    def esc(s):
        return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    lines = [f"<b>{esc(channel)}</b> {esc(title)}"]
    if date:
        lines.append(f"📅 {date}")
    if url:
        lines.append(f"🔗 <a href=\"{url}\">영상 보기</a>")
    if sectors:
        lines.append(f"📊 섹터: {', '.join(str(s) for s in sectors)}")
    if tickers:
        lines.append(f"💹 종목: {', '.join(str(t) for t in tickers)}")
    if themes:
        lines.append(f"🎯 테마: {', '.join(str(t) for t in themes)}")
    if sentiment:
        lines.append(f"📈 센티먼트: {sentiment}")

    return "\n".join(lines)

def send_message(text):
    payload = {"chat_id": CHAT_ID, "parse_mode": "HTML", "text": text}
    resp = requests.post(TELEGRAM_URL, json=payload, timeout=15)
    try:
        return resp.status_code, resp.json()
    except Exception:
        return resp.status_code, {"ok": False, "raw": resp.text[:200]}

def find_unsent_files():
    files = []
    for root, dirs, filenames in os.walk(DATA_DIR):
        for fname in filenames:
            if not fname.endswith(".md"):
                continue
            fpath = os.path.join(root, fname)
            with open(fpath, "r", encoding="utf-8") as f:
                content = f.read()
            if "telegram_sent: false" in content:
                files.append(fpath)
    return sorted(files)

def main():
    files = find_unsent_files()
    print(f"Found {len(files)} unsent files")

    sent = 0
    failed = 0
    for i, fpath in enumerate(files):
        with open(fpath, "r", encoding="utf-8") as f:
            content = f.read()
        fm, _ = parse_frontmatter(content)
        if fm is None:
            print(f"  [{i+1}] SKIP (no frontmatter): {fpath}")
            failed += 1
            continue

        msg = build_message(fm, fpath)
        status_code, result = send_message(msg)

        if status_code == 200 and result.get("ok"):
            set_telegram_sent(fpath)
            print(f"  [{i+1}] SENT: {os.path.basename(fpath)}")
            sent += 1
        else:
            print(f"  [{i+1}] FAILED ({status_code}): {result} | {os.path.basename(fpath)}")
            failed += 1

        if i < len(files) - 1:
            time.sleep(1)

    print(f"\nDone: {sent} sent, {failed} failed out of {len(files)} total")

if __name__ == "__main__":
    main()
