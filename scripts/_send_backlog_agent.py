#!/usr/bin/env python3
"""Send Telegram alerts for all unsent markdown files and mark them sent."""
import os
import re
import time
import glob
import requests
import yaml

BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
DATA_DIR = "/home/user/youtubesearch/data"

FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---", re.DOTALL)


def read_frontmatter(text):
    m = FRONTMATTER_RE.match(text)
    if not m:
        return None, text
    try:
        fm = yaml.safe_load(m.group(1))
        return fm, text
    except Exception:
        return None, text


def is_unsent(filepath):
    with open(filepath, encoding="utf-8") as f:
        text = f.read()
    fm, _ = read_frontmatter(text)
    if fm is None:
        return False
    sent = fm.get("telegram_sent")
    return sent is None or sent is False or sent == "false"


def mark_sent(filepath):
    with open(filepath, encoding="utf-8") as f:
        text = f.read()
    new_text = re.sub(
        r"(telegram_sent:\s*)false",
        "telegram_sent: true",
        text,
        count=1,
    )
    if "telegram_sent:" not in new_text:
        # insert before closing ---
        new_text = re.sub(r"\n---\n", "\ntelegram_sent: true\n---\n", new_text, count=1)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(new_text)


def build_message(fm, filepath):
    channel = fm.get("channel") or fm.get("source") or os.path.basename(os.path.dirname(filepath))
    title = fm.get("title", "제목 없음")
    date = fm.get("date", "")
    url = fm.get("url", "")

    sectors = fm.get("sectors") or []
    tickers = fm.get("tickers") or []
    themes = fm.get("themes") or []
    sentiment = fm.get("sentiment", "")

    lines = [
        f"<b>[{channel}]</b> {title}",
        f"📅 {date}",
    ]
    if url:
        lines.append(f'🔗 <a href="{url}">영상 보기</a>')
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
    r = requests.post(API_URL, json=payload, timeout=15)
    try:
        resp = r.json()
    except Exception:
        resp = {"ok": False, "raw": r.text[:200]}
    return r.status_code, resp


def find_unsent_files():
    files = sorted(glob.glob(f"{DATA_DIR}/**/*.md", recursive=True))
    return [f for f in files if is_unsent(f)]


def main():
    unsent = find_unsent_files()
    print(f"Found {len(unsent)} unsent files.")
    sent_count = 0
    failed = []

    for i, filepath in enumerate(unsent):
        with open(filepath, encoding="utf-8") as f:
            text = f.read()
        fm, _ = read_frontmatter(text)
        if fm is None:
            print(f"  SKIP (no frontmatter): {filepath}")
            continue

        msg = build_message(fm, filepath)
        status, resp = send_message(msg)

        if status == 200 and resp.get("ok"):
            mark_sent(filepath)
            sent_count += 1
            print(f"  [{i+1}/{len(unsent)}] SENT: {os.path.basename(filepath)}")
        else:
            failed.append(filepath)
            print(f"  [{i+1}/{len(unsent)}] FAILED ({status}): {resp} — {os.path.basename(filepath)}")

        time.sleep(1)

    print(f"\nDone. Sent: {sent_count}, Failed: {len(failed)}")
    if failed:
        print("Failed files:")
        for f in failed:
            print(f"  {f}")


if __name__ == "__main__":
    main()
