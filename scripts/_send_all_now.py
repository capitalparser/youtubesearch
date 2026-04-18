#!/usr/bin/env python3
"""Send all unsent Telegram alerts and mark files as sent."""
import os
import re
import time
import glob
import requests

BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

def parse_frontmatter(content):
    if not content.startswith("---"):
        return {}, content
    end = content.find("\n---", 3)
    if end == -1:
        return {}, content
    fm_text = content[3:end].strip()
    body = content[end+4:]
    meta = {}
    for line in fm_text.split("\n"):
        line = line.strip()
        if ":" in line:
            k, _, v = line.partition(":")
            meta[k.strip()] = v.strip()
    return meta, body, fm_text, content[:end+4]

def parse_list_field(val):
    val = val.strip()
    if val.startswith("[") and val.endswith("]"):
        inner = val[1:-1]
        if not inner.strip():
            return []
        return [x.strip() for x in inner.split(",") if x.strip()]
    return [val] if val else []

def build_message(meta):
    channel = meta.get("channel", "").strip('"')
    title = meta.get("title", "").strip('"')
    date = meta.get("date", "").strip('"')
    url = meta.get("url", "").strip('"')
    sectors = parse_list_field(meta.get("sectors", "[]"))
    tickers = parse_list_field(meta.get("tickers", "[]"))
    themes = parse_list_field(meta.get("themes", "[]"))
    sentiment = meta.get("sentiment", "").strip('"')

    lines = [
        f"<b>{channel}</b> {title}",
        f"📅 {date}",
        f'🔗 <a href="{url}">영상 보기</a>',
        f"📊 섹터: {', '.join(sectors) if sectors else '-'}",
    ]
    if tickers:
        lines.append(f"💹 종목: {', '.join(tickers)}")
    lines.append(f"🎯 테마: {', '.join(themes) if themes else '-'}")
    lines.append(f"📈 센티먼트: {sentiment}")
    return "\n".join(lines)

def set_telegram_sent(filepath, content):
    new_content = re.sub(
        r"(telegram_sent:\s*)false",
        r"\1true",
        content,
        count=1
    )
    if "telegram_sent:" not in new_content:
        new_content = re.sub(
            r"(sentiment:.*?\n)",
            r"\1telegram_sent: true\n",
            new_content,
            count=1,
            flags=re.DOTALL
        )
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(new_content)

def main():
    md_files = glob.glob("data/**/*.md", recursive=True)
    unsent = []
    for fp in sorted(md_files):
        with open(fp, "r", encoding="utf-8") as f:
            content = f.read()
        # Check if telegram_sent is missing or false
        if "telegram_sent: true" in content:
            continue
        unsent.append(fp)

    print(f"Found {len(unsent)} unsent files")
    sent = 0
    failed = 0

    for fp in unsent:
        with open(fp, "r", encoding="utf-8") as f:
            content = f.read()

        # Parse frontmatter
        if not content.startswith("---"):
            print(f"  SKIP (no frontmatter): {fp}")
            continue

        end = content.find("\n---", 3)
        if end == -1:
            print(f"  SKIP (malformed): {fp}")
            continue

        fm_text = content[3:end].strip()
        meta = {}
        for line in fm_text.split("\n"):
            line = line.strip()
            if ":" in line:
                k, _, v = line.partition(":")
                meta[k.strip()] = v.strip()

        # Skip if not enriched
        if meta.get("enriched", "").strip('"') != "true":
            print(f"  SKIP (not enriched): {fp}")
            continue

        msg = build_message(meta)
        try:
            resp = requests.post(API_URL, json={
                "chat_id": CHAT_ID,
                "parse_mode": "HTML",
                "text": msg
            }, timeout=15)
            data = resp.json()
            if resp.status_code == 200 and data.get("ok"):
                set_telegram_sent(fp, content)
                sent += 1
                print(f"  SENT [{sent}]: {os.path.basename(fp)}")
            else:
                failed += 1
                print(f"  FAIL: {fp} => {data}")
        except Exception as e:
            failed += 1
            print(f"  ERROR: {fp} => {e}")

        time.sleep(1)

    print(f"\nDone. Sent: {sent}, Failed: {failed}")

if __name__ == "__main__":
    main()
