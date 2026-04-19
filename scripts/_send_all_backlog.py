#!/usr/bin/env python3
"""Send Telegram alerts for all unsent files."""
import os, re, time, json, requests
from pathlib import Path

BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
DATA_DIR = Path("/home/user/youtubesearch/data")


def parse_frontmatter(text):
    m = re.match(r'^---\n(.*?)\n---', text, re.DOTALL)
    if not m:
        return None, text
    fm_str = m.group(1)
    data = {}
    for line in fm_str.split('\n'):
        kv = re.match(r'^(\w+):\s*(.*)', line)
        if kv:
            key, val = kv.group(1), kv.group(2).strip()
            # Parse list values like [a, b, c]
            if val.startswith('[') and val.endswith(']'):
                inner = val[1:-1].strip()
                if inner:
                    items = [x.strip().strip('"\'') for x in inner.split(',')]
                    data[key] = [x for x in items if x]
                else:
                    data[key] = []
            elif val.startswith('"') and val.endswith('"'):
                data[key] = val[1:-1]
            elif val.startswith("'") and val.endswith("'"):
                data[key] = val[1:-1]
            else:
                data[key] = val
    return data, m.group(0)


def set_telegram_sent(filepath):
    text = filepath.read_text(encoding='utf-8')
    # Replace telegram_sent: false with telegram_sent: true
    new_text = re.sub(r'telegram_sent:\s*false', 'telegram_sent: true', text)
    filepath.write_text(new_text, encoding='utf-8')


def build_message(fm):
    channel = fm.get('channel', '')
    title = fm.get('title', '').strip('"')
    date = fm.get('date', '')
    url = fm.get('url', '').strip('"')
    sectors = fm.get('sectors', [])
    tickers = fm.get('tickers', [])
    themes = fm.get('themes', [])
    sentiment = fm.get('sentiment', '')

    sectors_str = ', '.join(sectors) if sectors else '-'
    themes_str = ', '.join(themes) if themes else '-'

    msg = f"<b>{channel}</b> {title}\n"
    msg += f"📅 {date}\n"
    msg += f"🔗 <a href=\"{url}\">영상 보기</a>\n"
    msg += f"📊 섹터: {sectors_str}\n"
    if tickers:
        msg += f"💹 종목: {', '.join(tickers)}\n"
    msg += f"🎯 테마: {themes_str}\n"
    msg += f"📈 센티먼트: {sentiment}"
    return msg


def find_unsent_files():
    files = []
    for md in sorted(DATA_DIR.rglob("*.md")):
        text = md.read_text(encoding='utf-8')
        if 'telegram_sent: true' in text:
            continue
        files.append(md)
    return files


def main():
    unsent = find_unsent_files()
    print(f"Found {len(unsent)} unsent files")

    sent = 0
    failed = 0
    for i, fp in enumerate(unsent):
        text = fp.read_text(encoding='utf-8')
        fm, _ = parse_frontmatter(text)
        if fm is None:
            print(f"  [{i+1}] SKIP (no frontmatter): {fp.name}")
            continue

        # Skip if not enriched
        if fm.get('enriched') != 'true' and fm.get('enriched') is not True:
            print(f"  [{i+1}] SKIP (not enriched): {fp.name}")
            continue

        msg = build_message(fm)
        payload = {"chat_id": CHAT_ID, "parse_mode": "HTML", "text": msg}

        try:
            r = requests.post(API_URL, json=payload, timeout=15)
            data = r.json()
            if r.status_code == 200 and data.get('ok'):
                set_telegram_sent(fp)
                sent += 1
                print(f"  [{i+1}] SENT: {fp.name}")
            else:
                failed += 1
                print(f"  [{i+1}] FAILED ({r.status_code}): {data.get('description', '')} — {fp.name}")
        except Exception as e:
            failed += 1
            print(f"  [{i+1}] ERROR: {e} — {fp.name}")

        if i < len(unsent) - 1:
            time.sleep(1)

    print(f"\nDone: {sent} sent, {failed} failed out of {len(unsent)} total")


if __name__ == '__main__':
    main()
