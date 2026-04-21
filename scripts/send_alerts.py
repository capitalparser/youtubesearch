#!/usr/bin/env python3
"""Send Telegram alerts for all unsent markdown files."""
import os
import re
import time
import glob
import requests

BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
DATA_DIR = "/home/user/youtubesearch/data"

def parse_frontmatter(content):
    match = re.match(r'^---\n(.*?)\n---', content, re.DOTALL)
    if not match:
        return {}
    fm_text = match.group(1)
    fields = {}
    for line in fm_text.split('\n'):
        if ':' not in line:
            continue
        key, _, val = line.partition(':')
        key = key.strip()
        val = val.strip()
        if val.startswith('[') and val.endswith(']'):
            inner = val[1:-1].strip()
            fields[key] = [x.strip() for x in inner.split(',')] if inner else []
        else:
            fields[key] = val.strip('"').strip("'")
    return fields

def set_telegram_sent(filepath, content):
    if 'telegram_sent: false' in content:
        new_content = content.replace('telegram_sent: false', 'telegram_sent: true', 1)
    else:
        new_content = re.sub(r'(sentiment: \S+)\n(---)', r'\1\ntelegram_sent: true\n\2', content, count=1)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)

def build_message(fm):
    channel = fm.get('channel', '')
    title = fm.get('title', '')
    date = fm.get('date', '')
    url = fm.get('url', '')
    sectors = fm.get('sectors', [])
    tickers = fm.get('tickers', [])
    themes = fm.get('themes', [])
    sentiment = fm.get('sentiment', '')

    sectors_str = ', '.join(sectors) if isinstance(sectors, list) and sectors else '-'
    themes_str = ', '.join(themes) if isinstance(themes, list) and themes else '-'

    lines = [
        f"<b>{channel}</b> {title}",
        f"\u{1F4C5} {date}",
        f'🔗 <a href="{url}">영상 보기</a>',
        f"📊 섹터: {sectors_str}",
    ]
    if tickers and isinstance(tickers, list):
        lines.append(f"💹 종목: {', '.join(tickers)}")
    lines.append(f"🎯 테마: {themes_str}")
    lines.append(f"📈 센티먼트: {sentiment}")
    return '\n'.join(lines)

def find_unsent_files():
    all_md = glob.glob(os.path.join(DATA_DIR, '**', '*.md'), recursive=True)
    unsent = []
    for path in sorted(all_md):
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
        fm = parse_frontmatter(content)
        ts = fm.get('telegram_sent', None)
        if ts is None or str(ts).lower() == 'false':
            unsent.append((path, content, fm))
    return unsent

def main():
    unsent = find_unsent_files()
    print(f"Found {len(unsent)} unsent files.")
    sent = 0
    failed = 0
    for i, (path, content, fm) in enumerate(unsent):
        filename = os.path.basename(path)
        if not fm.get('title'):
            print(f"[SKIP] No title: {filename}")
            continue
        msg = build_message(fm)
        try:
            resp = requests.post(API_URL, json={
                "chat_id": CHAT_ID,
                "parse_mode": "HTML",
                "text": msg
            }, timeout=15)
            data = resp.json()
            if resp.status_code == 200 and data.get('ok'):
                set_telegram_sent(path, content)
                sent += 1
                print(f"[OK {sent}] {filename}")
            else:
                failed += 1
                print(f"[FAIL] {filename}: {data}")
        except Exception as e:
            failed += 1
            print(f"[ERROR] {filename}: {e}")
        if i < len(unsent) - 1:
            time.sleep(1)
    print(f"\nDone. Sent: {sent}, Failed: {failed}")

if __name__ == '__main__':
    main()
