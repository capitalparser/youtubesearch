#!/usr/bin/env python3
"""Send Telegram alerts for all unsent markdown files."""

import glob
import re
import time
import json
import urllib.request
import urllib.error

BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"


def parse_frontmatter(content):
    """Extract frontmatter as dict."""
    match = re.match(r'^---\n(.*?)\n---', content, re.DOTALL)
    if not match:
        return {}
    fm_text = match.group(1)
    data = {}
    
    # Parse simple key: value and key: [list] patterns
    for line in fm_text.split('\n'):
        kv = re.match(r'^(\w+):\s*(.*)', line)
        if not kv:
            continue
        key, val = kv.group(1), kv.group(2).strip()
        # Strip quotes
        if val.startswith('"') and val.endswith('"'):
            val = val[1:-1]
        elif val.startswith("'") and val.endswith("'"):
            val = val[1:-1]
        # Parse list
        if val.startswith('[') and val.endswith(']'):
            inner = val[1:-1]
            if inner.strip():
                items = [x.strip().strip('"\'') for x in inner.split(',')]
                val = [x for x in items if x]
            else:
                val = []
        data[key] = val
    return data


def build_message(fm, filepath):
    """Build Telegram HTML message from frontmatter."""
    channel = fm.get('channel') or fm.get('source') or fm.get('author') or 'Unknown'
    title = fm.get('title', filepath.split('/')[-1])
    date = fm.get('date', '')
    url = fm.get('url', '')
    
    sectors = fm.get('sectors', [])
    tickers = fm.get('tickers', [])
    themes = fm.get('themes', [])
    sentiment = fm.get('sentiment', '')

    if isinstance(sectors, list):
        sectors_str = ', '.join(sectors)
    else:
        sectors_str = str(sectors)
    
    if isinstance(tickers, list):
        tickers_str = ', '.join(tickers)
    else:
        tickers_str = str(tickers)
    
    if isinstance(themes, list):
        themes_str = ', '.join(themes)
    else:
        themes_str = str(themes)

    # Escape HTML special chars
    def esc(s):
        return str(s).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    lines = [
        f"<b>{esc(channel)}</b> {esc(title)}",
        f"📅 {esc(date)}",
    ]
    
    if url:
        lines.append(f"🔗 <a href=\"{url}\">영상 보기</a>")
    
    if sectors_str:
        lines.append(f"📊 섹터: {esc(sectors_str)}")
    
    if tickers_str:
        lines.append(f"💹 종목: {esc(tickers_str)}")
    
    if themes_str:
        lines.append(f"🎯 테마: {esc(themes_str)}")
    
    if sentiment:
        lines.append(f"📈 센티먼트: {esc(sentiment)}")

    return '\n'.join(lines)


def send_message(text):
    """Send message via Telegram Bot API. Returns True if successful."""
    payload = json.dumps({
        "chat_id": CHAT_ID,
        "parse_mode": "HTML",
        "text": text
    }).encode('utf-8')
    
    req = urllib.request.Request(
        API_URL,
        data=payload,
        headers={'Content-Type': 'application/json'}
    )
    
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())
            return result.get('ok', False)
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='replace')
        print(f"  HTTP {e.code}: {body[:200]}")
        return False
    except Exception as ex:
        print(f"  Error: {ex}")
        return False


def set_telegram_sent(filepath):
    """Update telegram_sent: false to telegram_sent: true."""
    with open(filepath, 'r') as f:
        content = f.read()
    new_content = content.replace('telegram_sent: false', 'telegram_sent: true', 1)
    with open(filepath, 'w') as f:
        f.write(new_content)


def main():
    files = glob.glob('/home/user/youtubesearch/data/**/*.md', recursive=True)
    unsent = []
    for f in sorted(files):
        with open(f) as fp:
            content = fp.read()
        if 'telegram_sent: false' in content:
            unsent.append(f)
    
    print(f"Found {len(unsent)} unsent files")
    
    sent_count = 0
    failed_count = 0
    
    for i, filepath in enumerate(unsent):
        with open(filepath) as f:
            content = f.read()
        
        fm = parse_frontmatter(content)
        msg = build_message(fm, filepath)
        
        filename_short = filepath.split('/')[-1][:60]
        print(f"[{i+1}/{len(unsent)}] Sending: {filename_short}")
        
        success = send_message(msg)
        if success:
            set_telegram_sent(filepath)
            sent_count += 1
            print(f"  ✓ sent")
        else:
            failed_count += 1
            print(f"  ✗ FAILED")
        
        time.sleep(1)
    
    print(f"\n=== Telegram Summary ===")
    print(f"Sent: {sent_count}/{len(unsent)}")
    print(f"Failed: {failed_count}")


if __name__ == "__main__":
    main()
