"""
Process all unsent markdown files:
- Parse frontmatter
- Send Telegram alert
- Mark telegram_sent: true
"""
import re
import sys
import time
import json
import urllib.request
import urllib.error
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "data"

BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
TG_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"


def parse_frontmatter(text: str) -> dict:
    """Extract YAML frontmatter fields as raw strings."""
    m = re.match(r'^---\n(.*?)\n---', text, re.DOTALL)
    if not m:
        return {}
    block = m.group(1)

    result = {}
    # Parse simple key: value and key: [list] forms
    for line in block.splitlines():
        kv = re.match(r'^(\w+):\s*(.*)', line)
        if not kv:
            continue
        key, val = kv.group(1), kv.group(2).strip()
        # List value: [a, b, c]
        lm = re.match(r'^\[(.*)\]$', val)
        if lm:
            inner = lm.group(1).strip()
            if inner:
                items = [x.strip().strip('"').strip("'") for x in inner.split(',')]
                result[key] = [x for x in items if x]
            else:
                result[key] = []
        elif val.startswith('"') and val.endswith('"'):
            result[key] = val[1:-1]
        elif val.startswith("'") and val.endswith("'"):
            result[key] = val[1:-1]
        else:
            result[key] = val
    return result


def build_message(fm: dict) -> str:
    channel = fm.get('channel') or fm.get('source', '알 수 없음')
    title = fm.get('title', '제목 없음')
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

    lines = [
        f"<b>[{channel}]</b> {title}",
        f"📅 {date}",
        f"🔗 <a href=\"{url}\">영상 보기</a>",
        f"📊 섹터: {sectors_str}",
    ]
    if tickers_str:
        lines.append(f"💹 종목: {tickers_str}")
    lines.append(f"🎯 테마: {themes_str}")
    lines.append(f"📈 센티먼트: {sentiment}")

    return '\n'.join(lines)


def send_telegram(text: str) -> bool:
    payload = json.dumps({
        "chat_id": CHAT_ID,
        "parse_mode": "HTML",
        "text": text,
    }).encode('utf-8')
    req = urllib.request.Request(
        TG_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read())
            return body.get('ok', False)
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='replace')
        print(f"  HTTP error {e.code}: {body[:200]}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"  Error: {e}", file=sys.stderr)
        return False


def mark_sent(filepath: Path):
    text = filepath.read_text(encoding='utf-8')
    new_text = re.sub(r'^telegram_sent: false\s*$', 'telegram_sent: true', text, flags=re.MULTILINE)
    if 'telegram_sent' not in text:
        # Insert before closing ---
        new_text = re.sub(r'(\n---\n)', '\ntelegram_sent: true\1', text, count=1)
    filepath.write_text(new_text, encoding='utf-8')


def find_unsent_files():
    files = []
    for md in sorted(DATA_DIR.rglob("*.md")):
        # Skip analysis and links directories
        parts = md.parts
        if 'analysis' in parts or 'links' in parts:
            continue
        text = md.read_text(encoding='utf-8')
        # Must NOT have telegram_sent: true
        if 'telegram_sent: true' in text:
            continue
        files.append(md)
    return files


def main():
    files = find_unsent_files()
    print(f"Found {len(files)} unsent files")

    sent = 0
    failed = 0

    for i, filepath in enumerate(files):
        text = filepath.read_text(encoding='utf-8')
        fm = parse_frontmatter(text)

        msg = build_message(fm)
        rel = filepath.relative_to(REPO_ROOT)
        print(f"[{i+1}/{len(files)}] Sending: {rel}")
        print(f"  Channel: {fm.get('channel', fm.get('source', '?'))}")

        ok = send_telegram(msg)
        if ok:
            mark_sent(filepath)
            sent += 1
            print(f"  ✓ Sent and marked")
        else:
            failed += 1
            print(f"  ✗ Failed to send")

        # Rate limit: 1 second between messages
        if i < len(files) - 1:
            time.sleep(1)

    print(f"\nDone: {sent} sent, {failed} failed")


if __name__ == "__main__":
    main()
