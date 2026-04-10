#!/usr/bin/env python3
"""Send Telegram alerts for all unsent enriched markdown files."""
import os, re, time, json, yaml, requests

BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID   = "7698095566"
API_URL   = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
DATA_DIR  = "/home/user/youtubesearch/data"

FM_RE = re.compile(r'^---\s*\n(.*?)\n---\s*\n', re.DOTALL)

def read_fm(path):
    with open(path, encoding='utf-8') as f:
        raw = f.read()
    m = FM_RE.match(raw)
    if not m:
        return None, raw
    try:
        fm = yaml.safe_load(m.group(1)) or {}
    except Exception:
        fm = {}
    return fm, raw

def html_esc(s):
    return str(s).replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')

def list_str(v):
    if isinstance(v, list):
        return ', '.join(str(x) for x in v)
    return str(v) if v else ''

def build_msg(fm, path):
    title   = html_esc(fm.get('title') or os.path.basename(path))
    channel = html_esc(fm.get('channel') or fm.get('source') or '알 수 없음')
    date    = str(fm.get('date', ''))
    url     = fm.get('url', '')
    source  = str(fm.get('source', 'YouTube'))
    sectors = list_str(fm.get('sectors', []))
    tickers = list_str(fm.get('tickers', []))
    themes  = list_str(fm.get('themes', []))
    sentiment = fm.get('sentiment', 'neutral')

    if 'YouTube' in source:
        link_text = '영상 보기'
    elif 'BOK' in source or 'bok' in source.lower():
        link_text = '논문 보기'
    elif 'X' in source:
        link_text = '포스트 보기'
    else:
        link_text = '보기'

    lines = [f'<b>[{channel}]</b> {title}', f'📅 {date}']
    if url:
        lines.append(f'🔗 <a href="{url}">{link_text}</a>')
    if sectors:
        lines.append(f'📊 섹터: {sectors}')
    if tickers:
        lines.append(f'💹 종목: {tickers}')
    if themes:
        lines.append(f'🎯 테마: {themes}')
    lines.append(f'📈 센티먼트: {sentiment}')
    return '\n'.join(lines)

def send(text):
    try:
        r = requests.post(API_URL, json={'chat_id': CHAT_ID, 'parse_mode': 'HTML', 'text': text}, timeout=30)
        data = r.json()
        return data.get('ok', False), data
    except Exception as e:
        return False, str(e)

def mark_sent(path, raw):
    if 'telegram_sent: false' in raw:
        new = raw.replace('telegram_sent: false', 'telegram_sent: true', 1)
    else:
        # insert before closing ---
        new = FM_RE.sub(lambda m: m.group(0).replace(
            '\n---\n', '\ntelegram_sent: true\n---\n', 1), raw)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(new)

def find_unsent():
    files = []
    for root, _, fnames in os.walk(DATA_DIR):
        for fn in sorted(fnames):
            if not fn.endswith('.md'):
                continue
            fp = os.path.join(root, fn)
            try:
                with open(fp, encoding='utf-8') as f:
                    txt = f.read()
                if 'telegram_sent: false' in txt:
                    files.append(fp)
                elif 'telegram_sent:' not in txt and txt.startswith('---'):
                    files.append(fp)
            except Exception:
                pass
    return sorted(files)

def main():
    files = find_unsent()
    print(f"Found {len(files)} unsent files\n")
    sent = failed = skipped = 0

    for fp in files:
        fm, raw = read_fm(fp)
        if not fm:
            print(f"  SKIP (no FM): {os.path.basename(fp)}")
            skipped += 1
            continue

        # Skip files without telegram_sent field AND without enrichment
        if not fm.get('enriched') and 'telegram_sent' not in raw:
            print(f"  SKIP (not enriched): {os.path.basename(fp)}")
            skipped += 1
            continue

        msg = build_msg(fm, fp)
        label = os.path.basename(fp)[:70]
        print(f"→ {label}")

        ok, resp = send(msg)
        if ok:
            mark_sent(fp, raw)
            sent += 1
            print(f"  ✓ sent")
        else:
            failed += 1
            print(f"  ✗ FAILED: {resp}")

        time.sleep(1)

    print(f"\n=== Done: {sent} sent, {failed} failed, {skipped} skipped ===")

if __name__ == '__main__':
    main()
