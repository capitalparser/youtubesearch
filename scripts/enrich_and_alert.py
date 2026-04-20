#!/usr/bin/env python3
"""Enrich unenriched markdown files and send Telegram alerts for all unsent files."""

import os
import re
import time
import requests

DATA_DIR = "/home/user/youtubesearch/data"
BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
TODAY = "2026-04-20"

# Manual enrichment for files without transcripts (based on title analysis)
MANUAL_ENRICHMENTS = {
    "2026-04-18_미국 증시 많이 올랐지만 더 간다는데 _ 월가백브리핑.md": {
        "categories": ["매크로", "시장동향"],
        "sectors": ["금융"],
        "tickers": [],
        "themes": ["미국증시", "주가상승", "월가전망", "강세론", "시장동향"],
        "sentiment": "bullish",
    },
    "2026-04-18_기업 실적부터 전쟁 실황까지 한눈에..미국에서 가장 많이 쓰이는 우주 데이터 기업 _ 바이아메리카 in 뉴욕.md": {
        "categories": ["종목분석", "산업분석"],
        "sectors": ["AI", "방산"],
        "tickers": [],
        "themes": ["우주데이터", "기업실적", "위성데이터", "바이아메리카", "미국기업분석"],
        "sentiment": "neutral",
    },
    "2026-04-18__세탁기가 월세 1000달러 좌우_ 100년 된 뉴욕 아파트 비밀 _ 홍기자의 美쿡 _ 홍성용 특파원.md": {
        "categories": ["투자아이디어", "시장동향"],
        "sectors": ["부동산"],
        "tickers": [],
        "themes": ["뉴욕부동산", "아파트임대", "월세시장", "미국생활비", "부동산투자"],
        "sentiment": "neutral",
    },
    "2026-04-18_[어바웃 뉴욕] 미국 1020 열광…세컨핸즈, 유통 공룡될까 _ 길금희 특파원.md": {
        "categories": ["산업분석", "투자아이디어"],
        "sectors": ["소비재"],
        "tickers": [],
        "themes": ["중고거래", "세컨핸즈", "MZ세대소비", "리세일시장", "유통산업변화"],
        "sentiment": "neutral",
    },
}


def find_all_md_files():
    result = []
    for root, dirs, files in os.walk(DATA_DIR):
        for f in files:
            if f.endswith(".md"):
                result.append(os.path.join(root, f))
    return sorted(result)


def parse_frontmatter(content):
    """Extract YAML frontmatter dict (simple key:value parser)."""
    m = re.match(r'^---\n(.*?)\n---\n', content, re.DOTALL)
    if not m:
        return None, None, None
    fm_str = m.group(1)
    rest = content[m.end():]
    return fm_str, rest, m.group(0)


def get_field(fm_str, key):
    """Get a simple string field from frontmatter string."""
    m = re.search(rf'^{key}:\s*(.+)$', fm_str, re.MULTILINE)
    if m:
        return m.group(1).strip().strip('"').strip("'")
    return ""


def get_list_field(fm_str, key):
    """Get a list field from frontmatter string."""
    m = re.search(rf'^{key}:\s*\[([^\]]*)\]', fm_str, re.MULTILINE)
    if m:
        items = [x.strip().strip('"').strip("'") for x in m.group(1).split(',') if x.strip()]
        return items
    return []


def is_enriched(fm_str):
    return bool(re.search(r'^enriched:\s*true', fm_str, re.MULTILINE))


def has_telegram_sent_true(fm_str):
    return bool(re.search(r'^telegram_sent:\s*true', fm_str, re.MULTILINE))


def enrich_file(filepath, enrichment):
    """Insert enrichment fields into frontmatter after transcript_language."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    fm_str, rest, full_fm = parse_frontmatter(content)
    if fm_str is None:
        print(f"  [SKIP] No frontmatter: {filepath}")
        return

    categories = enrichment["categories"]
    sectors = enrichment["sectors"]
    tickers = enrichment["tickers"]
    themes = enrichment["themes"]
    sentiment = enrichment["sentiment"]

    cats_str = "[" + ", ".join(categories) + "]"
    sects_str = "[" + ", ".join(sectors) + "]"
    ticks_str = "[" + ", ".join(tickers) + "]"
    themes_str = "[" + ", ".join(themes) + "]"

    new_fields = (
        f"  enriched: true\n"
        f"  enriched_at: {TODAY}\n"
        f"  categories: {cats_str}\n"
        f"  sectors: {sects_str}\n"
        f"  tickers: {ticks_str}\n"
        f"  themes: {themes_str}\n"
        f"  sentiment: {sentiment}\n"
        f"  telegram_sent: false"
    )

    # Insert after transcript_language line
    def replacer(m):
        return m.group(0) + "\n" + new_fields

    new_fm_str, count = re.subn(
        r'^(transcript_language:.*)$',
        replacer,
        fm_str,
        count=1,
        flags=re.MULTILINE
    )

    if count == 0:
        # Append at end of frontmatter
        new_fm_str = fm_str + "\n" + new_fields

    new_content = "---\n" + new_fm_str + "\n---\n" + rest
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)
    print(f"  [ENRICHED] {os.path.basename(filepath)}")


def html_escape(text):
    return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


def build_message(fm_str):
    """Build Telegram HTML message from frontmatter."""
    channel = get_field(fm_str, 'channel')
    title = get_field(fm_str, 'title')
    date = get_field(fm_str, 'date')
    url = get_field(fm_str, 'url')
    sectors = get_list_field(fm_str, 'sectors')
    tickers = get_list_field(fm_str, 'tickers')
    themes = get_list_field(fm_str, 'themes')
    sentiment = get_field(fm_str, 'sentiment')

    lines = [
        f"<b>{html_escape(channel)}</b> {html_escape(title)}",
        f"📅 {date}",
        f'🔗 <a href="{url}">영상 보기</a>',
        f"📊 섹터: {', '.join(sectors) if sectors else '-'}",
    ]
    if tickers:
        lines.append(f"💹 종목: {', '.join(tickers)}")
    lines.append(f"🎯 테마: {', '.join(themes) if themes else '-'}")
    lines.append(f"📈 센티먼트: {sentiment}")

    return "\n".join(lines)


def send_telegram(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    resp = requests.post(url, json={
        "chat_id": CHAT_ID,
        "parse_mode": "HTML",
        "text": text,
    }, timeout=15)
    return resp.status_code, resp.json()


def mark_sent(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    new_content = re.sub(
        r'^telegram_sent:\s*false',
        'telegram_sent: true',
        content,
        flags=re.MULTILINE
    )
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)


def main():
    all_files = find_all_md_files()
    print(f"Total markdown files: {len(all_files)}")

    # Step 1: Enrich unenriched files
    print("\n=== Enriching unenriched files ===")
    for filepath in all_files:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        fm_str, rest, _ = parse_frontmatter(content)
        if fm_str is None:
            continue
        if not is_enriched(fm_str):
            fname = os.path.basename(filepath)
            if fname in MANUAL_ENRICHMENTS:
                enrich_file(filepath, MANUAL_ENRICHMENTS[fname])
            else:
                print(f"  [NO ENRICHMENT DATA] {fname}")

    # Step 2: Send Telegram alerts for all unsent files
    print("\n=== Sending Telegram alerts ===")
    sent_count = 0
    fail_count = 0
    skip_count = 0

    for filepath in all_files:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        fm_str, rest, _ = parse_frontmatter(content)
        if fm_str is None:
            skip_count += 1
            continue
        if has_telegram_sent_true(fm_str):
            skip_count += 1
            continue

        # Check it's enriched now
        if not is_enriched(fm_str):
            print(f"  [SKIP - not enriched] {os.path.basename(filepath)}")
            skip_count += 1
            continue

        msg = build_message(fm_str)
        status_code, resp_json = send_telegram(msg)

        if status_code == 200 and resp_json.get('ok'):
            mark_sent(filepath)
            sent_count += 1
            print(f"  [SENT {sent_count}] {os.path.basename(filepath)}")
            time.sleep(1)
        else:
            fail_count += 1
            print(f"  [FAIL] {os.path.basename(filepath)} — {status_code} {resp_json}")

    print(f"\n=== Done: {sent_count} sent, {fail_count} failed, {skip_count} skipped ===")


if __name__ == "__main__":
    main()
