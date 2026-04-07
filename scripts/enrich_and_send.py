#!/usr/bin/env python3
"""Enrich unenriched files and send Telegram alerts for all unsent files."""

import os
import re
import time
import json
import urllib.request
import urllib.error
from datetime import date
from pathlib import Path

DATA_DIR = Path("/home/user/youtubesearch/data")
BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
TODAY = date.today().isoformat()

ENRICHMENT_MAP = {
    # MK_Invest 2026-04-07 unenriched file
    "2026-04-07_이스라엘, 이란 철도 인프라 파괴 시작": {
        "enriched": True,
        "enriched_at": TODAY,
        "categories": ["시장동향", "종목분석"],
        "sectors": ["에너지", "헬스케어", "반도체", "AI"],
        "tickers": ["UNH", "AVGO", "GOOGL"],
        "themes": ["이스라엘이란공습", "메디케어MA인상", "유나이티드헬스급등", "브로드컴TPU", "구글AI반도체", "매일뉴욕브리핑"],
        "sentiment": "mixed",
        "telegram_sent": False,
    }
}


def parse_frontmatter(content):
    """Parse YAML frontmatter from markdown. Returns (meta_dict, body)."""
    if not content.startswith("---"):
        return {}, content
    end = content.find("\n---", 3)
    if end == -1:
        return {}, content
    fm_text = content[4:end]
    body = content[end + 4:]

    meta = {}
    for line in fm_text.splitlines():
        if ": " in line:
            key, _, val = line.partition(": ")
            key = key.strip()
            val = val.strip()
            # Parse lists
            if val.startswith("[") and val.endswith("]"):
                inner = val[1:-1].strip()
                if inner:
                    items = [i.strip().strip('"').strip("'") for i in inner.split(",")]
                    meta[key] = [i for i in items if i]
                else:
                    meta[key] = []
            elif val.lower() == "true":
                meta[key] = True
            elif val.lower() == "false":
                meta[key] = False
            else:
                meta[key] = val.strip('"').strip("'")
        elif line.strip().startswith("- "):
            pass  # skip nested list items for now
    return meta, body


def build_frontmatter(meta, original_fm_text):
    """Rebuild frontmatter, inserting enrichment fields after transcript_language."""
    lines = original_fm_text.splitlines()
    result = []
    enrichment_inserted = False

    enrichment_keys = ["enriched", "enriched_at", "categories", "sectors", "tickers", "themes", "sentiment", "telegram_sent"]

    def format_value(v):
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, list):
            if not v:
                return "[]"
            return "[" + ", ".join(str(i) for i in v) + "]"
        return str(v)

    for line in lines:
        key = line.split(":")[0].strip() if ":" in line else ""
        # Skip existing enrichment keys (we'll rewrite them)
        if key in enrichment_keys:
            if not enrichment_inserted:
                # Insert all enrichment keys here
                for ek in enrichment_keys:
                    if ek in meta:
                        result.append(f"{ek}: {format_value(meta[ek])}")
                enrichment_inserted = True
            continue

        result.append(line)

        # After transcript_language, insert enrichment if not yet done
        if key == "transcript_language" and not enrichment_inserted:
            for ek in enrichment_keys:
                if ek in meta:
                    result.append(f"{ek}: {format_value(meta[ek])}")
            enrichment_inserted = True

    # If still not inserted (no transcript_language field), append at end
    if not enrichment_inserted:
        for ek in enrichment_keys:
            if ek in meta:
                result.append(f"{ek}: {format_value(meta[ek])}")

    return "\n".join(result)


def rewrite_file_with_enrichment(filepath, enrich_data):
    """Add enrichment fields to a file's frontmatter."""
    content = filepath.read_text(encoding="utf-8")
    if not content.startswith("---"):
        return

    end = content.find("\n---", 3)
    if end == -1:
        return

    fm_text = content[4:end]
    body = content[end + 4:]

    # Parse existing meta
    meta, _ = parse_frontmatter(content)
    # Merge enrichment
    for k, v in enrich_data.items():
        meta[k] = v

    new_fm = build_frontmatter(meta, fm_text)
    new_content = f"---\n{new_fm}\n---{body}"
    filepath.write_text(new_content, encoding="utf-8")
    print(f"  [enriched] {filepath.name}")


def update_telegram_sent(filepath):
    """Set telegram_sent: true in file frontmatter."""
    content = filepath.read_text(encoding="utf-8")
    # Simple replacement
    if "telegram_sent: false" in content:
        new_content = content.replace("telegram_sent: false", "telegram_sent: true", 1)
        filepath.write_text(new_content, encoding="utf-8")
    elif "telegram_sent: False" in content:
        new_content = content.replace("telegram_sent: False", "telegram_sent: true", 1)
        filepath.write_text(new_content, encoding="utf-8")


def send_telegram(text):
    """Send a message via Telegram Bot API. Returns True if successful."""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": CHAT_ID,
        "parse_mode": "HTML",
        "text": text
    }).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read())
            return result.get("ok", False)
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"    [Telegram error {e.code}] {body[:200]}")
        return False
    except Exception as e:
        print(f"    [Telegram error] {e}")
        return False


def build_telegram_message(meta, filepath):
    """Build Telegram message based on file type."""
    source = meta.get("source", "YouTube")
    title = meta.get("title", filepath.stem)
    dt = str(meta.get("date", ""))
    url = meta.get("url", "")
    sectors = meta.get("sectors", [])
    tickers = meta.get("tickers", [])
    themes = meta.get("themes", [])
    sentiment = meta.get("sentiment", "neutral")
    channel = meta.get("channel", "")
    author = meta.get("author", "")
    file_type = meta.get("type", "")

    sectors_str = ", ".join(sectors) if sectors else "-"
    tickers_str = ", ".join(tickers) if tickers else ""
    themes_str = ", ".join(themes) if themes else "-"

    sentiment_emoji = {"bullish": "📈", "bearish": "📉", "neutral": "➡️", "mixed": "↔️"}.get(sentiment, "📈")

    if source == "YouTube":
        header = f"<b>{channel}</b>"
        lines = [
            f"{header} {title}",
            f"📅 {dt}",
        ]
        if url:
            lines.append(f'🔗 <a href="{url}">영상 보기</a>')
        lines.append(f"📊 섹터: {sectors_str}")
        if tickers_str:
            lines.append(f"💹 종목: {tickers_str}")
        lines.append(f"🎯 테마: {themes_str}")
        lines.append(f"{sentiment_emoji} 센티먼트: {sentiment}")

    elif source == "BOK경제연구":
        lines = [
            f"<b>[BOK경제연구]</b> {title}",
            f"📅 {dt}",
        ]
        if url:
            lines.append(f'🔗 <a href="{url}">논문 보기</a>')
        lines.append(f"📊 섹터: {sectors_str}")
        if tickers_str:
            lines.append(f"💹 종목: {tickers_str}")
        lines.append(f"🎯 테마: {themes_str}")
        lines.append(f"{sentiment_emoji} 센티먼트: {sentiment}")

    elif source == "X":
        lines = [
            f"<b>{author}</b> {title}",
            f"📅 {dt}",
        ]
        lines.append(f"📊 섹터: {sectors_str}")
        if tickers_str:
            lines.append(f"💹 종목: {tickers_str}")
        lines.append(f"🎯 테마: {themes_str}")
        lines.append(f"{sentiment_emoji} 센티먼트: {sentiment}")

    else:
        # analysis files
        type_label = {"cross_channel_consensus": "크로스채널 컨센서스", "bok_cross_reference": "BOK 교차검증"}.get(file_type, "분석")
        lines = [
            f"<b>[{type_label}]</b> {dt}",
            f"📊 섹터: {sectors_str}",
        ]
        if tickers_str:
            lines.append(f"💹 종목: {tickers_str}")
        lines.append(f"🎯 테마: {themes_str}")
        lines.append(f"{sentiment_emoji} 센티먼트: {sentiment}")

    return "\n".join(lines)


def main():
    print("=== Step 2: Enrich unenriched files ===")

    # Find files without enriched: true
    unenriched = []
    for md in DATA_DIR.rglob("*.md"):
        content = md.read_text(encoding="utf-8")
        if "enriched: true" not in content and "enriched: True" not in content:
            unenriched.append(md)

    print(f"Found {len(unenriched)} unenriched files")
    for filepath in unenriched:
        stem = filepath.stem
        # Match against enrichment map
        matched = None
        for key, data in ENRICHMENT_MAP.items():
            if key in stem:
                matched = data
                break

        if matched:
            rewrite_file_with_enrichment(filepath, matched)
        else:
            # For x_posts that already have enrichment data but no enriched: true flag
            content = filepath.read_text(encoding="utf-8")
            meta, _ = parse_frontmatter(content)
            if meta.get("categories") and meta.get("themes") and meta.get("sentiment"):
                # Already has enrichment data, just add enriched flag
                enrich_data = {
                    "enriched": True,
                    "enriched_at": TODAY,
                }
                # Preserve existing enrichment
                for k in ["categories", "sectors", "tickers", "themes", "sentiment"]:
                    if k in meta:
                        enrich_data[k] = meta[k]
                if "telegram_sent" not in meta:
                    enrich_data["telegram_sent"] = False
                rewrite_file_with_enrichment(filepath, enrich_data)
            else:
                print(f"  [SKIP - no enrichment data] {filepath.name}")

    print("\n=== Step 3+4: Find and send unsent files ===")

    # Find all unsent files
    unsent = []
    for md in DATA_DIR.rglob("*.md"):
        content = md.read_text(encoding="utf-8")
        if "telegram_sent: false" in content or "telegram_sent: False" in content:
            unsent.append(md)
        elif "telegram_sent" not in content:
            # Missing telegram_sent entirely
            unsent.append(md)

    print(f"Found {len(unsent)} unsent files")

    sent_count = 0
    skip_count = 0

    for i, filepath in enumerate(sorted(unsent)):
        content = filepath.read_text(encoding="utf-8")
        meta, _ = parse_frontmatter(content)

        # Skip if not enriched yet
        if not meta.get("enriched"):
            print(f"  [{i+1}/{len(unsent)}] SKIP (not enriched): {filepath.name[:60]}")
            skip_count += 1
            continue

        msg = build_telegram_message(meta, filepath)
        print(f"  [{i+1}/{len(unsent)}] Sending: {filepath.name[:60]}")

        ok = send_telegram(msg)
        if ok:
            update_telegram_sent(filepath)
            sent_count += 1
            print(f"    ✓ sent")
        else:
            print(f"    ✗ FAILED")

        time.sleep(1)

    print(f"\n=== Done: {sent_count} sent, {skip_count} skipped ===")


if __name__ == "__main__":
    main()
