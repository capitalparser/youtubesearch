#!/usr/bin/env python3
"""
Pipeline: enrich unenriched files, then send Telegram alerts for all unsent files.
"""

import os
import re
import time
import glob
import json
import urllib.request
import urllib.parse

DATA_DIR = "/home/user/youtubesearch/data"
TODAY = "2026-04-14"
BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
TELEGRAM_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

# ── Step 2: Enrichment rules for files without transcripts ───────────────────

ENRICH_RULES = {
    "韓정부, 이란에 50만 달러 인도적 지원": {
        "categories": ["시장동향", "종목분석"],
        "sectors": ["헬스케어", "AI"],
        "tickers": ["AMZN", "NVO"],
        "themes": ["한국정부이란지원", "아마존글로벌스타인수", "노보노디스크오픈AI파트너십", "신약개발가속", "뉴욕증시동향"],
        "sentiment": "neutral",
    },
    "1년 만에 2800% 상승, 샌디스크": {
        "categories": ["종목분석", "시장동향"],
        "sectors": ["반도체"],
        "tickers": ["SNDK", "WDC"],
        "themes": ["샌디스크급등", "나스닥100편입", "낸드플래시", "메모리반도체", "주가급등"],
        "sentiment": "bullish",
    },
    "시장은 당신이 옳은지에 관심없다": {
        "categories": ["투자아이디어"],
        "sectors": [],
        "tickers": [],
        "themes": ["투자심리", "시장예측실패", "리스크관리", "매매전략", "역발상투자"],
        "sentiment": "neutral",
    },
    "개장전 요것만": {
        "categories": ["시장동향"],
        "sectors": [],
        "tickers": [],
        "themes": ["미국장개장전브리핑", "특징주분석", "주요뉴스", "투자전략", "미국증시동향"],
        "sentiment": "neutral",
    },
}


def find_enrich_rule(title: str):
    for key, rule in ENRICH_RULES.items():
        if key in title:
            return rule
    return None


def parse_frontmatter(content: str):
    """Return (frontmatter_str, body_str) or (None, content) if no frontmatter."""
    if not content.startswith("---"):
        return None, content
    end = content.find("\n---", 3)
    if end == -1:
        return None, content
    fm = content[3:end].strip()
    body = content[end + 4:]
    return fm, body


def build_frontmatter(fm_str: str, extra: dict) -> str:
    """Insert extra fields after transcript_language line."""
    lines = fm_str.split("\n")
    result = []
    inserted = False
    for line in lines:
        result.append(line)
        if line.startswith("transcript_language:") and not inserted:
            for k, v in extra.items():
                if isinstance(v, list):
                    if v:
                        result.append(f"{k}: [{', '.join(str(i) for i in v)}]")
                    else:
                        result.append(f"{k}: []")
                else:
                    result.append(f"{k}: {v}")
            inserted = True
    if not inserted:
        for k, v in extra.items():
            if isinstance(v, list):
                if v:
                    result.append(f"{k}: [{', '.join(str(i) for i in v)}]")
                else:
                    result.append(f"{k}: []")
            else:
                result.append(f"{k}: {v}")
    return "\n".join(result)


def read_fm_field(fm_str: str, field: str):
    """Extract a field value from frontmatter string."""
    for line in fm_str.split("\n"):
        if line.startswith(f"{field}:"):
            val = line[len(field) + 1:].strip()
            # strip quotes
            if val.startswith('"') and val.endswith('"'):
                val = val[1:-1]
            return val
    return ""


def parse_list_field(fm_str: str, field: str):
    """Parse a YAML list field like: field: [a, b, c]"""
    for line in fm_str.split("\n"):
        if line.startswith(f"{field}:"):
            val = line[len(field) + 1:].strip()
            if val.startswith("[") and val.endswith("]"):
                inner = val[1:-1].strip()
                if not inner:
                    return []
                return [x.strip() for x in inner.split(",")]
            return [val] if val else []
    return []


def enrich_file(path: str):
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    fm_str, body = parse_frontmatter(content)
    if fm_str is None:
        print(f"  [SKIP] No frontmatter: {path}")
        return

    title = read_fm_field(fm_str, "title")
    rule = find_enrich_rule(title)
    if rule is None:
        print(f"  [WARN] No enrich rule matched for title: {title!r}")
        # fallback: generic enrichment
        rule = {
            "categories": ["시장동향"],
            "sectors": [],
            "tickers": [],
            "themes": ["시장분석", "투자정보"],
            "sentiment": "neutral",
        }

    extra = {
        "enriched": "true",
        "enriched_at": TODAY,
        "categories": rule["categories"],
        "sectors": rule["sectors"],
        "tickers": rule["tickers"],
        "themes": rule["themes"],
        "sentiment": rule["sentiment"],
        "telegram_sent": "false",
    }

    new_fm = build_frontmatter(fm_str, extra)
    new_content = f"---\n{new_fm}\n---{body}"

    with open(path, "w", encoding="utf-8") as f:
        f.write(new_content)
    print(f"  [OK] Enriched: {os.path.basename(path)}")


def mark_sent(path: str):
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    new_content = content.replace("telegram_sent: false", "telegram_sent: true", 1)
    with open(path, "w", encoding="utf-8") as f:
        f.write(new_content)


def send_telegram(text: str) -> bool:
    payload = json.dumps({
        "chat_id": CHAT_ID,
        "parse_mode": "HTML",
        "text": text,
    }).encode("utf-8")
    req = urllib.request.Request(
        TELEGRAM_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read())
            return body.get("ok", False)
    except Exception as e:
        print(f"  [ERROR] Telegram send failed: {e}")
        return False


def build_message(fm_str: str) -> str:
    channel = read_fm_field(fm_str, "channel")
    title = read_fm_field(fm_str, "title")
    date = read_fm_field(fm_str, "date")
    url = read_fm_field(fm_str, "url")
    sectors = parse_list_field(fm_str, "sectors")
    tickers = parse_list_field(fm_str, "tickers")
    themes = parse_list_field(fm_str, "themes")
    sentiment = read_fm_field(fm_str, "sentiment")

    lines = [
        f"<b>[{channel}]</b> {title}",
        f"📅 {date}",
        f'🔗 <a href="{url}">영상 보기</a>',
        f"📊 섹터: {', '.join(sectors) if sectors else '없음'}",
    ]
    if tickers:
        lines.append(f"💹 종목: {', '.join(tickers)}")
    lines.append(f"🎯 테마: {', '.join(themes) if themes else '없음'}")
    lines.append(f"📈 센티먼트: {sentiment}")
    return "\n".join(lines)


def main():
    # ── Step 2: Enrich unenriched files ──────────────────────────────────────
    print("\n=== Step 2: Enriching unenriched files ===")
    all_md = glob.glob(os.path.join(DATA_DIR, "**/*.md"), recursive=True)
    unenriched = []
    for path in all_md:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        if "enriched: true" not in content:
            unenriched.append(path)

    print(f"  Found {len(unenriched)} unenriched files")
    for path in unenriched:
        enrich_file(path)

    # ── Step 3: Find all unsent files ────────────────────────────────────────
    print("\n=== Step 3: Finding unsent files ===")
    all_md = glob.glob(os.path.join(DATA_DIR, "**/*.md"), recursive=True)
    unsent = []
    for path in sorted(all_md):
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        fm_str, _ = parse_frontmatter(content)
        if fm_str is None:
            continue
        sent = read_fm_field(fm_str, "telegram_sent")
        if sent != "true":
            unsent.append((path, fm_str))

    print(f"  Found {len(unsent)} unsent files")

    # ── Step 4: Send Telegram alerts ─────────────────────────────────────────
    print("\n=== Step 4: Sending Telegram alerts ===")
    sent_count = 0
    fail_count = 0
    for i, (path, fm_str) in enumerate(unsent):
        msg = build_message(fm_str)
        print(f"  [{i+1}/{len(unsent)}] Sending: {os.path.basename(path)[:60]}")
        ok = send_telegram(msg)
        if ok:
            mark_sent(path)
            sent_count += 1
            print(f"    -> Sent OK, marked telegram_sent: true")
        else:
            fail_count += 1
            print(f"    -> FAILED")
        if i < len(unsent) - 1:
            time.sleep(1)

    print(f"\n=== Done: {sent_count} sent, {fail_count} failed ===")


if __name__ == "__main__":
    main()
