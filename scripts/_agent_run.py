#!/usr/bin/env python3
"""Agent run script: enrich unenriched files, then send all unsent Telegram alerts."""

import os
import re
import time
import json
import urllib.request
import urllib.error

DATA_DIR = "/home/user/youtubesearch/data"
TODAY = "2026-04-13"
BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
TELEGRAM_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

# Enrichment rules based on title/channel analysis
def infer_enrichment(title, channel, has_transcript):
    """Infer enrichment fields from title and channel."""
    title_lower = title.lower()

    categories = []
    sectors = []
    tickers = []
    themes = []
    sentiment = "neutral"

    # Common macro signals
    macro_keywords = ["금리", "연준", "fed", "cpi", "gdp", "인플레", "경기", "경제", "달러", "환율",
                      "관세", "무역", "협상", "전쟁", "이란", "호르무즈", "원유", "유가", "에너지",
                      "국채", "tlt", "tmf", "트럼프", "바이든", "연은", "통화정책"]

    sector_map = {
        "반도체": ["반도체", "엔비디아", "nvidia", "삼성", "sk하이닉스", "마이크론", "micron", "tsmc", "인텔", "intel",
                   "퀄컴", "qualcomm", "브로드컴", "broadcom", "amd", "암"],
        "AI": ["ai", "인공지능", "gpt", "오픈ai", "openai", "앤트로픽", "anthropic", "llm", "딥러닝"],
        "에너지": ["에너지", "원유", "oil", "wti", "호르무즈", "이란", "석유", "가스", "lng", "tlt", "tmf"],
        "원자재": ["금", "은", "silver", "gold", "원자재", "copper", "구리", "원유"],
        "금융": ["금리", "연준", "fed", "은행", "금융", "채권", "국채", "etf", "금융주"],
        "부동산": ["부동산", "주택", "mortgage", "모기지", "리츠", "reit"],
        "헬스케어": ["헬스케어", "의료", "약", "biotech", "바이오", "medicare", "메디케어"],
        "소비재": ["소비재", "소매", "retail", "브랜드", "consumer", "레고", "나이키", "갭", "gap", "허먼밀러"],
        "방산": ["방산", "방위", "무기", "군사", "국방", "전쟁"],
        "크립토": ["비트코인", "bitcoin", "crypto", "크립토", "코인"],
        "자동차/EV": ["테슬라", "tesla", "전기차", "ev", "자율주행", "waymo", "웨이모"],
        "통신": ["통신", "5g", "스페이스x", "spacex", "위성"],
    }

    bullish_kw = ["상승", "급등", "bull", "긍정", "호재", "상향", "매수", "부활", "돌파", "상장"]
    bearish_kw = ["하락", "급락", "bear", "부정", "악재", "하향", "매도", "위기", "폭락", "공포", "눈물", "충격"]

    # Determine categories
    is_macro = any(kw in title_lower for kw in macro_keywords)
    if is_macro:
        categories.append("매크로")

    # Sector analysis
    for sec, kws in sector_map.items():
        if any(kw in title_lower for kw in kws):
            sectors.append(sec)

    if not sectors and is_macro:
        sectors = ["에너지"]

    if not categories:
        if any(kw in title_lower for kw in ["분석", "투자", "포트폴리오", "종목"]):
            categories.append("종목분석")
        else:
            categories.append("시장동향")

    # Sentiment
    bull_count = sum(1 for kw in bullish_kw if kw in title_lower)
    bear_count = sum(1 for kw in bearish_kw if kw in title_lower)
    if bull_count > bear_count:
        sentiment = "bullish"
    elif bear_count > bull_count:
        sentiment = "bearish"
    elif bull_count > 0 and bear_count > 0:
        sentiment = "mixed"
    else:
        sentiment = "neutral"

    return categories, sectors, tickers, themes, sentiment


def enrich_file(filepath, title, channel):
    """Add enrichment fields to a file's frontmatter."""
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # Check if already enriched
    if "enriched: true" in content:
        return False

    # Find transcript section for theme extraction
    transcript_match = re.search(r"## 스크립트\s*\n(.*?)(?=\n##|\Z)", content, re.DOTALL)
    transcript_text = transcript_match.group(1) if transcript_match else ""

    categories, sectors, tickers, themes, sentiment = infer_enrichment(title, channel, bool(transcript_text.strip()))

    # Build themes from title keywords
    title_words = re.findall(r'[\w가-힣]+', title)
    stop_words = {"의", "이", "가", "은", "는", "을", "를", "에", "도", "에서", "로", "으로", "와", "과", "들",
                  "한", "하", "있", "없", "그", "저", "이다", "있다", "없다", "하다", "되다", "된", "된다",
                  "LIVE", "live", "shorts", "Shorts"}
    meaningful_words = [w for w in title_words if len(w) >= 2 and w not in stop_words]

    # Focus on substantive Korean words
    theme_candidates = []
    for word in meaningful_words:
        if len(word) >= 2 and re.search(r'[가-힣]', word):
            theme_candidates.append(word)

    # Deduplicate and pick 3-7
    seen = set()
    unique_themes = []
    for t in theme_candidates:
        if t not in seen and len(unique_themes) < 7:
            seen.add(t)
            unique_themes.append(t)

    themes = unique_themes if unique_themes else [title[:20]]

    # Build enrichment YAML block
    cats_yaml = json.dumps(categories, ensure_ascii=False)
    secs_yaml = json.dumps(sectors, ensure_ascii=False)
    tick_yaml = json.dumps(tickers, ensure_ascii=False)
    themes_yaml = json.dumps(themes, ensure_ascii=False)

    enrichment = f"""  enriched: true
  enriched_at: {TODAY}
  categories: {cats_yaml}
  sectors: {secs_yaml}
  tickers: {tick_yaml}
  themes: {themes_yaml}
  sentiment: {sentiment}
  telegram_sent: false"""

    # Insert after transcript_language line
    new_content = re.sub(
        r'(transcript_language:.*?\n)(---)',
        r'\1' + enrichment + '\n' + r'---',
        content,
        count=1,
        flags=re.DOTALL
    )

    if new_content == content:
        # Try inserting before closing ---
        new_content = re.sub(
            r'\n---\n',
            '\n' + enrichment + '\n---\n',
            content,
            count=1
        )

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(new_content)

    return True


def parse_frontmatter(content):
    """Parse YAML frontmatter from markdown content."""
    match = re.match(r'^---\s*\n(.*?)\n---', content, re.DOTALL)
    if not match:
        return {}

    fm = {}
    yaml_text = match.group(1)

    for line in yaml_text.split('\n'):
        line = line.strip()
        if ':' in line:
            key, _, val = line.partition(':')
            key = key.strip()
            val = val.strip()

            # Handle quoted strings (must check before array check)
            if (val.startswith('"') and val.endswith('"')) or \
               (val.startswith("'") and val.endswith("'")):
                val = val[1:-1]
            # Handle JSON arrays (only if NOT a quoted string)
            elif val.startswith('['):
                try:
                    val = json.loads(val)
                except:
                    val = [v.strip().strip('"\'') for v in val.strip('[]').split(',') if v.strip()]

            fm[key] = val

    return fm


def send_telegram(message):
    """Send a message via Telegram Bot API."""
    payload = json.dumps({
        "chat_id": CHAT_ID,
        "parse_mode": "HTML",
        "text": message
    }).encode("utf-8")

    req = urllib.request.Request(
        TELEGRAM_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            return result.get("ok", False)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        print(f"  HTTP Error {e.code}: {body}")
        return False
    except Exception as ex:
        print(f"  Error: {ex}")
        return False


def mark_sent(filepath):
    """Mark a file as sent in its frontmatter."""
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    new_content = content.replace("telegram_sent: false", "telegram_sent: true", 1)
    if "telegram_sent" not in content:
        # Add it before closing ---
        new_content = re.sub(r'(\n---\n)', '\n  telegram_sent: true\1', content, count=1)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(new_content)


def build_message(fm, filepath):
    """Build Telegram message from frontmatter."""
    channel = fm.get("channel", "Unknown")
    title = fm.get("title", os.path.basename(filepath))
    date = fm.get("date", "")
    url = fm.get("url", "")

    sectors = fm.get("sectors", [])
    if isinstance(sectors, list):
        sectors_str = ", ".join(sectors) if sectors else "-"
    else:
        sectors_str = str(sectors) if sectors else "-"

    tickers = fm.get("tickers", [])
    if isinstance(tickers, list):
        tickers_str = ", ".join(tickers) if tickers else ""
    else:
        tickers_str = str(tickers).strip("[]'\"") if tickers else ""

    themes = fm.get("themes", [])
    if isinstance(themes, list):
        themes_str = ", ".join(themes) if themes else "-"
    else:
        themes_str = str(themes) if themes else "-"

    sentiment = fm.get("sentiment", "neutral")

    # Build message
    lines = [
        f"<b>{channel}</b> {title}",
        f"📅 {date}",
        f"🔗 <a href=\"{url}\">영상 보기</a>",
        f"📊 섹터: {sectors_str}",
    ]

    if tickers_str:
        lines.append(f"💹 종목: {tickers_str}")

    lines.append(f"🎯 테마: {themes_str}")
    lines.append(f"📈 센티먼트: {sentiment}")

    return "\n".join(lines)


def main():
    # Step 1: Find and enrich unenriched files
    print("=== Step 1: Enriching unenriched files ===")
    all_md_files = []
    for root, dirs, files in os.walk(DATA_DIR):
        for f in sorted(files):
            if f.endswith(".md"):
                all_md_files.append(os.path.join(root, f))

    all_md_files.sort()
    print(f"Total markdown files: {len(all_md_files)}")

    enriched_count = 0
    for filepath in all_md_files:
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        if "enriched: true" not in content:
            fm = parse_frontmatter(content)
            title = fm.get("title", os.path.basename(filepath))
            channel = fm.get("channel", "")
            print(f"  Enriching: {os.path.basename(filepath)}")
            if enrich_file(filepath, title, channel):
                enriched_count += 1

    print(f"Enriched {enriched_count} files.")

    # Step 2: Find all unsent files
    print("\n=== Step 2: Finding all unsent files ===")
    unsent_files = []
    for filepath in all_md_files:
        # Skip non-channel data (links directory)
        if "/links/" in filepath:
            continue

        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        # Check telegram_sent
        if "telegram_sent: true" not in content:
            fm = parse_frontmatter(content)
            unsent_files.append((filepath, fm))

    print(f"Unsent files: {len(unsent_files)}")

    # Step 3: Send alerts
    print("\n=== Step 3: Sending Telegram alerts ===")
    sent_count = 0
    failed_count = 0

    for i, (filepath, fm) in enumerate(unsent_files):
        filename = os.path.basename(filepath)
        print(f"  [{i+1}/{len(unsent_files)}] Sending: {filename[:60]}...")

        message = build_message(fm, filepath)

        success = send_telegram(message)
        if success:
            mark_sent(filepath)
            sent_count += 1
            print(f"    ✓ Sent")
        else:
            failed_count += 1
            print(f"    ✗ Failed")

        # Rate limiting delay
        if i < len(unsent_files) - 1:
            time.sleep(1)

    print(f"\n=== Done: {sent_count} sent, {failed_count} failed ===")
    return sent_count, failed_count


if __name__ == "__main__":
    sent, failed = main()
    print(f"\n__RESULT_JSON__:{json.dumps({'sent': sent, 'failed': failed})}")
