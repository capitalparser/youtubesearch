#!/usr/bin/env python3
"""
Comprehensive script to:
1. Enrich unenriched files based on title/content analysis
2. Send Telegram alerts for all unsent files
3. Update telegram_sent: true after successful sends
"""

import os
import re
import time
import json
import urllib.request
import urllib.parse
import urllib.error
from pathlib import Path
from datetime import date

TODAY = date.today().strftime("%Y-%m-%d")
DATA_DIR = Path("/home/user/youtubesearch/data")
BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
TELEGRAM_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"


def parse_frontmatter(content):
    """Parse YAML frontmatter from markdown content."""
    if not content.startswith("---"):
        return {}, content
    end = content.find("\n---", 3)
    if end == -1:
        return {}, content
    fm_text = content[3:end].strip()
    body = content[end + 4:]

    fm = {}
    # Simple line-by-line YAML parse (handles basic types)
    current_key = None
    current_list = None
    for line in fm_text.split("\n"):
        # Check for list continuation
        if line.strip().startswith("- ") and current_list is not None:
            val = line.strip()[2:].strip()
            current_list.append(val)
            continue
        # Check for key: value
        m = re.match(r'^(\w+):\s*(.*)', line)
        if m:
            current_list = None
            key = m.group(1)
            val = m.group(2).strip()
            if val.startswith("[") and val.endswith("]"):
                # Inline list
                inner = val[1:-1].strip()
                if inner:
                    fm[key] = [v.strip().strip('"').strip("'") for v in inner.split(",")]
                else:
                    fm[key] = []
            elif val.startswith("["):
                # Multi-line list start
                fm[key] = []
                current_list = fm[key]
                current_key = key
                inner = val[1:].strip()
                if inner:
                    current_list.append(inner.strip().strip('"').strip("'"))
            elif val in ("true", "false"):
                fm[key] = val == "true"
            elif val.startswith('"') and val.endswith('"'):
                fm[key] = val[1:-1]
            elif val.startswith("'") and val.endswith("'"):
                fm[key] = val[1:-1]
            else:
                fm[key] = val
    return fm, body


def set_frontmatter_field(content, key, value):
    """Set or add a field in YAML frontmatter."""
    if not content.startswith("---"):
        return content
    end = content.find("\n---", 3)
    if end == -1:
        return content
    fm_text = content[3:end]
    body = content[end + 4:]

    # Remove existing key if present
    lines = fm_text.split("\n")
    new_lines = []
    skip_list = False
    for line in lines:
        if re.match(rf'^{re.escape(key)}:', line):
            skip_list = True
            continue
        if skip_list:
            if line.strip().startswith("- ") or (line.strip().startswith("[") and not re.match(r'^\w+:', line)):
                continue
            else:
                skip_list = False
        new_lines.append(line)

    # Format value
    if isinstance(value, bool):
        val_str = "true" if value else "false"
    elif isinstance(value, list):
        val_str = "[" + ", ".join(value) + "]"
    else:
        val_str = str(value)

    # Find insertion point: after transcript_language if exists, else at end
    insert_after = "transcript_language"
    insert_idx = len(new_lines)
    for i, line in enumerate(new_lines):
        if re.match(rf'^{re.escape(insert_after)}:', line):
            insert_idx = i + 1
            break

    new_lines.insert(insert_idx, f"{key}: {val_str}")
    new_fm = "\n".join(new_lines)
    return f"---{new_fm}\n---{body}"


def enrich_file(filepath, content, fm):
    """Add enrichment fields based on title and content analysis."""
    title = fm.get("title", "")
    channel = fm.get("channel", "")
    source = fm.get("source", "YouTube")

    # --- Category detection ---
    categories = []
    macro_keywords = ["금리", "연준", "Fed", "PCE", "GDP", "인플레", "고용", "경기침체", "매크로", "통화정책", "FOMC", "국채", "달러", "BOJ", "BOK", "중앙은행", "경제성장", "무역"]
    stock_keywords = ["목표가", "투자의견", "매수", "매도", "주가", "종목", "실적", "어닝", "EPS", "PER"]
    idea_keywords = ["투자전략", "포트폴리오", "자산배분", "알파", "투자아이디어"]
    industry_keywords = ["반도체", "AI", "인공지능", "배터리", "전기차", "EV", "바이오", "헬스케어", "방산", "에너지", "원자재", "금융", "부동산", "소비재", "크립토", "공급망", "파운드리"]
    market_keywords = ["시황", "증시", "지수", "S&P", "나스닥", "다우", "코스피", "개장전", "장마감", "뉴욕", "월스트리트", "글로벌마켓"]

    combined = title + " " + content[:2000]

    if any(k in combined for k in macro_keywords):
        categories.append("매크로")
    if any(k in combined for k in stock_keywords):
        categories.append("종목분석")
    if any(k in combined for k in idea_keywords):
        categories.append("투자아이디어")
    if any(k in combined for k in industry_keywords):
        categories.append("산업분석")
    if any(k in combined for k in market_keywords) or "매일뉴욕" in title or "개장전" in title:
        categories.append("시장동향")

    if not categories:
        categories = ["시장동향"]

    # --- Sector detection ---
    sectors = []
    sector_map = {
        "반도체": ["반도체", "메모리", "DRAM", "HBM", "파운드리", "마이크론", "엔비디아", "TSMC", "인텔", "퀄컴", "브로드컴", "AMD"],
        "AI": ["AI", "인공지능", "LLM", "데이터센터", "오픈AI", "앤트로픽", "클로드", "GPT", "딥시크"],
        "에너지": ["에너지", "유가", "원유", "WTI", "브렌트", "호르무즈", "석유", "가스", "LNG"],
        "원자재": ["금", "은", "구리", "원자재", "원유", "commodity"],
        "금융": ["연준", "Fed", "금리", "은행", "채권", "달러", "금융", "CBDC", "사모"],
        "부동산": ["부동산", "주택", "모기지", "REITs"],
        "헬스케어": ["헬스케어", "바이오", "제약", "비만치료제", "GLP", "일라이릴리", "LLY", "노보"],
        "소비재": ["소비재", "소비자", "브랜드", "유통", "나이키", "갭", "Gap", "레고"],
        "방산": ["방산", "방위산업", "무기", "군사"],
        "크립토": ["비트코인", "크립토", "암호화폐", "이더리움"],
        "자동차/EV": ["테슬라", "전기차", "EV", "자동차", "배터리", "웨이모"],
        "통신": ["통신", "5G", "위성", "스페이스X", "스타링크"],
    }

    combined_lower = combined.lower()
    for sector, keywords in sector_map.items():
        if any(k.lower() in combined_lower for k in keywords):
            sectors.append(sector)

    if not sectors:
        sectors = ["금융"]

    # --- Ticker detection ---
    ticker_patterns = {
        "NVDA": ["엔비디아", "NVDA", "Nvidia"],
        "MU": ["마이크론", "MU", "Micron"],
        "META": ["메타", "META"],
        "GOOGL": ["구글", "GOOGL", "Alphabet"],
        "MSFT": ["마이크로소프트", "MSFT"],
        "AAPL": ["애플", "AAPL"],
        "TSLA": ["테슬라", "TSLA"],
        "AMZN": ["아마존", "AMZN"],
        "INTC": ["인텔", "INTC"],
        "AVGO": ["브로드컴", "AVGO"],
        "AMD": ["AMD", "에이엠디"],
        "LLY": ["일라이릴리", "LLY", "1라리"],
        "NFLX": ["넷플릭스", "NFLX"],
        "PLTR": ["팔란티어", "PLTR"],
        "ORCL": ["오라클", "ORCL"],
        "NKE": ["나이키", "NKE"],
        "UNH": ["유나이티드헬스", "UNH"],
        "QCOM": ["퀄컴", "QCOM"],
        "삼성전자": ["삼성전자", "삼성"],
        "SK하이닉스": ["SK하이닉스", "하이닉스"],
        "CRWV": ["코어위브", "CoreWeave", "CRWV"],
    }

    tickers = []
    for ticker, keywords in ticker_patterns.items():
        if any(k in combined for k in keywords):
            tickers.append(ticker)

    # --- Theme detection (3-7 keywords) ---
    themes = []
    theme_candidates = []

    # Extract from title
    if "호르무즈" in title:
        theme_candidates.append("호르무즈해협")
    if "이란" in title:
        theme_candidates.append("이란사태")
    if "전쟁" in title:
        theme_candidates.append("중동전쟁")
    if "휴전" in title:
        theme_candidates.append("휴전협상")
    if "금리" in title:
        theme_candidates.append("금리정책")
    if "연준" in title or "Fed" in title:
        theme_candidates.append("연준통화정책")
    if "반도체" in title:
        theme_candidates.append("반도체산업")
    if "AI" in title or "인공지능" in title:
        theme_candidates.append("AI투자")
    if "엔비디아" in title:
        theme_candidates.append("엔비디아")
    if "테슬라" in title:
        theme_candidates.append("테슬라")
    if "중국" in title or "中" in title:
        theme_candidates.append("중국경제")
    if "메모리" in title:
        theme_candidates.append("메모리반도체")
    if "유가" in title or "원유" in title or "WTI" in title:
        theme_candidates.append("국제유가")
    if "달러" in title:
        theme_candidates.append("달러강세")
    if "PCE" in title:
        theme_candidates.append("PCE물가지수")
    if "고용" in title:
        theme_candidates.append("고용지표")
    if "증시" in title or "시황" in title:
        theme_candidates.append("증시시황")
    if "개장전" in title:
        theme_candidates.append("개장전요약")
    if "비만" in title or "GLP" in title:
        theme_candidates.append("비만치료제")
    if "공급망" in title:
        theme_candidates.append("공급망재편")
    if "IPO" in title:
        theme_candidates.append("IPO")
    if "스페이스X" in title or "SpaceX" in title:
        theme_candidates.append("스페이스X")
    if "금" in title and ("금값" in title or "금가격" in title or "은" in title):
        theme_candidates.append("귀금속")
    if "CBDC" in title or "디지털화폐" in title:
        theme_candidates.append("CBDC")

    # Ensure at least 3 themes
    if len(theme_candidates) < 3:
        # Add generic themes based on channel/source
        if channel in ["한경 글로벌마켓"]:
            theme_candidates.extend(["글로벌시장동향", "월스트리트분석", "뉴욕증시"])
        elif channel in ["매경 월가월부"]:
            theme_candidates.extend(["뉴욕시황", "월가분석", "글로벌투자"])
        elif channel in ["박종훈의 지식한방"]:
            theme_candidates.extend(["지정학리스크", "경제분석", "투자전략"])
        elif source == "BOK경제연구":
            theme_candidates.extend(["한국은행연구", "경제분석", "금융정책"])
        else:
            theme_candidates.extend(["글로벌경제", "투자전략", "시장분석"])

    themes = list(dict.fromkeys(theme_candidates))[:7]  # dedupe, max 7
    if len(themes) < 3:
        themes = (themes + ["글로벌경제", "투자전략", "시장분석"])[:3]

    # --- Sentiment detection ---
    bullish_words = ["급등", "상승", "반등", "매수", "강세", "호재", "기회", "상향", "긍정", "bullish", "기대"]
    bearish_words = ["급락", "하락", "폭락", "매도", "약세", "악재", "위험", "하향", "부정", "bearish", "위기", "침체"]

    bull_count = sum(1 for w in bullish_words if w in combined)
    bear_count = sum(1 for w in bearish_words if w in combined)

    if bull_count > bear_count + 1:
        sentiment = "bullish"
    elif bear_count > bull_count + 1:
        sentiment = "bearish"
    elif bull_count > 0 and bear_count > 0:
        sentiment = "mixed"
    else:
        sentiment = "neutral"

    return {
        "enriched": True,
        "enriched_at": TODAY,
        "categories": categories,
        "sectors": sectors,
        "tickers": tickers,
        "themes": themes,
        "sentiment": sentiment,
        "telegram_sent": False,
    }


def update_frontmatter(content, updates):
    """Update multiple frontmatter fields."""
    if not content.startswith("---"):
        return content
    end = content.find("\n---", 3)
    if end == -1:
        return content
    fm_text = content[3:end]
    body = content[end + 4:]

    # Parse existing lines, tracking list fields to skip
    lines = fm_text.split("\n")
    cleaned_lines = []
    skip_until_next_key = False

    for line in lines:
        is_key_line = bool(re.match(r'^\w+:', line))
        if is_key_line:
            key = re.match(r'^(\w+):', line).group(1)
            if key in updates:
                skip_until_next_key = True
                continue
            else:
                skip_until_next_key = False
                cleaned_lines.append(line)
        elif skip_until_next_key:
            # Skip list items belonging to a replaced key
            if line.strip().startswith("- ") or not line.strip():
                continue
            else:
                skip_until_next_key = False
                cleaned_lines.append(line)
        else:
            cleaned_lines.append(line)

    # Find insertion point after transcript_language
    insert_after_key = "transcript_language"
    insert_idx = len(cleaned_lines)
    for i, line in enumerate(cleaned_lines):
        if re.match(rf'^{re.escape(insert_after_key)}:', line):
            insert_idx = i + 1
            break

    # Build new field lines
    new_field_lines = []
    field_order = ["enriched", "enriched_at", "categories", "sectors", "tickers", "themes", "sentiment", "telegram_sent"]
    for key in field_order:
        if key in updates:
            val = updates[key]
            if isinstance(val, bool):
                val_str = "true" if val else "false"
            elif isinstance(val, list):
                val_str = "[" + ", ".join(val) + "]"
            else:
                val_str = str(val)
            new_field_lines.append(f"{key}: {val_str}")

    # Insert new fields
    final_lines = cleaned_lines[:insert_idx] + new_field_lines + cleaned_lines[insert_idx:]
    new_fm = "\n".join(final_lines)
    return f"---{new_fm}\n---{body}"


def send_telegram(text):
    """Send a Telegram message. Returns True on success."""
    data = json.dumps({"chat_id": CHAT_ID, "parse_mode": "HTML", "text": text}).encode("utf-8")
    req = urllib.request.Request(
        TELEGRAM_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            return result.get("ok", False)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        print(f"  [HTTP ERROR {e.code}] {body[:200]}")
        return False
    except Exception as e:
        print(f"  [SEND ERROR] {e}")
        return False


def build_telegram_message(fm, filepath):
    """Build Telegram message text from frontmatter."""
    channel = fm.get("channel", "Unknown")
    title = fm.get("title", Path(filepath).stem)
    dt = fm.get("date", "")
    url = fm.get("url", "")
    sectors = fm.get("sectors", [])
    tickers = fm.get("tickers", [])
    themes = fm.get("themes", [])
    sentiment = fm.get("sentiment", "neutral")
    source = fm.get("source", "YouTube")

    # Escape HTML special chars
    def esc(s):
        return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    lines = []
    lines.append(f"<b>{esc(channel)}</b> {esc(title)}")
    if dt:
        lines.append(f"📅 {esc(str(dt))}")
    if url:
        lines.append(f"🔗 <a href=\"{url}\">영상 보기</a>")
    if sectors:
        lines.append(f"📊 섹터: {esc(', '.join(str(s) for s in sectors))}")
    if tickers:
        lines.append(f"💹 종목: {esc(', '.join(str(t) for t in tickers))}")
    if themes:
        lines.append(f"🎯 테마: {esc(', '.join(str(t) for t in themes))}")
    lines.append(f"📈 센티먼트: {esc(sentiment)}")

    return "\n".join(lines)


def process_all():
    # Collect all markdown files
    md_files = sorted(DATA_DIR.rglob("*.md"))
    print(f"Total markdown files: {len(md_files)}")

    unenriched = []
    unsent = []

    for fp in md_files:
        content = fp.read_text(encoding="utf-8")
        fm, _ = parse_frontmatter(content)
        if not fm.get("enriched"):
            unenriched.append(fp)
        if not fm.get("telegram_sent"):
            unsent.append(fp)

    print(f"Files needing enrichment: {len(unenriched)}")
    print(f"Files needing Telegram send: {len(unsent)}")

    # Step 2: Enrich unenriched files
    print("\n=== STEP 2: Enriching files ===")
    for fp in unenriched:
        print(f"  Enriching: {fp.name}")
        content = fp.read_text(encoding="utf-8")
        fm, body_text = parse_frontmatter(content)

        # Get script content for analysis
        script_match = re.search(r'## 스크립트\s*\n(.*?)(?=\n##|\Z)', content, re.DOTALL)
        script_content = script_match.group(1) if script_match else ""

        enrichment = enrich_file(fp, content + script_content[:3000], fm)
        new_content = update_frontmatter(content, enrichment)
        fp.write_text(new_content, encoding="utf-8")
        print(f"    -> enriched: categories={enrichment['categories']}, sentiment={enrichment['sentiment']}")

    # Step 4: Send Telegram for all unsent files
    print(f"\n=== STEP 4: Sending {len(unsent)} Telegram alerts ===")
    sent_count = 0
    failed_count = 0

    for fp in unsent:
        content = fp.read_text(encoding="utf-8")
        fm, _ = parse_frontmatter(content)

        msg = build_telegram_message(fm, fp)
        print(f"  Sending ({sent_count+1}/{len(unsent)}): {fp.name[:60]}...")

        success = send_telegram(msg)
        if success:
            # Update telegram_sent: true
            new_content = update_frontmatter(content, {"telegram_sent": True})
            fp.write_text(new_content, encoding="utf-8")
            sent_count += 1
            print(f"    -> OK (sent)")
            time.sleep(1)
        else:
            failed_count += 1
            print(f"    -> FAILED")

    print(f"\n=== DONE ===")
    print(f"Enriched: {len(unenriched)} files")
    print(f"Sent: {sent_count}/{len(unsent)} messages")
    print(f"Failed: {failed_count}")


if __name__ == "__main__":
    process_all()
