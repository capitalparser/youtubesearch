#!/usr/bin/env python3
"""Bulk enrich unenriched files and send Telegram alerts for all unsent files."""

import os
import re
import time
import json
import urllib.request
import urllib.error
from pathlib import Path

BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
DATA_DIR = Path("/home/user/youtubesearch/data")

VALID_CATEGORIES = ["매크로", "종목분석", "투자아이디어", "산업분석", "시장동향"]
VALID_SECTORS = ["반도체", "AI", "에너지", "원자재", "금융", "부동산", "헬스케어", "소비재", "방산", "크립토", "자동차/EV", "통신"]


def infer_enrichment(title: str, transcript: str) -> dict:
    """Infer enrichment fields from title and transcript content."""
    text = (title + " " + transcript).lower()

    # Categories
    categories = []
    if any(k in text for k in ["매크로", "금리", "연준", "fed", "cpi", "pce", "gdp", "인플레", "기준금리", "통화정책", "전쟁", "이란", "호르무즈", "지정학"]):
        categories.append("매크로")
    if any(k in text for k in ["종목", "주가", "실적", "목표가", "밸류", "기업분석", "분기"]):
        categories.append("종목분석")
    if any(k in text for k in ["투자", "포트폴리오", "전략", "매수", "매도", "etf", "ipo", "상장"]):
        categories.append("투자아이디어")
    if any(k in text for k in ["산업", "섹터", "반도체", "ai 산업", "자동차", "에너지 산업"]):
        categories.append("산업분석")
    if any(k in text for k in ["증시", "시장", "s&p", "나스닥", "코스피", "뉴욕증시", "월가", "개장"]):
        categories.append("시장동향")
    if not categories:
        categories = ["매크로"]

    # Sectors
    sectors = []
    if any(k in text for k in ["반도체", "메모리", "낸드", "dram", "엔비디아", "마이크론", "tsmc", "인텔", "브로드컴", "퀄컴"]):
        sectors.append("반도체")
    if any(k in text for k in ["ai", "인공지능", "gpt", "오픈ai", "앤트로픽", "데이터센터", "gpu", "클로드"]):
        sectors.append("AI")
    if any(k in text for k in ["유가", "원유", "에너지", "호르무즈", "wti", "오일", "lng", "가스", "정유"]):
        sectors.append("에너지")
    if any(k in text for k in ["금", "은", "원자재", "commodity", "구리", "금속"]):
        sectors.append("원자재")
    if any(k in text for k in ["금리", "연준", "fed", "은행", "금융", "채권", "tlt", "tmf", "달러", "환율", "연은", "연방준비"]):
        sectors.append("금융")
    if any(k in text for k in ["부동산", "리츠", "주택", "모기지"]):
        sectors.append("부동산")
    if any(k in text for k in ["헬스케어", "의료", "바이오", "메디케어", "릴리", "eli lilly", "유나이티드헬스"]):
        sectors.append("헬스케어")
    if any(k in text for k in ["소비재", "소비", "리테일", "레고", "갭", "gap", "나이키", "스타벅스", "허먼밀러", "에스티로더", "버블티", "디즈니"]):
        sectors.append("소비재")
    if any(k in text for k in ["방산", "방위", "군사", "무기", "전투기"]):
        sectors.append("방산")
    if any(k in text for k in ["비트코인", "크립토", "암호화폐", "코인"]):
        sectors.append("크립토")
    if any(k in text for k in ["테슬라", "전기차", "ev", "자동차", "웨이모", "자율주행"]):
        sectors.append("자동차/EV")
    if any(k in text for k in ["통신", "5g", "스페이스x", "spacex", "위성"]):
        sectors.append("통신")
    if not sectors:
        sectors = ["에너지"]

    # Tickers
    ticker_patterns = [
        r'\bNVDA\b', r'\bMU\b', r'\bTSMC\b', r'\bINTC\b', r'\bAVGO\b', r'\bQCOM\b',
        r'\bMETA\b', r'\bGOOGL\b', r'\bAAPL\b', r'\bMSFT\b', r'\bAMZN\b', r'\bNFLX\b',
        r'\bTSLA\b', r'\bAMD\b', r'\bARM\b', r'\bMRVL\b', r'\bSMCI\b',
        r'\bTLT\b', r'\bTMF\b', r'\bSPY\b', r'\bQQQ\b',
        r'\bGS\b', r'\bJPM\b', r'\bBAC\b',
        r'\bLLY\b', r'\bUNH\b',
        r'\bMLKN\b', r'\bHNI\b', r'\bNKE\b', r'\bSBUX\b', r'\bDIS\b',
        r'\bPLTR\b', r'\bCRWD\b', r'\bPALO\b', r'\bSNOW\b',
        r'\bCVX\b', r'\bXOM\b',
        r'\bSPCE\b', r'\bGSAT\b',
        r'\b삼성전자\b', r'\bSK하이닉스\b', r'\bLG에너지솔루션\b',
    ]
    full_text = title + " " + transcript
    tickers = []
    for pattern in ticker_patterns:
        if re.search(pattern, full_text, re.IGNORECASE):
            ticker = re.sub(r'\\b', '', pattern).strip()
            tickers.append(ticker)

    # Themes - derive from title keywords
    themes = []
    title_lower = title.lower()
    if "이란" in title_lower or "호르무즈" in title_lower:
        themes.append("이란전쟁")
    if "종전" in title_lower or "휴전" in title_lower:
        themes.append("휴전협상")
    if "금리" in title_lower or "연준" in title_lower or "fed" in title_lower:
        themes.append("금리정책")
    if "반도체" in title_lower or "메모리" in title_lower or "엔비디아" in title_lower or "마이크론" in title_lower:
        themes.append("반도체시장")
    if "ai" in title_lower or "인공지능" in title_lower:
        themes.append("AI투자")
    if "유가" in title_lower or "원유" in title_lower or "wti" in title_lower:
        themes.append("유가변동")
    if "cpi" in title_lower or "인플레" in title_lower or "물가" in title_lower:
        themes.append("인플레이션")
    if "증시" in title_lower or "나스닥" in title_lower or "s&p" in title_lower or "월가" in title_lower:
        themes.append("증시동향")
    if "달러" in title_lower or "환율" in title_lower:
        themes.append("달러환율")
    if "금" in title_lower and ("금값" in title_lower or "금가격" in title_lower):
        themes.append("귀금속투자")
    if "은" in title_lower and "은값" in title_lower:
        themes.append("귀금속투자")
    if "테슬라" in title_lower or "tesla" in title_lower:
        themes.append("테슬라분석")
    if "스페이스x" in title_lower or "spacex" in title_lower or "ipo" in title_lower:
        themes.append("SpaceX상장")
    if "비트코인" in title_lower or "크립토" in title_lower:
        themes.append("크립토시장")
    if "중국" in title_lower or "베이징" in title_lower:
        themes.append("중국경제")
    if "일본" in title_lower or "도쿄" in title_lower or "엔화" in title_lower:
        themes.append("일본경제")
    if "관세" in title_lower:
        themes.append("관세정책")
    if "경기침체" in title_lower or "침체" in title_lower:
        themes.append("경기침체우려")
    if not themes:
        # Extract key nouns from title
        themes = ["글로벌시장동향"]

    # Limit to 3-7 themes
    themes = list(dict.fromkeys(themes))[:7]
    if len(themes) < 3:
        themes.extend(["미국증시", "글로벌경제"][:3 - len(themes)])

    # Sentiment
    bullish_words = ["급등", "상승", "반등", "매수", "강세", "폭등", "호재", "기회", "상향", "상장", "종전", "휴전", "해결"]
    bearish_words = ["급락", "하락", "폭락", "매도", "약세", "공포", "위기", "하향", "봉쇄", "전쟁", "공격", "파괴"]
    bull_count = sum(1 for w in bullish_words if w in text)
    bear_count = sum(1 for w in bearish_words if w in text)
    if bull_count > bear_count + 1:
        sentiment = "bullish"
    elif bear_count > bull_count + 1:
        sentiment = "bearish"
    elif bull_count > 0 and bear_count > 0:
        sentiment = "mixed"
    else:
        sentiment = "neutral"

    return {
        "categories": list(dict.fromkeys(categories)),
        "sectors": list(dict.fromkeys(sectors)),
        "tickers": tickers,
        "themes": themes,
        "sentiment": sentiment,
    }


def parse_frontmatter(content: str):
    """Parse YAML frontmatter from markdown content."""
    if not content.startswith("---"):
        return None, content
    end = content.find("---", 3)
    if end == -1:
        return None, content
    fm_str = content[3:end].strip()
    body = content[end + 3:]
    return fm_str, body


def build_frontmatter(fm_str: str, extra_fields: dict) -> str:
    """Insert extra fields after transcript_language line."""
    lines = fm_str.split("\n")
    result = []
    inserted = False
    for line in lines:
        result.append(line)
        if line.startswith("transcript_language:") and not inserted:
            for key, value in extra_fields.items():
                if isinstance(value, list):
                    result.append(f"{key}: {json.dumps(value, ensure_ascii=False)}")
                elif isinstance(value, bool):
                    result.append(f"{key}: {str(value).lower()}")
                else:
                    result.append(f"{key}: {value}")
            inserted = True
    if not inserted:
        for key, value in extra_fields.items():
            if isinstance(value, list):
                result.append(f"{key}: {json.dumps(value, ensure_ascii=False)}")
            elif isinstance(value, bool):
                result.append(f"{key}: {str(value).lower()}")
            else:
                result.append(f"{key}: {value}")
    return "\n".join(result)


def get_field(fm_str: str, field: str):
    """Extract a field value from frontmatter string."""
    for line in fm_str.split("\n"):
        if line.startswith(f"{field}:"):
            val = line[len(field)+1:].strip()
            # Remove quotes
            if val.startswith('"') and val.endswith('"'):
                val = val[1:-1]
            elif val.startswith("'") and val.endswith("'"):
                val = val[1:-1]
            return val
    return ""


def get_list_field(fm_str: str, field: str):
    """Extract a list field from frontmatter."""
    for line in fm_str.split("\n"):
        if line.startswith(f"{field}:"):
            val = line[len(field)+1:].strip()
            try:
                lst = json.loads(val)
                if isinstance(lst, list):
                    return lst
            except Exception:
                pass
            # Try simple bracket parse
            val = val.strip("[]")
            if not val:
                return []
            return [x.strip().strip('"').strip("'") for x in val.split(",") if x.strip()]
    return []


def send_telegram(text: str) -> bool:
    """Send a message via Telegram Bot API."""
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
        body = e.read().decode("utf-8", errors="replace")
        print(f"  HTTP error {e.code}: {body[:200]}")
        return False
    except Exception as ex:
        print(f"  Error: {ex}")
        return False


def update_telegram_sent(filepath: Path, sent: bool = True):
    """Update telegram_sent field in file."""
    content = filepath.read_text(encoding="utf-8")
    new_content = re.sub(
        r"(telegram_sent:\s*)false",
        f"telegram_sent: {'true' if sent else 'false'}",
        content
    )
    filepath.write_text(new_content, encoding="utf-8")


def enrich_file(filepath: Path):
    """Add enrichment fields to an unenriched file."""
    content = filepath.read_text(encoding="utf-8")
    fm_str, body = parse_frontmatter(content)
    if fm_str is None:
        return

    title = get_field(fm_str, "title")
    transcript_section = ""
    if "## 스크립트" in body:
        idx = body.find("## 스크립트")
        end_idx = body.find("##", idx + 5)
        if end_idx == -1:
            transcript_section = body[idx + len("## 스크립트"):]
        else:
            transcript_section = body[idx + len("## 스크립트"):end_idx]

    enrichment = infer_enrichment(title, transcript_section)
    extra_fields = {
        "enriched": "true",
        "enriched_at": "2026-04-12",
        "categories": enrichment["categories"],
        "sectors": enrichment["sectors"],
        "tickers": enrichment["tickers"],
        "themes": enrichment["themes"],
        "sentiment": enrichment["sentiment"],
        "telegram_sent": "false",
    }

    new_fm = build_frontmatter(fm_str, extra_fields)
    new_content = f"---\n{new_fm}\n---{body}"
    filepath.write_text(new_content, encoding="utf-8")
    print(f"  Enriched: {filepath.name}")


def process_all():
    """Main processing loop."""
    # Step 1: Enrich unenriched files
    print("\n=== Step 2: Enriching unenriched files ===")
    unenriched = []
    for md in DATA_DIR.rglob("*.md"):
        if "analysis" in str(md) or "links" in str(md):
            continue
        content = md.read_text(encoding="utf-8")
        if "enriched: true" not in content:
            unenriched.append(md)

    print(f"Found {len(unenriched)} unenriched files")
    for f in unenriched:
        enrich_file(f)

    # Step 2: Find all unsent files
    print("\n=== Step 3: Finding all unsent files ===")
    unsent = []
    for md in sorted(DATA_DIR.rglob("*.md")):
        if "analysis" in str(md) or "links" in str(md):
            continue
        content = md.read_text(encoding="utf-8")
        if "telegram_sent: true" not in content:
            unsent.append(md)

    print(f"Found {len(unsent)} unsent files to process")

    # Step 3: Send Telegram alerts
    print("\n=== Step 4: Sending Telegram alerts ===")
    sent_count = 0
    failed_count = 0

    for i, filepath in enumerate(unsent):
        content = filepath.read_text(encoding="utf-8")
        fm_str, _ = parse_frontmatter(content)
        if fm_str is None:
            print(f"  [SKIP] No frontmatter: {filepath.name}")
            continue

        channel = get_field(fm_str, "channel")
        title = get_field(fm_str, "title")
        date = get_field(fm_str, "date")
        url = get_field(fm_str, "url")
        sectors = get_list_field(fm_str, "sectors")
        tickers = get_list_field(fm_str, "tickers")
        themes = get_list_field(fm_str, "themes")
        sentiment = get_field(fm_str, "sentiment")

        # Build message
        lines = [
            f"<b>{channel}</b> {title}",
            f"📅 {date}",
            f'🔗 <a href="{url}">영상 보기</a>',
            f"📊 섹터: {', '.join(sectors) if sectors else '기타'}",
        ]
        if tickers:
            lines.append(f"💹 종목: {', '.join(tickers)}")
        lines.append(f"🎯 테마: {', '.join(themes) if themes else '일반'}")
        lines.append(f"📈 센티먼트: {sentiment if sentiment else 'neutral'}")
        message = "\n".join(lines)

        print(f"  [{i+1}/{len(unsent)}] Sending: {filepath.name[:60]}...")
        success = send_telegram(message)
        if success:
            update_telegram_sent(filepath, True)
            sent_count += 1
            print(f"    ✓ Sent")
        else:
            failed_count += 1
            print(f"    ✗ Failed")

        if i < len(unsent) - 1:
            time.sleep(1)

    print(f"\n=== Done: {sent_count} sent, {failed_count} failed ===")
    return sent_count, failed_count


if __name__ == "__main__":
    process_all()
