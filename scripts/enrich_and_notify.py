"""
Enrichment + Telegram 전송 통합 스크립트
- 미처리 YouTube/링크 파일을 Claude API로 enrichment
- 결과를 텔레그램으로 전송

환경변수:
    ANTHROPIC_API_KEY: Claude API 키
    TELEGRAM_BOT_TOKEN: 텔레그램 봇 토큰
    TELEGRAM_CHAT_ID: 텔레그램 채팅 ID
"""

import json
import os
import re
import sys
import time
import requests
from pathlib import Path
from datetime import datetime, timezone

import anthropic

# ── 설정 ──────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
REPO_ROOT = SCRIPT_DIR.parent
DATA_DIR = REPO_ROOT / "data"

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# 진행자 매핑
HOST_MAP = {
    "빈난새의 개장전요것만": "빈난새 | 개장전요것만",
    "빈난새의 빈틈없이월가": "빈난새 | 빈틈없이월가",
    "김종학의 뉴욕, 지금": "김종학 | 뉴욕지금",
    "김현석의 월스트리트나우": "김현석 | 월스트리트나우",
    "김은정의 베이징나우": "김은정 | 베이징나우",
    "김일규의 도쿄나우": "김일규 | 도쿄나우",
    "홍장원의 불앤베어": "홍장원 | 불앤베어",
    "홍키자의 매일뉴욕": "홍키자 | 매일뉴욕",
    "박종훈의 지식한방": "박종훈 | 지식한방",
}


def send_telegram(text: str) -> bool:
    """텔레그램 메시지 전송 (4000자 제한)"""
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram credentials not set, skipping", file=sys.stderr)
        return False
    # 4000자 초과 시 자름
    if len(text) > 4000:
        text = text[:3990] + "\n...(잘림)"
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": int(CHAT_ID), "text": text},
            timeout=10,
        )
        time.sleep(1)
        return resp.json().get("ok", False)
    except Exception as e:
        print(f"Telegram 전송 실패: {e}", file=sys.stderr)
        return False


def find_unenriched_files() -> list[Path]:
    """enriched: true가 없는 YouTube .md 파일 찾기"""
    files = []
    for channel_dir in ["hkglobalmarket", "MK_Invest", "kpunch"]:
        d = DATA_DIR / channel_dir
        if not d.exists():
            continue
        for f in d.glob("*.md"):
            content = f.read_text(encoding="utf-8")
            if "enriched: true" not in content:
                files.append(f)
    return files


def find_unenriched_links() -> list[Path]:
    """enriched가 없는 링크 파일 찾기"""
    files = []
    for sub in ["youtube", "articles", "x_posts", "texts"]:
        d = DATA_DIR / "links" / sub
        if not d.exists():
            continue
        for f in d.glob("*.md"):
            content = f.read_text(encoding="utf-8")
            if "enriched: true" not in content:
                files.append(f)
    return files


def find_new_link_jsons() -> list[dict]:
    """오늘 수집된 미처리 링크 JSON 로드"""
    links_dir = DATA_DIR / "links"
    if not links_dir.exists():
        return []
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    json_file = links_dir / f"{today}_links.json"
    if not json_file.exists():
        return []
    return json.loads(json_file.read_text())


def enrich_with_claude(content: str, file_type: str = "youtube") -> dict | None:
    """Claude API로 enrichment 수행"""
    if not ANTHROPIC_KEY:
        print("ANTHROPIC_API_KEY not set, 규칙 기반 fallback", file=sys.stderr)
        return None

    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

    has_transcript = "has_transcript: true" in content or "## 스크립트" in content
    content_snippet = content[:8000] if has_transcript else content[:3000]

    if file_type == "youtube":
        context_note = "아래는 YouTube 영상의 전체 스크립트야." if has_transcript else "아래는 YouTube 영상의 제목과 메타데이터만 있어 (자막 없음). 제목에서 최대한 추론해줘."
        prompt = f"""{context_note}

{content_snippet}

반환 형식 (JSON만, 다른 텍스트 없이):
{{
  "categories": ["매크로", "종목분석"],
  "sectors": ["반도체", "에너지"],
  "tickers": ["MU", "GOOGL"],
  "themes": ["KV캐시압축", "이란전쟁"],
  "sentiment": "bearish",
  "summary_macro": ["유가 WTI 94달러(+4.5%)", "금리 2Y 3.93%"],
  "summary_stocks": ["MU: 5일간 -20%, 과잉반응 시각"],
  "summary_ideas": ["현금 비중 확대 - 골드만삭스 권고"],
  "narrative": "서술형 3~5단락. 비전문가가 이해할 수 있도록 WHY 설명, 전문용어 정의, 인과관계 연결, 구체적 숫자 포함."
}}

categories는 매크로/종목분석/투자아이디어/산업분석/시장동향 중 선택.
sectors는 반도체/AI/소프트웨어/클라우드/에너지/원자재/금융/부동산/헬스케어/소비재/방산/크립토/자동차EV/통신 중 선택.
tickers는 미국=심볼(MU), 한국=종목명(삼성전자), ETF 포함.
narrative는 한국어로 작성. 비전문가도 이해할 수 있게 배경과 인과관계를 설명.
자막이 없으면 summary 항목은 제목에서 추론 가능한 것만, narrative에 "자막 미제공 — 제목 기반 분석"이라고 명시."""
    else:
        prompt = f"""다음 콘텐츠를 분석하여 JSON으로 반환해줘.

{content_snippet}

반환 형식 (JSON만):
{{
  "categories": [],
  "sectors": [],
  "tickers": [],
  "themes": [],
  "sentiment": "neutral",
  "summary": ["핵심 포인트 1", "핵심 포인트 2"],
  "narrative": "서술형 2~3단락. 한국어. 비전문가 이해 가능하게."
}}"""

    try:
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}],
        )
        text = msg.content[0].text.strip()
        # JSON 추출
        if text.startswith("{"):
            return json.loads(text)
        # ```json ... ``` 패턴
        m = re.search(r'\{[\s\S]*\}', text)
        if m:
            return json.loads(m.group())
    except Exception as e:
        print(f"Claude API 오류: {e}", file=sys.stderr)
    return None


def identify_host(title: str) -> tuple[str, str]:
    """제목에서 진행자|프로그램과 토픽 추출"""
    for pattern, host in HOST_MAP.items():
        if pattern in title:
            # 토픽: 패턴 이후 내용
            idx = title.find(pattern) + len(pattern)
            topic = re.sub(r'^[\s\-\]|]+', '', title[idx:])
            topic = topic[:60] if topic else title[:60]
            return host, topic
    if "월가백브리핑" in title:
        topic = title.replace("월가백브리핑", "").strip(" -_|[]")
        return "월가백브리핑", topic[:60]
    return "", title[:60]


def enrich_youtube_file(filepath: Path) -> dict | None:
    """YouTube .md 파일에 enrichment 추가"""
    content = filepath.read_text(encoding="utf-8")

    # 스크립트 부분 추출
    result = enrich_with_claude(content, "youtube")
    if not result:
        return None

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # frontmatter에 enrichment 필드 추가
    enrichment_fields = f"""enriched: true
enriched_at: {today}
categories: {json.dumps(result.get('categories', []), ensure_ascii=False)}
sectors: {json.dumps(result.get('sectors', []), ensure_ascii=False)}
tickers: {json.dumps(result.get('tickers', []), ensure_ascii=False)}
themes: {json.dumps(result.get('themes', []), ensure_ascii=False)}
sentiment: {result.get('sentiment', 'neutral')}"""

    # frontmatter 끝(---) 앞에 삽입
    parts = content.split("---", 2)
    if len(parts) >= 3:
        new_content = parts[0] + "---" + parts[1] + enrichment_fields + "\n---" + parts[2]
    else:
        return None

    # 핵심 요약 섹션 삽입
    summary_lines = ["## 핵심 요약\n"]
    if result.get("summary_macro"):
        summary_lines.append("### 매크로\n")
        for b in result["summary_macro"]:
            summary_lines.append(f"- {b}")
        summary_lines.append("")
    if result.get("summary_stocks"):
        summary_lines.append("### 종목 분석\n")
        for b in result["summary_stocks"]:
            summary_lines.append(f"- {b}")
        summary_lines.append("")
    if result.get("summary_ideas"):
        summary_lines.append("### 투자 아이디어\n")
        for b in result["summary_ideas"]:
            summary_lines.append(f"- {b}")
        summary_lines.append("")

    summary_text = "\n".join(summary_lines)

    # ## 영상 정보와 ## 스크립트 사이에 삽입
    if "## 스크립트" in new_content:
        new_content = new_content.replace(
            "## 스크립트",
            f"{summary_text}\n## 스크립트"
        )

    filepath.write_text(new_content, encoding="utf-8")
    return result


def build_overview_message(results: list[tuple[Path, dict]]) -> str:
    """축약본 메시지 생성"""
    lines = [f"📺 YouTube 신규 ({datetime.now().strftime('%m/%d')}) — {len(results)}건\n"]

    for i, (filepath, result) in enumerate(results, 1):
        title = ""
        # frontmatter에서 title 추출
        content = filepath.read_text(encoding="utf-8")
        m = re.search(r'title:\s*"(.+?)"', content)
        if m:
            title = m.group(1)

        host, topic = identify_host(title)
        display = host if host else filepath.parent.name
        tickers = ", ".join(result.get("tickers", [])[:5]) or "—"
        themes = ", ".join(result.get("themes", [])[:3]) or "—"
        sentiment = result.get("sentiment", "neutral")

        lines.append(f"━━━━━━━━━━━━━━━━━━")
        lines.append(f"{'1️⃣2️⃣3️⃣4️⃣5️⃣6️⃣7️⃣8️⃣9️⃣🔟'[(i-1)*3:(i-1)*3+3] if i <= 10 else f'{i}.'} {display}")
        lines.append(f'"{topic}"')
        lines.append("")

        # 카테고리별 bullet
        if result.get("summary_macro"):
            lines.append("[매크로]")
            for b in result["summary_macro"][:3]:
                lines.append(f"• {b}")
            lines.append("")
        if result.get("summary_stocks"):
            lines.append("[종목 분석]")
            for b in result["summary_stocks"][:3]:
                lines.append(f"• {b}")
            lines.append("")
        if result.get("summary_ideas"):
            lines.append("[투자 아이디어]")
            for b in result["summary_ideas"][:2]:
                lines.append(f"• {b}")
            lines.append("")

    return "\n".join(lines)


def build_detail_message(filepath: Path, result: dict, idx: int, total: int) -> str:
    """상세본 (서술형) 메시지 생성"""
    content = filepath.read_text(encoding="utf-8")
    m = re.search(r'title:\s*"(.+?)"', content)
    title = m.group(1) if m else filepath.stem
    m2 = re.search(r'url:\s*"(.+?)"', content)
    url = m2.group(1) if m2 else ""

    host, topic = identify_host(title)
    display = host if host else filepath.parent.name
    sentiment = result.get("sentiment", "neutral")
    sectors = ", ".join(result.get("sectors", []))

    lines = [
        f"📋 {idx}/{total} — {display}",
        f'"{topic}"',
        f"{datetime.now().strftime('%Y-%m-%d')} | {sentiment} | {sectors}",
        "",
        result.get("narrative", ""),
        "",
    ]
    if url:
        lines.append(f"🔗 {url}")

    return "\n".join(lines)


def process_links(link_data: list[dict]) -> list[tuple[str, dict]]:
    """링크 처리 + enrichment"""
    results = []
    for link in link_data:
        link_type = link.get("type", "")
        memo = link.get("memo", "")
        url = link.get("url", "")

        if link_type == "youtube" and url:
            # YouTube 링크 — 자막 추출
            vid_match = re.search(r'[?&]v=([^&]+)', url)
            if vid_match:
                vid = vid_match.group(1)
                try:
                    from youtube_transcript_api import YouTubeTranscriptApi
                    ytt = YouTubeTranscriptApi()
                    fetched = ytt.fetch(vid)
                    transcript = "\n".join(s.text for s in fetched.snippets[:200])
                    content = f"YouTube 영상 자막:\n{transcript}"
                except Exception:
                    content = f"YouTube URL: {url}\n메모: {memo}"
            else:
                content = f"YouTube URL: {url}\n메모: {memo}"
            result = enrich_with_claude(content, "other")
            if result:
                results.append((f"🎥 YouTube — {url}", result))

        elif link_type == "article" and url:
            # 기사/블로그 — 본문 추출
            try:
                resp = requests.get(url, timeout=10, headers={
                    "User-Agent": "Mozilla/5.0"
                })
                # 간단한 텍스트 추출 (HTML 태그 제거)
                text = re.sub(r'<[^>]+>', ' ', resp.text)
                text = re.sub(r'\s+', ' ', text)[:5000]
                content = f"기사 URL: {url}\n본문:\n{text}"
            except Exception:
                content = f"기사 URL: {url}\n메모: {memo}"
            result = enrich_with_claude(content, "other")
            if result:
                results.append((f"📰 기사 — {url}", result))

        elif link_type == "x_post" and url:
            content = f"X 게시글 URL: {url}\n메모: {memo}"
            result = enrich_with_claude(content, "other")
            if result:
                results.append((f"🐦 X — {url}", result))

        elif link_type == "text" and memo:
            content = f"사용자 텍스트:\n{memo}"
            result = enrich_with_claude(content, "other")
            if result:
                results.append(("📝 텍스트", result))

    return results


def build_fallback_message(files: list[Path]) -> str:
    """Claude API 실패 시 제목만으로 축약본 생성"""
    lines = [f"📺 YouTube 신규 ({datetime.now().strftime('%m/%d')}) — {len(files)}건\n"]
    lines.append("(자동 분석 실패 — 제목만 표시)\n")
    for i, f in enumerate(files, 1):
        content = f.read_text(encoding="utf-8")
        m = re.search(r'title:\s*"(.+?)"', content)
        title = m.group(1) if m else f.stem
        host, topic = identify_host(title)
        display = host if host else f.parent.name
        m2 = re.search(r'url:\s*"(.+?)"', content)
        url = m2.group(1) if m2 else ""
        lines.append(f"{i}. {display}")
        lines.append(f'   "{topic}"')
        if url:
            lines.append(f"   {url}")
        lines.append("")
    return "\n".join(lines)


def main():
    print("=== Enrich & Notify 시작 ===")

    # ── YouTube enrichment ──
    unenriched = find_unenriched_files()
    print(f"미처리 YouTube 파일: {len(unenriched)}건")

    yt_results = []
    for f in unenriched:
        print(f"  Enriching: {f.name[:50]}")
        result = enrich_youtube_file(f)
        if result:
            yt_results.append((f, result))

    # ── 링크 처리 ──
    link_data = find_new_link_jsons()
    print(f"신규 링크: {len(link_data)}건")
    link_results = process_links(link_data)

    # ── 텔레그램 전송 ──
    has_yt = bool(yt_results) or bool(unenriched)
    has_links = bool(link_results) or bool(link_data)

    if not has_yt and not has_links:
        print("신규 콘텐츠 없음 — 스킵")
        return

    # YouTube 축약본
    if yt_results:
        # enrichment 성공 — 풀 메시지
        overview = build_overview_message(yt_results)
        print(f"축약본 전송 ({len(overview)}자)")
        send_telegram(overview)

        # YouTube 상세본 (투자 관련만)
        investment_cats = {"매크로", "종목분석", "투자아이디어", "산업분석"}
        inv_results = [
            (f, r) for f, r in yt_results
            if set(r.get("categories", [])) & investment_cats
        ]
        for i, (f, r) in enumerate(inv_results, 1):
            detail = build_detail_message(f, r, i, len(inv_results))
            print(f"상세본 {i}/{len(inv_results)} 전송 ({len(detail)}자)")
            send_telegram(detail)
    elif unenriched:
        # enrichment 실패 — 제목만 전송
        fallback = build_fallback_message(unenriched)
        print(f"Fallback 축약본 전송 ({len(fallback)}자)")
        send_telegram(fallback)

    # 링크 다이제스트
    if link_results:
        lines = [f"📎 링크 다이제스트 ({datetime.now().strftime('%m/%d')}) — {len(link_results)}건\n"]
        for i, (label, result) in enumerate(link_results, 1):
            themes = ", ".join(result.get("themes", [])[:3])
            lines.append(f"{i}. {label}")
            lines.append(f"   테마: {themes}")
            lines.append("")
        send_telegram("\n".join(lines))

        # 링크 상세본
        for label, result in link_results:
            narrative = result.get("narrative", "")
            if narrative:
                msg = f"{label}\n\n{narrative}"
                send_telegram(msg)
    elif link_data:
        # 링크 enrichment 실패 — 최소 알림
        lines = [f"📎 링크 수집 ({datetime.now().strftime('%m/%d')}) — {len(link_data)}건\n"]
        lines.append("(자동 분석 실패 — 목록만 표시)\n")
        for i, link in enumerate(link_data, 1):
            url = link.get("url", "")
            memo = link.get("memo", "")[:80]
            lines.append(f"{i}. {link.get('type', '?')} — {url or memo}")
        send_telegram("\n".join(lines))

    print(f"=== 완료: YouTube {len(yt_results)}건(미처리 {len(unenriched)}건), 링크 {len(link_results)}건 ===")


if __name__ == "__main__":
    main()
