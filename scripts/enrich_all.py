#!/usr/bin/env python3
"""Enrich all unenriched markdown files with YAML frontmatter fields."""

import glob
import re
import sys

TODAY = "2026-04-07"

# Enrichment data keyed by partial filename match (enough to be unique)
ENRICHMENTS = {
    # === BOK Research Papers ===
    "Predicting_the_Payment_Preferen": {
        "categories": ["매크로", "산업분석"],
        "sectors": ["금융"],
        "tickers": [],
        "themes": ["CBDC", "디지털화폐", "지급결제선호", "소비자선택실험"],
        "sentiment": "neutral",
    },
    "주택담보대출_차입자의_금리_선택": {
        "categories": ["매크로", "산업분석"],
        "sectors": ["금융", "부동산"],
        "tickers": [],
        "themes": ["주택담보대출", "변동금리", "고정금리선택", "가계금융취약성"],
        "sentiment": "neutral",
    },
    "U.S.-Korea_Yield_Synchronizatio": {
        "categories": ["매크로"],
        "sectors": ["금융"],
        "tickers": [],
        "themes": ["한미금리동조화", "국채수익률", "통화정책파급효과", "금융시장연동"],
        "sentiment": "neutral",
    },
    "Brains_to_the_Capital_Wage_Gaps": {
        "categories": ["매크로"],
        "sectors": [],
        "tickers": [],
        "themes": ["임금격차", "인적자본이동", "지역불평등", "두뇌유출"],
        "sentiment": "neutral",
    },
    "Lending_to_vulnerable_household": {
        "categories": ["매크로", "산업분석"],
        "sectors": ["금융"],
        "tickers": [],
        "themes": ["취약계층대출", "금융포용", "가계부채위험", "신용접근성"],
        "sentiment": "neutral",
    },
    "ES_Activities_and_Labor_Product": {
        "categories": ["매크로"],
        "sectors": [],
        "tickers": [],
        "themes": ["창업활동", "노동생산성", "기업가정신", "혁신경제"],
        "sentiment": "neutral",
    },
    "Effects_of_New_Technology_Adopt": {
        "categories": ["매크로", "투자아이디어"],
        "sectors": ["AI", "반도체"],
        "tickers": [],
        "themes": ["AI기술도입효과", "일자리양극화", "자동화충격", "숙련전환비용"],
        "sentiment": "neutral",
    },
    # === MK_Invest ===
    "일시적 반등이냐 다시 대세상승이냐": {
        "categories": ["매크로", "시장동향"],
        "sectors": ["에너지"],
        "tickers": [],
        "themes": ["이란종전기대", "증시반등", "대세상승논쟁", "지정학리스크"],
        "sentiment": "mixed",
    },
    "호르무즈 해협 폐쇄유지에도 전쟁 종료 용의": {
        "categories": ["매크로", "종목분석", "시장동향"],
        "sectors": ["에너지", "AI", "반도체"],
        "tickers": ["NVDA", "MRVL"],
        "themes": ["호르무즈종전협상", "휘발유가격", "엔비디아투자", "마벨투자"],
        "sentiment": "mixed",
    },
    "엔화 160엔선, BOJ 조기인상 압박": {
        "categories": ["매크로"],
        "sectors": ["금융"],
        "tickers": [],
        "themes": ["엔화약세", "BOJ금리인상", "일본통화정책", "외환시장"],
        "sentiment": "neutral",
    },
    "3월 ADP민간고용": {
        "categories": ["매크로", "종목분석"],
        "sectors": ["소비재", "금융"],
        "tickers": ["NKE"],
        "themes": ["ADP고용지표", "나이키실적부진", "美성장률전망", "마진하락"],
        "sentiment": "bearish",
    },
    "긴축 끝이냐, 잠시 멈춘거냐": {
        "categories": ["매크로"],
        "sectors": ["금융"],
        "tickers": [],
        "themes": ["연준통화정책", "금리방향불확실", "긴축사이클종료", "금리전망"],
        "sentiment": "neutral",
    },
    "추락한 Gap, 부활할까": {
        "categories": ["종목분석"],
        "sectors": ["소비재"],
        "tickers": ["GPS"],
        "themes": ["갭부활전략", "소매혁신", "패션리테일변화", "소비자행동"],
        "sentiment": "neutral",
    },
    "호르무즈 한국, 유럽이 하게 두자": {
        "categories": ["매크로"],
        "sectors": ["에너지"],
        "tickers": [],
        "themes": ["트럼프외교", "호르무즈한국", "동맹갈등", "에너지안보"],
        "sentiment": "bearish",
    },
    "우편 투표 했던 트럼프": {
        "categories": ["매크로"],
        "sectors": [],
        "tickers": [],
        "themes": ["트럼프선거제도", "우편투표제한", "미국정치", "선거정책"],
        "sentiment": "neutral",
    },
    "WTI 원유 배럴당 113달러": {
        "categories": ["매크로", "종목분석"],
        "sectors": ["에너지", "자동차/EV"],
        "tickers": ["TSLA"],
        "themes": ["WTI유가급등", "테슬라중국판매", "원자재최고가", "에너지위기"],
        "sentiment": "mixed",
    },
    "아폴로 젤터": {
        "categories": ["매크로", "투자아이디어"],
        "sectors": ["금융"],
        "tickers": ["APO"],
        "themes": ["아폴로글로벌", "사모펀드투자기준", "선별투자시대", "대체자산"],
        "sentiment": "bearish",
    },
    "영국 외무장관": {
        "categories": ["매크로", "시장동향"],
        "sectors": ["에너지"],
        "tickers": [],
        "themes": ["이란호르무즈봉쇄", "영국외교", "세계경제인질", "증시회복이유"],
        "sentiment": "mixed",
    },
    "파키스탄에서 열릴 뻔 했던 미-이란 협상": {
        "categories": ["매크로"],
        "sectors": ["에너지", "방산"],
        "tickers": [],
        "themes": ["미이란협상좌절", "파키스탄중재", "F15전투기격추", "전쟁확전"],
        "sentiment": "bearish",
    },
    "매달 41만원 몰래 샌다": {
        "categories": ["투자아이디어", "산업분석"],
        "sectors": ["소비재"],
        "tickers": [],
        "themes": ["구독경제", "다크패턴", "구독해지방해", "소비자보호"],
        "sentiment": "neutral",
    },
    "버블티 전쟁터 뉴욕": {
        "categories": ["종목분석", "산업분석"],
        "sectors": ["소비재"],
        "tickers": [],
        "themes": ["버블티시장", "뉴욕소비트렌드", "아시아음료브랜드", "저가경쟁"],
        "sentiment": "neutral",
    },
    "미국 부자는 집을 요새로": {
        "categories": ["투자아이디어", "산업분석"],
        "sectors": ["부동산", "소비재"],
        "tickers": [],
        "themes": ["미국부동산", "프리미엄보안주택", "럭셔리주거", "치안불안"],
        "sentiment": "neutral",
    },
    "테슬라, 웨이모 추격": {
        "categories": ["종목분석", "산업분석"],
        "sectors": ["자동차/EV", "AI", "반도체"],
        "tickers": ["TSLA", "NVDA"],
        "themes": ["자율주행경쟁", "테슬라FSD", "웨이모", "엔비디아칩전략변화"],
        "sentiment": "bullish",
    },
    "월요일 협상 타결 임박한 상황": {
        "categories": ["매크로"],
        "sectors": ["에너지"],
        "tickers": [],
        "themes": ["이란협상임박", "트럼프발표", "종전시나리오", "타코트레이드경계"],
        "sentiment": "mixed",
    },
    "월요일 증시 개장 직후, 지옥불": {
        "categories": ["매크로", "시장동향"],
        "sectors": ["에너지"],
        "tickers": [],
        "themes": ["증시개장전망", "지정학불확실성", "이란전쟁영향", "시장변동성"],
        "sentiment": "bearish",
    },
    "이란 석유 갖고 싶지만": {
        "categories": ["매크로"],
        "sectors": ["에너지"],
        "tickers": [],
        "themes": ["트럼프이란협상", "이란석유자원", "종전조건협상", "외교전략"],
        "sentiment": "mixed",
    },
    "스페이스 X 상장 이후, 테슬라 합병": {
        "categories": ["종목분석", "투자아이디어"],
        "sectors": ["AI"],
        "tickers": ["TSLA"],
        "themes": ["스페이스X상장", "테슬라합병시나리오", "일론머스크전략", "IPO"],
        "sentiment": "bullish",
    },
    "키뱅크, 인텔 목표가 70달러": {
        "categories": ["종목분석"],
        "sectors": ["반도체", "소비재"],
        "tickers": ["INTC", "NFLX"],
        "themes": ["인텔목표주가상향", "넷플릭스매수의견", "애널리스트리포트", "증권사투자의견"],
        "sentiment": "bullish",
    },
    # === hkglobalmarket ===
    "알아서 석유 가져와라": {
        "categories": ["매크로", "종목분석", "시장동향"],
        "sectors": ["에너지", "반도체", "소비재"],
        "tickers": ["AAPL", "MRVL", "NKE", "CRWV"],
        "themes": ["이란종전기대", "에너지자립", "종목브리핑", "증시반등"],
        "sentiment": "mixed",
    },
    "종전 베팅 폭발..5% 폭등한": {
        "categories": ["매크로", "시장동향"],
        "sectors": ["AI", "반도체"],
        "tickers": [],
        "themes": ["Mag7급등", "기술적반등", "종전베팅", "증시분석"],
        "sentiment": "mixed",
    },
    "호르무즈 봉쇄상태로 종전 검토": {
        "categories": ["매크로", "종목분석", "시장동향"],
        "sectors": ["에너지", "반도체", "금융"],
        "tickers": ["NVDA", "MRVL", "BRK.B"],
        "themes": ["호르무즈봉쇄종전검토", "버크셔단기채매입", "엔비디아마벨투자", "개장전브리핑"],
        "sentiment": "mixed",
    },
    "한국 오전 10시 대국민 연설": {
        "categories": ["매크로", "종목분석"],
        "sectors": ["에너지", "소비재", "방산"],
        "tickers": ["NKE", "SBUX", "BA", "ORCL", "GM", "LLY"],
        "themes": ["대국민연설", "스페이스XIPO신청", "이란전황", "종목브리핑"],
        "sentiment": "neutral",
    },
    "200일선에 막힌 랠리..바닥 vs 일시적 반등": {
        "categories": ["매크로", "시장동향"],
        "sectors": ["에너지"],
        "tickers": [],
        "themes": ["증시바닥논쟁", "200일이동평균", "종전단계선언", "반등지속성"],
        "sentiment": "mixed",
    },
    "미국장 개장전 알아야 할 5가지": {
        "categories": ["시장동향"],
        "sectors": [],
        "tickers": [],
        "themes": ["개장전브리핑", "미국증시", "특징주", "주요뉴스"],
        "sentiment": "neutral",
    },
    "호르무즈 열려야만 진짜 끝": {
        "categories": ["매크로", "투자아이디어"],
        "sectors": ["에너지"],
        "tickers": [],
        "themes": ["호르무즈개방조건", "진정한반등조건", "3가지시나리오", "이란협상"],
        "sentiment": "mixed",
    },
    "오픈AI, 수익 전략 있을까": {
        "categories": ["종목분석", "산업분석"],
        "sectors": ["AI"],
        "tickers": [],
        "themes": ["오픈AI확장전략", "AI수익화모델", "실리콘밸리진출", "AI사업전략"],
        "sentiment": "neutral",
    },
    "멀어진 종전 기대..트럼프": {
        "categories": ["매크로", "종목분석", "시장동향"],
        "sectors": ["에너지", "자동차/EV"],
        "tickers": ["TSLA", "KO", "AMZN", "COIN"],
        "themes": ["종전기대후퇴", "이란공격강화", "해협통항규칙", "증시하락"],
        "sentiment": "bearish",
    },
    "3월 고용보고서 발표": {
        "categories": ["매크로"],
        "sectors": [],
        "tickers": [],
        "themes": ["고용보고서", "비농업취업자수", "실업률", "연준통화정책"],
        "sentiment": "neutral",
    },
    "통행료 받을 준비": {
        "categories": ["매크로", "시장동향"],
        "sectors": ["에너지"],
        "tickers": [],
        "themes": ["이란통행료", "호르무즈통항협상", "지상군투입우려", "월가반응"],
        "sentiment": "mixed",
    },
    "아마존, GSAT 인수 추진": {
        "categories": ["매크로", "종목분석", "시장동향"],
        "sectors": ["AI", "자동차/EV", "소비재"],
        "tickers": ["AMZN", "GSAT", "TSLA", "LLY", "WMT"],
        "themes": ["아마존위성인수", "개장전브리핑", "이란전황업데이트", "특징주분석"],
        "sentiment": "neutral",
    },
    "중국 차에 관세 장벽": {
        "categories": ["매크로", "산업분석"],
        "sectors": ["자동차/EV"],
        "tickers": [],
        "themes": ["중국전기차수출", "관세장벽", "보호무역주의", "전기차경쟁"],
        "sentiment": "bearish",
    },
    "지난 월요일이 저점": {
        "categories": ["매크로", "시장동향"],
        "sectors": [],
        "tickers": [],
        "themes": ["증시저점론", "바닥신호", "월가분석", "반등전망"],
        "sentiment": "bullish",
    },
    "트리플 약세": {
        "categories": ["매크로"],
        "sectors": ["금융", "에너지"],
        "tickers": [],
        "themes": ["일본트리플약세", "엔화약세", "일본국채하락", "일본증시전쟁충격"],
        "sentiment": "bearish",
    },
    "알맹이 없던 대국민 연설": {
        "categories": ["매크로"],
        "sectors": ["에너지"],
        "tickers": [],
        "themes": ["대국민연설분석", "이란슈퍼파워", "중동패권변화", "전략목적"],
        "sentiment": "neutral",
    },
    "특수부대 배낭까지 인수": {
        "categories": ["종목분석", "투자아이디어"],
        "sectors": ["방산", "소비재"],
        "tickers": [],
        "themes": ["미국방산기업", "특수부대장비", "낚시브랜드인수", "틈새시장"],
        "sentiment": "neutral",
    },
    "이란 전쟁 기자회견": {
        "categories": ["매크로"],
        "sectors": ["에너지"],
        "tickers": [],
        "themes": ["이란전쟁기자회견", "전황집중분석", "협상전망", "이란외교입장"],
        "sentiment": "neutral",
    },
    "이란, 하룻밤이면 제거": {
        "categories": ["매크로", "종목분석", "시장동향"],
        "sectors": ["에너지", "반도체"],
        "tickers": ["MU", "ORCL"],
        "themes": ["이란공격임박", "마이크론실적전망", "지정학위기고조", "증시전망"],
        "sentiment": "mixed",
    },
    "한국 시간 기준 오전 6시 30분 LIVE": {
        "categories": ["시장동향"],
        "sectors": [],
        "tickers": [],
        "themes": ["미국증시생방송", "월스트리트나우", "이란전쟁", "개장동향"],
        "sentiment": "neutral",
    },
    "7일 최후통첩 후 외교적 돌파구": {
        "categories": ["매크로", "종목분석", "시장동향"],
        "sectors": ["반도체", "에너지", "자동차/EV"],
        "tickers": ["MU", "STX", "TSLA", "AMKR", "KTOS", "AAOI"],
        "themes": ["이란최후통첩", "외교협상기대", "반도체사이클구조적", "골디락스종료"],
        "sentiment": "mixed",
    },
    "조금씩 열리는 호르무즈": {
        "categories": ["매크로"],
        "sectors": ["에너지"],
        "tickers": [],
        "themes": ["호르무즈부분개방", "적국동맹구분", "통항기준", "외교협상진전"],
        "sentiment": "bullish",
    },
    "위스키 가격 폭락": {
        "categories": ["투자아이디어", "산업분석"],
        "sectors": ["소비재"],
        "tickers": [],
        "themes": ["위스키시장하락", "주류가격폭락", "소비재투자", "명품주류시장"],
        "sentiment": "bearish",
    },
    # === kpunch ===
    "미국과 이란은 어쩌다 최악의 원수": {
        "categories": ["매크로"],
        "sectors": ["에너지"],
        "tickers": [],
        "themes": ["미이란관계역사", "이란핵협상", "중동분쟁원인", "지정학배경"],
        "sentiment": "neutral",
    },
    # === analysis ===
    "bok_cross_reference": {
        "categories": ["매크로", "투자아이디어"],
        "sectors": ["반도체", "AI"],
        "tickers": ["MU"],
        "themes": ["BOK연구교차검증", "메모리반도체수요", "AI효율화재번스역설", "KV캐시압축"],
        "sentiment": "neutral",
    },
    "cross_channel_consensus": {
        "categories": ["매크로", "투자아이디어"],
        "sectors": ["반도체", "AI", "에너지"],
        "tickers": ["MU", "WDC"],
        "themes": ["크로스채널컨센서스", "메모리반도체", "이란전쟁장기화", "채널분석"],
        "sentiment": "mixed",
    },
}

# x_posts are already enriched - just need telegram_sent added
X_POST_FILES = [
    "dons_korea_oil_shockwave",
    "dons_korea_hartnett_flowshow",
    "rocketesla_iran_ground_troops",
]


def find_enrichment_key(filepath, content):
    """Find matching enrichment key for a file."""
    filename = filepath.split("/")[-1]
    # Check x_posts
    for xp in X_POST_FILES:
        if xp in filename:
            return None  # Will be handled separately (just add telegram_sent)
    # Check analysis
    for key in ["bok_cross_reference", "cross_channel_consensus"]:
        if key in filename:
            return key
    # Check by title content
    for key in ENRICHMENTS:
        if key in content or key in filename:
            return key
    return None


def insert_after_transcript_language(content, fields_str):
    """Insert enrichment fields after transcript_language line."""
    pattern = r'(transcript_language:.*?\n)'
    match = re.search(pattern, content)
    if match:
        pos = match.end()
        return content[:pos] + fields_str + content[pos:]
    # If no transcript_language, insert before closing ---
    # Find the second ---
    second_dash = content.find('---', 3)
    if second_dash != -1:
        return content[:second_dash] + fields_str + content[second_dash:]
    return content


def insert_telegram_sent_after_sentiment(content):
    """Insert telegram_sent: false after sentiment field."""
    pattern = r'(sentiment:.*?\n)'
    match = re.search(pattern, content)
    if match:
        pos = match.end()
        return content[:pos] + 'telegram_sent: false\n' + content[pos:]
    # Fallback: insert before closing ---
    second_dash = content.find('---', 3)
    if second_dash != -1:
        return content[:second_dash] + 'telegram_sent: false\n' + content[second_dash:]
    return content


def format_list(items):
    if not items:
        return "[]"
    return "[" + ", ".join(items) + "]"


def build_enrichment_fields(data):
    cats = format_list(data["categories"])
    sects = format_list(data["sectors"])
    ticks = format_list(data["tickers"])
    themes = format_list(data["themes"])
    sent = data["sentiment"]
    return (
        f"enriched: true\n"
        f"enriched_at: {TODAY}\n"
        f"categories: {cats}\n"
        f"sectors: {sects}\n"
        f"tickers: {ticks}\n"
        f"themes: {themes}\n"
        f"sentiment: {sent}\n"
        f"telegram_sent: false\n"
    )


def main():
    files = glob.glob('/home/user/youtubesearch/data/**/*.md', recursive=True)
    enriched_count = 0
    telegram_only_count = 0
    skipped = []

    for filepath in sorted(files):
        with open(filepath, 'r') as f:
            content = f.read()

        # Skip already enriched files that already have telegram_sent set
        if 'enriched: true' in content and 'telegram_sent:' in content:
            continue

        # Case 1: Already enriched, just need telegram_sent
        if 'enriched: true' in content and 'telegram_sent:' not in content:
            new_content = insert_telegram_sent_after_sentiment(content)
            with open(filepath, 'w') as f:
                f.write(new_content)
            telegram_only_count += 1
            print(f"[telegram_sent] {filepath.split('/')[-1][:60]}")
            continue

        # Case 2: x_posts (have sentiment but not enriched: true) - just add telegram_sent
        filename = filepath.split("/")[-1]
        is_xpost = any(xp in filename for xp in X_POST_FILES)
        if is_xpost and 'sentiment:' in content and 'telegram_sent:' not in content:
            new_content = insert_telegram_sent_after_sentiment(content)
            with open(filepath, 'w') as f:
                f.write(new_content)
            telegram_only_count += 1
            print(f"[telegram_sent xpost] {filename[:60]}")
            continue

        # Case 3: Analysis files with sentiment already
        if 'sentiment:' in content and 'enriched: true' not in content and 'telegram_sent:' not in content:
            new_content = insert_telegram_sent_after_sentiment(content)
            # Also add enriched fields if we have them
            key = find_enrichment_key(filepath, content)
            if key and key in ENRICHMENTS:
                fields = build_enrichment_fields(ENRICHMENTS[key])
                # Replace just telegram_sent line with full enrichment
                new_content = content  # reset
                # Insert after end of frontmatter fields
                second_dash = new_content.find('---', 3)
                if second_dash != -1:
                    new_content = new_content[:second_dash] + fields + new_content[second_dash:]
            with open(filepath, 'w') as f:
                f.write(new_content)
            enriched_count += 1
            print(f"[enriched] {filename[:60]}")
            continue

        # Case 4: Full enrichment needed
        key = find_enrichment_key(filepath, content)
        if key is None:
            # Try harder: match by title
            title_match = re.search(r'title:\s*["\']?(.*?)["\']?\s*\n', content)
            title = title_match.group(1) if title_match else ""
            for k in ENRICHMENTS:
                if k in title:
                    key = k
                    break

        if key is None:
            skipped.append(filepath)
            print(f"[SKIP - no match] {filename[:60]}")
            continue

        if key not in ENRICHMENTS:
            skipped.append(filepath)
            print(f"[SKIP - key not in enrichments] {key}")
            continue

        fields = build_enrichment_fields(ENRICHMENTS[key])

        # For BOK files - no transcript_language, insert before closing ---
        if 'transcript_language:' in content:
            new_content = insert_after_transcript_language(content, fields)
        else:
            # Insert before closing --- of frontmatter
            second_dash = content.find('---', 3)
            if second_dash != -1:
                new_content = content[:second_dash] + fields + content[second_dash:]
            else:
                new_content = content
                print(f"[ERROR] Could not find frontmatter end for {filename}")
                skipped.append(filepath)
                continue

        with open(filepath, 'w') as f:
            f.write(new_content)
        enriched_count += 1
        print(f"[enriched] {filename[:60]}")

    print(f"\n=== Summary ===")
    print(f"Fully enriched: {enriched_count}")
    print(f"Telegram-sent-only: {telegram_only_count}")
    print(f"Skipped: {len(skipped)}")
    if skipped:
        for s in skipped:
            print(f"  SKIPPED: {s}")


if __name__ == "__main__":
    main()
