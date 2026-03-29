"""
텔레그램 링크 수집기
- Telegram Bot API로 사용자가 보낸 메시지에서 URL 추출
- 처리된 update_id를 기록하여 중복 방지
- 결과를 JSON으로 출력

Usage:
    python3 link_processor.py
"""

import json
import re
import sys
import urllib.request
from pathlib import Path
from datetime import datetime, timezone

SCRIPT_DIR = Path(__file__).parent
REPO_ROOT = SCRIPT_DIR.parent
OFFSET_FILE = SCRIPT_DIR / "telegram_offset.json"
LINKS_DIR = REPO_ROOT / "data" / "links"

import os

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = int(os.environ.get("TELEGRAM_CHAT_ID", "0"))
API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"

URL_PATTERN = re.compile(r'https?://[^\s<>"\']+')


def load_offset() -> int:
    if OFFSET_FILE.exists():
        return json.loads(OFFSET_FILE.read_text()).get("offset", 0)
    return 0


def save_offset(offset: int):
    OFFSET_FILE.write_text(json.dumps({"offset": offset}))


def get_updates(offset: int) -> list:
    url = f"{API_BASE}/getUpdates?offset={offset}&limit=100"
    try:
        req = urllib.request.Request(url)
        resp = urllib.request.urlopen(req, timeout=10)
        data = json.loads(resp.read())
        if data.get("ok"):
            return data.get("result", [])
    except Exception as e:
        print(f"Error fetching updates: {e}", file=sys.stderr)
    return []


def classify_url(url: str) -> str:
    """URL 유형 분류"""
    if "youtube.com/watch" in url or "youtu.be/" in url:
        return "youtube"
    if "x.com/" in url or "twitter.com/" in url:
        return "x_post"
    # 나머지는 article (기사, 블로그, 아티클)
    return "article"


def extract_links_from_updates(updates: list) -> list:
    """업데이트에서 링크 추출"""
    links = []
    for update in updates:
        msg = update.get("message", {})
        chat = msg.get("chat", {})

        # 본인 채팅만 처리
        if chat.get("id") != CHAT_ID:
            continue

        text = msg.get("text", "")
        urls = URL_PATTERN.findall(text)
        received_at = datetime.fromtimestamp(
            msg.get("date", 0), tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M UTC")

        # 봇 커맨드 스킵 (/start 등)
        if text.startswith("/"):
            continue

        if urls:
            # URL이 있는 메시지 → 링크로 처리
            memo = URL_PATTERN.sub("", text).strip()
            for url in urls:
                links.append({
                    "url": url.rstrip(".,;:)"),
                    "type": classify_url(url),
                    "memo": memo,
                    "received_at": received_at,
                    "update_id": update["update_id"],
                })
        elif len(text.strip()) > 10:
            # URL 없는 텍스트 (10자 이상) → 텍스트 메모로 수집
            links.append({
                "url": "",
                "type": "text",
                "memo": text.strip(),
                "received_at": received_at,
                "update_id": update["update_id"],
            })

    return links


def save_links(links: list):
    """링크를 날짜별 JSON 파일로 저장"""
    LINKS_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    filepath = LINKS_DIR / f"{today}_links.json"

    existing = []
    if filepath.exists():
        existing = json.loads(filepath.read_text())

    existing.extend(links)
    filepath.write_text(json.dumps(existing, ensure_ascii=False, indent=2))
    return filepath


def main():
    offset = load_offset()
    updates = get_updates(offset)

    if not updates:
        print(json.dumps({"new_links": 0, "links": []}))
        return

    links = extract_links_from_updates(updates)

    # offset 업데이트 (마지막 update_id + 1)
    max_update_id = max(u["update_id"] for u in updates)
    save_offset(max_update_id + 1)

    if links:
        filepath = save_links(links)
        print(f"Saved {len(links)} links to {filepath.relative_to(REPO_ROOT)}")

    # 결과 출력
    result = {
        "new_links": len(links),
        "links": links,
    }
    print(f"\n__RESULT_JSON__:{json.dumps(result, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
