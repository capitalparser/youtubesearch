"""
YouTube 채널 자막 자동 수집기
- 지정 채널의 RSS 피드를 파싱하여 신규 영상 감지
- youtube-transcript-api로 자막 추출
- 80_Resources/YouTube/[채널명]/YYYY-MM-DD_[제목].md 에 저장

Usage:
    python3 yt_collector.py              # 신규 영상만 수집
    python3 yt_collector.py --all        # 인덱스 무시하고 전체 수집
    python3 yt_collector.py --video ID   # 특정 영상만 수집
    python3 yt_collector.py --channel NAME  # 특정 채널만 수집
"""

import json
import sys
import re
import logging
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

# ── 경로 설정 ──────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
PKM_ROOT = SCRIPT_DIR.parent.parent.parent  # Personal_AI_DB_Local/
CHANNELS_FILE = SCRIPT_DIR / "channels.json"
INDEX_FILE = SCRIPT_DIR / "collected_index.json"
OUTPUT_BASE = PKM_ROOT / "80_Resources" / "YouTube"
LOG_FILE = SCRIPT_DIR / "yt_collector.log"

RSS_BASE = "https://www.youtube.com/feeds/videos.xml?channel_id="
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
RSS_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "yt": "http://www.youtube.com/xml/schemas/2015",
    "media": "http://search.yahoo.com/mrss/",
}

# ── 로깅 설정 ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ── 인덱스 관리 ────────────────────────────────────────────
def load_index() -> set:
    if INDEX_FILE.exists():
        return set(json.loads(INDEX_FILE.read_text())["collected"])
    return set()


def save_index(collected: set):
    INDEX_FILE.write_text(
        json.dumps({"collected": sorted(collected)}, ensure_ascii=False, indent=2)
    )


# ── 채널 목록 로드 ──────────────────────────────────────────
def load_channels() -> list[dict]:
    return json.loads(CHANNELS_FILE.read_text())["channels"]


# ── RSS 파싱 ────────────────────────────────────────────────
def fetch_rss(channel_id: str) -> list[dict]:
    """RSS 피드에서 최신 영상 목록 반환 (최대 15개)"""
    url = RSS_BASE + channel_id
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        xml_data = urllib.request.urlopen(req, timeout=15).read()
    except Exception as e:
        log.error(f"RSS 가져오기 실패 ({channel_id}): {e}")
        return []

    root = ET.fromstring(xml_data)
    videos = []
    for entry in root.findall("atom:entry", RSS_NS):
        video_id_el = entry.find("yt:videoId", RSS_NS)
        title_el = entry.find("atom:title", RSS_NS)
        published_el = entry.find("atom:published", RSS_NS)
        link_el = entry.find("atom:link", RSS_NS)

        if video_id_el is None or title_el is None:
            continue

        videos.append({
            "video_id": video_id_el.text,
            "title": title_el.text or "",
            "published": (published_el.text or "")[:10],  # YYYY-MM-DD
            "url": link_el.get("href", "") if link_el is not None else
                   f"https://www.youtube.com/watch?v={video_id_el.text}",
        })
    return videos


# ── 자막 추출 ───────────────────────────────────────────────
def get_transcript(video_id: str, lang_priority: list[str]) -> tuple[list, str, bool]:
    """
    자막 추출. 반환: (snippets, language_code, is_generated)
    자막 없으면 ([], "", False)
    """
    try:
        from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled
        ytt = YouTubeTranscriptApi()
        transcript_list = ytt.list(video_id)

        # 수동 자막 우선
        try:
            t = transcript_list.find_transcript(lang_priority)
            fetched = t.fetch()
            return fetched.snippets, t.language_code, t.is_generated
        except NoTranscriptFound:
            pass

        # 자동 생성 자막 fallback
        try:
            t = transcript_list.find_generated_transcript(lang_priority)
            fetched = t.fetch()
            return fetched.snippets, t.language_code, t.is_generated
        except NoTranscriptFound:
            pass

    except Exception as e:
        log.warning(f"자막 추출 실패 ({video_id}): {e}")

    return [], "", False


# ── 타임스탬프 포맷 ─────────────────────────────────────────
def format_transcript(snippets) -> str:
    """[HH:MM:SS] 형식 타임스탬프 포함 텍스트 생성"""
    lines = []
    for s in snippets:
        start = s.start
        h = int(start // 3600)
        m = int((start % 3600) // 60)
        sec = int(start % 60)
        lines.append(f"[{h:02d}:{m:02d}:{sec:02d}] {s.text}")
    return "\n".join(lines)


# ── 파일명 정리 ─────────────────────────────────────────────
def sanitize_filename(title: str) -> str:
    """파일명에 쓸 수 없는 문자 제거/치환"""
    # macOS/Linux 파일명 금지 문자
    title = re.sub(r'[/\\:*?"<>|]', "_", title)
    # 연속 공백, 앞뒤 공백 정리
    title = re.sub(r"\s+", " ", title).strip()
    # 너무 긴 파일명 자르기 (80자)
    if len(title) > 80:
        title = title[:80].rstrip()
    return title


# ── 마크다운 생성 ───────────────────────────────────────────
def generate_markdown(channel: dict, video: dict, transcript_text: str,
                      lang_code: str, has_transcript: bool) -> str:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    safe_title = video["title"].replace('"', '\\"')

    frontmatter = f"""---
title: "{safe_title}"
source: YouTube
channel: "{channel['display_name']}"
channel_id: "{channel['channel_id']}"
video_id: "{video['video_id']}"
date: {video['published']}
url: "{video['url']}"
collected_at: {today}
has_transcript: {str(has_transcript).lower()}
transcript_language: "{lang_code}"
---"""

    channel_url = f"https://www.youtube.com/channel/{channel['channel_id']}"
    info_section = f"""
## 영상 정보

- 채널: [{channel['display_name']}]({channel_url})
- 게시일: {video['published']}
- 링크: [영상 보기]({video['url']})"""

    if has_transcript:
        script_section = f"""
## 스크립트

{transcript_text}"""
    else:
        script_section = """
## 스크립트

자막을 가져올 수 없습니다."""

    notes_section = """
## 메모

"""
    return frontmatter + info_section + script_section + notes_section


# ── 마크다운 저장 ───────────────────────────────────────────
def save_markdown(channel_name: str, video: dict, content: str) -> Path:
    channel_dir = OUTPUT_BASE / channel_name
    channel_dir.mkdir(parents=True, exist_ok=True)

    safe_title = sanitize_filename(video["title"])
    filename = f"{video['published']}_{safe_title}.md"
    filepath = channel_dir / filename
    filepath.write_text(content, encoding="utf-8")
    return filepath


# ── 메인 ────────────────────────────────────────────────────
def main():
    args = sys.argv[1:]
    force_all = "--all" in args
    target_video = None
    target_channel = None

    if "--video" in args:
        idx = args.index("--video")
        target_video = args[idx + 1] if idx + 1 < len(args) else None

    if "--channel" in args:
        idx = args.index("--channel")
        target_channel = args[idx + 1] if idx + 1 < len(args) else None

    log.info("=== YouTube 자막 수집 시작 ===")
    channels = load_channels()
    collected = load_index()
    new_count = 0

    for channel in channels:
        if target_channel and channel["name"] != target_channel:
            continue

        log.info(f"채널 처리: {channel['display_name']} ({channel['handle']})")

        if target_video:
            # 특정 영상 모드
            videos = [{
                "video_id": target_video,
                "title": target_video,
                "published": datetime.now().strftime("%Y-%m-%d"),
                "url": f"https://www.youtube.com/watch?v={target_video}",
            }]
        else:
            videos = fetch_rss(channel["channel_id"])
            log.info(f"  RSS에서 {len(videos)}개 영상 발견")

        for video in videos:
            vid = video["video_id"]

            if not force_all and vid in collected:
                log.debug(f"  건너뜀 (이미 수집): {vid}")
                continue

            log.info(f"  수집 중: [{video['published']}] {video['title'][:50]}")

            snippets, lang_code, is_generated = get_transcript(
                vid, channel["language_priority"]
            )
            has_transcript = len(snippets) > 0
            transcript_text = format_transcript(snippets) if has_transcript else ""

            if has_transcript:
                log.info(f"    자막 추출 성공: {len(snippets)}개 항목 ({lang_code}, generated={is_generated})")
            else:
                log.info(f"    자막 없음")

            content = generate_markdown(channel, video, transcript_text, lang_code, has_transcript)
            filepath = save_markdown(channel["name"], video, content)
            log.info(f"    저장: {filepath.relative_to(PKM_ROOT)}")

            collected.add(vid)
            new_count += 1

    save_index(collected)
    log.info(f"=== 완료: {new_count}개 신규 수집 ===")


if __name__ == "__main__":
    main()
