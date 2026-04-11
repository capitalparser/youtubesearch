"""
Step 2+4: Enrich unenriched files and send Telegram alerts for all unsent files.
"""

import os
import re
import json
import time
import urllib.request
import urllib.parse
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "data"
TODAY = "2026-04-11"

BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
TG_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"


def parse_frontmatter(text):
    """Parse YAML frontmatter from markdown text. Returns (dict, body_start_index)."""
    if not text.startswith("---"):
        return {}, 0
    end = text.find("\n---", 3)
    if end == -1:
        return {}, 0
    fm_text = text[4:end]
    data = {}
    for line in fm_text.split("\n"):
        if ": " in line:
            k, v = line.split(": ", 1)
            k = k.strip()
            v = v.strip()
            # Parse lists
            if v.startswith("[") and v.endswith("]"):
                inner = v[1:-1].strip()
                if inner:
                    items = []
                    for item in inner.split(","):
                        item = item.strip().strip('"').strip("'")
                        if item:
                            items.append(item)
                    data[k] = items
                else:
                    data[k] = []
            elif v.lower() == "true":
                data[k] = True
            elif v.lower() == "false":
                data[k] = False
            else:
                data[k] = v.strip('"').strip("'")
        elif line.strip().startswith("- "):
            pass  # skip list items handled elsewhere
    return data, end + 4  # +4 for '\n---'


def set_frontmatter_field(text, field, value):
    """Set a field in YAML frontmatter. If field exists, replace it."""
    if not text.startswith("---"):
        return text
    end = text.find("\n---", 3)
    if end == -1:
        return text
    fm_text = text[4:end]

    # Check if field exists
    pattern = re.compile(rf"^{re.escape(field)}:.*$", re.MULTILINE)
    if isinstance(value, bool):
        str_value = str(value).lower()
    else:
        str_value = str(value)

    new_line = f"{field}: {str_value}"

    if pattern.search(fm_text):
        new_fm = pattern.sub(new_line, fm_text)
    else:
        new_fm = fm_text + f"\n{new_line}"

    return "---" + new_fm + "\n---" + text[end + 4:]


def mark_sent(filepath):
    """Mark a file as telegram_sent: true."""
    text = filepath.read_text(encoding="utf-8")
    text = set_frontmatter_field(text, "telegram_sent", True)
    filepath.write_text(text, encoding="utf-8")


def send_telegram(message):
    """Send a Telegram message. Returns True if successful."""
    payload = json.dumps({
        "chat_id": CHAT_ID,
        "parse_mode": "HTML",
        "text": message
    }).encode("utf-8")
    req = urllib.request.Request(
        TG_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read())
            return result.get("ok", False)
    except Exception as e:
        print(f"  Telegram error: {e}")
        return False


def build_message(fm, filepath):
    """Build Telegram HTML message from frontmatter."""
    source = fm.get("source", "")
    title = fm.get("title", filepath.stem)
    date = fm.get("date", "")
    url = fm.get("url", "")
    channel = fm.get("channel", fm.get("author", source or filepath.parent.name))
    sectors = fm.get("sectors", [])
    tickers = fm.get("tickers", [])
    themes = fm.get("themes", [])
    sentiment = fm.get("sentiment", "")

    sectors_str = ", ".join(sectors) if sectors else "-"
    themes_str = ", ".join(themes) if themes else "-"

    # Escape HTML in title
    title_esc = title.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    channel_esc = str(channel).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    link_label = "영상 보기" if source == "YouTube" else "보기"

    lines = [
        f"<b>[{channel_esc}]</b> {title_esc}",
        f"📅 {date}",
    ]

    if url:
        url_esc = url.replace("&", "&amp;")
        lines.append(f'🔗 <a href="{url_esc}">{link_label}</a>')

    lines.append(f"📊 섹터: {sectors_str}")

    if tickers:
        lines.append(f"💹 종목: {', '.join(tickers)}")

    lines.append(f"🎯 테마: {themes_str}")
    lines.append(f"📈 센티먼트: {sentiment}")

    return "\n".join(lines)


def find_unsent_files():
    """Find all markdown files where telegram_sent is missing or false."""
    unsent = []
    for md_file in sorted(DATA_DIR.rglob("*.md")):
        text = md_file.read_text(encoding="utf-8")
        if "telegram_sent: true" in text:
            continue
        unsent.append(md_file)
    return unsent


def main():
    print(f"=== Step 3: Finding all unsent files ===")
    unsent = find_unsent_files()
    print(f"Found {len(unsent)} unsent files\n")

    sent_count = 0
    failed = []

    print(f"=== Step 4: Sending Telegram alerts ===")
    for i, filepath in enumerate(unsent, 1):
        text = filepath.read_text(encoding="utf-8")
        fm, _ = parse_frontmatter(text)

        msg = build_message(fm, filepath)
        rel = filepath.relative_to(REPO_ROOT)
        print(f"[{i}/{len(unsent)}] Sending: {rel}")
        print(f"  Title: {fm.get('title', '')[:60]}")

        ok = send_telegram(msg)
        if ok:
            mark_sent(filepath)
            sent_count += 1
            print(f"  ✓ Sent and marked")
        else:
            failed.append(str(rel))
            print(f"  ✗ Failed")

        if i < len(unsent):
            time.sleep(1)

    print(f"\n=== Done: {sent_count}/{len(unsent)} sent ===")
    if failed:
        print(f"Failed files ({len(failed)}):")
        for f in failed:
            print(f"  - {f}")

    result = {"sent": sent_count, "total": len(unsent), "failed": failed}
    print(f"\n__RESULT_JSON__:{json.dumps(result, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
