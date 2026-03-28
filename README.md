# YouTube Search & Alert

YouTube 채널 자막 수집 + AI enrichment + Telegram 알림 자동화

## 구조

```
scripts/          → 수집기 스크립트
  yt_collector_remote.py  → 원격 실행용 수집기
  channels.json           → 모니터링 채널 목록
  collected_index.json    → 수집 이력
data/             → 수집된 영상 마크다운
  hkglobalmarket/
  MK_Invest/
  kpunch/
  daily_digest/
```

## 자동화 흐름

1. Claude Code Trigger (6시간마다) → 수집 + enrichment + Telegram 전송
2. 로컬 맥북에서 git pull로 PKM 동기화
