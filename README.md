# yanolza_backend

## Run

```bash
python3 web/server.py --host 0.0.0.0 --port 8787
```

## Environment Variables

- `NTFY_TOPIC`: ntfy topic for `/notify`.

`/monitors/start`로 백그라운드 감시를 등록하면, 첫 감지부터 예약 가능 상태를 찾는 즉시 `NTFY_TOPIC`으로 알림을 보냅니다.

## Monitor Payload

`/monitors/start`, `/check`에서 대실 종료 시각 필터가 필요하면 `dayuse_end_time`에 `HH:MM` 형식으로 값을 보냅니다.

- 예: `stay_type="대실"`, `dayuse_end_time="22:00"`
- 이 값은 `(운영시간 12:00 ~ 22:00)` 같은 대실 운영시간의 마지막 시각과 일치하는 경우만 매치합니다.
