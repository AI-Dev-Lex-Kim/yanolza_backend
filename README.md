# yanolza_backend

## Run

```bash
python3 web/server.py --host 0.0.0.0 --port 8787
```

## Environment Variables

- `NTFY_TOPIC`: ntfy topic for `/notify`.

`/monitors/start`로 백그라운드 감시를 등록하면, 첫 감지부터 예약 가능 상태를 찾는 즉시 `NTFY_TOPIC`으로 알림을 보냅니다.

## Monitor Payload

`/monitors/start`, `/check`에서 대실 종료 시각 필터가 필요하면 `dayuse_end_time`을 보냅니다.

- 예: `stay_type="대실"`, `dayuse_end_time="22:00"`
- 예: `stay_type="대실"`, `dayuse_end_time="오후 9~10"`
- `null`, 빈 문자열, `상관없음`, `무관`, `any`는 종료 시각 필터를 끕니다.
- `22:00`은 `(운영시간 12:00 ~ 22:00)`처럼 마지막 시각이 정확히 같은 경우만 매치합니다.
- `오후 9~10` 같은 범위 입력은 종료 시각이 그 구간 안에 들어오는 대실을 매치합니다.
