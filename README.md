# yanolza_backend

## Run

```bash
python3 web/server.py --host 0.0.0.0 --port 8787
```

## Environment Variables

- `NTFY_TOPIC`: ntfy topic for `/notify`.

`/monitors/start`로 백그라운드 감시를 등록하면, 첫 감지부터 예약 가능 상태를 찾는 즉시 `NTFY_TOPIC`으로 알림을 보냅니다.
