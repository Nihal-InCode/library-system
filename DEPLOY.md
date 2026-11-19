# Railway deployment configuration

The Flask backend and Telegram bot are started together by `start.py` (`Procfile` runs `python start.py`). Both processes use the same `LIBRARY_DB_PATH` value.

## Railway setup

Attach a persistent volume mounted at `/data`, then set these service variables:

- `LIBRARY_DB_PATH=/data/islamic_library.db`
- `LIBRARY_API_KEY=<long random secret>`
- `PORT` is supplied by Railway and used by Flask and the bot's localhost API URL.

Migrate the existing production database to `/data/islamic_library.db` when preparing the volume. The application refuses to start on Railway if the variable is missing or the configured file does not exist; it will not create a new production database or restore one from GitHub. Keep both processes in the same Railway service so they share the mounted volume and environment.

Call `/api/external/library/...` over HTTPS from the Attendance backend and send `X-API-Key` with the configured key. These routes are read-only and require the key. Existing Telegram bot routes remain unauthenticated for localhost communication.

## Local development

Outside Railway, if `LIBRARY_DB_PATH` is unset, the Flask backend and Telegram bot continue using `islamic_library.db` in the project directory.
