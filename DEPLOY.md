# Railway deployment configuration

The Flask backend and Telegram bot are started together by `start.py` (`Procfile` runs `python start.py`). Both read the same `LIBRARY_DB_PATH` value.

## Railway setup

Attach a persistent volume mounted at `/data`, then set these service variables:

- `LIBRARY_DB_PATH=/data/islamic_library.db`
- `LIBRARY_API_KEY=<long random secret>`
- `PORT` is supplied by Railway and is used by the Flask backend and the bot's localhost URL.

Copy the existing production database to `/data/islamic_library.db` once when preparing the volume. The application will not create or seed a production database. On Railway it refuses to start when `LIBRARY_DB_PATH` is missing, and the backend exits if the configured database file does not exist. Keep the Telegram bot and Flask backend in the same Railway service so they share the mounted volume and environment.

The Attendance service should call the `/api/external/library/...` GET endpoints over HTTPS and send `X-API-Key` with the configured key. The external API has no write routes. Legacy bot-to-Flask routes remain available on localhost and do not require this key.

## Local desktop setup

When `LIBRARY_DB_PATH` is unset outside Railway, the Flask backend and Telegram bot continue using `islamic_library.db` next to the application, preserving the existing desktop behavior.
