import os
import threading

import uvicorn

from app.bot import run_bot
from app.config import is_mini_app_enabled
from app.miniapp import create_mini_app


_bot_thread_started = False
_bot_thread_lock = threading.Lock()


def _run_bot_in_thread() -> None:
    run_bot(background=True)


def _ensure_bot_thread_started() -> None:
    global _bot_thread_started
    with _bot_thread_lock:
        if _bot_thread_started:
            return
        bot_thread = threading.Thread(
            target=_run_bot_in_thread,
            daemon=True,
            name="TelegramBotThread",
        )
        bot_thread.start()
        _bot_thread_started = True


app = create_mini_app()


@app.on_event("startup")
async def _startup_bot_runner() -> None:
    if is_mini_app_enabled():
        _ensure_bot_thread_started()


if __name__ == "__main__":
    if is_mini_app_enabled():
        port = int(os.getenv("PORT", "8000"))
        uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
    else:
        run_bot()
