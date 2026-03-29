import os
import threading

import uvicorn

from app.bot import run_bot
from app.config import is_mini_app_enabled
from app.miniapp import create_mini_app


def _run_bot_in_thread() -> None:
    run_bot(background=True)


if __name__ == "__main__":
    if is_mini_app_enabled():
        bot_thread = threading.Thread(target=_run_bot_in_thread, daemon=True, name="TelegramBotThread")
        bot_thread.start()

        port = int(os.getenv("PORT", "8000"))
        uvicorn.run(create_mini_app(), host="0.0.0.0", port=port)
    else:
        run_bot()
