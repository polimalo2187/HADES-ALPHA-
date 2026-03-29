import os

from app.config import is_mini_app_enabled


def _run_bot_only() -> None:
    from app.bot import run_bot

    run_bot()


def _create_web_app():
    from app.miniapp import create_mini_app

    return create_mini_app()


if is_mini_app_enabled():
    # Modo WEB: solo Mini App / API.
    app = _create_web_app()
else:
    # Modo BOT: exponemos una app mínima para que una importación ASGI accidental
    # no falle, pero NO arrancamos la Mini App ni threads del bot aquí.
    from fastapi import FastAPI

    app = FastAPI(title="HADES BOT Process")

    @app.get("/health")
    async def health() -> dict:
        return {"ok": True, "service": "bot"}


if __name__ == "__main__":
    if is_mini_app_enabled():
        import uvicorn

        port = int(os.getenv("PORT", "8000"))
        uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
    else:
        _run_bot_only()
