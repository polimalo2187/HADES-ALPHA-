import os

from app.config import get_runtime_role


_RUNTIME_ROLE = get_runtime_role()



def _run_role() -> None:
    if _RUNTIME_ROLE == "web":
        import uvicorn

        port = int(os.getenv("PORT", "8000"))
        uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
        return

    if _RUNTIME_ROLE == "signal_worker":
        from app.bot import run_signal_worker

        run_signal_worker()
        return

    if _RUNTIME_ROLE == "scheduler":
        from app.bot import run_scheduler_worker

        run_scheduler_worker()
        return

    from app.bot import run_bot

    if _RUNTIME_ROLE == "bot_ui":
        run_bot(enable_scanner=False, enable_scheduler=False)
        return

    run_bot()



def _create_runtime_app():
    from fastapi import FastAPI

    service_name = "miniapp" if _RUNTIME_ROLE == "web" else _RUNTIME_ROLE
    runtime_app = FastAPI(title=f"HADES {service_name} Process")

    @runtime_app.get("/health")
    async def health() -> dict:
        return {"ok": True, "service": service_name, "runtime_role": _RUNTIME_ROLE}

    return runtime_app


if _RUNTIME_ROLE == "web":
    from app.miniapp import create_mini_app

    app = create_mini_app()
else:
    app = _create_runtime_app()


if __name__ == "__main__":
    _run_role()
