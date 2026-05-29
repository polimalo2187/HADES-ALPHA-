"""Central operating-session gate for HADES.

The server can stay alive 24/7, but heavy market work is allowed only during the
business trading session. Defaults are intentionally hardcoded in code, not in
cloud environment variables, per production policy:

    America/Havana, 08:00 → 23:00

Outside this window scanner/market refresh/provider calls should pause and any
active/pending signals are discarded by the scheduler/scanner policy.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

# Hardcoded operational policy. Do not depend on server local timezone.
TRADING_SESSION_ENABLED = True
TRADING_SESSION_TIMEZONE = "America/Havana"
TRADING_SESSION_START = time(8, 0)
TRADING_SESSION_END = time(23, 0)
TRADING_SESSION_LABEL = "08:00 - 23:00 hora de Cuba"
TRADING_SESSION_POLICY_VERSION = "hades_operating_session_cuba_v1"


@dataclass(frozen=True)
class TradingSessionStatus:
    enabled: bool
    is_open: bool
    timezone: str
    now_local: datetime
    start_time: time
    end_time: time
    next_open_local: Optional[datetime]
    next_close_local: Optional[datetime]
    label: str
    policy_version: str

    @property
    def now_label(self) -> str:
        return self.now_local.strftime("%Y-%m-%d %H:%M:%S %Z")

    @property
    def next_open_label(self) -> Optional[str]:
        return self.next_open_local.strftime("%Y-%m-%d %H:%M:%S %Z") if self.next_open_local else None

    @property
    def next_close_label(self) -> Optional[str]:
        return self.next_close_local.strftime("%Y-%m-%d %H:%M:%S %Z") if self.next_close_local else None

    @property
    def seconds_until_next_open(self) -> int:
        if not self.next_open_local:
            return 0
        return max(0, int((self.next_open_local - self.now_local).total_seconds()))

    @property
    def seconds_until_next_close(self) -> int:
        if not self.next_close_local:
            return 0
        return max(0, int((self.next_close_local - self.now_local).total_seconds()))

    def to_public_dict(self) -> Dict[str, Any]:
        return {
            "enabled": bool(self.enabled),
            "is_open": bool(self.is_open),
            "state": "open" if self.is_open else "closed",
            "timezone": self.timezone,
            "timezone_label": "Hora de Cuba",
            "start": self.start_time.strftime("%H:%M"),
            "end": self.end_time.strftime("%H:%M"),
            "schedule_label": self.label,
            "now_local": self.now_local.isoformat(),
            "now_label": self.now_label,
            "next_open_local": self.next_open_local.isoformat() if self.next_open_local else None,
            "next_open_label": self.next_open_label,
            "next_close_local": self.next_close_local.isoformat() if self.next_close_local else None,
            "next_close_label": self.next_close_label,
            "seconds_until_next_open": self.seconds_until_next_open,
            "seconds_until_next_close": self.seconds_until_next_close,
            "message": session_public_message(self),
            "policy_version": self.policy_version,
        }


def _tz() -> ZoneInfo:
    return ZoneInfo(TRADING_SESSION_TIMEZONE)


def _combine(day, clock: time, tz: ZoneInfo) -> datetime:
    return datetime.combine(day, clock, tzinfo=tz)


def get_trading_session_status(now: Optional[datetime] = None) -> TradingSessionStatus:
    tz = _tz()
    if now is None:
        now_local = datetime.now(tz)
    else:
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        now_local = now.astimezone(tz)

    if not TRADING_SESSION_ENABLED:
        return TradingSessionStatus(
            enabled=False,
            is_open=True,
            timezone=TRADING_SESSION_TIMEZONE,
            now_local=now_local,
            start_time=TRADING_SESSION_START,
            end_time=TRADING_SESSION_END,
            next_open_local=None,
            next_close_local=None,
            label=TRADING_SESSION_LABEL,
            policy_version=TRADING_SESSION_POLICY_VERSION,
        )

    today = now_local.date()
    start_dt = _combine(today, TRADING_SESSION_START, tz)
    end_dt = _combine(today, TRADING_SESSION_END, tz)

    # Supports both normal same-day windows and overnight windows if ever needed.
    if start_dt < end_dt:
        is_open = start_dt <= now_local < end_dt
        if is_open:
            next_close = end_dt
            next_open = start_dt
        elif now_local < start_dt:
            next_open = start_dt
            next_close = end_dt
        else:
            next_open = _combine(today + timedelta(days=1), TRADING_SESSION_START, tz)
            next_close = _combine(today + timedelta(days=1), TRADING_SESSION_END, tz)
    else:
        # Overnight: e.g. 22:00 -> 06:00
        is_open = now_local >= start_dt or now_local < end_dt
        if is_open:
            next_close = end_dt if now_local < end_dt else _combine(today + timedelta(days=1), TRADING_SESSION_END, tz)
            next_open = start_dt if now_local >= start_dt else _combine(today - timedelta(days=1), TRADING_SESSION_START, tz)
        else:
            next_open = start_dt
            next_close = _combine(today + timedelta(days=1), TRADING_SESSION_END, tz)

    return TradingSessionStatus(
        enabled=True,
        is_open=is_open,
        timezone=TRADING_SESSION_TIMEZONE,
        now_local=now_local,
        start_time=TRADING_SESSION_START,
        end_time=TRADING_SESSION_END,
        next_open_local=next_open,
        next_close_local=next_close,
        label=TRADING_SESSION_LABEL,
        policy_version=TRADING_SESSION_POLICY_VERSION,
    )


def is_trading_session_open(now: Optional[datetime] = None) -> bool:
    return get_trading_session_status(now).is_open


def seconds_until_trading_session_open(now: Optional[datetime] = None) -> int:
    return get_trading_session_status(now).seconds_until_next_open


def session_public_message(status: Optional[TradingSessionStatus] = None) -> str:
    status = status or get_trading_session_status()
    if status.is_open:
        close_label = status.next_close_local.strftime("%H:%M") if status.next_close_local else status.end_time.strftime("%H:%M")
        return f"Plataforma operativa hasta las {close_label} hora de Cuba."
    open_label = status.next_open_local.strftime("%H:%M") if status.next_open_local else status.start_time.strftime("%H:%M")
    return f"Mercado pausado. Próxima activación: {open_label} hora de Cuba."


def get_trading_session_public_payload() -> Dict[str, Any]:
    return get_trading_session_status().to_public_dict()
