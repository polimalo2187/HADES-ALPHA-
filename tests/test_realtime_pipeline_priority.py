from datetime import datetime
import importlib
import sys
import types


def _load_pipeline():
    sys.modules.setdefault("telegram", types.SimpleNamespace(Bot=object))
    sys.modules.setdefault("bson", types.SimpleNamespace(ObjectId=lambda value: value))
    sys.modules["pymongo"] = types.SimpleNamespace(UpdateOne=lambda *args, **kwargs: (args, kwargs))
    sys.modules["pymongo.errors"] = types.SimpleNamespace(BulkWriteError=Exception)
    fake_db = types.SimpleNamespace(
        signal_deliveries_collection=lambda: None,
        signal_jobs_collection=lambda: None,
        signals_collection=lambda: None,
        user_signals_collection=lambda: None,
    )
    sys.modules.setdefault("app.database", fake_db)
    sys.modules.setdefault("app.models", types.SimpleNamespace(new_signal_delivery=lambda **kwargs: kwargs, new_signal_job=lambda **kwargs: kwargs))
    sys.modules.setdefault("app.notifier", types.SimpleNamespace(_eligible_users_for_alert=lambda *_args, **_kwargs: [], send_signal_alerts=lambda *_args, **_kwargs: None))
    sys.modules.setdefault("app.observability", types.SimpleNamespace(heartbeat=lambda *_args, **_kwargs: None, record_audit_event=lambda *_args, **_kwargs: None))
    sys.modules.setdefault("app.signals", types.SimpleNamespace(build_user_signal_document=lambda *_args, **_kwargs: {}))
    sys.modules.pop("app.realtime_pipeline", None)
    return importlib.import_module("app.realtime_pipeline")


class _Collection:
    def __init__(self):
        self.updates = []

    def update_one(self, *args, **kwargs):
        self.updates.append((args, kwargs))

    def find_one(self, *_args, **_kwargs):
        return {"_id": "sig-1", "visibility": "free"}


class _ImmediateFuture:
    def __init__(self, value):
        self._value = value

    def result(self):
        return self._value


class _ImmediateExecutor:
    def __init__(self, value):
        self._value = value

    def submit(self, *_args, **_kwargs):
        return _ImmediateFuture(self._value)


def test_pipeline_does_not_retry_when_push_succeeds_but_user_sync_fails(monkeypatch):
    pipeline = _load_pipeline()

    jobs = _Collection()
    deliveries = _Collection()
    signals = _Collection()

    monkeypatch.setattr(pipeline, "_claim_job", lambda _job_id: {"_id": "job-1", "signal_id": "sig-1"})
    monkeypatch.setattr(pipeline, "signal_jobs_collection", lambda: jobs)
    monkeypatch.setattr(pipeline, "signals_collection", lambda: signals)
    monkeypatch.setattr(pipeline, "signal_deliveries_collection", lambda: deliveries)
    monkeypatch.setattr(pipeline, "_eligible_users_for_alert", lambda _visibility: [101])
    monkeypatch.setattr(pipeline, "_mark_signal_dispatch", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pipeline, "_upsert_user_signals", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("sync exploded")))
    monkeypatch.setattr(pipeline, "_ensure_delivery_records", lambda *_args, **_kwargs: None)

    retry_calls = []
    monkeypatch.setattr(pipeline, "_schedule_retry", lambda *_args, **_kwargs: retry_calls.append(True))

    push_results = {
        "requested": 1,
        "sent": 1,
        "failed": 0,
        "first_push_at": datetime.utcnow(),
        "last_push_at": datetime.utcnow(),
        "results": [{"user_id": 101, "status": "sent", "sent_at": datetime.utcnow(), "error": None}],
    }
    monkeypatch.setattr(pipeline, "_push_executor", _ImmediateExecutor(push_results))
    monkeypatch.setattr(pipeline, "_run_push_dispatch", lambda *_args, **_kwargs: push_results)

    pipeline._process_job("job-1")

    assert retry_calls == []
    assert any(update[0][1].get("$set", {}).get("status") == "completed" for update in jobs.updates)
    assert deliveries.updates


class _RecordingUpdateOne:
    def __init__(self, flt, update, upsert=False):
        self.filter = flt
        self.update = update
        self.upsert = upsert


def test_delivery_upserts_do_not_repeat_mutable_fields(monkeypatch):
    pipeline = _load_pipeline()

    monkeypatch.setattr(pipeline, "UpdateOne", _RecordingUpdateOne)

    inserts = []

    class _BulkCollection:
        def bulk_write(self, ops, ordered=False):
            inserts.extend(ops)

    monkeypatch.setattr(pipeline, "signal_deliveries_collection", lambda: _BulkCollection())

    pipeline._ensure_delivery_records("sig-1", "free", [101])
    assert inserts
    op = inserts[0]
    assert "updated_at" not in op.update["$setOnInsert"]
    assert "status" not in op.update["$setOnInsert"]

    updates = []

    class _UpdateCollection:
        def update_one(self, flt, update, upsert=False):
            updates.append((flt, update, upsert))

    monkeypatch.setattr(pipeline, "signal_deliveries_collection", lambda: _UpdateCollection())
    pipeline._update_delivery_results("sig-1", "free", {"results": [{"user_id": 101, "status": "sent", "sent_at": datetime.utcnow(), "error": None}]})

    assert updates
    _flt, update, _upsert = updates[0]
    assert "updated_at" not in update["$setOnInsert"]
    assert "status" not in update["$setOnInsert"]
    assert update["$set"]["status"] == "sent"
