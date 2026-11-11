#!/usr/bin/env python3
"""
Крон: сповіщення підписників про КРИТИЧНИЙ стан обраних районів (РЕМ).

Читає accidents_summary (_id=latest), знаходить РЕМ з coverage_pct >= CRITICAL_PCT
(або streets_pct), і шле DM користувачам з:
  notify_prefs.district == true
  notify_districts містить цей РЕМ (нормалізована назва без « РЕМ»)

Антиспам у users:
  last_district_notified: { "Коростенський": "2026-11-11", ... }
  — не частіше одного разу на день на район (крім FORCE_DISTRICT_NOTIFY=1).

Запуск: після ztoe_accidents_summary (той самий schedule або окремий cron).
Env: ZTOE_BOT_TOKEN, MONGO_URI, MONGO_DB, TELEGRAM_LOG_CHANNEL_ID (опц.)
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import Any

import httpx

try:
    from zoneinfo import ZoneInfo
    KYIV = ZoneInfo("Europe/Kyiv")
except Exception:
    import pytz
    KYIV = pytz.timezone("Europe/Kyiv")

CRITICAL_PCT = float(os.getenv("DISTRICT_CRITICAL_PCT") or "30")
TG_MSG_LIMIT = 4000

_log_lines: list[str] = []


def log(msg: str) -> None:
    print(msg)
    _log_lines.append(msg)


def _norm_rem(name: str) -> str:
    s = (name or "").strip().replace(" РЕМ", "").replace(" рем", "").strip()
    return s


def _is_quiet_now(u: dict) -> bool:
    if not u.get("quiet_mode"):
        return False
    try:
        fh = int(u.get("quiet_from", 23)) % 24
        th = int(u.get("quiet_to", 7)) % 24
    except (TypeError, ValueError):
        fh, th = 23, 7
    try:
        fm = max(0, min(59, int(u.get("quiet_from_min", 0) or 0)))
        tm = max(0, min(59, int(u.get("quiet_to_min", 0) or 0)))
    except (TypeError, ValueError):
        fm, tm = 0, 0
    now = datetime.now(KYIV)
    cur = now.hour * 60 + now.minute
    start = fh * 60 + fm
    end = th * 60 + tm
    if start == end:
        return True
    if start < end:
        return start <= cur < end
    return cur >= start or cur < end


def _user_wants_district(u: dict) -> bool:
    if u.get("notify", True) is False:
        return False
    raw = u.get("notify_prefs")
    if isinstance(raw, dict):
        return bool(raw.get("district", False))
    return False


def _user_districts(u: dict) -> list[str]:
    raw = u.get("notify_districts")
    if not isinstance(raw, list):
        return []
    out = []
    seen = set()
    for x in raw:
        n = _norm_rem(str(x or ""))
        if n and n not in seen:
            seen.add(n)
            out.append(n)
    return out


def _coverage(st: dict) -> float | None:
    for key in ("coverage_pct", "streets_pct"):
        v = st.get(key)
        if v is None:
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return None


def _severity_emoji(pct: float | None, streets: int) -> str:
    if pct is not None:
        if pct >= 30:
            return "🔴"
        if pct >= 15:
            return "🟠"
        if pct >= 5:
            return "🟡"
        if pct > 0:
            return "🟢"
        return "⚪"
    if streets >= 400:
        return "🔴"
    if streets >= 150:
        return "🟠"
    if streets >= 50:
        return "🟡"
    if streets > 0:
        return "🟢"
    return "⚪"


def format_critical_rem(name: str, st: dict, as_of: str) -> str:
    short = _norm_rem(name)
    streets = int(st.get("streets") or 0)
    cities = int(st.get("cities") or 0)
    pct = _coverage(st)
    emoji = _severity_emoji(pct, streets)
    streets_s = f"{streets:,}".replace(",", " ")
    pct_s = f"{pct:.1f}% району" if pct is not None else "—"
    lines = [
        f"{emoji} <b>КРИТИЧНИЙ стан: {short}</b>",
        "",
        f"Зараз без світла: <b>{cities}</b> нас.п. / <b>{streets_s}</b> вул. "
        f"(<b>{pct_s}</b>).",
        "",
        "<i>Будь ласка, не користуйтесь потужними електроприладами.</i>",
        "",
        f"🕐 {as_of}",
    ]
    city_streets = st.get("city_streets") or {}
    if isinstance(city_streets, dict) and city_streets:
        top = sorted(city_streets.items(), key=lambda x: -int(x[1] or 0))[:8]
        if top:
            lines.insert(-2, "Найбільше зачеплені:")
            for cname, cnt in top:
                lines.insert(-2, f"  · {cname} — {cnt} вул.")
            lines.insert(-2, "")
    return "\n".join(lines)


def send_log(title: str = "📊 ЛОГ · РАЙОНИ") -> None:
    token = os.getenv("ZTOE_BOT_TOKEN") or ""
    chat_id = os.getenv("TELEGRAM_LOG_CHANNEL_ID") or ""
    if not token or not chat_id or not _log_lines:
        return
    try:
        body = "\n".join(_log_lines)
        text_msg = f"{title}\n\n<pre>{body[:3800]}</pre>"
        httpx.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={
                "chat_id": chat_id,
                "text": text_msg,
                "parse_mode": "HTML",
                "disable_web_page_preview": "true",
            },
            timeout=15,
        )
    except Exception as e:
        print(f"log send failed: {e}")


def main() -> int:
    log("=" * 40)
    log("🚀 СТАРТ · Сповіщення по районах (РЕМ)")
    force = (os.getenv("FORCE_DISTRICT_NOTIFY") or "").strip().lower() in (
        "1", "true", "yes",
    )
    if force:
        log("FORCE_DISTRICT_NOTIFY=1")

    uri = (os.getenv("MONGO_URI") or "").strip()
    db_name = (os.getenv("MONGO_DB") or "").strip() or "vidkluchennya"
    token = (os.getenv("ZTOE_BOT_TOKEN") or "").strip()
    if not uri:
        log("❌ MONGO_URI немає")
        return 1
    if not token:
        log("❌ ZTOE_BOT_TOKEN немає")
        return 1

    try:
        from pymongo import MongoClient
        client = MongoClient(uri, serverSelectionTimeoutMS=8000)
        db = client[db_name]
    except Exception as e:
        log(f"❌ Mongo: {e}")
        return 1

    doc = db["accidents_summary"].find_one({"_id": "latest"})
    if not doc:
        log("⚠️ accidents_summary latest порожній")
        send_log()
        return 0

    rem = doc.get("rem") or {}
    if not isinstance(rem, dict):
        log("⚠️ rem не dict")
        send_log()
        return 0

    as_of_raw = doc.get("as_of") or datetime.now(KYIV).isoformat()
    as_of_s = str(as_of_raw)
    if "T" in as_of_s:
        as_of_s = as_of_s.replace("T", " ").replace("Z", "").split(".")[0][:16]
    today = datetime.now(KYIV).strftime("%Y-%m-%d")

    critical: dict[str, dict] = {}
    for name, st in rem.items():
        if not isinstance(st, dict):
            continue
        pct = _coverage(st)
        streets = int(st.get("streets") or 0)
        is_crit = (pct is not None and pct >= CRITICAL_PCT) or (
            pct is None and streets >= 400
        )
        if is_crit:
            critical[_norm_rem(name)] = st

    if not critical:
        log(f"ℹ️ Немає РЕМ з coverage ≥ {CRITICAL_PCT}%")
        send_log()
        return 0

    log(f"🔴 Критичні РЕМ: {', '.join(sorted(critical.keys()))}")

    try:
        users = list(
            db["users"].find(
                {
                    "notify": {"$ne": False},
                    "notify_prefs.district": True,
                    "telegram_id": {"$exists": True},
                }
            )
        )
    except Exception as e:
        log(f"❌ users query: {e}")
        return 1

    log(f"📬 candidates (district pref): {len(users)}")

    sent = failed = skipped = 0
    for u in users:
        if not _user_wants_district(u):
            continue
        tid = u.get("telegram_id")
        if not tid:
            continue
        wanted = set(_user_districts(u))
        if not wanted:
            continue
        hit = {k: v for k, v in critical.items() if k in wanted}
        if not hit:
            continue

        last_map = u.get("last_district_notified") or {}
        if not isinstance(last_map, dict):
            last_map = {}

        to_send: list[tuple[str, dict]] = []
        for k, st in hit.items():
            if not force and str(last_map.get(k) or "")[:10] == today:
                continue
            to_send.append((k, st))
        if not to_send:
            skipped += 1
            continue

        silent = _is_quiet_now(u)
        user_ok = True
        for name, st in to_send:
            text = format_critical_rem(name, st, as_of_s)
            try:
                r = httpx.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    data={
                        "chat_id": str(tid),
                        "text": text[:TG_MSG_LIMIT],
                        "parse_mode": "HTML",
                        "disable_web_page_preview": "true",
                        "disable_notification": "true" if silent else "false",
                    },
                    timeout=30,
                )
                if not (r.status_code == 200 and (r.json() or {}).get("ok")):
                    user_ok = False
                    log(f"❌ DM tid={tid} {name}: HTTP {r.status_code}")
                    break
            except Exception as e:
                user_ok = False
                log(f"❌ DM tid={tid} {name}: {e}")
                break

        if user_ok:
            sent += 1
            new_map = dict(last_map)
            for name, _ in to_send:
                new_map[name] = today
            try:
                db["users"].update_one(
                    {"_id": u["_id"]},
                    {"$set": {"last_district_notified": new_map}},
                )
            except Exception as e:
                log(f"⚠️ update last_district_notified: {e}")
        else:
            failed += 1

    log(f"📬 district DM: sent={sent} failed={failed} skipped_today={skipped}")
    send_log()
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
