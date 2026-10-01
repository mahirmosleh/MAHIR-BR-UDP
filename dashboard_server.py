# -*- coding: utf-8 -*-
"""
MAHIR BR UDP - Professional Web Dashboard & Real-Time EXP Tracker
Embedded Async Web Server (aiohttp)
Optimized for ultra-smooth operation, zero memory leaks, and dynamic multi-account control.
"""

import asyncio
import json
import os
import time
from typing import Dict, List, Any, Optional
from aiohttp import web

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "index.html")

EXP_TABLE: Dict[int, int] = {
    1: 0, 2: 48, 3: 202, 4: 544, 5: 1012, 6: 1844, 7: 2792, 8: 3800,
    9: 4870, 10: 6004, 11: 7192, 12: 8448, 13: 9760, 14: 11140, 15: 12566,
    16: 14060, 17: 15610, 18: 17224, 19: 18902, 20: 20632, 21: 22424, 22: 24278,
    23: 26192, 24: 28166, 25: 30200, 26: 32294, 27: 34448, 28: 37804, 29: 41274,
    30: 44870, 31: 48582, 32: 53394, 33: 58566, 34: 64096, 35: 69994, 36: 76460,
    37: 83506, 38: 91128, 39: 99322, 40: 108092, 41: 120144, 42: 133266, 43: 147472,
    44: 162760, 45: 179126, 46: 196572, 47: 215368, 48: 235516, 49: 257010, 50: 279860,
    51: 304056, 52: 348318, 53: 394982, 54: 444044, 55: 495508, 56: 549364, 57: 633756,
    58: 721744, 59: 813336, 60: 908522, 61: 1041438, 62: 1180352, 63: 1325266,
    64: 1476184, 65: 1634300, 66: 1840946, 67: 2056594, 68: 2281242, 69: 2514880,
    70: 2757530, 71: 3059506, 72: 3372284, 73: 3699456, 74: 4041030, 75: 4397002,
    76: 4829104, 77: 5282204, 78: 5756304, 79: 6251408, 80: 6776502, 81: 7381324,
    82: 8043154, 83: 8752982, 84: 9510808, 85: 10316338, 86: 11277190, 87: 12291748,
    88: 13360304, 89: 14482858, 90: 15659418, 91: 17026708, 92: 18453950, 93: 19941280,
    94: 21488570, 95: 23095858, 96: 24763138, 97: 26490428, 98: 28378704, 99: 30124996,
    100: 32032884
}


def calculate_level_progress(level: int, current_exp: int) -> Dict[str, Any]:
    level = max(1, level)
    next_level = min(100, level + 1)
    base_exp = EXP_TABLE.get(level, 0)
    target_exp = EXP_TABLE.get(next_level, base_exp + 50000)

    needed_for_level = max(1, target_exp - base_exp)
    earned_in_level = max(0, current_exp - base_exp)
    remaining_exp = max(0, target_exp - current_exp)
    progress_pct = min(100.0, max(0.0, (earned_in_level / needed_for_level) * 100.0))

    return {
        "next_level": next_level,
        "base_exp": base_exp,
        "target_exp": target_exp,
        "needed_for_level": needed_for_level,
        "earned_in_level": earned_in_level,
        "remaining_exp": remaining_exp,
        "progress_pct": round(progress_pct, 1)
    }


# ==================== GLOBAL BOT STATE ====================
class BotState:
    def __init__(self):
        # Accounts indexed by GAME ID (in-game account id)
        self.accounts: Dict[str, Dict[str, Any]] = {}
        self.logs: List[Dict[str, Any]] = []
        self.max_logs = 200
        self.total_matches = 0
        self.total_matches_started = 0
        self.total_gained_exp = 0
        self.start_time = time.time()
        self.account_workers: Dict[str, asyncio.Task] = {}
        self.account_token_map: Dict[str, str] = {}
        self.auth_to_game_id: Dict[str, str] = {}   # guest_uid -> game_id
        self.game_to_auth_id: Dict[str, str] = {}   # game_id -> guest_uid
        self.paused_accounts: set = set()
        self.refresh_callbacks: Dict[str, Any] = {}
        self.account_credentials: Dict[str, Dict[str, Any]] = {}
        self.active_writers: Dict[str, set] = {}

    # ---------- Writers ----------
    def register_writer(self, uid: str, writer):
        uid_str = str(uid)
        if uid_str not in self.active_writers:
            self.active_writers[uid_str] = set()
        self.active_writers[uid_str].add(writer)

    def unregister_writer(self, uid: str, writer):
        uid_str = str(uid)
        if uid_str in self.active_writers:
            self.active_writers[uid_str].discard(writer)
            if not self.active_writers[uid_str]:
                self.active_writers.pop(uid_str, None)

    def close_writers_for_account(self, uid: str):
        uid_str = str(uid)
        candidates = {uid_str}
        if uid_str in self.auth_to_game_id:
            candidates.add(str(self.auth_to_game_id[uid_str]))
        if uid_str in self.game_to_auth_id:
            candidates.add(str(self.game_to_auth_id[uid_str]))
        if uid_str in self.account_token_map:
            mapped = self.account_token_map[uid_str]
            candidates.add(str(mapped))
            candidates.add(str(mapped)[:16])

        for c in list(candidates):
            writers = list(self.active_writers.get(c, []))
            for w in writers:
                try:
                    if hasattr(w, "close"):
                        if hasattr(w, "is_closing"):
                            if not w.is_closing():
                                w.close()
                        else:
                            w.close()
                except Exception:
                    pass
            self.active_writers.pop(c, None)

    # ---------- Logs ----------
    def log(self, message: str, level: str = "info", uid: Optional[str] = None):
        entry = {
            "time": time.strftime("%H:%M:%S"),
            "level": level,
            "message": message,
            "uid": str(uid) if uid else None
        }
        self.logs.append(entry)
        if len(self.logs) > self.max_logs:
            self.logs.pop(0)

    # ---------- Credentials Store ----------
    def register_credentials(self, acc_id: str, creds: Dict[str, Any]):
        """
        Store credential record indexed by every alias:
        - game_id (account_id)
        - guest_uid (auth_uid)
        - token prefix
        """
        try:
            acc_id = str(acc_id)
            self.account_credentials[acc_id] = creds

            if creds.get("auth_uid"):
                self.account_credentials[str(creds["auth_uid"])] = creds
            if creds.get("auth_token"):
                self.account_credentials[f"tok_{creds['auth_token'][:20]}"] = creds
            if creds.get("access_token"):
                self.account_credentials[f"tok_{creds['access_token'][:20]}"] = creds
            if creds.get("token"):
                self.account_credentials[f"tok_{creds['token'][:20]}"] = creds
        except Exception:
            pass

    def get_credentials(self, uid: str) -> Dict[str, Any]:
        """Look up credentials by any alias."""
        uid_str = str(uid)

        if uid_str in self.account_credentials:
            return self.account_credentials[uid_str]

        game_id = self.auth_to_game_id.get(uid_str)
        if game_id and str(game_id) in self.account_credentials:
            return self.account_credentials[str(game_id)]

        auth_uid = self.game_to_auth_id.get(uid_str)
        if auth_uid and str(auth_uid) in self.account_credentials:
            return self.account_credentials[str(auth_uid)]

        mapped = self.account_token_map.get(uid_str)
        if mapped and str(mapped) in self.account_credentials:
            return self.account_credentials[str(mapped)]

        # Fallback: scan by account_id match
        for k, v in self.account_credentials.items():
            if str(v.get("account_id")) == uid_str:
                return v

        return {}

    # ---------- Account Registration ----------
    def register_account(self, uid: str, nickname: str, region: str, level: int, exp: int,
                         likes: int = 0, token: Optional[str] = None, auth_uid: Optional[str] = None,
                         auth_password: Optional[str] = None, auth_token: Optional[str] = None,
                         auth_type: Optional[str] = None):
        """
        uid          -> GAME ID (in-game, e.g. 13974069062)
        auth_uid     -> GUEST LOGIN UID (e.g. 4311822408)
        auth_password-> guest password
        auth_token   -> access token (token login)
        """
        uid_str = str(uid)
        auth_uid_str = str(auth_uid) if auth_uid else self.game_to_auth_id.get(uid_str, "")

        if auth_uid_str:
            self.auth_to_game_id[auth_uid_str] = uid_str
            self.game_to_auth_id[uid_str] = auth_uid_str
            self.account_token_map[auth_uid_str] = uid_str
            self.account_token_map[uid_str] = auth_uid_str

        if token:
            self.account_token_map[uid_str] = token
            self.account_token_map[token[:16]] = uid_str
            if auth_uid_str:
                self.account_token_map[auth_uid_str] = token

        prog = calculate_level_progress(level or 1, exp)
        lvl_val = level or 1
        acc_mode = "BR" if lvl_val < 3 else "LONE_WOLF"
        acc_mode_label = "Battle Royale (Lvl < 3)" if lvl_val < 3 else "Lone Wolf (Lvl 3+)"

        if not auth_type:
            if auth_token:
                auth_type = "token"
            elif auth_password:
                auth_type = "guest"
            else:
                auth_type = "unknown"

        if uid_str not in self.accounts:
            self.accounts[uid_str] = {
                "uid": uid_str,                         # GAME ID
                "auth_uid": auth_uid_str or "",         # GUEST UID
                "auth_password": auth_password or "",
                "auth_token": auth_token or "",
                "auth_type": auth_type,
                "nickname": nickname or f"Player_{uid_str[:6]}",
                "region": region or "BD",
                "level": lvl_val,
                "next_level": prog["next_level"],
                "mode": acc_mode,
                "mode_label": acc_mode_label,
                "initial_exp": exp,
                "current_exp": exp,
                "gained_exp": 0,
                "remaining_exp": prog["remaining_exp"],
                "target_exp": prog["target_exp"],
                "needed_for_level": prog["needed_for_level"],
                "earned_in_level": prog["earned_in_level"],
                "progress_pct": prog["progress_pct"],
                "likes": likes or 0,
                "status": "PAUSED" if self.is_paused(uid_str) else "ONLINE",
                "matches_played": 0,
                "active_matches": 0,
                "last_match_time": None,
                "token": token or "",
                "start_time": time.time(),
                "is_paused": self.is_paused(uid_str),
                "paused_at": time.time() if self.is_paused(uid_str) else None,
                "total_pause_duration": 0.0,
                "last_updated": time.strftime("%H:%M:%S")
            }
        else:
            acc = self.accounts[uid_str]
            if auth_uid_str:
                acc["auth_uid"] = auth_uid_str
            if auth_password:
                acc["auth_password"] = auth_password
            if auth_token:
                acc["auth_token"] = auth_token
            if auth_type:
                acc["auth_type"] = auth_type
            if nickname:
                acc["nickname"] = nickname
            if region:
                acc["region"] = region
            if level:
                acc["level"] = level
            if token:
                acc["token"] = token
            acc["current_exp"] = exp
            acc["gained_exp"] = max(0, exp - acc["initial_exp"])
            acc["next_level"] = prog["next_level"]
            acc["remaining_exp"] = prog["remaining_exp"]
            acc["target_exp"] = prog["target_exp"]
            acc["needed_for_level"] = prog["needed_for_level"]
            acc["earned_in_level"] = prog["earned_in_level"]
            acc["progress_pct"] = prog["progress_pct"]
            acc["likes"] = likes
            if not acc.get("is_paused"):
                acc["status"] = "ONLINE"
            acc["last_updated"] = time.strftime("%H:%M:%S")

        # Mirror into central credential store
        self.register_credentials(uid_str, {
            "account_id": uid_str,
            "auth_uid": auth_uid_str or "",
            "auth_password": auth_password or "",
            "auth_token": auth_token or "",
            "auth_type": auth_type,
            "access_token": token or "",
            "token": token or "",
            "nickname": nickname,
            "region": region,
            "level": lvl_val,
        })

        self.recalc_totals()

    # ---------- Uptime / Pause ----------
    def get_account_uptime(self, uid_str: str) -> int:
        acc = self.accounts.get(uid_str)
        if not acc:
            mapped = self.game_to_auth_id.get(uid_str) or self.auth_to_game_id.get(uid_str)
            if mapped and mapped in self.accounts:
                acc = self.accounts[mapped]
        if not acc:
            return 0
        start_t = acc.get("start_time", time.time())
        total_pause = acc.get("total_pause_duration", 0.0)
        if acc.get("is_paused") and acc.get("paused_at"):
            return max(0, int(acc["paused_at"] - start_t - total_pause))
        return max(0, int(time.time() - start_t - total_pause))

    def is_paused(self, uid: str) -> bool:
        uid_str = str(uid)
        if uid_str in self.paused_accounts:
            return True
        game_id = self.auth_to_game_id.get(uid_str)
        if game_id and game_id in self.paused_accounts:
            return True
        auth_uid = self.game_to_auth_id.get(uid_str)
        if auth_uid and auth_uid in self.paused_accounts:
            return True
        acc = self.accounts.get(uid_str) or (self.accounts.get(game_id) if game_id else None)
        if acc and acc.get("is_paused"):
            return True
        return False

    def toggle_pause(self, uid: str) -> bool:
        uid_str = str(uid)
        candidates = {uid_str}
        if uid_str in self.auth_to_game_id:
            candidates.add(self.auth_to_game_id[uid_str])
        if uid_str in self.game_to_auth_id:
            candidates.add(self.game_to_auth_id[uid_str])

        target_acc = None
        target_key = uid_str
        for c in candidates:
            if c in self.accounts:
                target_acc = self.accounts[c]
                target_key = c
                break

        is_now_paused = not self.is_paused(uid_str)
        if is_now_paused:
            for c in candidates:
                self.paused_accounts.add(c)
                self.close_writers_for_account(c)
            if target_acc:
                target_acc["is_paused"] = True
                target_acc["paused_at"] = time.time()
                target_acc["status"] = "PAUSED"
            nick = target_acc.get("nickname", target_key) if target_acc else target_key
            self.log(f"⏸ UID {target_key} ({nick}) matchmaking PAUSED.", "warning", target_key)
            if "on_pause_toggle" in self.refresh_callbacks:
                try:
                    asyncio.create_task(self.refresh_callbacks["on_pause_toggle"](target_key, True))
                except Exception:
                    pass
        else:
            for c in candidates:
                self.paused_accounts.discard(c)
            if target_acc:
                target_acc["is_paused"] = False
                if target_acc.get("paused_at"):
                    pause_dur = time.time() - target_acc["paused_at"]
                    target_acc["total_pause_duration"] = target_acc.get("total_pause_duration", 0.0) + pause_dur
                    target_acc["paused_at"] = None
                target_acc["status"] = "ONLINE"
            nick = target_acc.get("nickname", target_key) if target_acc else target_key
            self.log(f"▶ UID {target_key} ({nick}) matchmaking RESUMED.", "success", target_key)
            if "on_pause_toggle" in self.refresh_callbacks:
                try:
                    asyncio.create_task(self.refresh_callbacks["on_pause_toggle"](target_key, False))
                except Exception:
                    pass

        return is_now_paused

    def toggle_pause_all(self) -> bool:
        any_active = any(not self.is_paused(k) for k in self.accounts.keys())
        for k in list(self.accounts.keys()):
            current_paused = self.is_paused(k)
            if any_active and not current_paused:
                self.toggle_pause(k)
            elif not any_active and current_paused:
                self.toggle_pause(k)
        return any_active

    # ---------- EXP ----------
    def update_exp(self, uid: str, current_exp: int, level: Optional[int] = None):
        uid_str = str(uid)
        if uid_str in self.accounts:
            acc = self.accounts[uid_str]
            old_exp = acc["current_exp"]
            old_level = acc.get("level", 1)
            acc["current_exp"] = current_exp
            if level is not None and level > 0:
                acc["level"] = level
            acc["gained_exp"] = max(0, current_exp - acc["initial_exp"])

            current_lvl = acc["level"]
            acc["mode"] = "BR" if current_lvl < 3 else "LONE_WOLF"
            acc["mode_label"] = "Battle Royale (Lvl < 3)" if current_lvl < 3 else "Lone Wolf (Lvl 3+)"

            prog = calculate_level_progress(acc["level"], current_exp)
            acc["next_level"] = prog["next_level"]
            acc["remaining_exp"] = prog["remaining_exp"]
            acc["target_exp"] = prog["target_exp"]
            acc["needed_for_level"] = prog["needed_for_level"]
            acc["earned_in_level"] = prog["earned_in_level"]
            acc["progress_pct"] = prog["progress_pct"]
            acc["last_updated"] = time.strftime("%H:%M:%S")

            if old_level < 3 and current_lvl >= 3:
                self.log(
                    f"🎉 LEVEL UP! UID {uid_str} ({acc['nickname']}) reached Level {current_lvl}!",
                    "success", uid_str
                )

            diff = current_exp - old_exp
            if diff > 0:
                self.log(
                    f"★ UID {uid_str} ({acc['nickname']}) gained +{diff:,} EXP | Level {acc['level']} [{acc['mode']}] ({prog['progress_pct']}% - {prog['remaining_exp']:,} EXP to Lvl {prog['next_level']})",
                    "success", uid_str
                )
            self.recalc_totals()

    def get_account_level(self, uid: str) -> int:
        uid_str = str(uid)
        acc = self.accounts.get(uid_str)
        if not acc:
            mapped = self.game_to_auth_id.get(uid_str) or self.auth_to_game_id.get(uid_str)
            if mapped and mapped in self.accounts:
                acc = self.accounts[mapped]
        if acc:
            return int(acc.get("level", 1) or 1)
        return 1

    def get_account_mode(self, uid: str) -> str:
        lvl = self.get_account_level(uid)
        return "BR" if lvl < 3 else "LONE_WOLF"

    def update_status(self, uid: str, status: str, active_matches: Optional[int] = None):
        uid_str = str(uid)
        if uid_str in self.accounts:
            self.accounts[uid_str]["status"] = status
            if active_matches is not None:
                self.accounts[uid_str]["active_matches"] = active_matches
            self.accounts[uid_str]["last_updated"] = time.strftime("%H:%M:%S")

    def increment_match_started(self):
        self.total_matches_started += 1

    def increment_match(self, uid: str):
        uid_str = str(uid)
        self.total_matches += 1
        if uid_str in self.accounts:
            self.accounts[uid_str]["matches_played"] += 1
            self.accounts[uid_str]["last_match_time"] = time.strftime("%H:%M:%S")
            self.accounts[uid_str]["last_updated"] = time.strftime("%H:%M:%S")
            self.log(f"⚔ Match #{self.accounts[uid_str]['matches_played']} finished for {self.accounts[uid_str]['nickname']} ({uid_str})", "info", uid_str)

    def recalc_totals(self):
        self.total_gained_exp = sum(acc.get("gained_exp", 0) for acc in self.accounts.values())


bot_state = BotState()


# Fallback HTML if templates/index.html missing
FALLBACK_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>MAHIR BR UDP</title></head>
<body style="background:#080d18;color:#e2e8f0;font-family:sans-serif;text-align:center;padding:50px;">
<h1 style="color:#22d3ee">MAHIR BR UDP — Running</h1>
<p>templates/index.html not found. Please add the dashboard HTML file.</p>
</body>
</html>"""


# ==================== HTTP HANDLERS ====================

async def handle_index(request: web.Request) -> web.Response:
    content = FALLBACK_INDEX_HTML
    if os.path.exists(TEMPLATE_PATH):
        try:
            with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
                content = f.read()
        except Exception:
            pass
    return web.Response(text=content, content_type="text/html", charset="utf-8")


async def handle_get_stats(request: web.Request) -> web.Response:
    accounts_data = list(bot_state.accounts.values())
    accounts_data.sort(key=lambda x: x.get("gained_exp", 0), reverse=True)
    uptime_sec = max(1, int(time.time() - bot_state.start_time))
    total_gained = bot_state.total_gained_exp
    exp_per_hour = int((total_gained / uptime_sec) * 3600)
    total_active_matches = sum(acc.get("active_matches", 0) for acc in accounts_data)

    # Enrich each account with credentials (Game ID / Guest UID / Password / Token)
    enriched: List[Dict[str, Any]] = []
    for acc in accounts_data:
        uid_k = str(acc.get("uid", ""))
        acc_copy = dict(acc)

        creds = bot_state.get_credentials(uid_k)

        # GAME ID (in-game account)
        acc_copy["uid"] = uid_k

        # GUEST UID (login credential) - never fall back to game_id
        acc_copy["auth_uid"] = (
            creds.get("auth_uid")
            or acc_copy.get("auth_uid")
            or ""
        )

        # Password (guest login)
        acc_copy["auth_password"] = (
            creds.get("auth_password")
            or acc_copy.get("auth_password")
            or ""
        )

        # Access token (token login)
        acc_copy["auth_token"] = (
            creds.get("auth_token")
            or creds.get("access_token")
            or acc_copy.get("auth_token")
            or ""
        )

        # Auth type
        acc_copy["auth_type"] = (
            creds.get("auth_type")
            or acc_copy.get("auth_type")
            or ("token" if acc_copy["auth_token"] else ("guest" if acc_copy["auth_password"] else "unknown"))
        )

        if creds.get("nickname") and not acc_copy.get("nickname"):
            acc_copy["nickname"] = creds["nickname"]
        if creds.get("region") and not acc_copy.get("region"):
            acc_copy["region"] = creds["region"]

        acc_copy["uptime_seconds"] = bot_state.get_account_uptime(uid_k)
        acc_copy["is_paused"] = bot_state.is_paused(uid_k)

        enriched.append(acc_copy)

    return web.json_response({
        "total_accounts": len(bot_state.accounts),
        "total_matches": bot_state.total_matches,
        "total_matches_started": bot_state.total_matches_started,
        "total_active_matches": total_active_matches,
        "total_gained_exp": total_gained,
        "exp_per_hour": exp_per_hour,
        "accounts": enriched,
        "logs": bot_state.logs[-80:],
        "uptime": uptime_sec
    })


async def handle_add_account(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        accounts_file = "accounts.json"
        existing = []
        if os.path.exists(accounts_file):
            try:
                with open(accounts_file, "r", encoding="utf-8") as f:
                    existing = json.load(f)
            except Exception:
                existing = []

        if "uid" in data and "password" in data:
            uid = str(data["uid"]).strip()
            pwd = str(data["password"]).strip()
            if not uid or not pwd:
                return web.json_response({"status": "error", "error": "UID and Password required"})

            if uid in bot_state.account_workers:
                try:
                    bot_state.account_workers[uid].cancel()
                except Exception:
                    pass
                bot_state.account_workers.pop(uid, None)

            existing = [acc for acc in existing if str(acc.get("uid", "")) != uid]
            existing.append({"uid": uid, "password": pwd})
            identifier = uid

            # Store guest credentials immediately
            bot_state.account_credentials[uid] = {
                "account_id": "",
                "auth_uid": uid,
                "auth_password": pwd,
                "auth_token": "",
                "auth_type": "guest",
                "access_token": "",
                "token": "",
            }

        elif "token" in data:
            token = str(data["token"]).strip()
            if not token:
                return web.json_response({"status": "error", "error": "Token is required"})

            tok_key = token[:16]
            for k in list(bot_state.account_workers.keys()):
                if k == tok_key or k.startswith(tok_key[:10]) or tok_key.startswith(k[:10]):
                    try:
                        bot_state.account_workers[k].cancel()
                    except Exception:
                        pass
                    bot_state.account_workers.pop(k, None)

            existing = [acc for acc in existing if acc.get("token", "") != token]
            existing.append({"token": token})
            identifier = f"Token_{token[:8]}..."

            bot_state.account_credentials[f"tok_{token[:20]}"] = {
                "account_id": "",
                "auth_uid": "",
                "auth_password": "",
                "auth_token": token,
                "auth_type": "token",
                "access_token": token,
                "token": token,
            }
        else:
            return web.json_response({"status": "error", "error": "Invalid payload"})

        with open(accounts_file, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2)

        bot_state.log(f"New account added: {identifier}", "success")

        if "on_account_added" in bot_state.refresh_callbacks:
            asyncio.create_task(bot_state.refresh_callbacks["on_account_added"](data))

        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_delete_account(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        req_uid = str(data.get("uid", "")).strip()
        req_auth_uid = str(data.get("auth_uid", "")).strip()
        if not req_uid and not req_auth_uid:
            return web.json_response({"status": "error", "error": "UID required"})

        candidate_ids = set()
        if req_uid:
            candidate_ids.add(req_uid)
        if req_auth_uid:
            candidate_ids.add(req_auth_uid)

        for cid in list(candidate_ids):
            if cid in bot_state.game_to_auth_id:
                candidate_ids.add(str(bot_state.game_to_auth_id[cid]))
            if cid in bot_state.auth_to_game_id:
                candidate_ids.add(str(bot_state.auth_to_game_id[cid]))

        target_tokens = set()
        for cid in list(candidate_ids):
            acc_info = bot_state.accounts.get(cid, {})
            if acc_info:
                if acc_info.get("auth_uid"):
                    candidate_ids.add(str(acc_info["auth_uid"]))
                if acc_info.get("uid"):
                    candidate_ids.add(str(acc_info["uid"]))
                t = acc_info.get("token") or acc_info.get("access_token")
                if t:
                    target_tokens.add(str(t))

        for cid in list(candidate_ids):
            creds = bot_state.account_credentials.get(cid, {})
            if creds:
                if creds.get("auth_uid"):
                    candidate_ids.add(str(creds["auth_uid"]))
                if creds.get("account_id"):
                    candidate_ids.add(str(creds["account_id"]))
                t = creds.get("token") or creds.get("access_token") or creds.get("auth_token")
                if t:
                    target_tokens.add(str(t))

        # Cleanup token_cache.json
        token_cache_file = "token_cache.json"
        if os.path.exists(token_cache_file):
            try:
                with open(token_cache_file, "r", encoding="utf-8") as f:
                    tcache = json.load(f)
                dirty = False
                for k, v in list(tcache.items()):
                    k_str = str(k)
                    v_acc_id = str(v.get("account_id", ""))
                    v_auth_uid = str(v.get("auth_uid", ""))
                    if k_str in candidate_ids or v_acc_id in candidate_ids or v_auth_uid in candidate_ids:
                        candidate_ids.add(k_str)
                        if v_acc_id:
                            candidate_ids.add(v_acc_id)
                        if v_auth_uid:
                            candidate_ids.add(v_auth_uid)
                        del tcache[k]
                        dirty = True
                if dirty:
                    with open(token_cache_file, "w", encoding="utf-8") as f:
                        json.dump(tcache, f, indent=2)
            except Exception:
                pass

        # Remove from accounts.json
        accounts_file = "accounts.json"
        if os.path.exists(accounts_file):
            try:
                with open(accounts_file, "r", encoding="utf-8") as f:
                    existing = json.load(f)
                new_existing = []
                for acc in existing:
                    acc_uid = str(acc.get("uid", "")).strip()
                    acc_tok = str(acc.get("token", "")).strip()
                    is_match = False
                    if acc_uid and acc_uid in candidate_ids:
                        is_match = True
                    if acc_tok and (acc_tok in candidate_ids or acc_tok in target_tokens):
                        is_match = True
                    for tok in target_tokens:
                        if acc_tok and (acc_tok.startswith(tok[:16]) or tok.startswith(acc_tok[:16])):
                            is_match = True
                    if not is_match:
                        new_existing.append(acc)
                with open(accounts_file, "w", encoding="utf-8") as f:
                    json.dump(new_existing, f, indent=2)
            except Exception:
                pass

        # Remove from devices.json
        devices_file = "devices.json"
        if os.path.exists(devices_file):
            try:
                with open(devices_file, "r", encoding="utf-8") as f:
                    devices_data = json.load(f)
                dirty = False
                for dev_k in list(devices_data.keys()):
                    dev_k_str = str(dev_k)
                    if dev_k_str in candidate_ids:
                        del devices_data[dev_k]
                        dirty = True
                    else:
                        for tok in target_tokens:
                            if dev_k_str == tok[:16] or tok.startswith(dev_k_str):
                                del devices_data[dev_k]
                                dirty = True
                                break
                if dirty:
                    with open(devices_file, "w", encoding="utf-8") as f:
                        json.dump(devices_data, f, indent=4)
            except Exception:
                pass

        # Remove from memory
        for cid in candidate_ids:
            bot_state.accounts.pop(cid, None)
            bot_state.account_credentials.pop(cid, None)
            bot_state.auth_to_game_id.pop(cid, None)
            bot_state.game_to_auth_id.pop(cid, None)
            bot_state.account_token_map.pop(cid, None)

        cancelled = []
        for k, worker in list(bot_state.account_workers.items()):
            k_str = str(k)
            should_cancel = False
            if k_str in candidate_ids:
                should_cancel = True
            for tok in target_tokens:
                if k_str == tok[:16] or tok.startswith(k_str[:10]):
                    should_cancel = True
            if should_cancel:
                try:
                    worker.cancel()
                except Exception:
                    pass
                cancelled.append(k)
        for k in cancelled:
            bot_state.account_workers.pop(k, None)

        for cid in candidate_ids:
            bot_state.close_writers_for_account(cid)

        if "on_account_deleted" in bot_state.refresh_callbacks:
            try:
                asyncio.create_task(bot_state.refresh_callbacks["on_account_deleted"](list(candidate_ids)))
            except Exception:
                pass

        target_repr = req_uid or req_auth_uid
        bot_state.log(f"Account {target_repr} deleted.", "warning", target_repr)
        bot_state.recalc_totals()
        return web.json_response({"status": "ok", "deleted": list(candidate_ids)})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_refresh_account(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
        if "on_refresh_account" in bot_state.refresh_callbacks:
            asyncio.create_task(bot_state.refresh_callbacks["on_refresh_account"](uid))
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_restart_account(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
        if "on_restart_account" in bot_state.refresh_callbacks:
            asyncio.create_task(bot_state.refresh_callbacks["on_restart_account"](uid))
        elif "on_refresh_account" in bot_state.refresh_callbacks:
            asyncio.create_task(bot_state.refresh_callbacks["on_refresh_account"](uid))
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_clear_logs(request: web.Request) -> web.Response:
    bot_state.logs.clear()
    return web.json_response({"status": "ok"})


async def handle_toggle_pause(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
        if not uid:
            return web.json_response({"status": "error", "error": "UID required"})
        is_paused = bot_state.toggle_pause(uid)
        return web.json_response({"status": "ok", "is_paused": is_paused})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_toggle_pause_all(request: web.Request) -> web.Response:
    try:
        paused_state = bot_state.toggle_pause_all()
        return web.json_response({"status": "ok", "all_paused": paused_state})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def start_web_dashboard(host: str = "0.0.0.0", port: int = 5000):
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/stats", handle_get_stats)
    app.router.add_post("/api/account/add", handle_add_account)
    app.router.add_post("/api/account/delete", handle_delete_account)
    app.router.add_post("/api/account/refresh", handle_refresh_account)
    app.router.add_post("/api/account/restart", handle_restart_account)
    app.router.add_post("/api/account/pause", handle_toggle_pause)
    app.router.add_post("/api/account/pause_all", handle_toggle_pause_all)
    app.router.add_post("/api/logs/clear", handle_clear_logs)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    return runner