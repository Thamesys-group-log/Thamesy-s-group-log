import os
import re
import json
import time
import random
import hashlib
import logging
import asyncio
import aiohttp
import discord
from urllib.parse import quote, urlparse
from discord import app_commands
from discord.ext import commands, tasks
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Set, Tuple

try:
    import motor.motor_asyncio
except ImportError:
    motor = None

# ==========================================
# CONFIGURATION
# ==========================================
WORKER_URL = os.getenv(
    "WORKER_URL", "https://roblox-audit-proxy.madefordiscordbot.workers.dev"
).rstrip("/")

AUTHORIZED_USER_IDS = {947558109503692802, 1219266886143967245, 1391931433521774742}

# Color Definitions for Client 2
GREEN_COLOR = discord.Color.from_rgb(46, 204, 113)   # Group Join
YELLOW_COLOR = discord.Color.from_rgb(241, 196, 15)  # Promotion
PURPLE_COLOR = discord.Color.from_rgb(138, 43, 226)  # Demotion / General Activity
RED_COLOR = discord.Color.from_rgb(231, 76, 60)      # Exile / Kick / Leave

DB_NAME = "roblox_audit_logger_client2"
BRAND = "TGE Logs"

AUDIT_POLL_INTERVAL = 30     # Seconds between audit-log polls per group
ROLES_CACHE_TTL = 21600      # 6 hours
MAX_PAGES_PER_ROLE = 2       # 2 pages x 100 = 200 members per role (bandwidth cap)
MAX_EVENTS_PER_POLL = 30     # More changes than this at once = stale baseline, resync quietly

log = logging.getLogger("roblox_audit_client2")

_mongo_client = None


def get_db():
    global _mongo_client
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri or not motor:
        return None
    if _mongo_client is None:
        try:
            _mongo_client = motor.motor_asyncio.AsyncIOMotorClient(
                mongo_uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
                socketTimeoutMS=20000,
            )
        except Exception:
            log.exception("Could not create the MongoDB client")
            return None
    try:
        return _mongo_client[DB_NAME]
    except Exception:
        return None


def is_authorized_user(user_id: int) -> bool:
    return user_id in AUTHORIZED_USER_IDS


def extract_group_id(input_val: str) -> Optional[int]:
    """Extracts a group ID from a number, a Roblox group/community link, or short text like 'Group 123'."""
    if not input_val:
        return None
    text = str(input_val).strip()
    if text.isdigit():
        return int(text) or None

    match = re.search(r"(?:groups|communities)/(\d+)", text, re.IGNORECASE)
    if match:
        return int(match.group(1)) or None

    # Last resort: accept text that contains exactly one number
    runs = re.findall(r"\d+", text)
    if len(runs) == 1:
        return int(runs[0]) or None
    return None


def format_footer_timestamp(dt: Optional[datetime] = None) -> str:
    """Formats timestamp to match: TGE Logs | YYYY/MM/DD, HH:MM AM/PM"""
    if dt is None:
        dt = datetime.now(timezone.utc)
    elif dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    return f"{BRAND} | {dt.strftime('%Y/%m/%d, %I:%M %p')}"


def audit_entry_key(entry: Dict[str, Any]) -> str:
    """Stable identifier for an audit-log entry (its id, or a hash if there is none)."""
    if entry.get("id") is not None:
        return str(entry["id"])
    raw = json.dumps(entry, sort_keys=True, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def safe_name(value: Any) -> str:
    """Escapes Discord markdown so names like a_b_c do not turn into italics."""
    if value is None:
        return "Unknown"
    return discord.utils.escape_markdown(str(value))


def user_link(name: Any, user_id: Any = None) -> str:
    if user_id:
        return f"[{safe_name(name)}](https://www.roblox.com/users/{user_id}/profile)"
    return f"**{safe_name(name)}**"


def error_embed(title: str, description: Optional[str] = None) -> discord.Embed:
    return discord.Embed(title=title, description=description, color=RED_COLOR)


def _rank(info: Dict[str, Any]) -> int:
    return info.get("rank") or 0


def diff_members(
    cached: Dict[str, Dict[str, Any]],
    current: Dict[str, Dict[str, Any]],
    truncated: Set[str],
    min_rank_val: int,
) -> Tuple[List[Tuple[str, str, Optional[Dict[str, Any]], Optional[Dict[str, Any]]]], Dict[str, Dict[str, Any]]]:
    """Compares two member snapshots.

    Returns (events, updated_cache). Each event is (kind, uid, new_info, old_info)
    where kind is join / promotion / demotion / leave.

    Members of roles in `truncated` (too big to read fully) are skipped for
    joins and leaves, because they may just have moved in or out of view.
    """
    events: List[Tuple[str, str, Optional[Dict[str, Any]], Optional[Dict[str, Any]]]] = []
    updated = dict(cached)

    for uid, info in current.items():
        old = cached.get(uid)

        if old is None:
            if info.get("role_name") in truncated or _rank(info) < min_rank_val:
                continue
            updated[uid] = info
            events.append(("join", uid, info, None))
            continue

        if old.get("role_name") == info.get("role_name"):
            continue

        updated[uid] = info
        if max(_rank(old), _rank(info)) < min_rank_val:
            continue
        kind = "demotion" if _rank(info) < _rank(old) else "promotion"
        events.append((kind, uid, info, old))

    for uid, old in cached.items():
        if uid in current:
            continue
        if old.get("role_name") in truncated:
            continue
        updated.pop(uid, None)
        if _rank(old) < min_rank_val:
            continue
        events.append(("leave", uid, None, old))

    return events, updated


# ==========================================
# DYNAMIC POLLING TRACKER
# ==========================================
class GroupTracker:
    """Tracks polling intervals individually for each config to optimize bandwidth."""

    def __init__(self, group_id: int):
        self.group_id = group_id
        self.last_polled: float = 0.0
        self.last_changed: float = time.time()
        self.current_interval: int = 300  # Default 5-minute loop

    def update_interval(self, has_changes: bool):
        now = time.time()
        self.last_polled = now

        if has_changes:
            self.last_changed = now
            self.current_interval = 300
            return

        quiet_duration = now - self.last_changed

        if quiet_duration > 3600:      # Quiet > 1 hour -> every 9 minutes
            self.current_interval = 540
        elif quiet_duration > 1800:    # Quiet > 30 minutes -> every 7 minutes
            self.current_interval = 420
        else:                          # Active -> every 5 minutes
            self.current_interval = 300

    def is_due(self) -> bool:
        return (time.time() - self.last_polled) >= self.current_interval


# ==========================================
# MAIN COG
# ==========================================
class RobloxAuditLoggerClient2(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.session: Optional[aiohttp.ClientSession] = None
        self.cycle_count: int = 0
        self.start_time: datetime = datetime.now(timezone.utc)
        self.roles_cache: Dict[int, Dict[str, Any]] = {}
        self.group_trackers: Dict[Any, GroupTracker] = {}   # keyed by config _id
        self.public_tasks: Dict[Any, asyncio.Task] = {}     # one running public poll per config
        self.last_cycle_at: float = time.time()
        self.proxy_last_ok: float = 0.0
        self.proxy_last_fail: float = 0.0

        cookie = (os.getenv("ROBLOX_COOKIE") or "").strip()
        if ".ROBLOSECURITY=" in cookie:
            cookie = cookie.split(".ROBLOSECURITY=")[-1]
        self._roblox_cookie: Optional[str] = cookie or None

        self.poll_logs_task.start()

    async def cog_unload(self):
        self.poll_logs_task.cancel()
        for task in list(self.public_tasks.values()):
            task.cancel()
        if self.session and not self.session.closed:
            await self.session.close()

    # ------------------------------------------------------------------
    # Small helpers
    # ------------------------------------------------------------------
    @staticmethod
    async def _deny(interaction: discord.Interaction):
        await interaction.response.send_message(
            embed=error_embed("Permission Denied", "You are not authorized to use this command."),
            ephemeral=True,
        )

    @staticmethod
    def _embed(
        title: str,
        description: str,
        color: discord.Color,
        fields: Optional[List[Tuple[str, str]]] = None,
        footer_dt: Optional[datetime] = None,
    ) -> discord.Embed:
        embed = discord.Embed(title=title, description=description, color=color)
        for name, value in fields or []:
            embed.add_field(name=name, value=value, inline=False)
        embed.set_footer(text=format_footer_timestamp(footer_dt))
        return embed

    async def _post(self, channel, embed: discord.Embed, react: Optional[str] = None):
        try:
            msg = await channel.send(embed=embed)
        except discord.Forbidden:
            log.warning("Missing permission to send in channel %s", getattr(channel, "id", "?"))
            return
        except discord.HTTPException:
            log.warning("Failed to send log embed in channel %s", getattr(channel, "id", "?"), exc_info=True)
            return
        if react:
            try:
                await msg.add_reaction(react)
            except discord.HTTPException:
                log.debug("Could not add reaction in channel %s", getattr(channel, "id", "?"))

    @staticmethod
    def _rank_for(roles: List[Dict[str, Any]], role_id: Any = None, role_name: Optional[str] = None) -> Optional[int]:
        """Looks up a rank number by role ID first, then by role name."""
        if role_id is not None:
            for r in roles:
                if str(r.get("id")) == str(role_id):
                    return r.get("rank", 0)
        if role_name:
            for r in roles:
                if r.get("name") == role_name:
                    return r.get("rank", 0)
        return None

    def _proxy_status(self) -> str:
        if not self.proxy_last_ok and not self.proxy_last_fail:
            return "No requests yet"
        if self.proxy_last_ok >= self.proxy_last_fail:
            return f"Online (last OK {int(time.time() - self.proxy_last_ok)}s ago)"
        return "Failing"

    def _event_embed(
        self,
        kind: str,
        user: str,
        link: str,
        group_link: str,
        footer_dt: Optional[datetime] = None,
        new_role: Optional[str] = None,
        old_role: Optional[str] = None,
    ) -> discord.Embed:
        user = safe_name(user)
        if kind == "join":
            return self._embed(
                "Group Join",
                f"There has been a new join\n\n{user} joined as `{new_role}`",
                GREEN_COLOR,
                [("Username", link), ("Group", group_link), ("New rank", str(new_role))],
                footer_dt,
            )
        if kind == "promotion":
            return self._embed(
                "Promotion",
                f"There has been a promotion\n\n{user} was promoted to `{new_role}`",
                YELLOW_COLOR,
                [("Username", link), ("Group", group_link), ("Old rank", str(old_role)), ("New rank", str(new_role))],
                footer_dt,
            )
        if kind == "demotion":
            return self._embed(
                "Demotion",
                f"There has been a demotion\n\n{user} was demoted to `{new_role}`",
                PURPLE_COLOR,
                [("Username", link), ("Group", group_link), ("Old rank", str(old_role)), ("New rank", str(new_role))],
                footer_dt,
            )
        if kind == "rank_change":
            return self._embed(
                "Rank Change",
                f"There has been a rank change\n\n{user}'s rank was changed to `{new_role}`",
                PURPLE_COLOR,
                [("Username", link), ("Group", group_link), ("Old rank", str(old_role)), ("New rank", str(new_role))],
                footer_dt,
            )
        if kind == "leave":
            return self._embed(
                "Exile",
                f"There has been an exile or leave\n\n{user} left the group",
                RED_COLOR,
                [("Username", link), ("Group", group_link), ("Old rank", str(old_role))],
                footer_dt,
            )
        # exile
        return self._embed(
            "Exile",
            f"There has been an exile\n\n{user} was exiled",
            RED_COLOR,
            [("Username", link), ("Group", group_link)],
            footer_dt,
        )

    # ------------------------------------------------------------------
    # Fun listener (1 in 10 chance when pinged)
    # ------------------------------------------------------------------
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or not self.bot.user:
            return

        if self.bot.user in message.mentions and random.randint(1, 10) == 1:
            try:
                await message.channel.send("Son why are you pinging me? \nMy job is to log not talking")
            except Exception:
                pass

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------
    async def get_http_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "en-US,en;q=0.9",
            }
            # No cookies on the session itself: a session-wide cookie would be
            # sent to every host, including the Cloudflare Worker.
            self.session = aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=10))
        return self.session

    def _headers_for(self, url: str) -> Dict[str, str]:
        """Attaches the Roblox cookie only to requests that go to roblox.com."""
        if not self._roblox_cookie:
            return {}
        host = (urlparse(url).hostname or "").lower()
        if host == "roblox.com" or host.endswith(".roblox.com"):
            return {"Cookie": f".ROBLOSECURITY={self._roblox_cookie}"}
        return {}

    async def _safe_fetch_json(self, url: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """Fetches JSON with backoff for rate limits and transient server errors."""
        session = await self.get_http_session()
        headers = self._headers_for(url)
        for attempt in range(max_retries):
            is_last = attempt == max_retries - 1
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        return await resp.json(content_type=None)

                    if resp.status == 429:
                        retry_after = resp.headers.get("Retry-After", "")
                        delay = float(retry_after) if retry_after.replace(".", "", 1).isdigit() else 2.0 * (attempt + 1)
                        log.warning("Rate limited (429) on %s", url)
                        if not is_last:
                            await asyncio.sleep(min(delay, 15.0))
                        continue

                    if resp.status in (500, 502, 503, 504):
                        if not is_last:
                            await asyncio.sleep(0.5 * (2 ** attempt))
                        continue

                    log.debug("GET %s returned %s", url, resp.status)
                    return None
            except Exception:
                if is_last:
                    log.warning("GET %s failed after %s attempts", url, max_retries, exc_info=True)
                    return None
                await asyncio.sleep(0.5 * (2 ** attempt))
        return None

    async def _fetch_proxy_json(self, url: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """Fetch through the Cloudflare Worker and remember whether it is working."""
        data = await self._safe_fetch_json(url, max_retries=max_retries)
        if data is None:
            self.proxy_last_fail = time.time()
        else:
            self.proxy_last_ok = time.time()
        return data

    async def _fetch_group_details(self, group_id: int) -> Optional[Dict[str, Any]]:
        data = await self._safe_fetch_json(f"https://groups.roblox.com/v1/groups/{group_id}", max_retries=2)
        if data and "name" in data:
            return data

        fallback = await self._safe_fetch_json(f"https://groups.roblox.com/v2/groups?groupIds={group_id}", max_retries=2)
        if fallback and fallback.get("data"):
            return fallback["data"][0]
        return None

    # --------------------------------------------------------------------
    # PROXIED FETCHERS (Cloudflare Worker)
    # --------------------------------------------------------------------
    async def _fetch_group_roles(self, group_id: int) -> List[Dict[str, Any]]:
        now = time.time()
        entry = self.roles_cache.get(group_id)
        if entry and now - entry["timestamp"] < ROLES_CACHE_TTL:
            return entry["roles"]

        data = await self._fetch_proxy_json(f"{WORKER_URL}/roles?groupId={group_id}", max_retries=3)
        if data and "roles" in data:
            roles = data["roles"]
            self.roles_cache[group_id] = {"timestamp": now, "roles": roles}
            return roles
        # Fall back to a stale cache rather than nothing
        return entry["roles"] if entry else []

    async def _fetch_filtered_group_members(
        self, group_id: int, min_rank_val: int = 0
    ) -> Tuple[Dict[str, Dict[str, Any]], bool, Set[str]]:
        """Returns (member_map, all_succeeded, truncated_role_names).

        A role is "truncated" when it has more members than we could read
        (or the Worker did not let us page through them). Join and leave events
        for those roles are skipped because they would be unreliable.
        """
        roles = await self._fetch_group_roles(group_id)
        if not roles:
            return {}, False, set()

        # Rank 0 is the Guest role, which is not a real membership list
        target_roles = [r for r in roles if r.get("rank", 0) >= max(min_rank_val, 1)]
        if not target_roles:
            return {}, False, set()

        member_map: Dict[str, Dict[str, Any]] = {}
        truncated: Set[str] = set()
        all_succeeded = True

        for role in target_roles:
            role_id = role.get("id")
            role_name = role.get("name")
            role_rank = role.get("rank")

            seen_in_role: Set[str] = set()
            cursor: Optional[str] = None
            pages = 0
            role_truncated = False

            while True:
                url = f"{WORKER_URL}/members?groupId={group_id}&roleId={role_id}"
                if cursor:
                    url += f"&cursor={quote(cursor, safe='')}"

                m_data = await self._fetch_proxy_json(url, max_retries=2)
                if m_data is None:
                    all_succeeded = False
                    break

                batch = m_data.get("data") or []
                new_ids = 0
                for u in batch:
                    uid = str(u.get("userId"))
                    if uid not in seen_in_role:
                        seen_in_role.add(uid)
                        new_ids += 1
                    member_map[uid] = {
                        "username": u.get("username"),
                        "role_name": role_name,
                        "rank": role_rank,
                    }

                pages += 1
                next_cursor = m_data.get("nextPageCursor")

                if not next_cursor:
                    # A full page with no cursor key at all means the Worker may be hiding the rest
                    if "nextPageCursor" not in m_data and len(batch) >= 100:
                        role_truncated = True
                    break

                # Stop if we hit the cap, or the Worker ignored the cursor and repeated itself
                if pages >= MAX_PAGES_PER_ROLE or new_ids == 0 or next_cursor == cursor:
                    role_truncated = True
                    break

                cursor = next_cursor
                await asyncio.sleep(0.2)

            if not all_succeeded:
                break
            if role_truncated:
                truncated.add(role_name)

        return member_map, all_succeeded, truncated

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------
    async def _handle_setup(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel,
        group_input: str,
        min_rank_name: Optional[str] = None,
        visible: bool = False,
    ):
        if not is_authorized_user(interaction.user.id):
            return await self._deny(interaction)

        is_ephemeral = not visible
        await interaction.response.defer(ephemeral=is_ephemeral)

        async def reply(embed: discord.Embed):
            await interaction.followup.send(embed=embed, ephemeral=is_ephemeral)

        group_id = extract_group_id(group_input)
        if not group_id:
            return await reply(error_embed(
                "Invalid Group Input",
                "Please provide a valid numeric Group ID or Roblox Group link.",
            ))

        db = get_db()
        if db is None:
            return await reply(error_embed("Database Error", "Could not connect to MongoDB Atlas."))

        me = interaction.guild.me
        if me is not None:
            perms = channel.permissions_for(me)
            if not (perms.view_channel and perms.send_messages and perms.embed_links):
                return await reply(error_embed(
                    "Missing Channel Permissions",
                    f"I need **View Channel**, **Send Messages** and **Embed Links** in {channel.mention}.",
                ))

        try:
            group_data = await self._fetch_group_details(group_id)
            roles = await self._fetch_group_roles(group_id)

            if not group_data and not roles:
                return await reply(error_embed(
                    "Invalid Roblox Group",
                    f"Group ID `{group_id}` could not be retrieved from Roblox.",
                ))

            group_name = group_data.get("name") if group_data else f"Group {group_id}"

            min_rank_value = 0
            selected_role_name = None

            if min_rank_name:
                wanted = min_rank_name.strip().lower()
                matched_role = next((r for r in roles if r.get("name", "").strip().lower() == wanted), None)
                if not matched_role:
                    role_list_str = (
                        "\n".join(f"- `{r.get('name')}` (Rank {r.get('rank')})" for r in roles)
                        if roles else "*(Unable to fetch role list)*"
                    )
                    return await reply(error_embed(
                        "Rank Name Not Found",
                        f"Could not find a rank matching **`{min_rank_name}`** in **{group_name}**.\n\n"
                        f"**Available Group Ranks:**\n{role_list_str}"[:4000],
                    ))
                min_rank_value = matched_role.get("rank", 0)
                selected_role_name = matched_role.get("name")

            # Roblox only accepts limit = 10, 25, 50 or 100 (limit=1 is rejected with 400)
            audit_url = f"https://groups.roblox.com/v1/groups/{group_id}/audit-log?limit=10&sortOrder=Desc"
            session = await self.get_http_session()

            last_audit_key: Optional[str] = None
            mode = "audit_log"

            try:
                async with session.get(audit_url, headers=self._headers_for(audit_url)) as resp:
                    if resp.status == 200:
                        body = await resp.json(content_type=None)
                        logs = body.get("data", [])
                        # Empty string = "log is empty, anything that appears later is new"
                        last_audit_key = audit_entry_key(logs[0]) if logs else ""
                    else:
                        mode = "public_polling"
            except Exception:
                mode = "public_polling"

            guild_id = str(interaction.guild.id)

            config_payload: Dict[str, Any] = {
                "guild_id": guild_id,
                "guild_name": interaction.guild.name,
                "group_id": group_id,
                "group_name": group_name,
                "channel_id": str(channel.id),
                "mode": mode,
                "min_rank_name": selected_role_name,
                "min_rank_value": min_rank_value,
                "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            }
            update_doc: Dict[str, Any] = {"$set": config_payload}

            if mode == "public_polling":
                initial_cache, success, _ = await self._fetch_filtered_group_members(group_id, min_rank_value)
                config_payload["member_cache"] = initial_cache if success else {}
                update_doc["$unset"] = {"last_audit_id": ""}
            else:
                config_payload["last_audit_id"] = last_audit_key
                update_doc["$unset"] = {"member_cache": ""}

            await db["group_configs"].update_one(
                {"guild_id": guild_id, "group_id": group_id},
                update_doc,
                upsert=True,
            )

            mode_msg = "Audit Log Stream (Cookie)" if mode == "audit_log" else "Public Polling Stream (Cloudflare Proxy)"
            rank_filter_info = (
                f"**Minimum Rank**: `{selected_role_name}` (Rank {min_rank_value}+)\n"
                if selected_role_name else "**Minimum Rank**: `All Ranks (0+)`\n"
            )

            await reply(self._embed(
                "TGE Logs Configured",
                (
                    f"**Group**: [{group_name}](https://www.roblox.com/groups/{group_id})\n"
                    f"**Group ID**: `{group_id}`\n"
                    f"**Channel**: {channel.mention}\n"
                    f"**Tracking Mode**: `{mode_msg}`\n"
                    f"{rank_filter_info}\n"
                    f"*Setup complete. Active monitoring initialized.*"
                ),
                GREEN_COLOR,
            ))

        except Exception as e:
            log.exception("Setup failed for group input %r", group_input)
            await reply(error_embed("Setup Error", f"An unexpected error occurred: `{e}`"))

    # ====================================================================
    # Slash Commands
    # ====================================================================

    @app_commands.command(
        name="ping",
        description="View system latency, uptime, database status, and tracking overview."
    )
    @app_commands.describe(visible="Set to True to make the response visible to everyone (default: False).")
    async def ping_cmd(self, interaction: discord.Interaction, visible: bool = False):
        start_time = time.perf_counter()
        await interaction.response.defer(ephemeral=not visible)
        response_ms = round((time.perf_counter() - start_time) * 1000)
        gateway_ms = round(self.bot.latency * 1000)

        db = get_db()
        db_status = "Disconnected"
        db_ms_str = "N/A"
        total_groups = 0

        if db is not None:
            try:
                db_start = time.perf_counter()
                await db.command("ping")
                db_ms_str = f"{round((time.perf_counter() - db_start) * 1000)} ms"
                db_status = "Connected"
                total_groups = await db["group_configs"].count_documents({})
            except Exception:
                db_status = "Error"

        now = datetime.now(timezone.utc)
        hours, remainder = divmod(int((now - self.start_time).total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        days, hours = divmod(hours, 24)
        uptime_str = f"{days}d {hours}h {minutes}m {seconds}s" if days > 0 else f"{hours}h {minutes}m {seconds}s"

        embed = discord.Embed(title="System Status Overview", color=PURPLE_COLOR, timestamp=now)
        embed.add_field(
            name="Latency",
            value=(
                f"Gateway Ping: `{gateway_ms} ms`\n"
                f"Response Time: `{response_ms} ms`\n"
                f"Database Latency: `{db_ms_str}`"
            ),
            inline=True,
        )
        embed.add_field(
            name="System Health",
            value=(
                f"Database: `{db_status}`\n"
                f"Uptime: `{uptime_str}`\n"
                f"Poll Cycles: `{self.cycle_count}`"
            ),
            inline=True,
        )
        embed.add_field(
            name="Tracking Stats",
            value=(
                f"Tracked Groups: `{total_groups}`\n"
                f"Total Servers: `{len(self.bot.guilds)}`\n"
                f"Proxy Status: `{self._proxy_status()}`"
            ),
            inline=False,
        )
        embed.set_footer(text=format_footer_timestamp(now))
        await interaction.followup.send(embed=embed, ephemeral=not visible)

    @app_commands.command(
        name="log-help",
        description="View commands, usage guides, and recommended settings for optimal performance."
    )
    @app_commands.describe(visible="Set to True to make the response visible to everyone (default: False).")
    async def log_help_cmd(self, interaction: discord.Interaction, visible: bool = False):
        if not is_authorized_user(interaction.user.id):
            return await self._deny(interaction)

        embed = self._embed(
            "TGE Logs - Usage Guide",
            "Use the commands below to configure and monitor Roblox group audit events.",
            PURPLE_COLOR,
            [
                (
                    "Available Commands",
                    "- **/ping**\n"
                    "  View system status, ping latency, uptime, and tracking stats.\n\n"
                    "- **/setup `[channel]` `[group]`**\n"
                    "  Tracks all events for all ranks in the group.\n\n"
                    "- **/rank-setup `[channel]` `[group]` `[min_rank_name]`**\n"
                    "  Tracks events exclusively for members at or above a specific rank.\n\n"
                    "- **/remove-setup `[group]`**\n"
                    "  Stops logging for a specific group.\n\n"
                    "- **/list-setups**\n"
                    "  Displays all configured group logs in this server.",
                ),
                (
                    "Good to Know",
                    "- **Public polling cap**: only the first 200 members of each rank are scanned, and join/leave events are skipped for ranks larger than that.\n"
                    "- **Rank filters in public polling**: someone promoted into the tracked ranks shows as a join, and someone demoted out of them shows as a leave. Audit Log mode does not have this limit.\n"
                    "- **Case-insensitive rank names**: type the rank title as shown on Roblox.",
                ),
            ],
        )
        await interaction.response.send_message(embed=embed, ephemeral=not visible)

    @app_commands.command(
        name="setup",
        description="Setup Roblox Group activity logging for ALL ranks."
    )
    @app_commands.guild_only()
    @app_commands.describe(
        channel="The text channel where logs should be sent",
        group="The Roblox Group ID or full Group Link to track",
        visible="Set to True to make the response visible to everyone (default: False).",
    )
    async def setup_cmd(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel,
        group: str,
        visible: bool = False,
    ):
        await self._handle_setup(interaction, channel, group, min_rank_name=None, visible=visible)

    @app_commands.command(
        name="rank-setup",
        description="Setup Roblox Group logging for a specific MINIMUM rank name and above."
    )
    @app_commands.guild_only()
    @app_commands.describe(
        channel="The text channel where logs should be sent",
        group="The Roblox Group ID or full Group Link to track",
        min_rank_name="The exact rank name to start logging from",
        visible="Set to True to make the response visible to everyone (default: False).",
    )
    async def rank_setup_cmd(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel,
        group: str,
        min_rank_name: str,
        visible: bool = False,
    ):
        await self._handle_setup(interaction, channel, group, min_rank_name=min_rank_name, visible=visible)

    @app_commands.command(
        name="remove-setup",
        description="Remove a Roblox Group audit setup from this server."
    )
    @app_commands.guild_only()
    @app_commands.describe(
        group="The Roblox Group ID or full Group Link to untrack",
        visible="Set to True to make the response visible to everyone (default: False).",
    )
    async def remove_setup_cmd(self, interaction: discord.Interaction, group: str, visible: bool = False):
        if not is_authorized_user(interaction.user.id):
            return await self._deny(interaction)

        await interaction.response.defer(ephemeral=not visible)

        async def reply(embed: discord.Embed):
            await interaction.followup.send(embed=embed, ephemeral=not visible)

        group_id = extract_group_id(group)
        if not group_id:
            return await reply(error_embed("Invalid Group Input", "Please provide a valid numeric Group ID or Roblox Group link."))

        db = get_db()
        if db is None:
            return await reply(error_embed("Database Error", "Could not connect to MongoDB Atlas."))

        deleted = await db["group_configs"].find_one_and_delete(
            {"guild_id": str(interaction.guild.id), "group_id": group_id}
        )
        if not deleted:
            return await reply(self._embed("Not Found", f"No configuration found for Group ID `{group_id}`.", RED_COLOR))

        # Clean up in-memory state for the removed config
        key = deleted["_id"]
        self.group_trackers.pop(key, None)
        task = self.public_tasks.pop(key, None)
        if task:
            task.cancel()

        await reply(self._embed("Setup Removed", f"Stopped tracking Group ID `{group_id}`.", GREEN_COLOR))

    @app_commands.command(
        name="list-setups",
        description="List active group configurations."
    )
    @app_commands.guild_only()
    @app_commands.describe(visible="Set to True to make the response visible to everyone (default: False).")
    async def list_setups_cmd(self, interaction: discord.Interaction, visible: bool = False):
        if not is_authorized_user(interaction.user.id):
            return await self._deny(interaction)

        await interaction.response.defer(ephemeral=not visible)

        db = get_db()
        if db is None:
            return await interaction.followup.send(
                embed=error_embed("Database Error", "Could not connect to MongoDB Atlas."),
                ephemeral=not visible,
            )

        configs = await db["group_configs"].find({"guild_id": str(interaction.guild.id)}).to_list(length=100)
        lines = []
        for doc in configs:
            g_name = doc.get("group_name", "Group")
            g_id = doc.get("group_id")
            ch_id = doc.get("channel_id")
            mode = doc.get("mode", "audit_log")
            min_r = doc.get("min_rank_name") or "All"
            lines.append(
                f"- [{g_name}](https://www.roblox.com/groups/{g_id}) (`{g_id}`) -> <#{ch_id}> [{mode} | Min Rank: {min_r}]"
            )

        description = "\n".join(lines) if lines else "*(No active group setups)*"
        if len(description) > 4000:
            description = description[:3990].rsplit("\n", 1)[0] + "\n*(list truncated)*"

        await interaction.followup.send(
            embed=self._embed("TGE Logs - Server Configurations", description, PURPLE_COLOR),
            ephemeral=not visible,
        )

    # ====================================================================
    # Background Poller Loop
    # ====================================================================

    @tasks.loop(seconds=10.0)
    async def poll_logs_task(self):
        try:
            await asyncio.wait_for(self._poll_cycle(), timeout=120)
        except asyncio.TimeoutError:
            log.warning("Poll cycle took longer than 120s and was cancelled. Continuing.")
        except Exception:
            log.exception("Poll cycle failed. Will retry on the next tick.")
        finally:
            self.last_cycle_at = time.time()

    @poll_logs_task.before_loop
    async def before_poll_logs_task(self):
        await self.bot.wait_until_ready()
        self.last_cycle_at = time.time()

    async def _poll_cycle(self):
        db = get_db()
        if db is None:
            return

        session = await self.get_http_session()
        configs = await db["group_configs"].find().to_list(length=1000)
        self.cycle_count += 1

        for cfg in configs:
            # One bad group must never stop the others
            try:
                await self._poll_one(db, session, cfg)
            except Exception:
                log.exception("Polling failed for group %s", cfg.get("group_id"))

    async def _poll_one(self, db, session, cfg):
        group_id = cfg.get("group_id")
        group_name = cfg.get("group_name", "Roblox Group")
        channel_id = cfg.get("channel_id")
        mode = cfg.get("mode", "audit_log")
        min_rank_val = cfg.get("min_rank_value", 0)

        if not channel_id or not group_id:
            return

        channel = self.bot.get_channel(int(channel_id))
        if not channel:
            return

        # Trackers are per config (a group can be set up in more than one server)
        key = cfg["_id"]
        tracker = self.group_trackers.get(key)
        if tracker is None:
            tracker = self.group_trackers[key] = GroupTracker(group_id)

        if mode == "audit_log":
            # Throttle so many groups do not hit the audit-log endpoint every 10s
            if time.time() - tracker.last_polled < AUDIT_POLL_INTERVAL:
                return
            tracker.last_polled = time.time()
            await self.poll_audit_log_mode(db, session, cfg, channel, group_id, group_name, min_rank_val)
            return

        # Public polling: only when due, and never two at once for the same config
        if key in self.public_tasks or not tracker.is_due():
            return

        tracker.last_polled = time.time()  # Claim the slot so the next tick does not start a duplicate
        task = asyncio.create_task(
            self.poll_public_mode(db, cfg, channel, group_id, group_name, min_rank_val, tracker)
        )
        self.public_tasks[key] = task  # Keep a reference so the task is not garbage collected
        task.add_done_callback(lambda t, k=key: self._on_public_task_done(k, t))

    def _on_public_task_done(self, key, task: asyncio.Task):
        self.public_tasks.pop(key, None)
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            log.error("Public poll task crashed", exc_info=exc)

    async def poll_audit_log_mode(self, db, session, cfg, channel, group_id, group_name, min_rank_val):
        url = f"https://groups.roblox.com/v1/groups/{group_id}/audit-log?limit=25&sortOrder=Desc"

        try:
            async with session.get(url, headers=self._headers_for(url)) as resp:
                status = resp.status
                body = await resp.json(content_type=None) if status == 200 else None
        except Exception:
            log.warning("Audit log request failed for group %s", group_id, exc_info=True)
            return

        if status in (401, 403):
            log.warning("Audit log returned %s for group %s. Switching it to public polling.", status, group_id)
            await db["group_configs"].update_one(
                {"_id": cfg["_id"]},
                {"$set": {"mode": "public_polling"}, "$unset": {"last_audit_id": ""}},
            )
            return

        if status != 200 or not body:
            return

        logs = body.get("data", [])
        if not logs:
            return

        last_key = cfg.get("last_audit_id")

        # No watermark yet: set it without logging old entries
        if last_key is None:
            await db["group_configs"].update_one(
                {"_id": cfg["_id"]}, {"$set": {"last_audit_id": audit_entry_key(logs[0])}}
            )
            return

        last_key = str(last_key)
        new_logs = []
        for item in logs:
            if audit_entry_key(item) == last_key:
                break
            new_logs.append(item)

        if not new_logs:
            return

        await db["group_configs"].update_one(
            {"_id": cfg["_id"]},
            {"$set": {"last_audit_id": audit_entry_key(new_logs[0])}},
        )

        for entry in reversed(new_logs):
            try:
                await self.send_formatted_log(channel, entry, group_id, group_name, min_rank_val)
            except Exception:
                log.exception("Failed to format audit entry for group %s", group_id)

    async def poll_public_mode(self, db, cfg, channel, group_id, group_name, min_rank_val, tracker: GroupTracker):
        cached_members: Dict[str, Dict[str, Any]] = cfg.get("member_cache") or {}
        has_changes = False

        try:
            current_members, is_complete, truncated = await self._fetch_filtered_group_members(group_id, min_rank_val)

            # Failed or incomplete fetch: never compare, it would produce false leaves
            if not is_complete or not current_members:
                return

            # First run: record the baseline without announcing existing members
            if not cached_members:
                await db["group_configs"].update_one({"_id": cfg["_id"]}, {"$set": {"member_cache": current_members}})
                return

            events, updated_cache = diff_members(cached_members, current_members, truncated, min_rank_val)

            # A huge batch almost always means the saved baseline is stale (long downtime)
            if len(events) > MAX_EVENTS_PER_POLL:
                log.warning(
                    "Group %s: %s changes at once, refreshing the baseline instead of posting them.",
                    group_id, len(events),
                )
                await db["group_configs"].update_one({"_id": cfg["_id"]}, {"$set": {"member_cache": updated_cache}})
                await self._post(channel, self._embed(
                    "Group Activity",
                    f"{len(events)} membership changes were detected at once, so they were not listed individually.",
                    PURPLE_COLOR,
                    [("Group", f"[{group_name}](https://www.roblox.com/groups/{group_id})")],
                ))
                has_changes = True
                return

            group_link = f"[{group_name}](https://www.roblox.com/groups/{group_id})"

            for kind, uid, info, old in events:
                source = info if info is not None else old
                name = source.get("username") or f"User {uid}"
                embed = self._event_embed(
                    kind,
                    name,
                    user_link(name, uid),
                    group_link,
                    new_role=info.get("role_name") if info else None,
                    old_role=old.get("role_name", "Unknown") if old else None,
                )
                await self._post(channel, embed, react="🎉" if kind == "promotion" else None)

            if updated_cache != cached_members:
                await db["group_configs"].update_one({"_id": cfg["_id"]}, {"$set": {"member_cache": updated_cache}})

            has_changes = bool(events)

        except Exception:
            log.exception("Public polling failed for group %s", group_id)
        finally:
            tracker.update_interval(has_changes)

    # ====================================================================
    # Audit-log embed formatting
    # ====================================================================

    async def send_formatted_log(self, channel, entry, group_id, group_name, min_rank_val: int = 0):
        actor_data = (entry.get("actor") or {}).get("user") or {}
        actor_name = actor_data.get("username", "System")
        actor_id = actor_data.get("userId")

        action_type = entry.get("actionType", "Audit Action")
        description = entry.get("description") or {}
        created_dt = entry.get("created")

        event_dt = datetime.now(timezone.utc)
        if created_dt:
            try:
                event_dt = datetime.fromisoformat(created_dt.replace("Z", "+00:00"))
                if event_dt.tzinfo is None:
                    event_dt = event_dt.replace(tzinfo=timezone.utc)
            except Exception:
                pass

        group_link = f"[{group_name}](https://www.roblox.com/groups/{group_id})"

        if action_type in ("Change Rank", "Promote Member", "Demote Member"):
            target_user = description.get("TargetHeader", description.get("TargetName", "Target User"))
            target_link = user_link(target_user, description.get("TargetId"))

            old_role = description.get("OldRoleSetHeader", description.get("OldRoleSetName", "Unknown"))
            new_role = description.get("NewRoleSetHeader", description.get("NewRoleSetName", "Unknown"))

            roles = await self._fetch_group_roles(group_id)
            old_rank = self._rank_for(roles, description.get("OldRoleSetId"), old_role)
            new_rank = self._rank_for(roles, description.get("NewRoleSetId"), new_role)

            if max(old_rank or 0, new_rank or 0) < min_rank_val:
                return

            ranks_known = old_rank is not None and new_rank is not None
            if action_type == "Demote Member" or (ranks_known and new_rank < old_rank):
                kind = "demotion"
            elif action_type == "Promote Member" or (ranks_known and new_rank > old_rank):
                kind = "promotion"
            else:
                kind = "rank_change"

            embed = self._event_embed(
                kind, target_user, target_link, group_link, event_dt,
                new_role=str(new_role), old_role=str(old_role),
            )
            await self._post(channel, embed, react="🎉" if kind == "promotion" else None)

        elif action_type in ("Accept Join Request", "Join Group"):
            role_name = description.get("NewRoleSetHeader", description.get("RoleSetName", "Member"))
            roles = await self._fetch_group_roles(group_id)
            role_rank = self._rank_for(roles, description.get("NewRoleSetId", description.get("RoleSetId")), role_name)

            if (role_rank or 0) < min_rank_val:
                return

            target_user = description.get("TargetHeader", description.get("TargetName", actor_name))
            target_link = user_link(target_user, description.get("TargetId", actor_id))

            embed = self._event_embed(
                "join", target_user, target_link, group_link, event_dt, new_role=str(role_name)
            )
            await self._post(channel, embed)

        elif action_type in ("Remove Member", "Exile Member"):
            if min_rank_val > 0:
                return  # Cannot know the removed member's rank from the audit log

            target_user = description.get("TargetHeader", description.get("TargetName", "Target User"))
            target_link = user_link(target_user, description.get("TargetId"))

            embed = self._event_embed("exile", target_user, target_link, group_link, event_dt)
            await self._post(channel, embed)

        else:
            if min_rank_val > 0:
                return

            embed = self._embed(
                "Group Activity",
                f"An activity occurred in the group\n\n**Action**: `{action_type}`",
                PURPLE_COLOR,
                [("Performer", user_link(actor_name, actor_id)), ("Group", group_link)],
                event_dt,
            )
            await self._post(channel, embed)


async def setup(bot: commands.Bot):
    await bot.add_cog(RobloxAuditLoggerClient2(bot))
