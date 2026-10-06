"""
Chat Service
============
Core business logic for the chat domain.

Responsibilities
----------------
1. Session CRUD (create, list, get, delete).
2. History management — sliding-window load + persistence.
3. Agentic loop — Gemini multi-turn with tool calling.
4. Session summarisation — evicts old turns and compresses them into
   `ChatSession.summary` to bound token usage across long conversations.

Token-reduction strategies applied
-----------------------------------
* Sliding window: only the last HISTORY_WINDOW_SIZE turns are sent to Gemini.
  Older turns are replaced by the rolling `summary` string.
* Compact tool responses: tool fns return minimal dicts (no raw text blobs).
* Lean system prompt: terse, no boilerplate filler.
* Single Gemini client reused across the app lifetime (module-level singleton).
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any
from uuid import UUID

from google import genai
from google.genai import types as genai_types
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.domains.chat.models import ChatMessage, ChatSession, MessageRole
from app.domains.chat.schemas import BotResponse, UIAction, UIActionType
from app.domains.chat.tools import TOOL_DEFINITIONS, dispatch
from app.domains.users.models import User

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Number of most-recent DB turns included verbatim in every Gemini request.
# Older turns are represented only by ChatSession.summary.
HISTORY_WINDOW_SIZE = 20

# When turn_count reaches this threshold, summarisation is triggered.
SUMMARISE_AFTER_TURNS = 30

# Safety cap on agentic hops per user message.
MAX_TOOL_HOPS = 6

# Gemini model used for the chatbot. Flash is fast and cheap.
CHAT_MODEL = settings.GEMINI_CHAT_MODEL

# Gemini model used for summarisation — can be the same or lighter.
SUMMARY_MODEL = settings.GEMINI_SUMMARY_MODEL

# ---------------------------------------------------------------------------
# Module-level Gemini client (instantiated once, reused across requests)
# ---------------------------------------------------------------------------
_gemini_client: genai.Client | None = None


def _get_client() -> genai.Client:
    global _gemini_client
    if _gemini_client is None:
        _gemini_client = genai.Client(api_key=settings.GEMINI_API_KEY)
    return _gemini_client


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT_TEMPLATE = """\
You are a helpful AI assistant embedded in a recruitment platform called SoloBuild.
You help recruiters manage campaigns, upload candidates, and track screening progress.

Rules:
- NEVER guess or invent UUIDs. Use list_campaigns() to discover IDs.
- When a resource is not found, call the appropriate list tool first.
- Keep responses short, natural, and professional.
- NEVER output raw internal IDs, batch IDs, or UUIDs (e.g. batch_id, campaign_id, candidate_id, screen_id) in the user-visible text. Refer to entities using their human-readable names or titles (e.g. "Software Developer campaign", "Alice Smith"). Place internal IDs ONLY inside the ACTION JSON payload.
- If you need to show data or open interactive panels in the UI, end your response with a JSON block on its own line:
  ACTION: {{"type": "UI_ACTION_TYPE", "payload": {{...}}}}
  Valid types:
    SHOW_CAMPAIGN_LIST        \u2192 payload: {{ campaigns: [{{id, title, created_at}}, ...] }}
    SHOW_CAMPAIGN_DETAIL      \u2192 payload: {{ campaign_id, campaign: {{id, title, required_fields, ...}} }}
    SHOW_CAMPAIGN_CREATE_FORM \u2192 payload: {{ initial_title, initial_text }}
    SHOW_CANDIDATE_LIST       \u2192 payload: {{ campaign_id, candidates: [{{id, name, email, workflow_step, step_status}}, ...] }}
    SHOW_CANDIDATE_UPLOAD     \u2192 payload: {{ campaign_id, campaign_title }}
    SHOW_SCREENING_STATUS     \u2192 payload: {{ campaign_id, batch_id }}
    SHOW_BATCH_STATUS         \u2192 payload: {{ batch_id }}
    SHOW_CAMPAIGN_PICKER      \u2192 payload: {{ campaigns: [{{id, title}}, ...] }}

{summary_block}
"""


def _build_system_prompt(summary: str | None) -> str:
    summary_block = (
        f"Previous context:\n{summary}" if summary else ""
    )
    return _SYSTEM_PROMPT_TEMPLATE.format(summary_block=summary_block).strip()


# ---------------------------------------------------------------------------
# Gemini content serialisation helpers
# ---------------------------------------------------------------------------

def _content_to_dict(content: genai_types.Content) -> dict:
    """Serialise a Gemini Content object to a plain dict for JSONB storage."""
    parts = []
    for part in (content.parts or []):
        if part.text is not None:
            parts.append({"text": part.text})
        elif part.function_call is not None:
            parts.append({
                "function_call": {
                    "name": part.function_call.name,
                    "args": dict(part.function_call.args or {}),
                }
            })
        elif part.function_response is not None:
            parts.append({
                "function_response": {
                    "name": part.function_response.name,
                    "response": part.function_response.response,
                }
            })
    role = "user" if content.role == "tool" else content.role
    return {"role": role, "parts": parts}


def _dict_to_content(data: dict) -> genai_types.Content:
    """Deserialise a stored JSONB dict back to a Gemini Content object."""
    role = "user" if data.get("role") == "tool" else data.get("role", "user")
    parts = []
    for p in data.get("parts", []):
        if "text" in p:
            parts.append(genai_types.Part(text=p["text"]))
        elif "function_call" in p:
            fc = p["function_call"]
            parts.append(genai_types.Part(
                function_call=genai_types.FunctionCall(
                    name=fc["name"],
                    args=fc.get("args", {}),
                )
            ))
        elif "function_response" in p:
            fr = p["function_response"]
            parts.append(genai_types.Part(
                function_response=genai_types.FunctionResponse(
                    name=fr["name"],
                    response=fr.get("response", {}),
                )
            ))
    return genai_types.Content(role=role, parts=parts)


def _extract_text(content: genai_types.Content) -> str | None:
    """Pull the first text part out of a Content object."""
    for part in (content.parts or []):
        if part.text:
            return part.text
    return None


def _parse_ui_action(text: str) -> tuple[str, UIAction | None]:
    """
    Detect and strip an ACTION line from the model's text response.
    Returns (clean_text, UIAction | None).
    """
    lines = text.splitlines()
    action: UIAction | None = None
    clean_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("ACTION:"):
            try:
                raw = stripped[len("ACTION:"):].strip()
                data = json.loads(raw)
                action = UIAction(
                    type=data.get("type", ""),
                    payload=data.get("payload", {}),
                )
            except (json.JSONDecodeError, Exception):
                clean_lines.append(line)
        else:
            clean_lines.append(line)
    return "\n".join(clean_lines).strip(), action


# Keys whose values are large data arrays/objects — strip before persisting.
_PAYLOAD_DATA_KEYS = frozenset({
    "candidates", "campaigns", "campaign", "batch_details",
})


def _strip_action_payload_for_storage(text: str) -> str:
    """
    Rewrite any ACTION: JSON in *text* so that large data arrays/objects are
    removed from the payload before the turn is persisted to the DB.

    Only reference keys (ids, type, titles) are kept, so the stored text stays
    lean and doesn't bloat Gemini's context window on subsequent requests.
    The live ui_action sent to the frontend is always assembled from fresh
    tool_results (auto-hydration), so nothing useful is lost.
    """
    lines = text.splitlines()
    out: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("ACTION:"):
            try:
                raw = stripped[len("ACTION:"):].strip()
                data = json.loads(raw)
                payload = data.get("payload", {})
                # Remove every key that holds a large data structure
                slim_payload = {k: v for k, v in payload.items() if k not in _PAYLOAD_DATA_KEYS}
                data["payload"] = slim_payload
                out.append("ACTION: " + json.dumps(data, separators=(",", ":")))
                continue
            except Exception:
                pass  # If parsing fails, keep the line as-is
        out.append(line)
    return "\n".join(out)


# ---------------------------------------------------------------------------
# History helpers
# ---------------------------------------------------------------------------

async def _load_history(
    db: AsyncSession, session_id: UUID
) -> list[genai_types.Content]:
    """
    Load the most recent HISTORY_WINDOW_SIZE unsummarised text turns from DB and
    deserialise them into Gemini Content objects.

    Omits intermediate tool-call/response turns from past requests to ensure
    history is always a valid alternating (user -> model) sequence and to keep
    token usage low.
    """
    result = await db.execute(
        select(ChatMessage.content)
        .where(
            ChatMessage.session_id == session_id,
            ChatMessage.is_summarised.is_(False),
        )
        .order_by(ChatMessage.created_at.desc())
        .limit(HISTORY_WINDOW_SIZE * 2)
    )
    rows = result.scalars().all()

    contents: list[genai_types.Content] = []
    for row in reversed(rows):
        content = _dict_to_content(row)
        text = _extract_text(content)
        if text:
            # Merge adjacent turns with the same role if any exist in legacy data
            if contents and contents[-1].role == content.role:
                prev_text = _extract_text(contents[-1]) or ""
                contents[-1] = genai_types.Content(
                    role=content.role,
                    parts=[genai_types.Part(text=f"{prev_text}\n{text}")],
                )
            else:
                contents.append(genai_types.Content(
                    role=content.role,
                    parts=[genai_types.Part(text=text)],
                ))

    return contents[-HISTORY_WINDOW_SIZE:]


async def _persist_turns(
    db: AsyncSession,
    session_id: UUID,
    turns: list[genai_types.Content],
) -> None:
    """
    Persist new Content turns to chat_messages in a single flush.
    Increments ChatSession.turn_count atomically.
    """
    role_map = {"user": MessageRole.USER, "model": MessageRole.MODEL, "tool": MessageRole.TOOL}
    for turn in turns:
        # For model turns, strip large data payloads from ACTION blocks so that
        # candidate/campaign lists don't re-enter Gemini's context window.
        if turn.role == "model":
            text = _extract_text(turn)
            if text and "ACTION:" in text:
                slim_text = _strip_action_payload_for_storage(text)
                turn = genai_types.Content(
                    role=turn.role,
                    parts=[genai_types.Part(text=slim_text)],
                )
        db.add(ChatMessage(
            session_id=session_id,
            role=role_map.get(turn.role, MessageRole.MODEL),
            content=_content_to_dict(turn),
        ))

    # Increment turn_count without a SELECT
    session = await db.get(ChatSession, session_id)
    if session:
        session.turn_count = (session.turn_count or 0) + len(turns)
    await db.flush()


# ---------------------------------------------------------------------------
# Agentic loop
# ---------------------------------------------------------------------------

async def _run_agentic_loop(
    client: genai.Client,
    history: list[genai_types.Content],
    system_prompt: str,
    db: AsyncSession,
    user: User,
) -> tuple[genai_types.Content, dict[str, Any]]:
    """
    Core agentic loop. Sends history to Gemini, executes tool calls if any,
    and loops until a pure-text response is received or MAX_TOOL_HOPS is hit.

    Returns (final model Content turn, dictionary of latest tool results by name).
    """
    tool_config = genai_types.Tool(
        function_declarations=TOOL_DEFINITIONS  # type: ignore[arg-type]
    )
    tool_results: dict[str, Any] = {}

    for hop in range(MAX_TOOL_HOPS):
        response = await asyncio.wait_for(
            client.aio.models.generate_content(
                model=CHAT_MODEL,
                contents=history,
                config=genai_types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    tools=[tool_config],
                    tool_config=genai_types.ToolConfig(
                        function_calling_config=genai_types.FunctionCallingConfig(
                            mode="AUTO"
                        )
                    ),
                    temperature=0.3,
                ),
            ),
            timeout=settings.LLM_REQUEST_TIMEOUT_SECONDS,
        )

        model_turn = response.candidates[0].content
        history.append(model_turn)

        # Collect any function calls in this turn
        tool_calls = [
            p for p in (model_turn.parts or []) if p.function_call is not None
        ]

        if not tool_calls:
            # No more tool calls — Gemini is done.
            return model_turn, tool_results

        # Execute all tool calls (sequential; most responses are tiny)
        response_parts: list[genai_types.Part] = []
        for tc in tool_calls:
            fc = tc.function_call
            logger.debug("Chat tool call: %s(%s)", fc.name, fc.args)
            result = await dispatch(
                name=fc.name,
                args=dict(fc.args or {}),
                db=db,
                user=user,
            )
            tool_results[fc.name] = result
            response_parts.append(genai_types.Part(
                function_response=genai_types.FunctionResponse(
                    name=fc.name,
                    response={"result": result},
                )
            ))

        tool_turn = genai_types.Content(role="user", parts=response_parts)
        history.append(tool_turn)

    logger.warning("Chat agentic loop hit MAX_TOOL_HOPS (%d), returning last turn.", MAX_TOOL_HOPS)
    return history[-2], tool_results


# ---------------------------------------------------------------------------
# Summarisation
# ---------------------------------------------------------------------------

async def _maybe_summarise(db: AsyncSession, session: ChatSession) -> None:
    """
    If the session has accumulated enough turns, compress the oldest half into
    `ChatSession.summary` and delete those rows from `chat_messages`.

    This keeps the sliding window meaningful while bounding long-term storage.
    Runs in-process (no background task) after the response is sent, so it
    does not block the user.
    """
    if (session.turn_count or 0) < SUMMARISE_AFTER_TURNS:
        return

    # Load the oldest turns (everything outside the current window)
    evict_count = session.turn_count - HISTORY_WINDOW_SIZE
    if evict_count <= 0:
        return

    result = await db.execute(
        select(ChatMessage)
        .where(ChatMessage.session_id == session.id)
        .order_by(ChatMessage.created_at.asc())
        .limit(evict_count)
    )
    old_messages = result.scalars().all()
    if not old_messages:
        return

    # Build a plain-text representation for Gemini to summarise
    lines: list[str] = []
    for msg in old_messages:
        role = msg.role.value
        text = msg.content.get("parts", [{}])[0].get("text", "[tool interaction]")
        lines.append(f"{role}: {text[:300]}")  # truncate per-line to keep prompt small
    transcript = "\n".join(lines)

    prompt = (
        "Summarise the following conversation turns concisely for future context. "
        "Focus on entities (campaign names/IDs, candidate counts, batch IDs, actions taken). "
        "Max 200 words.\n\n" + transcript
    )

    try:
        client = _get_client()
        summary_response = await asyncio.wait_for(
            client.aio.models.generate_content(
                model=SUMMARY_MODEL,
                contents=prompt,
                config=genai_types.GenerateContentConfig(temperature=0.0),
            ),
            timeout=30,
        )
        new_summary_text = summary_response.text or ""
        # Prepend to any existing summary so context accumulates
        existing = session.summary or ""
        session.summary = (existing + "\n\n" + new_summary_text).strip()
    except Exception:
        logger.exception("Chat summarisation failed for session %s", session.id)
        return

    # Mark evicted rows as summarised — they stay in DB for audit/history.
    ids_to_mark = [m.id for m in old_messages]
    await db.execute(
        update(ChatMessage)
        .where(ChatMessage.id.in_(ids_to_mark))
        .values(is_summarised=True)
    )
    # turn_count now tracks only the unsummarised (live) window
    session.turn_count = session.turn_count - evict_count
    await db.flush()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def create_session(
    db: AsyncSession,
    user: User,
    title: str | None = None,
) -> ChatSession:
    session = ChatSession(user_id=user.id, title=title)
    db.add(session)
    await db.flush()
    return session


async def list_sessions(
    db: AsyncSession, user: User
) -> list[ChatSession]:
    result = await db.execute(
        select(ChatSession)
        .where(ChatSession.user_id == user.id)
        .order_by(ChatSession.updated_at.desc())
        .limit(50)
    )
    return list(result.scalars().all())


async def get_session(
    db: AsyncSession, session_id: UUID, user: User
) -> ChatSession | None:
    result = await db.execute(
        select(ChatSession).where(
            ChatSession.id == session_id,
            ChatSession.user_id == user.id,
        )
    )
    return result.scalar_one_or_none()


async def delete_session(
    db: AsyncSession, session_id: UUID, user: User
) -> bool:
    session = await get_session(db, session_id, user)
    if session is None:
        return False
    await db.delete(session)
    await db.flush()
    return True


async def get_session_messages(
    db: AsyncSession, session_id: UUID, user: User
) -> list[ChatMessage]:
    # Ownership guard
    session = await get_session(db, session_id, user)
    if session is None:
        return []
    result = await db.execute(
        select(ChatMessage)
        .where(ChatMessage.session_id == session_id)
        .order_by(ChatMessage.created_at.asc())
    )
    return list(result.scalars().all())


async def process_message(
    db: AsyncSession,
    session_id: UUID,
    user_message: str,
    user: User,
) -> BotResponse:
    """
    Main entry point: receives a user message, runs the agentic loop, persists
    new turns, optionally triggers summarisation, and returns a BotResponse.
    """
    session = await get_session(db, session_id, user)
    if session is None:
        raise ValueError(f"Session {session_id} not found.")

    # Auto-title the session from the first message
    if not session.title and user_message:
        session.title = user_message[:80]

    # 1. Load sliding-window history from DB
    history = await _load_history(db, session_id)
    initial_len = len(history)

    # 2. Append the new user turn
    user_turn = genai_types.Content(
        role="user",
        parts=[genai_types.Part(text=user_message)],
    )
    history.append(user_turn)

    # 3. Build system prompt (includes rolling summary for context)
    system_prompt = _build_system_prompt(session.summary)

    # 4. Run the agentic loop
    client = _get_client()
    final_model_turn, tool_results = await _run_agentic_loop(
        client=client,
        history=history,
        system_prompt=system_prompt,
        db=db,
        user=user,
    )

    # 5. Extract text + optional UI action
    raw_text = _extract_text(final_model_turn) or "I'm sorry, I couldn't generate a response."
    clean_text, ui_action = _parse_ui_action(raw_text)

    # Auto-hydrate ui_action payload with fresh tool execution data if missing
    if ui_action:
        if ui_action.type == UIActionType.SHOW_CANDIDATE_LIST and "candidates" not in ui_action.payload:
            if "get_candidates" in tool_results and isinstance(tool_results["get_candidates"], list):
                ui_action.payload["candidates"] = tool_results["get_candidates"]
        elif ui_action.type in (UIActionType.SHOW_CAMPAIGN_LIST, UIActionType.SHOW_CAMPAIGN_PICKER) and "campaigns" not in ui_action.payload:
            if "list_campaigns" in tool_results and isinstance(tool_results["list_campaigns"], list):
                ui_action.payload["campaigns"] = tool_results["list_campaigns"]
        elif ui_action.type == UIActionType.SHOW_CAMPAIGN_DETAIL and "campaign" not in ui_action.payload:
            for tool_key in ("get_campaign", "create_campaign"):
                if tool_key in tool_results and isinstance(tool_results[tool_key], dict):
                    ui_action.payload["campaign"] = tool_results[tool_key]
                    break
        elif ui_action.type in (UIActionType.SHOW_BATCH_STATUS, UIActionType.SHOW_SCREENING_STATUS) and "batch_details" not in ui_action.payload:
            for tool_key in ("get_batch_status", "screen_candidates"):
                if tool_key in tool_results and isinstance(tool_results[tool_key], dict):
                    ui_action.payload["batch_details"] = tool_results[tool_key]
                    break

    # 6. Persist: user turn + all new model/tool turns (everything appended since load)
    new_turns = history[initial_len:]
    await _persist_turns(db, session_id, new_turns)

    # 7. Maybe summarise in the background (non-blocking — fire and forget)
    asyncio.create_task(_maybe_summarise(db, session))  # type: ignore[arg-type]

    return BotResponse(text=clean_text, ui_action=ui_action)
