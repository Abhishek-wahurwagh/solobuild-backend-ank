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
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import AsyncSessionLocal
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
You are the receptionist for the recruitment workspace called SoloBuild.
Your role is to understand the recruiter's intent, perform supported backend actions,
and direct the frontend to the existing campaign or candidate interface for the task.

Rules:
- Treat tool results as the source of truth. Tool errors mean the action failed; say so plainly and do not describe it as successful.
- Never infer a score, screening completion, candidate identity, campaign assignment, or result that is absent from tool output.
- Never expose internal IDs, batch IDs, or UUIDs in user-visible text. The frontend action protocol handles IDs separately.
- Resolve campaign names with list_campaigns, then get_campaign for the chosen campaign. If more than one campaign plausibly matches, ask the user to choose with present_ui(SHOW_CAMPAIGN_PICKER); do not choose arbitrarily.
- Resolve candidate names with find_candidates in the selected campaign. If there is no match, say so. If multiple candidates match, present the matches via the relevant candidate UI or ask a concise clarifying question; never act on one arbitrarily.
- Campaign creation and file upload require user interaction. Do not claim creation/upload happened before the user completes the UI form.
- Screening is a mutating operation you may perform. For a named candidate, resolve exactly one candidate first and pass that candidate ID to screen_candidates. For an explicit whole-campaign request, omit candidate_ids. Do not silently screen an entire campaign when the user named one candidate.
- screen_candidates only queues work. After it succeeds, say screening was queued/started (not completed) and show its batch progress using the returned batch_id. Only describe screening results after get_document_screenings or get_candidate_document_screenings returns persisted rows. An empty result means "not screened yet"/"no result available", not incompatible.
- Use list_agents when asked to assign/change the AI recruiter. Resolve exactly one campaign and one available agent, then call update_campaign_agent. Report success only after that tool succeeds.
- Use tools to obtain facts for answers and use present_ui to request an interface. present_ui is not a business action: call it only after the relevant tool succeeds. Its action must use the exact IDs returned by those tools. For a greeting or general question, do not open a panel.
- Do not print ACTION blocks or JSON in normal assistant text. Call present_ui as a function tool; the server returns its validated action separately.
- Use compact, human-friendly wording. Avoid repeating boilerplate or narrating every internal tool call. Ask one short clarification when required to proceed safely.

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
    marker_index = text.rfind("ACTION:")
    if marker_index < 0:
        return text.strip(), None
    try:
        data = json.loads(text[marker_index + len("ACTION:"):].strip())
        if not isinstance(data, dict):
            return text.strip(), None
        action = UIAction.model_validate(data)
    except (json.JSONDecodeError, TypeError, ValueError):
        return text.strip(), None
    return text[:marker_index].rstrip(), action


def _validate_ui_action(
    action_data: dict[str, Any],
    tool_results: dict[str, Any],
) -> tuple[UIAction | None, str | None]:
    """Validate display instructions against successful tool results for this turn."""
    try:
        action = UIAction.model_validate(action_data)
    except (TypeError, ValueError):
        return None, "The UI action type or payload was invalid."

    payload = action.payload
    campaign_id = payload.get("campaign_id")
    candidate_id = payload.get("candidate_id")
    batch_id = payload.get("batch_id")

    def result(name: str) -> Any:
        value = tool_results.get(name)
        if isinstance(value, dict) and value.get("error"):
            return None
        return value

    def campaign_matches(*names: str) -> bool:
        return any(
            isinstance(result(name), dict)
            and (
                result(name).get("campaign_id") == campaign_id
                or name == "get_campaign" and result(name).get("id") == campaign_id
            )
            for name in names
        )

    campaign_detail = result("get_campaign")
    candidate_screenings = result("get_candidate_document_screenings")
    requirements: dict[UIActionType, tuple[bool, str]] = {
        UIActionType.SHOW_CAMPAIGN_LIST: (
            isinstance(result("list_campaigns"), list),
            "Load campaigns before showing the campaign list.",
        ),
        UIActionType.SHOW_CAMPAIGN_PICKER: (
            isinstance(result("list_campaigns"), list),
            "Load campaigns before asking the user to choose one.",
        ),
        UIActionType.SHOW_CAMPAIGN_CREATE_FORM: (True, ""),
        UIActionType.SHOW_CAMPAIGN_DETAIL: (
            bool(campaign_id) and (
                isinstance(campaign_detail, dict) and campaign_detail.get("id") == campaign_id
                or campaign_matches("get_candidates", "get_document_screenings", "update_campaign_agent")
            ),
            "Resolve the requested campaign with a successful campaign tool before opening it.",
        ),
        UIActionType.SHOW_CANDIDATE_UPLOAD: (
            bool(campaign_id) and campaign_matches("get_campaign", "get_candidates"),
            "Resolve the requested campaign before opening its upload interface.",
        ),
        UIActionType.SHOW_CANDIDATE_LIST: (
            bool(campaign_id) and campaign_matches("get_candidates"),
            "Load candidates for the requested campaign before showing the candidate list.",
        ),
        UIActionType.SHOW_SCREENING_RESULTS: (
            bool(campaign_id) and campaign_matches("get_document_screenings"),
            "Load screening results for the requested campaign before showing them.",
        ),
        UIActionType.SHOW_CANDIDATE_SCREENING_RESULT: (
            bool(campaign_id and candidate_id)
            and isinstance(candidate_screenings, dict)
            and candidate_screenings.get("campaign_id") == campaign_id
            and isinstance(candidate_screenings.get("candidate"), dict)
            and candidate_screenings["candidate"].get("id") == candidate_id,
            "Resolve this candidate's screening record before opening the report.",
        ),
        UIActionType.SHOW_SCREENING_STATUS: (
            bool(campaign_id and batch_id) and any(
                isinstance(result(name), dict)
                and result(name).get("campaign_id") == campaign_id
                and result(name).get("batch_id") == batch_id
                for name in ("screen_candidates", "get_batch_status")
            ),
            "Start or resolve the screening batch before showing its status.",
        ),
        UIActionType.SHOW_BATCH_STATUS: (
            bool(campaign_id and batch_id) and any(
                isinstance(result(name), dict)
                and result(name).get("campaign_id") == campaign_id
                and result(name).get("batch_id") == batch_id
                for name in ("screen_candidates", "get_batch_status")
            ),
            "Start or resolve the batch before showing its status.",
        ),
    }
    valid, reason = requirements[action.type]
    if not valid:
        return None, reason

    required_fields: dict[UIActionType, tuple[str, ...]] = {
        UIActionType.SHOW_CAMPAIGN_LIST: (),
        UIActionType.SHOW_CAMPAIGN_PICKER: (),
        UIActionType.SHOW_CAMPAIGN_CREATE_FORM: (),
        UIActionType.SHOW_CAMPAIGN_DETAIL: ("campaign_id",),
        UIActionType.SHOW_CANDIDATE_UPLOAD: ("campaign_id",),
        UIActionType.SHOW_CANDIDATE_LIST: ("campaign_id",),
        UIActionType.SHOW_SCREENING_RESULTS: ("campaign_id",),
        UIActionType.SHOW_CANDIDATE_SCREENING_RESULT: ("campaign_id", "candidate_id"),
        UIActionType.SHOW_SCREENING_STATUS: ("campaign_id", "batch_id"),
        UIActionType.SHOW_BATCH_STATUS: ("campaign_id", "batch_id"),
    }
    for key in required_fields[action.type]:
        if not isinstance(payload.get(key), str) or not payload[key]:
            return None, f"The UI action is missing required field {key}."

    allowed_payload_fields: dict[UIActionType, tuple[str, ...]] = {
        UIActionType.SHOW_CAMPAIGN_LIST: (),
        UIActionType.SHOW_CAMPAIGN_PICKER: ("reason",),
        UIActionType.SHOW_CAMPAIGN_CREATE_FORM: ("initial_title", "initial_text"),
        UIActionType.SHOW_CAMPAIGN_DETAIL: ("campaign_id",),
        UIActionType.SHOW_CANDIDATE_UPLOAD: ("campaign_id", "campaign_title"),
        UIActionType.SHOW_CANDIDATE_LIST: ("campaign_id",),
        UIActionType.SHOW_SCREENING_RESULTS: ("campaign_id",),
        UIActionType.SHOW_CANDIDATE_SCREENING_RESULT: ("campaign_id", "candidate_id"),
        UIActionType.SHOW_SCREENING_STATUS: ("campaign_id", "batch_id"),
        UIActionType.SHOW_BATCH_STATUS: ("campaign_id", "batch_id", "batch_type"),
    }
    safe_payload = {
        key: value
        for key, value in payload.items()
        if key in allowed_payload_fields[action.type]
    }
    return UIAction(type=action.type, payload=safe_payload), None


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
    ui_action: UIAction | None = None,
) -> None:
    """
    Persist new Content turns to chat_messages in a single flush.
    Increments ChatSession.turn_count atomically.
    """
    role_map = {"user": MessageRole.USER, "model": MessageRole.MODEL, "tool": MessageRole.TOOL}
    last_text_turn_index = next(
        (
            index
            for index in range(len(turns) - 1, -1, -1)
            if turns[index].role == "model" and _extract_text(turns[index])
        ),
        None,
    )
    for index, turn in enumerate(turns):
        content = _content_to_dict(turn)
        if turn.role == "model":
            text = _extract_text(turn)
            if text and "ACTION:" in text:
                text, legacy_action = _parse_ui_action(text)
                if ui_action is None:
                    ui_action = legacy_action
                content = _content_to_dict(genai_types.Content(
                    role=turn.role,
                    parts=[genai_types.Part(text=text)],
                ))
            if ui_action is not None and index == last_text_turn_index and text:
                content["ui_action"] = ui_action.model_dump(mode="json")
        db.add(ChatMessage(
            session_id=session_id,
            role=role_map.get(turn.role, MessageRole.MODEL),
            content=content,
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
) -> tuple[genai_types.Content, dict[str, Any], UIAction | None]:
    """
    Core agentic loop. Sends history to Gemini, executes tool calls if any,
    and loops until a pure-text response is received or MAX_TOOL_HOPS is hit.

    Returns the final model turn, latest results by tool name, and a validated
    frontend action requested through the dedicated presentation tool.
    """
    tool_config = genai_types.Tool(
        function_declarations=TOOL_DEFINITIONS  # type: ignore[arg-type]
    )
    tool_results: dict[str, Any] = {}
    ui_action: UIAction | None = None
    last_model_turn: genai_types.Content | None = None

    for _ in range(MAX_TOOL_HOPS):
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
        last_model_turn = model_turn
        history.append(model_turn)

        # Collect any function calls in this turn
        tool_calls = [
            p for p in (model_turn.parts or []) if p.function_call is not None
        ]

        if not tool_calls:
            # No more tool calls — Gemini is done.
            return model_turn, tool_results, ui_action

        # Execute all tool calls (sequential; most responses are tiny)
        response_parts: list[genai_types.Part] = []
        requested_ui_actions: list[dict[str, Any]] = []
        for tc in tool_calls:
            fc = tc.function_call
            logger.debug("Chat tool call: %s(%s)", fc.name, fc.args)
            if fc.name == "present_ui":
                requested_ui_actions.append(dict(fc.args or {}))
                continue
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

        for action_data in requested_ui_actions:
            validated_action, error = _validate_ui_action(action_data, tool_results)
            if validated_action is not None:
                ui_action = validated_action
                response_parts.append(genai_types.Part(
                    function_response=genai_types.FunctionResponse(
                        name="present_ui",
                        response={"result": {"accepted": True}},
                    )
                ))
            else:
                response_parts.append(genai_types.Part(
                    function_response=genai_types.FunctionResponse(
                        name="present_ui",
                        response={"result": {"error": error}},
                    )
                ))

        tool_turn = genai_types.Content(role="user", parts=response_parts)
        history.append(tool_turn)

    logger.warning("Chat agentic loop hit MAX_TOOL_HOPS (%d).", MAX_TOOL_HOPS)
    if last_model_turn is None:
        raise RuntimeError("Chat agentic loop ended without a model response.")
    fallback_turn = genai_types.Content(
        role="model",
        parts=[genai_types.Part(
            text="I couldn't finish preparing the response. Please try that request again."
        )],
    )
    history.append(fallback_turn)
    return fallback_turn, tool_results, ui_action


# ---------------------------------------------------------------------------
# Summarisation
# ---------------------------------------------------------------------------

async def _maybe_summarise(session_id: UUID) -> None:
    """
    If the session has accumulated enough turns, compress old messages into
    `ChatSession.summary` and mark those rows as summarised.

    Database sessions are kept out of the Gemini request so a slow provider
    response cannot hold a database connection and transaction open.
    """
    try:
        async with AsyncSessionLocal() as db:
            session = await db.get(ChatSession, session_id)
            if session is None or (session.turn_count or 0) < SUMMARISE_AFTER_TURNS:
                return

            evict_count = session.turn_count - HISTORY_WINDOW_SIZE
            if evict_count <= 0:
                return

            result = await db.execute(
                select(ChatMessage)
                .where(
                    ChatMessage.session_id == session_id,
                    ChatMessage.is_summarised.is_(False),
                )
                .order_by(ChatMessage.created_at.asc())
                .limit(evict_count)
            )
            old_messages = result.scalars().all()
            if not old_messages:
                return

            old_message_ids = [message.id for message in old_messages]
            previous_summary = session.summary or ""
            lines: list[str] = []
            for msg in old_messages:
                role = msg.role.value
                text = msg.content.get("parts", [{}])[0].get("text", "[tool interaction]")
                lines.append(f"{role}: {text[:300]}")
            transcript = "\n".join(lines)

        prompt = (
            "Create an updated, cumulative conversation summary for future context. "
            "Combine the previous summary with the newly archived messages, preserving "
            "important entities, user preferences, decisions, and actions taken. "
            "Keep the complete updated summary to at most 200 words.\n\n"
            f"Previous summary:\n{previous_summary or '(none)'}\n\n"
            f"Newly archived messages:\n{transcript}"
        )

        client = _get_client()
        summary_response = await asyncio.wait_for(
            client.aio.models.generate_content(
                model=SUMMARY_MODEL,
                contents=prompt,
                config=genai_types.GenerateContentConfig(temperature=0.0),
            ),
            timeout=30,
        )
        new_summary_text = (summary_response.text or "").strip()
        if not new_summary_text:
            logger.warning("Chat summarisation returned empty text for session %s", session_id)
            return
        summary_words = new_summary_text.split()
        if len(summary_words) > 200:
            logger.warning(
                "Chat summary exceeded 200 words for session %s; truncating.",
                session_id,
            )
            new_summary_text = " ".join(summary_words[:200])

        async with AsyncSessionLocal() as db:
            # Mark only rows still eligible for summarisation, so overlapping
            # background tasks cannot decrement the live turn count twice.
            marked = await db.execute(
                update(ChatMessage)
                .where(
                    ChatMessage.id.in_(old_message_ids),
                    ChatMessage.is_summarised.is_(False),
                )
                .values(is_summarised=True)
                .returning(ChatMessage.id)
            )
            marked_count = len(marked.scalars().all())
            if not marked_count:
                return

            await db.execute(
                update(ChatSession)
                .where(ChatSession.id == session_id)
                .values(
                    summary=new_summary_text,
                    turn_count=func.greatest(ChatSession.turn_count - marked_count, 0),
                )
                .execution_options(synchronize_session=False)
            )
            await db.commit()
    except TimeoutError:
        logger.warning(
            "Chat summarisation timed out for session %s; archived messages remain "
            "eligible for a later retry.",
            session_id,
        )
    except Exception:
        logger.exception("Chat summarisation failed for session %s", session_id)


def _log_background_task_exception(task: asyncio.Task[Any]) -> None:
    if task.cancelled():
        return
    error = task.exception()
    if error is not None:
        logger.error(
            "Unexpected chat background task failure",
            exc_info=(type(error), error, error.__traceback__),
        )


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
    final_model_turn, tool_results, ui_action = await _run_agentic_loop(
        client=client,
        history=history,
        system_prompt=system_prompt,
        db=db,
        user=user,
    )

    # 5. Extract text + optional UI action
    raw_text = _extract_text(final_model_turn) or "I'm sorry, I couldn't generate a response."
    if _extract_text(final_model_turn) is None:
        final_model_turn = genai_types.Content(
            role="model",
            parts=[genai_types.Part(text=raw_text)],
        )
        history.append(final_model_turn)
    clean_text, legacy_action = _parse_ui_action(raw_text)
    clean_text = clean_text or "I couldn't prepare a response for that request."
    if ui_action is None and legacy_action is not None:
        ui_action, _ = _validate_ui_action(
            legacy_action.model_dump(mode="python"),
            tool_results,
        )

    # 6. Persist: user turn + all new model/tool turns (everything appended since load)
    new_turns = history[initial_len:]
    await _persist_turns(db, session_id, new_turns, ui_action=ui_action)

    # Ensure the background task can observe these turns using its own session.
    await db.commit()

    # 7. Maybe summarise in the background (non-blocking — fire and forget).
    if (session.turn_count or 0) >= SUMMARISE_AFTER_TURNS:
        task = asyncio.create_task(_maybe_summarise(session_id))
        task.add_done_callback(_log_background_task_exception)

    return BotResponse(text=clean_text, ui_action=ui_action)
