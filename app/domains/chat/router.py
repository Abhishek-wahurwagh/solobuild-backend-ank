"""
Chat Router
===========
Thin HTTP layer over the chat service. Follows the same conventions as other
domain routers in this project (Depends injection, explicit ownership guards,
standard HTTP status codes).
"""
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.domains.auth.dependencies import get_current_user
from app.domains.chat.schemas import (
    BotResponse,
    ChatSessionListItem,
    ChatSessionResponse,
    CreateSessionRequest,
    SendMessageRequest,
)
from app.domains.chat import service as chat_service
from app.domains.users.models import User

router = APIRouter(prefix="/chat")


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

@router.post(
    "/sessions",
    response_model=ChatSessionResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_session(
    body: CreateSessionRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Create a new chat session (conversation thread)."""
    session = await chat_service.create_session(db, current_user, title=body.title)
    return session


@router.get(
    "/sessions",
    response_model=list[ChatSessionListItem],
)
async def list_sessions(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List all chat sessions for the authenticated user."""
    return await chat_service.list_sessions(db, current_user)


@router.get(
    "/sessions/{session_id}",
    response_model=ChatSessionResponse,
)
async def get_session(
    session_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get a single session's metadata."""
    session = await chat_service.get_session(db, session_id, current_user)
    if session is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found.")
    return session


@router.delete(
    "/sessions/{session_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_session(
    session_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Delete a session and all its messages."""
    deleted = await chat_service.delete_session(db, session_id, current_user)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found.")


# ---------------------------------------------------------------------------
# Messaging
# ---------------------------------------------------------------------------

@router.post(
    "/sessions/{session_id}/message",
    response_model=BotResponse,
)
async def send_message(
    session_id: UUID,
    body: SendMessageRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Send a user message and receive the bot's response.

    The response always contains `text`. It may also contain a `ui_action`
    object that tells the frontend which component to render and with what data.
    """
    try:
        return await chat_service.process_message(
            db=db,
            session_id=session_id,
            user_message=body.message,
            user=current_user,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"AI service error: {exc}",
        )


@router.get(
    "/sessions/{session_id}/messages",
    response_model=list[dict],
)
async def get_messages(
    session_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Fetch the stored message history for a session.
    Returns simplified {role, text} objects suitable for rendering a chat UI.
    """
    messages = await chat_service.get_session_messages(db, session_id, current_user)
    if not messages and not await chat_service.get_session(db, session_id, current_user):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found.")

    result = []
    for msg in messages:
        parts = msg.content.get("parts", [])
        text = next((p["text"] for p in parts if "text" in p), None)
        # Only surface user and model text turns to the client
        if msg.role.value in ("user", "model") and text:
            result.append({"role": msg.role.value, "text": text})
    return result
