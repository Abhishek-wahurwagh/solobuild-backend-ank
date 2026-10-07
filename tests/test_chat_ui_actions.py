from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from google.genai import types as genai_types
from google.genai.errors import ServerError
from fastapi import HTTPException

from app.domains.chat.schemas import UIAction, UIActionType
from app.domains.chat.models import MessageRole
from app.domains.chat.router import send_message
from app.domains.chat.service import (
    SUMMARISE_AFTER_TURNS,
    _maybe_summarise,
    _parse_ui_action,
    _persist_turns,
    _validate_ui_action,
)
from app.domains.chat.tools import TOOL_DEFINITIONS


def test_campaign_list_action_requires_successful_campaign_list():
    action_data = {"type": "SHOW_CAMPAIGN_LIST", "payload": {}}

    action, error = _validate_ui_action(action_data, {"list_campaigns": []})
    assert action is not None
    assert action.type is UIActionType.SHOW_CAMPAIGN_LIST
    assert error is None

    action, error = _validate_ui_action(action_data, {})
    assert action is None
    assert error


def test_presentation_tool_declares_every_frontend_action():
    declaration = next(tool for tool in TOOL_DEFINITIONS if tool["name"] == "present_ui")
    declared_types = set(declaration["parameters"]["properties"]["type"]["enum"])
    assert declared_types == {action.value for action in UIActionType}
    assert "additionalProperties" not in declaration["parameters"]["properties"]["payload"]


def test_tool_declarations_convert_to_gemini_tool():
    tool = genai_types.Tool(function_declarations=TOOL_DEFINITIONS)
    assert tool.function_declarations


def test_candidate_list_action_requires_matching_campaign_result():
    action_data = {
        "type": "SHOW_CANDIDATE_LIST",
        "payload": {"campaign_id": "campaign-1"},
    }

    action, error = _validate_ui_action(
        action_data,
        {"get_candidates": {"campaign_id": "campaign-1", "candidates": []}},
    )
    assert action is not None
    assert error is None

    action, error = _validate_ui_action(
        action_data,
        {"get_candidates": {"campaign_id": "campaign-2", "candidates": []}},
    )
    assert action is None
    assert error


def test_candidate_screening_report_accepts_empty_screening_history_only_for_match():
    action_data = {
        "type": "SHOW_CANDIDATE_SCREENING_RESULT",
        "payload": {"campaign_id": "campaign-1", "candidate_id": "candidate-1"},
    }
    tool_results = {
        "get_candidate_document_screenings": {
            "campaign_id": "campaign-1",
            "candidate": {"id": "candidate-1"},
            "screenings": [],
        }
    }

    action, error = _validate_ui_action(action_data, tool_results)
    assert action is not None
    assert error is None

    tool_results["get_candidate_document_screenings"]["candidate"]["id"] = "candidate-2"
    action, error = _validate_ui_action(action_data, tool_results)
    assert action is None
    assert error


def test_legacy_action_parser_removes_valid_marker_and_ignores_malformed_marker():
    text, action = _parse_ui_action(
        'Campaigns loaded. ACTION: {"type":"SHOW_CAMPAIGN_LIST","payload":{}}'
    )
    assert text == "Campaigns loaded."
    assert action is not None
    assert action.type is UIActionType.SHOW_CAMPAIGN_LIST

    malformed_text, malformed_action = _parse_ui_action("Visible text ACTION: []")
    assert malformed_text == "Visible text ACTION: []"
    assert malformed_action is None


@pytest.mark.asyncio
async def test_persist_turns_stores_ui_action_as_structured_metadata():
    db = MagicMock()
    db.get = AsyncMock(return_value=None)
    db.flush = AsyncMock()
    db.get.return_value = None
    action = UIAction(type=UIActionType.SHOW_CAMPAIGN_LIST)
    turn = genai_types.Content(
        role="model",
        parts=[genai_types.Part(text="Here are your campaigns.")],
    )

    await _persist_turns(db, uuid4(), [turn], ui_action=action)

    message = db.add.call_args.args[0]
    assert message.content["parts"] == [{"text": "Here are your campaigns."}]
    assert message.content["ui_action"] == action.model_dump(mode="json")
    db.flush.assert_awaited_once()


@pytest.mark.asyncio
async def test_background_summarisation_bounds_cumulative_summary_and_releases_db_before_gemini():
    session_id = uuid4()
    chat_session = SimpleNamespace(
        id=session_id,
        turn_count=SUMMARISE_AFTER_TURNS,
        summary="Earlier context",
    )
    old_message = SimpleNamespace(
        id=uuid4(),
        role=MessageRole.USER,
        content={"parts": [{"text": "Earlier message"}]},
    )

    def result_for(rows):
        result = MagicMock()
        result.scalars.return_value.all.return_value = rows
        return result

    events = []
    db = MagicMock()
    db.__aenter__ = AsyncMock(side_effect=lambda: db)
    db.__aexit__ = AsyncMock(side_effect=lambda *_: events.append("db_closed"))
    db.get = AsyncMock(return_value=chat_session)
    db.execute = AsyncMock(side_effect=[
        result_for([old_message]),
        result_for([old_message.id]),
        result_for([]),
    ])
    db.commit = AsyncMock()

    client = MagicMock()
    async def generate_summary(**kwargs):
        assert events == ["db_closed"]
        assert "Earlier context" in kwargs["contents"]
        return SimpleNamespace(text=" ".join(["word"] * 205))

    client.aio.models.generate_content = AsyncMock(
        side_effect=generate_summary
    )

    with (
        patch("app.domains.chat.service.AsyncSessionLocal", return_value=db) as session_factory,
        patch("app.domains.chat.service._get_client", return_value=client),
    ):
        await _maybe_summarise(session_id)

    assert session_factory.call_count == 2
    db.commit.assert_awaited_once()
    assert db.execute.await_count == 3
    update_summary = db.execute.call_args_list[2].args[0].compile().params["summary"]
    assert len(update_summary.split()) == 200
    assert chat_session.turn_count == SUMMARISE_AFTER_TURNS


@pytest.mark.asyncio
async def test_background_summarisation_timeout_is_logged_without_traceback(caplog):
    session_id = uuid4()
    message = SimpleNamespace(
        id=uuid4(),
        role=MessageRole.USER,
        content={"parts": [{"text": "Earlier message"}]},
    )
    query_result = MagicMock()
    query_result.scalars.return_value.all.return_value = [message]

    db = MagicMock()
    db.__aenter__ = AsyncMock(return_value=db)
    db.__aexit__ = AsyncMock(return_value=None)
    db.get = AsyncMock(return_value=SimpleNamespace(
        turn_count=SUMMARISE_AFTER_TURNS,
        summary=None,
    ))
    db.execute = AsyncMock(return_value=query_result)

    client = MagicMock()
    client.aio.models.generate_content = AsyncMock(side_effect=TimeoutError)

    with (
        patch("app.domains.chat.service.AsyncSessionLocal", return_value=db),
        patch("app.domains.chat.service._get_client", return_value=client),
        caplog.at_level("WARNING"),
    ):
        await _maybe_summarise(session_id)

    assert "timed out" in caplog.text
    assert "Traceback" not in caplog.text
    assert db.execute.await_count == 1


@pytest.mark.asyncio
async def test_send_message_maps_gemini_server_error_to_service_unavailable(monkeypatch):
    async def fail_with_provider_error(**_kwargs):
        raise ServerError(
            503,
            {"error": {"code": 503, "message": "The service is currently unavailable."}},
        )

    monkeypatch.setattr(
        "app.domains.chat.service.process_message",
        fail_with_provider_error,
    )

    with pytest.raises(HTTPException) as error:
        await send_message(
            session_id=uuid4(),
            body=MagicMock(message="hello"),
            db=MagicMock(),
            current_user=MagicMock(),
        )

    assert error.value.status_code == 503
    assert error.value.detail == "The AI service is temporarily unavailable. Please try again shortly."
