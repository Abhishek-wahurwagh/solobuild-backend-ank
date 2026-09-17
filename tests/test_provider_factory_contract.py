import pytest
from app.core.config import settings
from app.integrations.voice.factory import VoiceProviderFactory
from app.integrations.voice.providers.vobiz import VobizVoiceProvider
from app.integrations.voice.providers.mock import MockVoiceProvider


def test_voice_provider_factory_mock(monkeypatch):
    monkeypatch.setattr(settings, "VOICE_PROVIDER", "mock")
    provider = VoiceProviderFactory.get_provider()
    assert isinstance(provider, MockVoiceProvider)


def test_voice_provider_factory_vobiz(monkeypatch):
    monkeypatch.setattr(settings, "VOICE_PROVIDER", "vobiz")
    provider = VoiceProviderFactory.get_provider()
    assert isinstance(provider, VobizVoiceProvider)

