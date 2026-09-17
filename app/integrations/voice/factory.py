import os
from app.integrations.voice.providers.base import BaseVoiceProvider
from app.integrations.voice.providers.mock import MockVoiceProvider
from app.integrations.voice.providers.vobiz import VobizVoiceProvider
from app.core.config import settings

class VoiceFactory:
    @staticmethod
    def get_provider() -> BaseVoiceProvider:
        provider_type = getattr(settings, "VOICE_PROVIDER", "mock").lower()
        if provider_type == "vobiz":
            return VobizVoiceProvider()
        return MockVoiceProvider()

VoiceProviderFactory = VoiceFactory

