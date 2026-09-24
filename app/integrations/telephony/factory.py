from app.core.config import settings
from app.integrations.telephony.base import BaseTelephonyCarrier
from app.integrations.telephony.vobiz import VobizTelephonyCarrier


class TelephonyCarrierFactory:
    @staticmethod
    def build() -> BaseTelephonyCarrier:
        carrier = settings.TELEPHONY_CARRIER.lower()
        if carrier == "vobiz":
            return VobizTelephonyCarrier()
        raise ValueError(f"Unsupported telephony carrier: {carrier}")
