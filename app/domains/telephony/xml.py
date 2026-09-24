from __future__ import annotations

from xml.etree.ElementTree import Element, SubElement, tostring


def build_vobiz_answer_xml(*, media_url: str, recording_url: str) -> str:
    """Build the synchronous Vobiz instructions returned after answer."""
    response = Element("Response")
    SubElement(
        response,
        "Record",
        {
            "fileFormat": "wav",
            "recordSession": "true",
            "callbackUrl": recording_url,
            "callbackMethod": "POST",
        },
    )
    stream = SubElement(
        response,
        "Stream",
        {
            "bidirectional": "true",
            "audioTrack": "inbound",
            "contentType": "audio/x-mulaw;rate=8000",
            "keepCallAlive": "true",
        },
    )
    stream.text = media_url
    return tostring(response, encoding="unicode")