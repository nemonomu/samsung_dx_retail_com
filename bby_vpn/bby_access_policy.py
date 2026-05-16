"""Access-control helpers for responsible crawling.

This module does not bypass access controls. It only detects likely block
signals and gives crawlers a consistent reason to stop and checkpoint.
"""

BLOCK_MARKERS = (
    "err_http2_protocol_error",
    "this site can't be reached",
    "access denied",
    "forbidden",
    "captcha",
    "verify you are human",
    "unusual traffic",
    "bot detection",
    "akamai",
)


def detect_block_signal(title="", html=""):
    """Return a short reason if the loaded page appears blocked."""
    title_text = (title or "").lower()
    html_text = (html or "").lower()
    sample = f"{title_text}\n{html_text[:5000]}"

    for marker in BLOCK_MARKERS:
        if marker in sample:
            return marker

    return None
