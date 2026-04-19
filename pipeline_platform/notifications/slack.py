from __future__ import annotations

import os

import requests


def send_failure_notification(
    webhook_url: str | None,
    pipeline_name: str,
    error_message: str,
    run_id: str,
) -> None:
    """
    POST a failure alert to a Slack webhook.

    - Uses webhook_url if provided, otherwise falls back to the
      SLACK_WEBHOOK_URL environment variable.
    - If neither is set, does nothing silently.
    - Notification failure never suppresses the original pipeline error.
    """
    url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        return

    payload = {
        "text": (
            f"*Pipeline Failed* ✗\n"
            f"*Pipeline:* `{pipeline_name}`\n"
            f"*Run ID:* `{run_id}`\n"
            f"*Error:* {error_message[:500]}"
        )
    }

    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        # Notification failure must NOT suppress the original pipeline error
        print(f"[Notify] Failed to send Slack notification: {e}")
