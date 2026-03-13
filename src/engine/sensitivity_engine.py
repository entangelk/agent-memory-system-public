from src.db import collections as col

SENSITIVITY_LEVELS = ("normal", "medium", "high")
DEFAULT_SENSITIVITY = "normal"
DEFAULT_POLICY = {
    "hide_sensitive_on_recall": False,
    "agent_instruction": (
        "Hide sensitive details in default responses, "
        "and only re-run with include_sensitive=true when the user's request is explicit."
    ),
}

_LEVEL_RANK = {level: idx for idx, level in enumerate(SENSITIVITY_LEVELS)}


def normalize_sensitivity(raw: str | None) -> str:
    if isinstance(raw, str):
        value = raw.strip().lower()
        if value in _LEVEL_RANK:
            return value
    return DEFAULT_SENSITIVITY


def normalize_policy(raw: dict | None) -> dict:
    policy = dict(DEFAULT_POLICY)
    if not isinstance(raw, dict):
        return policy

    hide_sensitive_on_recall = raw.get("hide_sensitive_on_recall")
    if isinstance(hide_sensitive_on_recall, bool):
        policy["hide_sensitive_on_recall"] = hide_sensitive_on_recall

    agent_instruction = raw.get("agent_instruction")
    if isinstance(agent_instruction, str) and agent_instruction.strip():
        policy["agent_instruction"] = agent_instruction.strip()

    return policy


async def load_policy() -> dict:
    profile = await col.profiles().find_one(
        {"user_id": "primary"},
        projection={"sensitivity_policy": 1},
    )
    if not profile:
        return normalize_policy(None)
    return normalize_policy(profile.get("sensitivity_policy"))


def should_hide_content(
    *,
    sensitivity: str | None,
    policy: dict | None,
    include_sensitive: bool = False,
) -> bool:
    if include_sensitive:
        return False
    if not isinstance(policy, dict):
        return False
    if not bool(policy.get("hide_sensitive_on_recall", False)):
        return False
    # Hide only explicitly high-sensitivity items by default to minimize false positives.
    return normalize_sensitivity(sensitivity) == "high"
