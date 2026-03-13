import json
from mcp.types import Tool, TextContent
from src.db import collections as col
from src.engine import sensitivity_engine


def get_tools() -> list[Tool]:
    return [
        Tool(
            name="memory_policy",
            description=(
                "Get or update memory policy settings. "
                "Currently supports default sensitivity exposure rules and agent instructions."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["get", "set"],
                        "description": "Retrieve policy settings (get) or update them (set)",
                    },
                    "hide_sensitive_on_recall": {
                        "type": "boolean",
                        "description": "Hide high-sensitivity details in default recall responses when true",
                    },
                    "agent_instruction": {
                        "type": "string",
                        "description": "Instruction text the agent should follow for sensitive memories",
                    },
                },
                "required": ["action"],
            },
        )
    ]


async def _ensure_primary_profile() -> dict:
    profile = await col.profiles().find_one({"user_id": "primary"})
    if profile:
        return profile

    doc = {
        "user_id": "primary",
        "summary": "",
        "preferences": {},
        "communication_style": "",
        "last_consolidated": None,
        "sensitivity_policy": dict(sensitivity_engine.DEFAULT_POLICY),
    }
    profile_id = await col.insert_one(col.profiles(), doc)
    doc["_id"] = profile_id
    return doc


def _select_policy_fields(args: dict) -> dict:
    updates: dict = {}
    if "hide_sensitive_on_recall" in args and isinstance(args.get("hide_sensitive_on_recall"), bool):
        updates["hide_sensitive_on_recall"] = args["hide_sensitive_on_recall"]
    if "agent_instruction" in args and isinstance(args.get("agent_instruction"), str):
        updates["agent_instruction"] = args["agent_instruction"].strip()
    return updates


async def handle(name: str, args: dict) -> list[TextContent] | None:
    if name != "memory_policy":
        return None

    action = args.get("action", "get")
    profile = await _ensure_primary_profile()
    current_policy = sensitivity_engine.normalize_policy(profile.get("sensitivity_policy"))

    if action == "get":
        return [TextContent(
            type="text",
            text=json.dumps({"status": "ok", "policy": current_policy}, ensure_ascii=False),
        )]

    if action != "set":
        return [TextContent(type="text", text=f"Unknown action: {action}")]

    policy_updates = _select_policy_fields(args)
    if not policy_updates:
        return [TextContent(type="text", text="No policy fields were provided to update.")]

    next_policy = dict(current_policy)
    next_policy.update(policy_updates)
    normalized_next_policy = sensitivity_engine.normalize_policy(next_policy)

    await col.update_one(
        col.profiles(),
        profile["_id"],
        {"$set": {"sensitivity_policy": normalized_next_policy}},
    )

    return [TextContent(
        type="text",
        text=json.dumps({"status": "updated", "policy": normalized_next_policy}, ensure_ascii=False),
    )]
