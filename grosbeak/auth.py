from fastapi import HTTPException, Header, Security, WebSocket, status
from fastapi.security import APIKeyHeader
from typing import Any, cast
from .db import api_db
from loguru import logger

api_key_header_auth = APIKeyHeader(name="Authorization", auto_error=True)

creds_collection = api_db["credentials"]


def check_auth(api_key: str) -> int | None:
    """
    Returns the auth level for the specified api key
    """
    # Check for standard auth key
    creds_item = cast(dict[str, Any], creds_collection.find_one({"api_key": api_key}))
    if creds_item is not None:
        return creds_item["level"]
    # Check for registered sheet id
    sheet_item = api_db["sheets"].find_one({"sheet_id": api_key})
    if sheet_item is not None:
        # Valid sheet id is worth an auth level of 1
        return 1
    logger.warning(f"Invalid API key {api_key}")
    return None


async def get_auth_level(api_key_header: str = Security(api_key_header_auth)):
    """
    Verifies the api key and returns the auth level
    """
    auth = check_auth(api_key_header)
    if auth is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key",
        )
    return auth


async def protect_websocket(websocket: WebSocket, authorization: str = Header(None)):
    auth = check_auth(authorization)
    if auth is None:
        # logger.info(f"Websocket connection denied with auth {auth}")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
    else:
        return auth
