from fastapi import APIRouter, Security, WebSocket
from grosbeak.auth import get_auth_level, protect_websocket
from grosbeak.env import env
from grosbeak.routers.picklist.live import websocket_picklist as ws_picklist
import grosbeak.routers.picklist.rest as rest

router = APIRouter(prefix="/picklist", dependencies=[Security(get_auth_level)])


@router.websocket("/live")
async def websocket_picklist(
    websocket: WebSocket, event_key: str = env.DB_NAME, auth=Security(protect_websocket)
):
    if auth is not None:
        return await ws_picklist(websocket, event_key)


router.include_router(rest.router)
