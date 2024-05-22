from fastapi import APIRouter, Query, Body, Request
from grosbeak.db import client, api_db
from loguru import logger
from grosbeak.routers.picklist.live import get_picklist
from pydantic import BaseModel
from grosbeak.env import env
from fastapi.responses import JSONResponse
from grosbeak.routers.api import ErrorMessage
from pymongo.results import DeleteResult
from typing import Annotated, Any

router = APIRouter(prefix="/rest")


@router.get("/list")
def get_list(event_key: Annotated[str, Query()] = env.DB_NAME):
    """
    This endpoint gets the picklist for the specified event key and returns it in a list of
    objects.
    """
    ranking, dnp = get_picklist(event_key)
    return {"ranking": ranking, "dnp": dnp}


class PicklistData(BaseModel):
    """
    This model represents the data sent to the set_picklist endpoint.
    """

    ranking: list[str]
    dnp: list[str]


class UpdateListResponse(BaseModel):
    """
    This model represents the response from the set_picklist endpoint.
    """

    deleted: int


def set_picklist(event_key: str, ranking: list[str], dnp: list[str]) -> DeleteResult:
    """
    Updates the picklist for the specified event key with the provided teams (order matters)
    Returns the result of the pymongo query that deletes stale teams
    """
    picklist = client[event_key]["picklist"]
    for i, team in enumerate(ranking):
        picklist.update_one(
            {"team_number": team}, {"$set": {"rank": i + 1, "dnp": False}}, upsert=True
        )
    for team in dnp:
        picklist.update_one(
            {"team_number": team}, {"$set": {"rank": -1, "dnp": True}}, upsert=True
        )
    # Remove any stale teams
    delete_resp = picklist.delete_many({"team_number": {"$nin": ranking + dnp}})
    return delete_resp


@router.put(
    "/list",
    responses={401: {"model": ErrorMessage}, 200: {"model": UpdateListResponse}},
)
async def update_list(
    data: Annotated[PicklistData, Body()],
    password: Annotated[str | None, Query()] = None,
    event_key: Annotated[str, Query()] = env.DB_NAME,
):
    """
    Update picklist using password
    """
    if password != env.PICKLIST_PASSWORD:
        logger.warning("Incorrect password for picklist update")
        return JSONResponse(status_code=401, content={"error": "Incorrect password"})
    logger.debug(
        f"Received picklist update request with {len(data.ranking)} ranks and {len(data.dnp)} dnps"
    )
    delete_resp = set_picklist(event_key, data.ranking, data.dnp)
    if delete_resp.deleted_count > 0:
        logger.info(f"Deleted {delete_resp.deleted_count} teams from picklist")
    return {
        "deleted": delete_resp.deleted_count,
    }


@router.put("/sheet")
def update_from_sheet(request: Request, data: Annotated[PicklistData, Body()]):
    """
    Update picklist using sheet id in Authorization header
    """
    sheet_id = (
        request.headers["Authorization"] if "Authorization" in request.headers else None
    )
    if sheet_id is None:
        return {"error": "Somehow there is no sheet id in the headers"}
    sheet_doc: dict[str, Any] | None = api_db["sheets"].find_one({"sheet_id": sheet_id})
    if sheet_doc is None:
        return {"error": "Somehow the sheet does not exist in the db"}
    event_key = sheet_doc.get("event_key")
    if event_key is None:
        return {"error": "No event ket"}
    delete_resp = set_picklist(event_key, data.ranking, data.dnp)
    return {"error": None, "deleted": delete_resp.deleted_count}
