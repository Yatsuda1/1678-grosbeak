from typing import Union
import uuid
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, validator, Field

from ..auth import get_auth_level
from ..db import STATIC_FILE_TYPES, api_db, client

router = APIRouter(prefix="/admin")


class NewCredentialRequest(BaseModel):
    """
    This model represents a request to create a new credential.
    """

    description: str
    level: int


class NewCredentialResponse(BaseModel):
    """
    This model represents a response to a request to create a new credential.
    """

    description: str
    level: int
    api_key: str


@router.post("/new-credential", response_model=NewCredentialResponse)
async def create_credential(
    *, data: NewCredentialRequest, user_level: int = Depends(get_auth_level)
):
    """
    This endpoint creates a new credential with the given description and level.
    """
    if user_level >= 2:
        creds = NewCredentialResponse(
            description=data.description,
            level=data.level,
            api_key=str(uuid.uuid4()).replace("-", ""),
        )
        api_db["credentials"].insert_one(creds.dict())
        return creds


class NewStaticRequest(BaseModel):
    """
    This model represents a request to create/update a static file.
    """

    type: str
    event_key: str = Field(..., min_length=5)
    data: Union[dict, list]

    @validator("type")
    def is_valid_type(cls, v):
        assert v in STATIC_FILE_TYPES, "Invalid type"
        return v


@router.put("/static")
def new_match_schedule(
    data: NewStaticRequest, user_level: int = Depends(get_auth_level)
):
    """
    This endpoint creates/updates a new static file with the given type and event key.
    """
    if user_level < 2:
        return JSONResponse(
            content={"error": f"Unauthorized level {user_level} user"},
            status_code=403,
        )
    collection = client["static"][data.type]
    collection.find_one_and_update(
        {"event_key": data.event_key}, {"$set": {"data": data.data}}, upsert=True
    )
    return {"error": None}


class SheetData(BaseModel):
    """
    This model represents a request to update the assigned Google Sheet ID for an event key.
    """

    event_key: str
    sheet_id: str


@router.post("/sheet-id")
def update_sheet_id(data: SheetData, user_level: int = Depends(get_auth_level)):
    """
    Updates the assigned Google Sheet ID for an event key
    This allows using a sheet id as authentication with automatic infer of event_key
    """
    if user_level < 2:
        return JSONResponse(
            content={"error": f"Unauthorized level {user_level} user"},
            status_code=403,
        )
    api_db["sheets"].update_one(
        {"event_key": data.event_key},
        {"$set": {"sheet_id": data.sheet_id}},
        upsert=True,
    )
    return {"error": None}
