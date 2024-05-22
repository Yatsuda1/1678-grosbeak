from enum import Enum
from functools import reduce
from typing import Annotated, Any, TypedDict, cast
from fastapi import APIRouter, Query, Security
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from ..auth import get_auth_level
from ..db import (
    COLLECTION_KEYS,
    STATIC_FILE_TYPES,
    AllianceColors,
    client,
    COLLECTIONS,
    DocumentTypes,
)
from ..env import env
from ..util import serialize_documents
from grosbeak.routers.notes import router as notes_router
import math
import numbers

router = APIRouter(prefix="/api", dependencies=[Security(get_auth_level)])

router.include_router(notes_router)


class ErrorMessage(BaseModel):
    """
    This model represents an error from the API.
    """

    error: str


@router.get("/collection/{collection_name}", responses={404: {"model": ErrorMessage}})
def read_collection(collection_name: str, event_key: str = env.DB_NAME):
    """
    This endpoint gets all documents from a collection in the database for a given event
    and returns them in list of objects.
    """
    db = client[event_key]
    if (
        collection_name in COLLECTIONS.keys()
        and collection_name in db.list_collection_names()
    ):
        collection = db[collection_name]
        return serialize_documents(list(collection.find()))
    else:
        return JSONResponse(
            content={"error": "Collection not found/allowed"}, status_code=404
        )


class ColorEnum(str, Enum):
    """
    This enum represents the color of an alliance.
    """

    red = "red"
    blue = "blue"


class MatchScheduleTeam(BaseModel):
    """
    This model represents a team in a match schedule.
    """

    number: str
    color: ColorEnum


class MatchScheduleMatch(BaseModel):
    """
    This model represents a match in a match schedule.
    """

    teams: list[MatchScheduleTeam]


@router.get(
    "/match-schedule/{event_key}",
    response_model=dict[str, MatchScheduleMatch],
    responses={404: {"model": ErrorMessage}},
)
def read_match_schedule(event_key: str):
    """
    This endpoint gets the match schedule for a given event and returns it
    """
    return read_static_json("match-schedule", event_key)


@router.get(
    "/team-list/{event_key}",
    response_model=list[str],
    responses={404: {"model": ErrorMessage}},
)
def read_team_list(event_key: str):
    """
    This endpoint gets the team list for a given event and returns it
    """
    return read_static_json("team-list", event_key)


class ViewerData(TypedDict):
    """
    This class represents the data that is returned by the viewer API.
    """

    team: dict[str, dict[str, Any]]
    tim: dict[str, dict[str, dict[str, Any]]]
    aim: dict[str, dict[AllianceColors, dict[str, Any]]]
    alliance: dict[str, dict[str, Any]]
    auto_paths: dict[str, dict[str, dict[str, Any]]]


def make_key(collection_type: DocumentTypes, document: dict[str, Any]) -> list[str]:
    """
    Makes a key from a team, tim, aim, or alliance document for referencing
    """
    keys = []
    for header in COLLECTION_KEYS[collection_type]:
        if header == "alliance_color_is_red":
            alliance_color_is_red = document["alliance_color_is_red"]
            if alliance_color_is_red:
                keys.append("red")
            else:
                keys.append("blue")
        else:
            keys.append(str(document[header]))
        document.pop(header)
    return keys


def get_by_path(dictionary: dict, path: list[str]) -> Any:
    """
    Gets a value in a nested dictionary by a list of strings
    """
    return reduce(lambda d, key: d.get(key) if d else None, path, dictionary)  # type: ignore


def set_by_path(dictionary: dict, path: list[str], value: Any):
    """
    Sets a value in a nested dictionary by a list of strings
    """
    reduce(lambda d, key: d.setdefault(key, {}), path[:-1], dictionary)[
        path[-1]
    ] = value


def serialize_viewer_document(document: dict[str, Any]):
    """
    Removes the _id from a document and returns the document
    """
    document.pop("_id", None)
    return document


@router.get("/viewer")
def get_viewer_data(
    use_strings: Annotated[bool, Query()] = False,
    event_key: Annotated[str, Query()] = env.DB_NAME,
    ignored_collections: Annotated[list[str] | None, Query()] = None,
    ignored_to_string_datapoints: Annotated[
        list[str] | None, Query(alias="itsd")
    ] = None,
    ignored_to_string_collections: Annotated[
        list[str] | None, Query(alias="itsc")
    ] = None,
) -> ViewerData:
    """
    This function uses hard code "collections of collections" to try to relate different collections.
    This data is much easier for viewer to understand.

    """
    db = client[event_key]
    data: ViewerData = cast(
        ViewerData, {collection_type: {} for collection_type in COLLECTION_KEYS}
    )

    for collection, collection_type in COLLECTIONS.items():
        if ignored_collections is not None and collection in ignored_collections:
            continue
        documents = db[collection].find()
        for doc in documents:
            key = make_key(collection_type, doc)
            # Dictionary for this specific key
            common_doc = get_by_path(data[collection_type], key)
            # If the dictionary doesn't exist, create it
            if common_doc is None:
                common_doc = {}
                set_by_path(data[collection_type], key, common_doc)
            # Sending _id to viewer is generally not a good idea and is just wasting space
            # https://stackoverflow.com/a/62706325
            sanitized = serialize_viewer_document(doc)
            if use_strings and (
                (ignored_to_string_collections is None)
                or (collection not in ignored_to_string_collections)
            ):
                # Loop through all the datapoints in the document
                for k, v in sanitized.items():
                    if (ignored_to_string_datapoints is not None) and (
                        k in ignored_to_string_datapoints
                    ):
                        continue
                    # Only convert non primitives (like lists, dicts, or other classes) to strings
                    if not isinstance(v, (float, int, str, bool, type(None))):
                        sanitized[k] = str(v)
            for k in sanitized:
                if isinstance(sanitized[k], numbers.Number) and math.isnan(
                    sanitized[k]
                ):
                    sanitized[k] = None
            # Add the document to the dictionary
            common_doc.update(sanitized)
    return data


def read_static_json(static_type: str, event_key: str):
    """
    This function reads a static file from the database and returns it
    """
    if static_type not in STATIC_FILE_TYPES:
        return JSONResponse(
            content={"error": "Static file type not found/allowed"},
            status_code=404,
        )
    collection = client["static"][static_type]
    document = collection.find_one({"event_key": event_key})
    if document is None:
        return JSONResponse(content={"error": "Static file not found"}, status_code=404)
    else:
        return JSONResponse(content=document["data"])
