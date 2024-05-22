from typing import Any
from fastapi import APIRouter, Security
from pydantic import BaseModel
from pymongo import ReplaceOne
from grosbeak.auth import get_auth_level
from grosbeak.env import env
from grosbeak.routers.api import ErrorMessage
from grosbeak.db import client

# define a router for the stand-strategist path
# all actions require authentication by API key
router = APIRouter(prefix="/stand-strategist", dependencies=[Security(get_auth_level)])


class StandStrategistData(BaseModel):
    """
    Model representing the data sent by the Stand Strategist app.
    """

    # outer keys are team numbers
    # inner keys are data point names
    # values are data point values
    teamData: dict[str, dict[str, Any]]

    # outer keys are match numbers
    # middle keys are team numbers
    # inner keys are data point names
    # values are data point values
    timData: dict[str, dict[str, dict[str, Any]]]


@router.get(
    "/users",
    responses={401: {"model": ErrorMessage}},
    summary="Get Stand Strategist user list",
)
async def users(event_key: str = env.DB_NAME) -> list[str]:
    """
    Gets a list of the Stand Strategist users.
    """

    # get the database
    db = client[event_key]
    # get the collections
    team_collection, tim_collection = db["ss_team"], db["ss_tim"]
    # create a set of usernames
    users: set[str] = set()
    # iterate over team data documents
    for doc in team_collection.find():
        # add the username to the users set if not present
        users.add(doc["username"])
    # iterate over TIM data documents
    for doc in tim_collection.find():
        # add the username to the users set if not present
        users.add(doc["username"])
    # convert the set to a list and return it
    return list(users)


@router.get(
    "", responses={401: {"model": ErrorMessage}}, summary="Get Stand Strategist data"
)
async def get(username: str, event_key: str = env.DB_NAME) -> StandStrategistData:
    """
    Gets the Stand Strategist data for the given username.
    """

    # get the database
    db = client[event_key]
    # get the collections
    team_collection, tim_collection = db["ss_team"], db["ss_tim"]

    # create a dict for the team data
    teamData: dict[str, dict[str, Any]] = {}
    # iterate over team data with matching username
    for doc in team_collection.find({"username": username}):
        # create copy of the document
        data = dict(doc)
        # remove unwanted keys
        for key in ["_id", "team_number", "username"]:
            data.pop(key)
        # put the document into the dict with team number as key
        teamData[doc["team_number"]] = data

    # create a dict for the TIM data
    timData: dict[str, dict[str, dict[str, Any]]] = {}
    # iterate over TIM data with matching username
    for doc in tim_collection.find({"username": username}):
        # create copy of the document
        data = dict(doc)
        # remove unwanted keys
        for key in ["_id", "match_number", "team_number", "username"]:
            data.pop(key)
        # create the inner dict for this match if it doesn't exist
        if doc["match_number"] not in timData:
            timData[doc["match_number"]] = {}
        # put the document into the inner dict with match and team numbers as keys
        timData[doc["match_number"]][doc["team_number"]] = data

    # convert to StandStrategistData and return
    return StandStrategistData(teamData=teamData, timData=timData)


@router.put(
    "",
    responses={401: {"model": ErrorMessage}},
    summary="Update Stand Strategist data",
)
async def update(
    data: StandStrategistData, username: str, event_key: str = env.DB_NAME
):
    """
    Writes the supplied Stand Strategist data to the database.
    """

    # get the database
    db = client[event_key]
    # get the collections
    team_collection, tim_collection = db["ss_team"], db["ss_tim"]

    # create list for write operations
    bulk_ops = []
    # iterate over teams
    for team, teamData in data.teamData.items():
        # add team and username to the dictionary
        teamData.update({"team_number": team, "username": username})
        # create a write operation to replace one document in the collection
        bulk_ops.append(
            ReplaceOne(
                # filter for the corresponding team number and username
                {"team_number": team, "username": username},
                # replace the existing document with the new data
                teamData,
                # if the document doesn't exist, create it
                upsert=True,
            )
        )
    # make sure the list of write operations isn't empty
    if bulk_ops:
        # perform a bulk write with all the write operations
        team_collection.bulk_write(bulk_ops)

    # clear the write operation list
    bulk_ops = []
    # iterate over matches
    for match, teams in data.timData.items():
        # iterate over teams
        for team, timData in teams.items():
            # add match, team, username to the dictionary
            timData.update(
                {"match_number": match, "team_number": team, "username": username}
            )
            # create a write operation to replace one document in the collection
            bulk_ops.append(
                ReplaceOne(
                    # filter for the corresponding match number, team number, and username
                    {
                        "match_number": match,
                        "team_number": team,
                        "username": username,
                    },
                    # replace the existing document with the new data
                    timData,
                    # if the document doesn't exist, create it
                    upsert=True,
                )
            )
    # make sure the list of write operations isn't empty
    if bulk_ops:
        # perform a bulk write with all the write operations
        tim_collection.bulk_write(bulk_ops)
