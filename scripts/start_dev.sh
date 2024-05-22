#!/bin/sh
uvicorn grosbeak.main:app --reload --env-file .env
