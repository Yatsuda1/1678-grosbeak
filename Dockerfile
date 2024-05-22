FROM python:3.11

WORKDIR /code

COPY ./pyproject.toml /code/pyproject.toml

ENV PYTHONPATH=${PYTHONPATH}:${PWD} 

RUN pip3 install poetry==1.3.2
RUN poetry config virtualenvs.create false
RUN poetry install --without dev

COPY ./grosbeak /code/grosbeak

COPY ./web /code/web

CMD ["uvicorn", "grosbeak.main:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "80"]
