FROM python:3.5

WORKDIR /code

RUN pip3 install jupyter
RUN pip3 install psycopg2

COPY . .

RUN pip install -e .
