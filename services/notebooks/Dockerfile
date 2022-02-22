FROM python:3.8-slim-buster

RUN pip install --upgrade pip

RUN mkdir src
WORKDIR src/
COPY . .

RUN pip install -r requirements.txt

WORKDIR /src/notebooks

CMD ["jupyter", "notebook", "--port=8888", "--no-browser", "--ip=0.0.0.0", "--allow-root"]