FROM python:latest

WORKDIR app

COPY ./main.py ./WWI_DATA.zip ./requirements.txt ./

RUN unzip ./WWI_DATA.zip

RUN pip3 install -r ./requirements.txt

CMD ["python3", "main.py"]
