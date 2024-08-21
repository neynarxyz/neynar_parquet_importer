FROM python:3.12

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir --use-pep517 -r requirements.txt

COPY . .

CMD [ "python", "-m", "example_neynar_parquet_importer.main" ]
