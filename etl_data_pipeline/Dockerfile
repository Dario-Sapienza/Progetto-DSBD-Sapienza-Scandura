FROM python:3.8.10
COPY . /etl_data_pipeline
WORKDIR /etl_data_pipeline

RUN pip install -r ./etl_data_pipeline/requirements.txt


EXPOSE 5000
#CMD ["python", "main.py", "runserver", "0.0.0.0:5000"]