FROM python:3.8.10
COPY . /sla_manager
WORKDIR /sla_manager

RUN pip install -r ./sla_manager/requirements.txt


EXPOSE 5000
#CMD ["python", "main.py", "runserver", "0.0.0.0:5000"]