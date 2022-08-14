# base Docker image that we will build on
FROM python:3.9

# set up our image by installing prerequisites; pandas in this case
RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow

# set up the working directory inside the container
WORKDIR /app
# copy the script to the container. First parameter is source file, second is destination
COPY ingest_data.py ingest_data.py

# define what to do first when the container runs
# in this example, we will just run the script
ENTRYPOINT [ "python", "ingest_data.py" ]