FROM python:3.11-slim

# Copy your local Parsl source into the image
COPY . /parsl-src
RUN pip install -e /parsl-src

# Install anything else you want the worker to have
WORKDIR /app

CMD ["python3"]
