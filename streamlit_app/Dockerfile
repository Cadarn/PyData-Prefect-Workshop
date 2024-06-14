# Use the official Python 3.11 image as a base
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file
COPY requirements.txt .

# Install the required Python packages
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install pymongo

# Copy the FastAPI app files
COPY app.py .
COPY Tweets.csv Tweets.csv

# Expose the port the app runs on
EXPOSE 8501

# Run the FastAPI app with uvicorn
CMD ["streamlit", "run", "app.py"]