# Use the official Python 3.11 image as a base
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install the required Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the FastAPI app files
COPY main.py .
COPY data/airline_tweets.csv /app/data/airline_tweets.csv

# Expose the port the app runs on
EXPOSE 8000

# Run the FastAPI app with uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]