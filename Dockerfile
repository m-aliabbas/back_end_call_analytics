# Use the official Python image as a parent image
FROM python:3.8

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install dependencies
RUN pip install -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Expose port 9000 for your FastAPI application
EXPOSE 9000

# Command to run your FastAPI application on port 9000
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "9000"]
