
# Use the official Python image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the requirements file into the container
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Make run.sh executable
RUN chmod +x run.sh

# Command to run the script
CMD ["./run.sh"]
