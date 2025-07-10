FROM python:3.10-slim

WORKDIR /app

# Copy requirements first and install all dependencies
COPY requirements.txt ./

# Ensure pip is up to date
RUN pip install --upgrade pip

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the source code
COPY . .

# Run the ESRGAN Flask app
CMD ["python", "esrgan_engine.py"]
