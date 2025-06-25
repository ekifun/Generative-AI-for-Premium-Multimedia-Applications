FROM python:3.10-slim

WORKDIR /app

# Copy only requirements.txt first to cache dependencies
COPY requirements.txt ./

# Install numpy<2.0 first to avoid compatibility issues with PyTorch or other native modules
RUN pip install --no-cache-dir "numpy<2.0"

# Then install the rest of the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy rest of the application
COPY . .

# Run the ESRGAN Flask service
CMD ["python", "esrgan_engine.py"]
