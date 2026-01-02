# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy project files
COPY . /app/

# Set the entrypoint
ENTRYPOINT ["python3", "src/main.py"]

# Default command (can be overridden)
CMD ["--help"]
