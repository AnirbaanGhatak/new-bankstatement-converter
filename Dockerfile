# 1. Base Image
FROM python:3.12-slim

# 2. Working Directory
WORKDIR /app

# 3. System Dependencies (for Camelot and OpenCV)
RUN apt-get update && apt-get install -y ghostscript && rm -rf /var/lib/apt/lists/*

# 4. Copy Requirements
COPY requirements.txt .

# 5. Install Python Libraries
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copy App Code
COPY core_parser.py .
COPY app.py .
COPY trainmodel.py .

# 7. Open the Door
EXPOSE 8080

# 8. Start the App
CMD ["streamlit", "run", "app.py", "--server.port=8080", "--server.address=0.0.0.0"]
