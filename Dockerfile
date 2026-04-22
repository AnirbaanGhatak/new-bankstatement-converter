# 1. Base Image
FROM python:3.10-slim

# 2. Working Directory
WORKDIR /app

# 3. System Dependencies (for Camelot and OpenCV)
RUN apt-get update && apt-get install -y ghostscript libgl1-mesa-glx libglib2.0-0

# 4. Copy Requirements
COPY requirements.txt .

# 5. Install Python Libraries
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copy App Code
COPY core_parser.py .
COPY app.py .

# 7. Open the Door
EXPOSE 8501

# 8. Start the App
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]