FROM docker.io/library/python:3.12-slim
WORKDIR /app
COPY src/dashboard/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/dashboard/app.py src/dashboard/analytics_view.py ./
# Streamlit ghi cấu hình vào $HOME; nobody không có home nên trỏ sang /tmp.
ENV HOME=/tmp
USER nobody
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true", "--browser.gatherUsageStats=false"]
