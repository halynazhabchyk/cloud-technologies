FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY mcp_server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY mcp_server ./mcp_server

# In a container the server listens on the network (Streamable HTTP) so that
# n8n can reach it at http://mcp:8100/mcp; stdio stays the default locally.
ENV MCP_TRANSPORT=streamable-http \
    MCP_HOST=0.0.0.0 \
    MCP_PORT=8100

EXPOSE 8100

CMD ["python", "-m", "mcp_server.server"]
