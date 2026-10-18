FROM python:3.14-alpine

LABEL org.opencontainers.image.title="FxSocket MCP" \
      org.opencontainers.image.description="FxSocket's read-only MT4/MT5 MCP server as a local stdio server" \
      org.opencontainers.image.source="https://github.com/fxsocket-com/FxSocket-MCP" \
      org.opencontainers.image.url="https://fxsocket.com" \
      org.opencontainers.image.licenses="MIT" \
      io.modelcontextprotocol.server.name="com.fxsocket/mcp"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY fxsocket_mcp.py /app/fxsocket_mcp.py

USER nobody
ENTRYPOINT ["python", "/app/fxsocket_mcp.py"]
