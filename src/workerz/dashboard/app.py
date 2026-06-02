import asyncio
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse
from starlette.routing import Route

from workerz.logging import setup_logging
from workerz.protocol import ListJobs, ListWorkers, Reply, recv_msg, send_msg
from workerz.dashboard.settings import settings

logger = setup_logging("dashboard", settings.log_file, settings.log_level)

HTTP_PORT        = settings.http_port
COORDINATOR_HOST = settings.coordinator_host
COORDINATOR_TCP  = settings.coordinator_tcp


async def _ask(msg) -> Reply:
    reader, writer = await asyncio.open_connection(COORDINATOR_HOST, COORDINATOR_TCP)
    try:
        await send_msg(writer, msg)
        return await recv_msg(reader)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def route_dashboard(request: Request):
    html = (Path(__file__).parent / "index.html").read_text()
    return HTMLResponse(html)



async def route_jobs(request: Request):
    try:
        reply = await _ask(ListJobs(rid=uuid.uuid4().hex))
        return JSONResponse(reply.data or [])
    except Exception:
        logger.exception("failed to fetch jobs from coordinator")
        return JSONResponse({"error": "coordinator_unreachable"}, status_code=502)


async def route_workers(request: Request):
    try:
        reply = await _ask(ListWorkers(rid=uuid.uuid4().hex))
        return JSONResponse(reply.data or [])
    except Exception:
        logger.exception("failed to fetch workers from coordinator")
        return JSONResponse({"error": "coordinator_unreachable"}, status_code=502)


@asynccontextmanager
async def lifespan(app):
    logger.info("dashboard listening http://0.0.0.0:{}  coordinator={}:{}  build={}",
                HTTP_PORT, COORDINATOR_HOST, COORDINATOR_TCP, settings.build_version)
    yield
    logger.info("dashboard stopped")


app = Starlette(
    lifespan=lifespan,
    routes=[
        Route("/",        route_dashboard),
        Route("/jobs",    route_jobs),
        Route("/workers", route_workers),
    ],
)


def run():
    """Entry point. Serves the dashboard until interrupted."""
    import uvicorn
    try:
        uvicorn.run(app, host="0.0.0.0", port=settings.http_port, log_level="warning")
    except KeyboardInterrupt:
        logger.info("dashboard stopped")
    except Exception:
        logger.exception("dashboard crashed")
        raise
