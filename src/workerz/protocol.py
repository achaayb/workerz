import struct
import msgspec


# ── worker <-> coordinator ────────────────────────────────────────────────────

class Register(msgspec.Struct, tag=True):
    worker_id: str
    labels:    list[str]


class Dispatch(msgspec.Struct, tag=True):
    job_id: str
    task:   str
    args:   list
    kwargs: dict


class Cancel(msgspec.Struct, tag=True):
    job_id: str


class JobStatus(msgspec.Struct, tag=True):
    job_id:   str
    status:   str                    # running | done | error | cancelled
    result:   str | None = None      # json-encoded
    error:    str | None = None
    warnings: list[str] = []
    infos:    list[str] = []
    debug:    list[str] = []


class JobUpdate(msgspec.Struct, tag=True):
    """Worker pushes arbitrary meta mid-run."""
    job_id: str
    meta:   dict


class Ping(msgspec.Struct, tag=True):
    pass


class Pong(msgspec.Struct, tag=True):
    worker_id: str


# ── client (SDK) <-> coordinator ──────────────────────────────────────────────
# Request/reply over a short-lived connection. rid correlates reply to request.

class SubmitJob(msgspec.Struct, tag=True):
    rid:    str
    task:   str
    args:   list
    kwargs: dict
    labels: list[str]


class GetJob(msgspec.Struct, tag=True):
    rid:    str
    job_id: str


class CancelJob(msgspec.Struct, tag=True):
    rid:    str
    job_id: str


class ListJobs(msgspec.Struct, tag=True):
    rid: str


class ListWorkers(msgspec.Struct, tag=True):
    rid: str


class Reply(msgspec.Struct, tag=True):
    """Generic reply. ok=False -> err carries an error code (str)."""
    rid:  str
    ok:   bool
    data: dict | list | None = None
    err:  str | None = None


Message = (
    Register | Dispatch | Cancel | JobStatus | JobUpdate | Ping | Pong
    | SubmitJob | GetJob | CancelJob | ListJobs | ListWorkers | Reply
)

_encoder = msgspec.json.Encoder()
_decoder = msgspec.json.Decoder(Message)


async def send_msg(writer, msg):
    data = _encoder.encode(msg)
    writer.write(struct.pack(">I", len(data)) + data)
    await writer.drain()


async def recv_msg(reader):
    raw_len = await reader.readexactly(4)
    length  = struct.unpack(">I", raw_len)[0]
    data    = await reader.readexactly(length)
    return _decoder.decode(data)
