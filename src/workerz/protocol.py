import struct
import msgspec


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


Message = Register | Dispatch | Cancel | JobStatus | JobUpdate | Ping | Pong

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
