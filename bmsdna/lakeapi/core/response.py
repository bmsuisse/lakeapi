import mimetypes
import os
import tempfile
from enum import Enum
from typing import Union
from uuid import uuid4
import polars as pl
import pyarrow as pa
from starlette.background import BackgroundTask
from starlette.datastructures import URL
from starlette.responses import FileResponse, Response, StreamingResponse
from email.utils import format_datetime, formatdate

from bmsdna.lakeapi.context.df_base import ExecutionContext, ResultData
from bmsdna.lakeapi.core.config import BasicConfig
from bmsdna.lakeapi.core.log import get_logger
from bmsdna.lakeapi.core.types import OutputFileType
from datetime import timedelta
import typing
from urllib.parse import quote
from pypika.queries import QueryBuilder
from mimetypes import guess_type
from starlette.concurrency import iterate_in_threadpool

import anyio

from starlette._compat import md5_hexdigest

logger = get_logger(__name__)


class OutputFormats(Enum):
    AVRO = 1
    CSV = 2
    CSV4EXCEL = 12
    SEMI_CSV = 11
    XLSX = 3
    HTML = 4
    ARROW_IPC = 5
    ND_JSON = 6
    PARQUET = 7
    JSON = 8
    XML = 9
    ORC = 10
    ARROW_STREAM = 14


async def parse_format(accept: Union[str, OutputFileType]) -> tuple[OutputFormats, str]:
    realaccept = accept.split(";")[0].strip().lower()
    if realaccept == "application/avro" or realaccept == "avro":
        return (OutputFormats.AVRO, ".avro")
    elif realaccept == "text/csv" or realaccept == "csv":
        return (OutputFormats.CSV, ".csv")
    elif realaccept == "text/csv+semicolon" or realaccept == "scsv":
        return (OutputFormats.SEMI_CSV, ".csv")
    elif realaccept == "x.text/csv+excel" or realaccept == "csv4excel":
        return (OutputFormats.CSV4EXCEL, ".csv")
    elif realaccept == "application/xml" or realaccept == "xml":
        return (OutputFormats.XML, ".xml")
    elif realaccept == "orc":
        return (OutputFormats.ORC, ".orc")
    elif realaccept == mimetypes.guess_type("file.xlsx") or realaccept == "xlsx":
        return (OutputFormats.XLSX, ".xlsx")
    elif realaccept == "text/html" or realaccept == "html":
        return (OutputFormats.HTML, ".html")
    elif realaccept == "application/vnd.apache.arrow.stream" or realaccept == "arrow-stream":
        return (OutputFormats.ARROW_STREAM, "")

    elif (
        realaccept == "application/x-arrow"
        or realaccept == "application/x-arrow"
        or realaccept == "application/vnd.apache.arrow.file"
        or realaccept == "arrow"
        or realaccept == "feather"
        or realaccept == "ipc"
    ):
        return (OutputFormats.ARROW_IPC, ".arrow")
    elif realaccept == "application/json+newline" or realaccept == "application/jsonl" or realaccept == "ndjson":
        return (OutputFormats.ND_JSON, ".ndjson")
    elif realaccept == "application/parquet" or realaccept == "parquet":
        return (OutputFormats.PARQUET, ".parquet")
    else:
        return (OutputFormats.JSON, ".json")


def write_frame(
    url: URL, content: ResultData, format: OutputFormats, out: str, basic_config: BasicConfig
) -> list[str]:
    if format == OutputFormats.AVRO:
        import polars as pl

        ds = pl.from_arrow(content.to_arrow_recordbatch(content.chunk_size))
        assert isinstance(ds, pl.DataFrame)
        ds.write_avro(out)
    elif format == OutputFormats.CSV:
        content.write_csv(out, separator=",")
    elif format == OutputFormats.SEMI_CSV:
        content.write_csv(out, separator=";")
    elif format == OutputFormats.CSV4EXCEL:  # need to write sep=, on first line
        content.write_csv(out + "_u8", separator=",")  # type: ignore
        with open(
            out, mode="wb"
        ) as f:  # excel wants utf-16le which polars does not support. therefore we need reencoding
            f.write(b"sep=,\n")  # add utf-8 bom at beginning
            with open(out + "_u8", mode="r", encoding="utf-8") as c8:
                line = c8.readline()
                while line != "":
                    f.write(line.encode("utf-16le"))
                    line = c8.readline()
            return [out + "_u8"]
    elif format == OutputFormats.XLSX:
        import polars as pl

        ds = pl.from_arrow(content.to_arrow_table())
        assert isinstance(ds, pl.DataFrame)
        ds.write_excel(out, autofit=True)
    elif format == OutputFormats.HTML:
        content.to_pandas().to_html(out, index=False)

    elif format == OutputFormats.XML:
        content.to_pandas().to_xml(out, index=False, parser="etree")

    elif format == OutputFormats.ARROW_IPC:
        with content.to_arrow_recordbatch(content.chunk_size) as batches:
            with pa.OSFile(out, "wb") as sink:
                with pa.ipc.new_file(sink, batches.schema) as writer:
                    for batch in batches:
                        writer.write(batch)

    elif format == OutputFormats.ARROW_STREAM:
        with content.to_arrow_recordbatch(content.chunk_size) as batches:
            with pa.OSFile(out, "wb") as sink:
                with pa.ipc.new_stream(sink, batches.schema) as writer:
                    for batch in batches:
                        writer.write_batch(batch)

    elif format == OutputFormats.ND_JSON:
        content.write_nd_json(out)
    elif format == OutputFormats.PARQUET:
        content.write_parquet(out)
    else:
        content.write_json(out)
    return []


Content = typing.Union[str, bytes]
SyncContentStream = typing.Iterator[Content]
AsyncContentStream = typing.AsyncIterable[Content]
ContentStream = typing.Union[AsyncContentStream, SyncContentStream]


class StreamingResponseWCharset(StreamingResponse):
    """StreamingResponseWCharset
    Combines file response with stream response
    """

    body_iterator: AsyncContentStream

    def __init__(
        self,
        content: ContentStream,
        status_code: int = 200,
        headers: typing.Optional[typing.Mapping[str, str]] = None,
        media_type: typing.Optional[str] = None,
        background: typing.Optional[BackgroundTask] = None,
        filename: typing.Optional[str] = None,
        stat_result: typing.Optional[os.stat_result] = None,
        method: typing.Optional[str] = None,
        content_disposition_type: str = "attachment",
        *args,
        **kwargs,
    ):
        if isinstance(content, typing.AsyncIterable):
            self.body_iterator = content
        else:
            self.body_iterator = iterate_in_threadpool(content)

        # taking over from FileResponse
        self.status_code = status_code
        self.filename = filename
        self.send_header_only = method is not None and method.upper() == "HEAD"
        if media_type is None:
            media_type = guess_type(filename or "text/pain")[0] or "text/plain"
        self.media_type = media_type
        self.background = background
        self.init_headers(headers)
        if self.filename is not None:
            content_disposition_filename = quote(self.filename)
            if content_disposition_filename != self.filename:
                content_disposition = "{}; filename*=utf-8''{}".format(
                    content_disposition_type, content_disposition_filename
                )
            else:
                content_disposition = '{}; filename="{}"'.format(content_disposition_type, self.filename)
            self.headers.setdefault("content-disposition", content_disposition)
        self.stat_result = stat_result
        if stat_result is not None:
            self.set_stat_headers(stat_result)

        if "charset" in kwargs:
            self.charset = kwargs.pop("charset")

    def set_stat_headers(self, stat_result: os.stat_result) -> None:
        content_length = str(stat_result.st_size)
        last_modified = formatdate(stat_result.st_mtime, usegmt=True)
        etag_base = str(stat_result.st_mtime) + "-" + str(stat_result.st_size)
        etag = md5_hexdigest(etag_base.encode(), usedforsecurity=False)

        self.headers.setdefault("content-length", content_length)
        self.headers.setdefault("last-modified", last_modified)
        self.headers.setdefault("etag", etag)


class TempFileWrapper:  # does not open the file which is important on windows
    def __init__(self, path: str) -> None:
        self.path = path

    @property
    def name(self):
        return self.path

    def close(self):
        os.unlink(self.path)


def get_temp_file(extension: str):
    if os.name == "nt":
        temp_file = TempFileWrapper(os.environ["TEMP"] + "/" + str(uuid4()))
    else:
        temp_file = tempfile.NamedTemporaryFile(delete=True, suffix=extension)
    return temp_file


async def create_response(
    url: URL,
    accept: str,
    context: ExecutionContext,
    sql: QueryBuilder | str,
    basic_config: BasicConfig,
    close_context=False,
):
    headers = {}

    format = await parse_format(accept)

    format, extension = await parse_format(accept)
    content_dispositiont_type = "attachment"
    filename = "file" + extension
    media_type = "text/csv" if extension == ".csv" else mimetypes.guess_type("file" + extension)[0]

    if format == OutputFormats.JSON:
        return Response(content=context.execute_sql(sql).to_json(), headers=headers, media_type=media_type)
    if format == OutputFormats.ND_JSON:
        return Response(content=context.execute_sql(sql).to_ndjson(), headers=headers, media_type=media_type)

    if format in [
        OutputFormats.JSON,
        OutputFormats.ND_JSON,
        OutputFormats.CSV,
        OutputFormats.SEMI_CSV,
        OutputFormats.CSV4EXCEL,
    ]:
        content_dispositiont_type = "inline"
        filename = None

    temp_file = get_temp_file(extension)

    async def response_stream(context, sql, url, format):
        chunk_size = 64 * 1024
        content = await anyio.to_thread.run_sync(context.execute_sql, sql)  # type: ignore
        additional_files = await anyio.to_thread.run_sync(  # type: ignore
            write_frame, url, content, format, temp_file.name, basic_config
        )

        async with await anyio.open_file(temp_file.name, mode="rb") as file:
            more_body = True
            while more_body:
                chunk = await file.read(chunk_size)
                more_body = len(chunk) == chunk_size
                yield chunk

    def clean_up():
        if close_context:
            context.close()
        try:
            temp_file.close()
        except FileNotFoundError:
            pass

    return StreamingResponseWCharset(
        content=response_stream(context, sql, url, format),
        headers=headers,
        media_type=media_type,
        content_disposition_type=content_dispositiont_type,
        filename=filename,
        background=BackgroundTask(clean_up),
        charset="utf-16le" if format == OutputFormats.CSV4EXCEL else "utf-8",
    )
