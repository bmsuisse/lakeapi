import io
import json
import mimetypes
import os
from enum import Enum
from typing import Union
from uuid import uuid4

import anyio
import pyarrow as pa
from fastapi import BackgroundTasks
from starlette.background import BackgroundTask
from starlette.datastructures import URL
from starlette.responses import FileResponse

from bmsdna.lakeapi.context.df_base import ExecutionContext, ResultData
from bmsdna.lakeapi.core.config import BasicConfig
from bmsdna.lakeapi.core.log import get_logger
from bmsdna.lakeapi.core.types import OutputFileType

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


def parse_format(accept: Union[str, OutputFileType]) -> tuple[OutputFormats, str]:
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

        ds = pl.from_arrow(content.to_arrow_recordbatch())
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

    elif format in (OutputFormats.ARROW_IPC, OutputFormats.ARROW_STREAM):
        with content.to_arrow_recordbatch() as batches:
            with pa.OSFile(out, "wb") as sink:
                with pa.ipc.new_file(sink, batches.schema) as writer:
                    for batch in batches:
                        writer.write(batch)

    elif format == OutputFormats.ND_JSON:
        content.write_nd_json(out)
    elif format == OutputFormats.PARQUET:
        content.write_parquet(out)
    else:
        content.write_json(out)
    return []


class FileResponseWCharset(FileResponse):
    def __init__(self, *args, **kwargs):
        if "charset" in kwargs:
            self.charset = kwargs.pop("charset")
        super().__init__(*args, **kwargs)


async def create_response(
    url: URL,
    accept: str,
    content: ResultData,
    context: ExecutionContext,
    basic_config: BasicConfig,
):
    headers = {}

    format = parse_format(accept)

    format, extension = parse_format(accept)
    content_dispositiont_type = "attachment"
    filename = "file" + extension
    if format in [
        OutputFormats.JSON,
        OutputFormats.ND_JSON,
        OutputFormats.CSV,
        OutputFormats.SEMI_CSV,
        OutputFormats.CSV4EXCEL,
    ]:
        content_dispositiont_type = "inline"
        filename = None
    path = os.path.join(basic_config.temp_folder_path, str(uuid4()) + extension)
    media_type = "text/csv" if extension == ".csv" else mimetypes.guess_type("file" + extension)[0]
    additional_files = write_frame(url=url, content=content, format=format, out=path, basic_config=basic_config)

    tasks = BackgroundTasks()
    import asyncio

    async def remove():
        context.close()
        await anyio.sleep(3)

        os.remove(path)
        for f in additional_files:
            os.remove(f)

    tasks.add_task(remove)

    fr = FileResponseWCharset(
        path=path,
        headers=headers,
        media_type=media_type,
        content_disposition_type=content_dispositiont_type,
        filename=filename,
        background=tasks,
        charset="utf-16le" if format == OutputFormats.CSV4EXCEL else "utf-8",
    )
    return fr
