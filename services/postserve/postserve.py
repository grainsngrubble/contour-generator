import io
import itertools
import os

import mercantile
import pyproj
import tornado.ioloop
import tornado.web
import yaml
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError


DEFAULT_TIMEOUT_MS = int(os.getenv("POSTSERVE_DEFAULT_TIMEOUT_MS", "29000"))
MIN_TIMEOUT_MS = int(os.getenv("POSTSERVE_MIN_TIMEOUT_MS", "1000"))
MAX_TIMEOUT_MS = int(os.getenv("POSTSERVE_MAX_TIMEOUT_MS", "180000"))
DEFAULT_IDLE_TIMEOUT_MS = int(os.getenv("POSTSERVE_IDLE_TIMEOUT_MS", "30000"))
DEFAULT_LOCK_TIMEOUT_MS = int(os.getenv("POSTSERVE_LOCK_TIMEOUT_MS", "5000"))
APPLICATION_NAME = os.getenv("POSTSERVE_APP_NAME", "postserve_streaming")


class TileTimeoutError(Exception):
    pass


class TileQueryError(Exception):
    pass


def clamp_timeout(timeout_ms):
    return max(MIN_TIMEOUT_MS, min(timeout_ms, MAX_TIMEOUT_MS))


def parse_timeout_ms(raw_value):
    if not raw_value:
        return DEFAULT_TIMEOUT_MS
    try:
        timeout_ms = int(raw_value)
    except Exception:
        return DEFAULT_TIMEOUT_MS
    return clamp_timeout(timeout_ms)


def get_tm2source(file_name):
    with open(file_name, "r") as stream:
        return yaml.load(stream)


def generate_prepared(layers):
    queries = []
    prepared = "PREPARE gettile(geometry, numeric, numeric, numeric) AS "
    for layer in layers["Layer"]:
        layer_query = layer["Datasource"]["table"].strip()
        layer_query = layer_query[1 : len(layer_query) - 6]
        layer_query = layer_query.replace(
            "geometry",
            "ST_AsMVTGeom(geometry,!bbox!,4096,0,true) AS mvtgeometry",
        )
        base_query = (
            "SELECT ST_ASMVT('"
            + layer["id"]
            + "', 4096, 'mvtgeometry', tile) FROM ("
            + layer_query
            + " WHERE ST_AsMVTGeom(geometry, !bbox!,4096,0,true) IS NOT NULL) AS tile"
        )
        queries.append(
            base_query.replace("!bbox!", "$1")
            .replace("!scale_denominator!", "$2")
            .replace("!pixel_width!", "$3")
            .replace("!pixel_height!", "$4")
        )
    return prepared + " UNION ALL ".join(queries) + ";"


layers = get_tm2source("/mapping/data.yml")
prepared_statement = generate_prepared(layers)
engine = create_engine(
    "postgresql://{user}:{password}@{host}:{port}/{db}".format(
        user=os.getenv("POSTGRES_USER", "openmaptiles"),
        password=os.getenv("POSTGRES_PASSWORD", "openmaptiles"),
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        db=os.getenv("POSTGRES_DB", "openmaptiles"),
    ),
    connect_args={"application_name": APPLICATION_NAME},
)


@event.listens_for(engine, "connect")
def prepare_gettile(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute(prepared_statement)
        dbapi_connection.commit()
    except Exception:
        dbapi_connection.rollback()
        raise
    finally:
        cursor.close()


def bounds(zoom, x, y):
    in_proj = pyproj.Proj(init="epsg:4326")
    out_proj = pyproj.Proj(init="epsg:3857")
    lnglatbbox = mercantile.bounds(x, y, zoom)
    ws = pyproj.transform(in_proj, out_proj, lnglatbbox[0], lnglatbbox[1])
    en = pyproj.transform(in_proj, out_proj, lnglatbbox[2], lnglatbbox[3])
    return {"w": ws[0], "s": ws[1], "e": en[0], "n": en[1]}


def zoom_to_scale_denom(zoom):
    map_width_in_metres = 40075016.68557849
    tile_width_in_pixels = 256.0
    standardized_pixel_size = 0.00028
    map_width_in_pixels = tile_width_in_pixels * (2.0 ** zoom)
    return str(map_width_in_metres / (map_width_in_pixels * standardized_pixel_size))


def replace_tokens(query, s, w, n, e, scale_denom):
    return (
        query.replace(
            "!bbox!",
            "ST_MakeBox2D(ST_Point({w}, {s}), ST_Point({e}, {n}))".format(
                w=w, s=s, e=e, n=n
            ),
        )
        .replace("!scale_denominator!", scale_denom)
        .replace("!pixel_width!", "256")
        .replace("!pixel_height!", "256")
    )


def is_timeout_error(error):
    message = str(error).lower()
    return (
        "statement timeout" in message
        or "canceling statement due to statement timeout" in message
        or "query_canceled" in message
        or "canceling statement due to lock timeout" in message
        or "lock timeout" in message
    )


def get_mvt(zoom, x, y, timeout_ms):
    try:
        sani_zoom, sani_x, sani_y = int(zoom), int(x), int(y)
    except Exception:
        raise TileQueryError("invalid tile coordinates")

    scale_denom = zoom_to_scale_denom(sani_zoom)
    tilebounds = bounds(sani_zoom, sani_x, sani_y)
    s = str(tilebounds["s"])
    w = str(tilebounds["w"])
    n = str(tilebounds["n"])
    e = str(tilebounds["e"])
    final_query = "EXECUTE gettile(!bbox!, !scale_denominator!, !pixel_width!, !pixel_height!);"
    sent_query = replace_tokens(final_query, s, w, n, e, scale_denom)
    idle_timeout_ms = max(timeout_ms + 1000, DEFAULT_IDLE_TIMEOUT_MS)
    lock_timeout_ms = min(timeout_ms, DEFAULT_LOCK_TIMEOUT_MS)

    connection = engine.connect()
    transaction = connection.begin()
    try:
        connection.execute(text("SET LOCAL statement_timeout = {0}".format(timeout_ms)))
        connection.execute(
            text(
                "SET LOCAL idle_in_transaction_session_timeout = {0}".format(
                    idle_timeout_ms
                )
            )
        )
        connection.execute(text("SET LOCAL lock_timeout = {0}".format(lock_timeout_ms)))
        response = list(connection.execute(text(sent_query)))
        transaction.commit()
    except SQLAlchemyError as error:
        transaction.rollback()
        if is_timeout_error(error):
            raise TileTimeoutError(str(error))
        raise TileQueryError(str(error))
    except Exception:
        transaction.rollback()
        raise
    finally:
        connection.close()

    rendered_layers = filter(None, list(itertools.chain.from_iterable(response)))
    final_tile = b""
    for layer in rendered_layers:
        final_tile += io.BytesIO(layer).getvalue()
    return final_tile


class HealthHandler(tornado.web.RequestHandler):
    def get(self):
        self.set_status(200)
        self.set_header("Content-Type", "text/plain; charset=utf-8")
        self.write("ok")

    def head(self):
        self.set_status(200)


class GetTile(tornado.web.RequestHandler):
    def get(self, zoom, x, y):
        timeout_ms = parse_timeout_ms(self.request.headers.get("X-Request-Timeout-Ms"))
        self.set_header("Content-Type", "application/x-protobuf")
        self.set_header("Content-Disposition", "attachment")
        self.set_header("Access-Control-Allow-Origin", "*")
        try:
            response = get_mvt(zoom, x, y, timeout_ms)
        except TileTimeoutError as error:
            self.set_status(504)
            self.write(str(error))
            return
        except TileQueryError as error:
            self.set_status(500)
            self.write(str(error))
            return

        if not response:
            self.set_status(204)
            return
        self.write(response)

    def head(self, zoom, x, y):
        self.set_status(405)


def main():
    application = tornado.web.Application(
        [
            (r"/health", HealthHandler),
            (r"/tiles/([0-9]+)/([0-9]+)/([0-9]+).pbf", GetTile),
        ]
    )
    print("Postserve streaming started..")
    application.listen(8080)
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
