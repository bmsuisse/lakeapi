import sqlglot
import sqlglot.expressions as ex

lat1 = ex.column("lat1")
lat2 = ex.column("lat1")
lon1 = ex.column("lat1")
lon2 = ex.column("lat1")

acos = lambda t: ex.func("acos", t)
cos = lambda t: ex.func("cos", t)
radians = lambda t: ex.func("radians", t)
sin = lambda t: ex.func("sin", t)
print(
    (
        ex.convert(6371000)
        * acos(
            cos(radians(lat1)) * cos(radians(lat2)) * cos(radians(lon2) - radians(lon1))
            + sin(radians(lat1)) * sin(radians(lat2))
        )
    ).sql("spark")
)
