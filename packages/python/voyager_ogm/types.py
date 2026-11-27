"""Voyager OGM Geospatial Types & Spatial Distance Engine.

Provides spatial Point data types (2D and 3D geographic WGS-84 and Cartesian)
with coordinate validation, in-memory distance calculations (Haversine and Euclidean),
AST expression builder integration, and PyArrow/Polars conversions.
"""

from __future__ import annotations

import datetime
import math
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from voyager_ogm.expressions import (
    Expression,
    FunctionExpr,
    LiteralExpr,
    to_expression,
)

try:
    from voyager_ogm._voyager_rs import AstExpr

    HAS_VOYAGER_RS = True
except ImportError:
    AstExpr = None  # type: ignore[assignment,misc]
    HAS_VOYAGER_RS = False

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa

# Standard Spatial Reference System Identifiers (SRID)
WGS_84_2D: int = 4326
WGS_84_3D: int = 4979
CARTESIAN_2D: int = 7203
CARTESIAN_3D: int = 9157

SUPPORTED_SRIDS: frozenset[int] = frozenset({WGS_84_2D, WGS_84_3D, CARTESIAN_2D, CARTESIAN_3D})

# Neo4j and openCypher standard mean Earth radius in meters
EARTH_RADIUS_METERS: float = 6371008.8

# CRS Name to SRID Mapping
_CRS_TO_SRID_2D: dict[str, int] = {
    "wgs-84": WGS_84_2D,
    "wgs84": WGS_84_2D,
    "epsg:4326": WGS_84_2D,
    "cartesian": CARTESIAN_2D,
    "cartesian-2d": CARTESIAN_2D,
    "epsg:7203": CARTESIAN_2D,
}

_CRS_TO_SRID_3D: dict[str, int] = {
    "wgs-84-3d": WGS_84_3D,
    "wgs84-3d": WGS_84_3D,
    "wgs-84": WGS_84_3D,
    "wgs84": WGS_84_3D,
    "epsg:4979": WGS_84_3D,
    "cartesian-3d": CARTESIAN_3D,
    "cartesian": CARTESIAN_3D,
    "epsg:9157": CARTESIAN_3D,
}

_SRID_TO_CRS: dict[int, str] = {
    WGS_84_2D: "wgs-84",
    WGS_84_3D: "wgs-84-3d",
    CARTESIAN_2D: "cartesian",
    CARTESIAN_3D: "cartesian-3d",
}


def _haversine_distance(
    lat1: float, lon1: float, lat2: float, lon2: float, radius: float = EARTH_RADIUS_METERS
) -> float:
    """Calculates spherical distance on Earth using the Haversine formula."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)

    a = (math.sin(dphi / 2.0) ** 2) + math.cos(phi1) * math.cos(phi2) * (math.sin(dlam / 2.0) ** 2)
    # Clamp a within [0, 1] to prevent domain errors with floating-point drift
    a = min(1.0, max(0.0, a))
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))
    return radius * c


class Point(Expression):
    """Geospatial Point representation across Bolt, Cypher, ISO GQL, and SQL/PGQ.

    Supports both geographic WGS-84 coordinates (latitude, longitude, height) and
    Cartesian coordinates (x, y, z) in 2D and 3D.

    Attributes:
        srid: Spatial Reference System Identifier (e.g. 4326, 4979, 7203, 9157).
        crs: Coordinate Reference System string identifier (e.g. 'wgs-84', 'cartesian').
        x: Longitude for geographic points or X-coordinate for Cartesian points.
        y: Latitude for geographic points or Y-coordinate for Cartesian points.
        z: Height for geographic points or Z-coordinate for Cartesian points (or None if 2D).
        latitude: Latitude for geographic points (or None if Cartesian).
        longitude: Longitude for geographic points (or None if Cartesian).
        height: Height in meters above ellipsoid for 3D geographic points (or None).
        is_3d: Whether the point has three dimensions.
    """

    def __init__(
        self,
        *coords: float,
        latitude: float | None = None,
        longitude: float | None = None,
        height: float | None = None,
        x: float | None = None,
        y: float | None = None,
        z: float | None = None,
        srid: int | None = None,
        crs: str | None = None,
    ) -> None:
        """Initializes a geospatial Point.

        Args:
            *coords: Optional positional coordinates (x, y) or (x, y, z).
            latitude: Latitude in degrees (-90 to 90) for WGS-84 points.
            longitude: Longitude in degrees (-180 to 180) for WGS-84 points.
            height: Height in meters for 3D WGS-84 points.
            x: X-coordinate (or longitude) for Cartesian/geographic points.
            y: Y-coordinate (or latitude) for Cartesian/geographic points.
            z: Z-coordinate (or height) for 3D Cartesian/geographic points.
            srid: Explicit SRID (4326, 4979, 7203, 9157).
            crs: Explicit CRS name ('wgs-84', 'wgs-84-3d', 'cartesian', 'cartesian-3d').
        """
        # Handle positional coordinates
        if coords:
            if len(coords) == 2:
                if x is None:
                    x = coords[0]
                if y is None:
                    y = coords[1]
            elif len(coords) == 3:
                if x is None:
                    x = coords[0]
                if y is None:
                    y = coords[1]
                if z is None:
                    z = coords[2]
            else:
                raise ValueError(
                    f"Point accepts either 2 or 3 positional coordinates, got {len(coords)}"
                )

        # Normalize crs string
        norm_crs = crs.strip().lower() if isinstance(crs, str) else None

        # Determine if 3D from coordinates
        has_3d = (height is not None) or (z is not None)

        # Determine SRID
        resolved_srid: int
        if srid is not None:
            resolved_srid = int(srid)
            if resolved_srid not in SUPPORTED_SRIDS:
                raise ValueError(
                    f"Unsupported spatial reference system identifier (SRID): {resolved_srid}. "
                    f"Supported SRIDs are: {sorted(SUPPORTED_SRIDS)} "
                    f"({WGS_84_2D} for WGS-84 2D, {WGS_84_3D} for WGS-84 3D, "
                    f"{CARTESIAN_2D} for Cartesian 2D, {CARTESIAN_3D} for Cartesian 3D)."
                )
        elif norm_crs is not None:
            if has_3d and norm_crs in _CRS_TO_SRID_3D:
                resolved_srid = _CRS_TO_SRID_3D[norm_crs]
            elif not has_3d and norm_crs in _CRS_TO_SRID_2D:
                resolved_srid = _CRS_TO_SRID_2D[norm_crs]
            elif norm_crs in _CRS_TO_SRID_3D:
                resolved_srid = _CRS_TO_SRID_3D[norm_crs]
            elif norm_crs in _CRS_TO_SRID_2D:
                resolved_srid = _CRS_TO_SRID_2D[norm_crs]
            else:
                raise ValueError(f"Unrecognized coordinate reference system (CRS): '{crs}'")
        elif latitude is not None or longitude is not None or height is not None:
            resolved_srid = WGS_84_3D if has_3d else WGS_84_2D
        else:
            resolved_srid = CARTESIAN_3D if has_3d else CARTESIAN_2D

        is_geo = resolved_srid in (WGS_84_2D, WGS_84_3D)
        is_3d_point = resolved_srid in (WGS_84_3D, CARTESIAN_3D)

        # Coordinate resolution
        res_x: float
        res_y: float
        res_z: float | None = None

        if is_geo:
            # For WGS-84: x is longitude, y is latitude
            if longitude is not None:
                res_x = float(longitude)
            elif x is not None:
                res_x = float(x)
            else:
                raise ValueError("Geographic WGS-84 Point requires 'longitude' or 'x'")

            if latitude is not None:
                res_y = float(latitude)
            elif y is not None:
                res_y = float(y)
            else:
                raise ValueError("Geographic WGS-84 Point requires 'latitude' or 'y'")

            if is_3d_point:
                if height is not None:
                    res_z = float(height)
                elif z is not None:
                    res_z = float(z)
                else:
                    res_z = 0.0

            # Validate coordinate bounds for WGS-84
            if res_y < -90.0 or res_y > 90.0:
                raise ValueError(
                    f"Invalid latitude {res_y}: must be between -90.0 and 90.0 degrees"
                )
            if res_x < -180.0 or res_x > 180.0:
                raise ValueError(
                    f"Invalid longitude {res_x}: must be between -180.0 and 180.0 degrees"
                )
        else:
            # Cartesian
            if x is not None:
                res_x = float(x)
            elif longitude is not None:
                res_x = float(longitude)
            else:
                raise ValueError("Cartesian Point requires 'x'")

            if y is not None:
                res_y = float(y)
            elif latitude is not None:
                res_y = float(latitude)
            else:
                raise ValueError("Cartesian Point requires 'y'")

            if is_3d_point:
                if z is not None:
                    res_z = float(z)
                elif height is not None:
                    res_z = float(height)
                else:
                    res_z = 0.0

        self._srid = resolved_srid
        self._x = res_x
        self._y = res_y
        self._z = res_z
        self._is_3d = is_3d_point
        self._is_geo = is_geo
        self._crs = _SRID_TO_CRS.get(
            resolved_srid,
            "wgs-84-3d"
            if (is_geo and is_3d_point)
            else "wgs-84"
            if is_geo
            else "cartesian-3d"
            if is_3d_point
            else "cartesian",
        )

        if HAS_VOYAGER_RS and AstExpr is not None:
            try:
                self._native_expr = AstExpr.function(
                    "point", [AstExpr.literal(self.to_cypher_dict())]
                )
            except Exception:
                self._native_expr = None

    @property
    def srid(self) -> int:
        """Spatial Reference System Identifier."""
        return self._srid

    @property
    def crs(self) -> str:
        """Coordinate Reference System name."""
        return self._crs

    @property
    def is_3d(self) -> bool:
        """True if this point is three-dimensional."""
        return self._is_3d

    @property
    def is_geographic(self) -> bool:
        """True if this point uses WGS-84 geographic coordinates."""
        return self._is_geo

    @property
    def x(self) -> float:
        """X coordinate (longitude for WGS-84)."""
        return self._x

    @property
    def y(self) -> float:
        """Y coordinate (latitude for WGS-84)."""
        return self._y

    @property
    def z(self) -> float | None:
        """Z coordinate (height for WGS-84, or None if 2D)."""
        return self._z

    @property
    def latitude(self) -> float | None:
        """Latitude in degrees for WGS-84 points (None for Cartesian)."""
        return self._y if self._is_geo else None

    @property
    def longitude(self) -> float | None:
        """Longitude in degrees for WGS-84 points (None for Cartesian)."""
        return self._x if self._is_geo else None

    @property
    def height(self) -> float | None:
        """Height in meters for 3D WGS-84 points (None if 2D or Cartesian)."""
        return self._z if (self._is_geo and self._is_3d) else None

    def distance(self, other: Point) -> float:
        """Calculates in-memory distance to another Point.

        Uses the Haversine formula (Earth radius 6,371,008.8m) for WGS-84 geographic points,
        and Euclidean distance for Cartesian points. Strictly in-memory calculation.
        For AST query expression building, use `distance_to(other)`.

        Note:
            When calculating distance between mixed 2D and 3D points, the height (or z coordinate)
            is ignored and 2D surface distance is computed.

        Args:
            other: Another Point instance.

        Returns:
            Distance in meters (for WGS-84) or coordinate units (for Cartesian) as a float.

        Raises:
            TypeError: If other is not a Point.
            ValueError: If coordinate reference systems are incompatible.
        """
        if not isinstance(other, Point):
            raise TypeError(
                f"Point.distance() calculates in-memory distance and requires another Point instance, "
                f"got {type(other).__name__}. For query expressions, use Point.distance_to()."
            )

        if self._is_geo != other._is_geo:
            raise ValueError(
                f"Cannot calculate distance between incompatible CRSs: {self.crs} (SRID {self.srid}) "
                f"and {other.crs} (SRID {other.srid})"
            )

        if self._is_geo:
            assert self.latitude is not None and self.longitude is not None
            assert other.latitude is not None and other.longitude is not None
            d = _haversine_distance(self.latitude, self.longitude, other.latitude, other.longitude)
            if (
                self._is_3d
                and other._is_3d
                and self.height is not None
                and other.height is not None
            ):
                dh = other.height - self.height
                return math.sqrt(d * d + dh * dh)
            return d
        else:
            dx = other.x - self.x
            dy = other.y - self.y
            if self._is_3d and other._is_3d and self.z is not None and other.z is not None:
                dz = other.z - self.z
                return math.sqrt(dx * dx + dy * dy + dz * dz)
            return math.sqrt(dx * dx + dy * dy)

    def distance_to(self, other: Any) -> FunctionExpr:
        """Creates a spatial distance AST expression `point.distance(self, other)` for query building.

        Args:
            other: A Point, BoundField, or Expression.

        Returns:
            FunctionExpr representing the spatial distance calculation.
        """
        return FunctionExpr("point.distance", [self, to_expression(other)])

    def to_cypher_dict(self) -> dict[str, Any]:
        """Returns the dictionary representation recognized by openCypher `point({...})`."""
        if self._is_geo:
            d: dict[str, Any] = {
                "latitude": self._y,
                "longitude": self._x,
            }
            if self._is_3d and self._z is not None:
                d["height"] = self._z
            return d
        else:
            d = {
                "x": self._x,
                "y": self._y,
            }
            if self._is_3d and self._z is not None:
                d["z"] = self._z
            return d

    def to_dict(self) -> dict[str, Any]:
        """Serializes this Point to a standard dictionary."""
        d: dict[str, Any] = {
            "srid": self._srid,
            "crs": self._crs,
            "x": self._x,
            "y": self._y,
        }
        if self._is_3d:
            d["z"] = self._z
        if self._is_geo:
            d["latitude"] = self._y
            d["longitude"] = self._x
            if self._is_3d:
                d["height"] = self._z
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Point:
        """Constructs a Point from a dictionary representation.

        Supports Bolt wire dictionaries (`srid`, `x`, `y`, `z`), geographic dictionaries
        (`latitude`, `longitude`, `height`), and Cartesian dictionaries (`x`, `y`, `z`).
        """
        if "__voyager_spatial__" in data and isinstance(data["__voyager_spatial__"], dict):
            data = data["__voyager_spatial__"]

        srid = data.get("srid")
        crs = data.get("crs")

        lat = data.get("latitude")
        lon = data.get("longitude")
        height = data.get("height")

        x = data.get("x")
        y = data.get("y")
        z = data.get("z")

        return cls(
            latitude=lat,
            longitude=lon,
            height=height,
            x=x,
            y=y,
            z=z,
            srid=srid,
            crs=crs,
        )

    @classmethod
    def from_spatial(cls, val: Any) -> Point:
        """Constructs a Point from a spatial object, dict, or existing Point.

        Supports:
        - voyager_ogm.Point instances (returned as-is)
        - Dictionaries (delegated to Point.from_dict)
        - Neo4j / Bolt spatial objects (e.g. neo4j.spatial.WGS84Point, CartesianPoint)
        - Duck-typed spatial objects with x, y, and optional srid/z/latitude/longitude
        """
        if isinstance(val, Point):
            return val
        if isinstance(val, dict):
            return cls.from_dict(val)
        if hasattr(val, "srid") and hasattr(val, "x") and hasattr(val, "y"):
            srid = getattr(val, "srid", None)
            is_geo = (srid in (WGS_84_2D, WGS_84_3D)) or hasattr(val, "latitude")
            if is_geo and hasattr(val, "latitude") and val.latitude is not None:
                return cls(
                    latitude=val.latitude,
                    longitude=getattr(val, "longitude", getattr(val, "x", None)),
                    height=getattr(val, "height", getattr(val, "z", None)),
                    srid=srid,
                )
            return cls(
                x=val.x,
                y=val.y,
                z=getattr(val, "z", None),
                srid=srid,
            )
        if hasattr(val, "latitude") and hasattr(val, "longitude"):
            return cls(
                latitude=val.latitude,
                longitude=val.longitude,
                height=getattr(val, "height", None),
                srid=getattr(val, "srid", None),
            )
        raise TypeError(f"Cannot convert {type(val).__name__} to Point")

    def to_arrow(self) -> pa.Scalar | dict[str, Any]:
        """Converts this Point into a PyArrow struct scalar (or dict fallback)."""
        try:
            import pyarrow as pa

            fields = [
                ("srid", pa.int64()),
                ("x", pa.float64()),
                ("y", pa.float64()),
            ]
            if self._is_3d:
                fields.append(("z", pa.float64()))
            if self._is_geo:
                fields.append(("latitude", pa.float64()))
                fields.append(("longitude", pa.float64()))
                if self._is_3d:
                    fields.append(("height", pa.float64()))
            struct_type = pa.struct(fields)
            return pa.scalar(self.to_dict(), type=struct_type)
        except ImportError:
            return self.to_dict()

    @classmethod
    def from_arrow(cls, scalar: Any) -> Point:
        """Constructs a Point from a PyArrow struct scalar."""
        if hasattr(scalar, "as_py"):
            d = scalar.as_py()
            if isinstance(d, dict):
                return cls.from_dict(d)
        if isinstance(scalar, dict):
            return cls.from_dict(scalar)
        raise TypeError(f"Cannot convert {type(scalar).__name__} to Point")

    def to_polars(self) -> pl.Series | dict[str, Any]:
        """Converts this Point into a Polars Series (struct representation)."""
        try:
            import polars as pl

            return pl.Series("point", [self.to_dict()])
        except ImportError:
            return self.to_dict()

    @classmethod
    def from_polars(cls, data: Any) -> Point:
        """Constructs a Point from a Polars Series, DataFrame, or struct element."""
        if hasattr(data, "to_list"):
            items = data.to_list()
            if items and isinstance(items[0], dict):
                return cls.from_dict(items[0])
        if hasattr(data, "to_dicts"):
            dicts = data.to_dicts()
            if dicts:
                return cls.from_dict(dicts[0])
        if isinstance(data, dict):
            return cls.from_dict(data)
        raise TypeError(f"Cannot convert {type(data).__name__} to Point")

    def to_spec(self) -> tuple[str, str, list[Any]]:
        """Converts this Point into an AST function specification tuple."""
        return ("fn", "point", [LiteralExpr(self.to_cypher_dict()).to_spec()])

    def __eq__(self, other: Any) -> Any:  # type: ignore[override]
        if isinstance(other, Point):
            if self._srid != other._srid:
                return False
            coords_match = math.isclose(self._x, other._x, abs_tol=1e-7) and math.isclose(
                self._y, other._y, abs_tol=1e-7
            )
            if not coords_match:
                return False
            if self._is_3d != other._is_3d:
                return False
            if self._is_3d:
                return math.isclose(self._z or 0.0, other._z or 0.0, abs_tol=1e-7)
            return True
        if (
            isinstance(other, dict)
            or (hasattr(other, "srid") and hasattr(other, "x") and hasattr(other, "y"))
            or (hasattr(other, "latitude") and hasattr(other, "longitude"))
        ):
            try:
                coerced = Point.from_spatial(other)
                return self == coerced
            except Exception:
                return False
        if isinstance(other, Expression):
            return super().__eq__(other)
        return False

    def __ne__(self, other: Any) -> Any:  # type: ignore[override]
        if (
            isinstance(other, (Point, dict))
            or (hasattr(other, "srid") and hasattr(other, "x"))
            or (hasattr(other, "latitude") and hasattr(other, "longitude"))
        ):
            eq_res = self.__eq__(other)
            if isinstance(eq_res, bool):
                return not eq_res
        if isinstance(other, Expression):
            return super().__ne__(other)
        return True

    def __hash__(self) -> int:
        return hash(
            (
                self._srid,
                round(self._x, 5),
                round(self._y, 5),
                round(self._z, 5) if self._z is not None else None,
            )
        )

    def __repr__(self) -> str:
        if self._is_geo:
            if self._is_3d:
                return f"Point(latitude={self.latitude}, longitude={self.longitude}, height={self.height})"
            return f"Point(latitude={self.latitude}, longitude={self.longitude})"
        else:
            if self._is_3d:
                return f"Point(x={self.x}, y={self.y}, z={self.z}, crs='{self.crs}')"
            return f"Point(x={self.x}, y={self.y}, crs='{self.crs}')"


def distance(point1: Any, point2: Any) -> float:
    """Calculates in-memory spatial distance between two Point instances.

    Strictly in-memory calculation. For AST query expressions, use `fn.point.distance()`
    or `Point.distance_to()`.

    Args:
        point1: First Point instance.
        point2: Second Point instance.

    Returns:
        Float distance in meters (WGS-84) or coordinate units (Cartesian).

    Raises:
        TypeError: If either argument is not a Point instance.
    """
    if not (isinstance(point1, Point) and isinstance(point2, Point)):
        raise TypeError(
            "distance() calculates in-memory distance between two Point instances. "
            "For query expressions, use fn.point.distance() or Point.distance_to()."
        )
    return point1.distance(point2)


def serialize_param_value(val: Any) -> Any:
    """Recursively serializes rich types (such as Point) to database-compatible parameter values.

    Maps `Point` objects to a tagged dictionary `{"__voyager_spatial__": ...}`.
    """
    if isinstance(val, Point):
        return {"__voyager_spatial__": val.to_dict()}
    if hasattr(val, "to_cypher_dict"):
        d = val.to_cypher_dict()
        if hasattr(val, "srid"):
            d["srid"] = val.srid
        return {"__voyager_spatial__": d}
    if isinstance(val, dict):
        return {k: serialize_param_value(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [serialize_param_value(item) for item in val]
    return val


def unwrap_spatial_param(val: Any) -> Any:
    """Recursively unwraps tagged spatial values `{"__voyager_spatial__": ...}` into plain Cypher/map dicts."""
    if isinstance(val, dict):
        if "__voyager_spatial__" in val:
            sp = val["__voyager_spatial__"]
            if isinstance(sp, dict):
                if "latitude" in sp and "longitude" in sp:
                    res: dict[str, Any] = {
                        "latitude": sp["latitude"],
                        "longitude": sp["longitude"],
                    }
                    if "srid" in sp and sp["srid"] is not None:
                        res["srid"] = sp["srid"]
                    if "crs" in sp and sp["crs"] is not None:
                        res["crs"] = sp["crs"]
                    if "height" in sp and sp["height"] is not None:
                        res["height"] = sp["height"]
                    return res
                if "x" in sp and "y" in sp:
                    res = {"x": sp["x"], "y": sp["y"]}
                    if "srid" in sp and sp["srid"] is not None:
                        res["srid"] = sp["srid"]
                    if "crs" in sp and sp["crs"] is not None:
                        res["crs"] = sp["crs"]
                    if "z" in sp and sp["z"] is not None:
                        res["z"] = sp["z"]
                    return res
            return sp
        return {k: unwrap_spatial_param(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [unwrap_spatial_param(x) for x in val]
    return val


_ISO_DURATION_REGEX = re.compile(
    r"^(?P<sign>[+-])?P"
    r"(?:(?P<years>[+-]?\d+(?:\.\d+)?)Y)?"
    r"(?:(?P<months>[+-]?\d+(?:\.\d+)?)M)?"
    r"(?:(?P<weeks>[+-]?\d+(?:\.\d+)?)W)?"
    r"(?:(?P<days>[+-]?\d+(?:\.\d+)?)D)?"
    r"(?:T"
    r"(?:(?P<hours>[+-]?\d+(?:\.\d+)?)H)?"
    r"(?:(?P<minutes>[+-]?\d+(?:\.\d+)?)M)?"
    r"(?:(?P<seconds>[+-]?\d+(?:\.\d+)?)S)?"
    r")?$"
)


def parse_iso_duration(val: str) -> datetime.timedelta:
    """Parses an ISO-8601 duration string into a datetime.timedelta.

    Supported formats include:
        - Full ISO: 'P1Y2M3DT4H5M6.789S'
        - Time only: 'PT1H30M', 'PT0.5S'
        - Date only: 'P2D', 'P1W'
        - Zero: 'PT0S', 'P0D'
        - Negative: '-PT5M'

    Note:
        Year and month components are approximated as 365.25 days and 30.4375 days
        respectively when converting to a fixed-duration timedelta, which loses
        calendar month-boundary fidelity by design (e.g. 'P1M' does not preserve
        differing calendar month lengths on round-trip).
    """
    val = val.strip().upper()
    match = _ISO_DURATION_REGEX.match(val)
    if not match:
        raise ValueError(f"Invalid ISO-8601 duration format: {val}")

    parts = match.groupdict()
    # ISO-8601 requires at least one component designator; bare 'P' or 'PT' is invalid
    if not any(
        parts[k] is not None
        for k in ("years", "months", "weeks", "days", "hours", "minutes", "seconds")
    ):
        raise ValueError(
            f"Invalid ISO-8601 duration format: {val} (must contain at least one designator)"
        )

    sign = -1 if parts["sign"] == "-" else 1

    years = float(parts["years"]) if parts["years"] is not None else 0.0
    months = float(parts["months"]) if parts["months"] is not None else 0.0
    weeks = float(parts["weeks"]) if parts["weeks"] is not None else 0.0
    days = float(parts["days"]) if parts["days"] is not None else 0.0
    hours = float(parts["hours"]) if parts["hours"] is not None else 0.0
    minutes = float(parts["minutes"]) if parts["minutes"] is not None else 0.0
    seconds = float(parts["seconds"]) if parts["seconds"] is not None else 0.0

    total_days = days + (weeks * 7.0) + (months * 30.4375) + (years * 365.25)
    total_seconds = (hours * 3600.0) + (minutes * 60.0) + seconds

    td = datetime.timedelta(days=total_days, seconds=total_seconds)
    return td if sign == 1 else -td


def to_iso_duration(td: datetime.timedelta) -> str:
    """Formats a datetime.timedelta as an ISO-8601 duration string (e.g. 'P2DT3H4M5S')."""
    total_seconds = td.total_seconds()
    if total_seconds == 0:
        return "PT0S"

    sign = "-" if total_seconds < 0 else ""
    abs_td = abs(td)

    days = abs_td.days
    secs = abs_td.seconds
    micros = abs_td.microseconds

    hours = secs // 3600
    minutes = (secs % 3600) // 60
    seconds = secs % 60

    parts = [f"{sign}P"]
    if days > 0:
        parts.append(f"{days}D")

    time_parts = []
    if hours > 0:
        time_parts.append(f"{hours}H")
    if minutes > 0:
        time_parts.append(f"{minutes}M")
    if seconds > 0 or micros > 0:
        if micros > 0:
            sec_float = seconds + micros / 1_000_000.0
            formatted = f"{sec_float:.6f}".rstrip("0").rstrip(".")
            time_parts.append(f"{formatted}S")
        else:
            time_parts.append(f"{seconds}S")

    if time_parts:
        parts.append("T" + "".join(time_parts))
    elif days == 0:
        parts.append("T0S")

    return "".join(parts)


@dataclass(frozen=True, slots=True, eq=False)
class Duration:
    """Calendar and nanosecond-resolution duration returned by graph and SQL queries.

    Represents ``(months, days, seconds, nanoseconds)`` without lossy month-to-day
    approximation or sub-microsecond truncation. When ``months == 0`` and
    ``nanoseconds % 1000 == 0``, :meth:`to_timedelta` converts losslessly to
    :class:`datetime.timedelta`.

    Equality and hashing compare ``(months, days, seconds, nanoseconds)``
    component-wise after normalizing only ``seconds``/``nanoseconds`` carry (for
    example ``Duration(seconds=1, nanoseconds=-500_000_000) == Duration(nanoseconds=500_000_000)``),
    without folding ``days`` into ``seconds``, matching how Neo4j and PostgreSQL
    preserve ``days`` separately from ``seconds``.
    """

    months: int = 0
    days: int = 0
    seconds: int = 0
    nanoseconds: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "months", int(self.months))
        object.__setattr__(self, "days", int(self.days))
        object.__setattr__(self, "seconds", int(self.seconds))
        object.__setattr__(self, "nanoseconds", int(self.nanoseconds))

    @property
    def total_nanoseconds(self) -> int:
        """Total nanoseconds of the ``(days, seconds, nanoseconds)`` part (excluding ``months``)."""
        return (self.days * 86_400 + self.seconds) * 1_000_000_000 + self.nanoseconds

    def _normalized_subday(self) -> tuple[int, int]:
        """Normalizes ``seconds`` and ``nanoseconds`` carry with shared sign toward zero."""
        total_ns = self.seconds * 1_000_000_000 + self.nanoseconds
        if total_ns == 0:
            return (0, 0)
        sign = -1 if total_ns < 0 else 1
        abs_ns = abs(total_ns)
        return (sign * (abs_ns // 1_000_000_000), sign * (abs_ns % 1_000_000_000))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Duration):
            return NotImplemented
        return (
            self.months == other.months
            and self.days == other.days
            and self._normalized_subday() == other._normalized_subday()
        )

    def __hash__(self) -> int:
        norm_secs, norm_ns = self._normalized_subday()
        return hash((self.months, self.days, norm_secs, norm_ns))

    def to_timedelta(self) -> datetime.timedelta:
        """Converts to :class:`datetime.timedelta` when ``months == 0`` and microsecond-exact."""
        if self.months != 0:
            raise ValueError(
                f"Cannot losslessly convert calendar Duration with months={self.months} to datetime.timedelta"
            )
        if self.nanoseconds % 1_000 != 0:
            raise ValueError(
                f"Cannot losslessly convert Duration with sub-microsecond nanoseconds={self.nanoseconds} to datetime.timedelta"
            )
        return datetime.timedelta(
            days=self.days,
            seconds=self.seconds,
            microseconds=self.nanoseconds // 1_000,
        )

    @classmethod
    def from_timedelta(cls, td: datetime.timedelta) -> Duration:
        """Constructs an exact :class:`Duration` from a :class:`datetime.timedelta`."""
        return cls(
            months=0,
            days=td.days,
            seconds=td.seconds,
            nanoseconds=td.microseconds * 1_000,
        )

    def to_iso(self) -> str:
        """Formats this duration as an ISO-8601 duration string."""
        total_ns = self.seconds * 1_000_000_000 + self.nanoseconds
        if self.months == 0 and self.days == 0 and total_ns == 0:
            return "PT0S"

        all_non_pos = self.months <= 0 and self.days <= 0 and total_ns <= 0
        if all_non_pos:
            m_val = abs(self.months)
            d_val = abs(self.days)
            ns_val = abs(total_ns)
            parts = ["-P"]
        else:
            m_val = self.months
            d_val = self.days
            ns_val = total_ns
            parts = ["P"]

        if m_val:
            years = int(m_val / 12)
            rem_months = m_val - years * 12
            if years:
                parts.append(f"{years}Y")
            if rem_months:
                parts.append(f"{rem_months}M")
        if d_val:
            parts.append(f"{d_val}D")
        if ns_val:
            parts.append("T")
            t_sign = "-" if ns_val < 0 else ""
            abs_ns = abs(ns_val)
            total_secs, sub_ns = divmod(abs_ns, 1_000_000_000)
            h, rem_s = divmod(total_secs, 3_600)
            m, s = divmod(rem_s, 60)
            if h:
                parts.append(f"{t_sign}{h}H")
            if m:
                parts.append(f"{t_sign}{m}M")
            if s or sub_ns:
                if sub_ns:
                    frac = f"{sub_ns:09d}".rstrip("0")
                    parts.append(f"{t_sign}{s}.{frac}S")
                else:
                    parts.append(f"{t_sign}{s}S")
        return "".join(parts)

    @classmethod
    def from_iso(cls, val: str) -> Duration:
        """Parses an ISO-8601 duration string into a :class:`Duration`."""
        text = val.strip().upper()
        match = _ISO_DURATION_REGEX.match(text)
        if not match:
            raise ValueError(f"Invalid ISO-8601 duration format: {val}")
        parts = match.groupdict()
        if not any(
            parts[k] is not None
            for k in ("years", "months", "weeks", "days", "hours", "minutes", "seconds")
        ):
            raise ValueError(
                f"Invalid ISO-8601 duration format: {val} (must contain at least one designator)"
            )
        sign = -1 if parts["sign"] == "-" else 1
        years = int(float(parts["years"])) if parts["years"] is not None else 0
        months = int(float(parts["months"])) if parts["months"] is not None else 0
        weeks = int(float(parts["weeks"])) if parts["weeks"] is not None else 0
        days = int(float(parts["days"])) if parts["days"] is not None else 0
        hours = int(float(parts["hours"])) if parts["hours"] is not None else 0
        minutes = int(float(parts["minutes"])) if parts["minutes"] is not None else 0
        sec_str = parts["seconds"]
        if sec_str is None:
            whole_secs = 0
            nanos = 0
            sec_sign = 1
        else:
            sec_sign = -1 if sec_str.startswith("-") else 1
            unsigned_sec = sec_str.lstrip("+-")
            if "." in unsigned_sec:
                whole_part, frac_part = unsigned_sec.split(".", 1)
                whole_secs = int(whole_part)
                padded = (frac_part + "000000000")[:9]
                nanos = int(padded)
            else:
                whole_secs = int(unsigned_sec)
                nanos = 0
        total_months = sign * (years * 12 + months)
        total_days = sign * (weeks * 7 + days)
        total_seconds = sign * (hours * 3600 + minutes * 60 + sec_sign * whole_secs)
        total_nanos = sign * sec_sign * nanos
        return cls(
            months=total_months, days=total_days, seconds=total_seconds, nanoseconds=total_nanos
        )

    def __str__(self) -> str:
        return self.to_iso()


_PG_INTERVAL_UNITS = frozenset(
    {
        "year",
        "years",
        "yr",
        "yrs",
        "y",
        "mon",
        "mons",
        "month",
        "months",
        "day",
        "days",
        "d",
        "hour",
        "hours",
        "hr",
        "hrs",
        "h",
        "min",
        "mins",
        "minute",
        "minutes",
        "m",
        "sec",
        "secs",
        "second",
        "seconds",
        "s",
    }
)


def _parse_pg_decimal_seconds(raw: str) -> tuple[int, int]:
    """Parses a signed decimal seconds token into ``(seconds, nanoseconds)``."""
    sign = -1 if raw.startswith("-") else 1
    unsigned = raw.lstrip("+-")
    if "." in unsigned:
        whole_part, frac_part = unsigned.split(".", 1)
        whole = int(whole_part) if whole_part else 0
        nanos = int((frac_part + "000000000")[:9])
    else:
        whole = int(unsigned)
        nanos = 0
    return (sign * whole, sign * nanos)


def parse_pg_interval_text(raw: str) -> Duration:
    """Parses PostgreSQL ``interval`` text output into a :class:`Duration`.

    Supports all four PostgreSQL ``IntervalStyle`` formats: ``postgres``,
    ``postgres_verbose``, ``sql_standard``, and ``iso_8601``.
    """
    text = raw.strip()
    if not text:
        raise ValueError("Empty PostgreSQL interval string")
    if text[0] in ("P", "p") or text.startswith(("-P", "-p", "+P", "+p")):
        return Duration.from_iso(text)

    if text.startswith("@"):
        text = text[1:].strip()

    months = 0
    days = 0
    seconds = 0
    nanoseconds = 0

    tokens = text.split()
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if ":" in tok:
            sign = -1 if tok.startswith("-") else 1
            hms = tok.lstrip("+-")
            hms_parts = hms.split(":")
            if len(hms_parts) != 3:
                raise ValueError(f"Invalid PostgreSQL interval time part: {tok!r}")
            h = int(hms_parts[0])
            m = int(hms_parts[1])
            sec_part = hms_parts[2]
            if "." in sec_part:
                s_whole, s_frac = sec_part.split(".", 1)
                sec_int = int(s_whole)
                ns_int = int((s_frac + "000000000")[:9])
            else:
                sec_int = int(sec_part)
                ns_int = 0
            seconds += sign * (h * 3600 + m * 60 + sec_int)
            nanoseconds += sign * ns_int
            i += 1
        elif tok.lower() == "ago":
            months = -months
            days = -days
            seconds = -seconds
            nanoseconds = -nanoseconds
            i += 1
        elif "-" in tok.lstrip("+-") and "." not in tok:
            ym_sign = -1 if tok.startswith("-") else 1
            ym_body = tok.lstrip("+-")
            y_str, m_str = ym_body.split("-", 1)
            if not (y_str.isdigit() and m_str.isdigit()):
                raise ValueError(
                    f"Invalid PostgreSQL sql_standard year-month token {tok!r} in {raw!r}"
                )
            months += ym_sign * (int(y_str) * 12 + int(m_str))
            i += 1
        elif i + 1 < len(tokens) and tokens[i + 1].lower() in _PG_INTERVAL_UNITS:
            unit = tokens[i + 1].lower().rstrip("s")
            if unit in ("year", "yr", "y"):
                months += int(tok) * 12
            elif unit in ("mon", "month"):
                months += int(tok)
            elif unit in ("day", "d"):
                days += int(tok)
            elif unit in ("hour", "hr", "h"):
                seconds += int(tok) * 3600
            elif unit in ("min", "minute", "m"):
                seconds += int(tok) * 60
            elif unit in ("sec", "second"):
                s_int, ns_int = _parse_pg_decimal_seconds(tok)
                seconds += s_int
                nanoseconds += ns_int
            else:
                raise ValueError(f"Unknown PostgreSQL interval unit {tokens[i + 1]!r} in {raw!r}")
            i += 2
        elif i + 1 >= len(tokens) or tokens[i + 1][0] in "+-0123456789" or ":" in tokens[i + 1]:
            days += int(tok)
            i += 1
        else:
            raise ValueError(f"Unknown PostgreSQL interval unit {tokens[i + 1]!r} in {raw!r}")

    return Duration(months=months, days=days, seconds=seconds, nanoseconds=nanoseconds)


def _hydrate_date(val: Any) -> Any:
    """Hydrates raw driver temporal values, ISO strings, or ordinal numbers into datetime.date."""
    if val is None:
        return None
    if isinstance(val, datetime.date) and not isinstance(val, datetime.datetime):
        return val
    if isinstance(val, datetime.datetime):
        return val.date()
    if hasattr(val, "to_native"):
        try:
            native = val.to_native()
            if isinstance(native, datetime.datetime):
                return native.date()
            if isinstance(native, datetime.date):
                return native
        except Exception:
            pass
    if hasattr(val, "year") and hasattr(val, "month") and hasattr(val, "day"):
        try:
            return datetime.date(val.year, val.month, val.day)
        except Exception:
            pass
    if isinstance(val, bytes):
        val = val.decode("utf-8")
    if isinstance(val, str):
        val = val.strip()
        try:
            return datetime.date.fromisoformat(val)
        except ValueError:
            try:
                clean = val.replace("Z", "+00:00")
                if "[" in clean:
                    clean = clean.split("[", 1)[0]
                return datetime.datetime.fromisoformat(clean).date()
            except Exception:
                pass
    if isinstance(val, (int, float)):
        try:
            # Heuristic epoch classification:
            # - |val| < 100,000 (~273 years): interpreted as epoch days (e.g. Neo4j Bolt Date epoch_days).
            # - |val| >= 100,000: interpreted as Unix epoch seconds.
            # Ambiguity window: raw Unix timestamps between 0 and 100,000 (Jan 1 to Jan 2 1970)
            # would be treated as epoch days. Graph protocols send epoch days for dates.
            if abs(val) < 100_000:
                return datetime.date(1970, 1, 1) + datetime.timedelta(days=int(val))
            return datetime.datetime.fromtimestamp(val, tz=datetime.UTC).date()
        except Exception:
            pass
    return val


def _hydrate_time(val: Any) -> Any:
    """Hydrates raw driver temporal values or ISO strings into datetime.time."""
    if val is None:
        return None
    if isinstance(val, datetime.time):
        return val
    if hasattr(val, "to_native"):
        try:
            native = val.to_native()
            if isinstance(native, datetime.time):
                return native
        except Exception:
            pass
    if hasattr(val, "hour") and hasattr(val, "minute") and hasattr(val, "second"):
        try:
            nanos = getattr(val, "nanosecond", 0)
            tzinfo = getattr(val, "tzinfo", None)
            return datetime.time(
                val.hour, val.minute, val.second, microsecond=nanos // 1000, tzinfo=tzinfo
            )
        except Exception:
            pass
    if isinstance(val, bytes):
        val = val.decode("utf-8")
    if isinstance(val, str):
        val = val.strip()
        try:
            return datetime.time.fromisoformat(val)
        except ValueError:
            try:
                clean = val.replace("Z", "+00:00")
                return datetime.datetime.fromisoformat(clean).timetz()
            except Exception:
                pass
    return val


def _hydrate_datetime(val: Any) -> Any:
    """Hydrates raw driver temporal values, ISO strings, or Unix timestamps into datetime.datetime."""
    if val is None:
        return None
    if isinstance(val, datetime.datetime):
        return val
    if hasattr(val, "to_native"):
        try:
            native = val.to_native()
            if isinstance(native, datetime.datetime):
                return native
        except Exception:
            pass
    if (
        hasattr(val, "year")
        and hasattr(val, "month")
        and hasattr(val, "day")
        and hasattr(val, "hour")
    ):
        try:
            nanos = getattr(val, "nanosecond", 0)
            tzinfo = getattr(val, "tzinfo", None)
            return datetime.datetime(
                val.year,
                val.month,
                val.day,
                val.hour,
                val.minute,
                val.second,
                microsecond=nanos // 1000,
                tzinfo=tzinfo,
            )
        except Exception:
            pass
    if isinstance(val, datetime.date) and not isinstance(val, datetime.datetime):
        return datetime.datetime(val.year, val.month, val.day)
    if isinstance(val, bytes):
        val = val.decode("utf-8")
    if isinstance(val, str):
        val = val.strip()
        tz_name = None
        clean = val
        if "[" in clean and clean.endswith("]"):
            clean, tz_name = clean[:-1].split("[", 1)
        clean = clean.replace("Z", "+00:00")
        try:
            dt = datetime.datetime.fromisoformat(clean)
            if tz_name:
                try:
                    import zoneinfo

                    # Offset-less bracketed strings carry local wall-clock time in tz_name;
                    # offset-bearing strings carry an exact instant to convert into tz_name.
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=zoneinfo.ZoneInfo(tz_name))
                    else:
                        dt = dt.astimezone(zoneinfo.ZoneInfo(tz_name))
                except Exception:
                    pass
            return dt
        except Exception:
            pass
    if isinstance(val, (int, float)):
        try:
            # Heuristic epoch timestamp classification:
            # - |val| >= 1e11 (100 billion): interpreted as epoch milliseconds (or scaled nanos).
            # - |val| < 1e11: interpreted as Unix epoch seconds.
            # Ambiguity window: epoch seconds beyond the year 5138 (1e11) would be treated as milliseconds.
            if abs(val) >= 1e11:
                return datetime.datetime.fromtimestamp(val / 1000.0, tz=datetime.UTC)
            return datetime.datetime.fromtimestamp(val, tz=datetime.UTC)
        except Exception:
            pass
    return val


def _hydrate_timedelta(val: Any) -> Any:
    """Hydrates raw driver Duration objects, ISO duration strings, or seconds into datetime.timedelta."""
    if val is None:
        return None
    if isinstance(val, datetime.timedelta):
        return val
    if (
        hasattr(val, "months")
        and hasattr(val, "days")
        and hasattr(val, "seconds")
        and hasattr(val, "nanoseconds")
    ):
        try:
            days = val.days + int(val.months * 30.4375)
            seconds = val.seconds
            micros = val.nanoseconds // 1000
            return datetime.timedelta(days=days, seconds=seconds, microseconds=micros)
        except Exception:
            pass
    if isinstance(val, bytes):
        val = val.decode("utf-8")
    if isinstance(val, str):
        try:
            return parse_iso_duration(val)
        except Exception:
            pass
    if isinstance(val, (int, float)):
        try:
            return datetime.timedelta(seconds=val)
        except Exception:
            pass
    return val
