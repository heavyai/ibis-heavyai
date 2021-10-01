"""
Module with geospatial specific functionality.

Everything that depends on geopandas, shapely or other
geospatial libraries should go here. When this module
is imported, it should be checked if an `ImportError` is
raised, and assume no geospatial support can be provided
because the lack of the soft dependencies.
"""
import geopandas
import pandas as pd
import shapely.wkt
from omnisci.cursor import Cursor
from omnisci.dtypes import TDatumType as pyomnisci_dtype

from .client import OmniSciDBDefaultCursor


class OmniSciDBGeoCursor(OmniSciDBDefaultCursor):
    """Cursor that exports result to GeoPandas Data Frame."""

    def to_df(self):
        """Convert the cursor to a data frame.

        Returns
        -------
        dataframe : pandas.DataFrame
        """
        cursor = self.cursor

        if not isinstance(cursor, Cursor):
            if cursor is None:
                return geopandas.GeoDataFrame([])
            return cursor

        cursor_description = cursor.description

        col_names = [c.name for c in cursor_description]
        result = pd.DataFrame(cursor.fetchall(), columns=col_names)

        # get geo types from pyomnisci
        geotypes = (
            pyomnisci_dtype.POINT,
            pyomnisci_dtype.LINESTRING,
            pyomnisci_dtype.POLYGON,
            pyomnisci_dtype.MULTIPOLYGON,
            pyomnisci_dtype.GEOMETRY,
            pyomnisci_dtype.GEOGRAPHY,
        )

        geo_column = None

        for d in cursor_description:
            field_name = d.name
            if d.type_code in geotypes:
                # use the first geo column found as default geometry
                # geopandas doesn't allow multiple GeoSeries
                # to specify other column as a geometry on a GeoDataFrame
                # use something like: df.set_geometry('buffers').plot()
                geo_column = geo_column or field_name
                result[field_name] = result[field_name].apply(
                    shapely.wkt.loads
                )
        if geo_column:
            result = geopandas.GeoDataFrame(result, geometry=geo_column)
        return result
