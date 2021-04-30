"""
Separate out the processing functions for the realtime & parallel dashboard
into a separate module.

For more information, see:
  1. https://discourse.holoviz.org/t/can-i-load-data-asynchronously-in-panel/452/11
     (particularly "update 2" in that post), and
  2. https://discourse.bokeh.org/t/pickling-problem-bokeh-server/3394
     (linked in 1.)

"""

import warnings

import cartopy.io.shapereader as shpreader
import dask.bag as db
import geopandas as gpd
import iris
import pandas as pd

import shapecutter


class DataHolder(object):
    """A container for app-specific data and analysis methods."""
    def __init__(self):
        self._state_code_name = 'gn_a1_code'
        
        self._a1b_cube = None
        self._e1_cube = None
        self._us_states = None
        self._state_codes = None
        
    @property
    def a1b_cube(self):
        ref = "a1b"
        if self._a1b_cube is None:
            self.a1b_cube = self.prepare_cube(ref)
        return self._a1b_cube
    
    @a1b_cube.setter
    def a1b_cube(self, value):
        self._a1b_cube = value
        
    @property
    def e1_cube(self):
        ref = "e1"
        if self._e1_cube is None:
            self.e1_cube = self.prepare_cube(ref)
        return self._e1_cube
    
    @e1_cube.setter
    def e1_cube(self, value):
        self._e1_cube = value
        
    @property
    def us_states(self):
        if self._us_states is None:
            self.us_states = self.prepare_df()
        return self._us_states
    
    @us_states.setter
    def us_states(self, value):
        self._us_states = value
        
    @property
    def state_codes(self):
        if self._state_codes is None:
            self.state_codes = self.us_states['gn_a1_code']
        return self._state_codes
    
    @state_codes.setter
    def state_codes(self, value):
        self._state_codes = value

    def prepare_cube(self, ref):
        """Load a specific dataset from the Iris sample data as an Iris cube."""
        return iris.load_cube(iris.sample_data_path(f"{ref}_north_america.nc"))

    def prepare_df(self):
        """Load US states shapefile data."""
        us_states_name = shpreader.natural_earth(
            category="cultural",
            name="admin_1_states_provinces")
        us_states = gpd.read_file(us_states_name)

        # We only need to keep a few columns of data from the US states geodataframe (gdf).
        gdf_drop_cols = list(set(us_states.columns) - set(["name", self._state_code_name, "geometry"]))
        us_states = us_states.drop(columns=gdf_drop_cols)
        
        # Add columns for the area and the centre of the geometry.
        us_states["area"] = us_states.apply(lambda r: r["geometry"].area, axis=1)
        # We do these two separately for the sake of simplicity.
        us_states["latitude"] = us_states.apply(lambda r: r["geometry"].centroid.y, axis=1)
        us_states["longitude"] = us_states.apply(lambda r: r["geometry"].centroid.x, axis=1)
        
        return us_states

    def calculate_one_mean(self, state_code, rcp, year):
        """_
        Calculate one mean value, for a specific combination of US state (defined by its state code),
        RCP scenario and year.

        """
        cube = self.a1b_cube if rcp.lower() == "a1b" else self.e1_cube
        cstr = iris.Constraint(time=lambda cell: cell.point.year == int(year))
        year_cube = cube.extract(cstr)
        geometry = self.us_states[self.us_states[self._state_code_name] == state_code]["geometry"]
        state_year_cube = shapecutter.cut_cube_to_shape(year_cube, geometry)
        if state_year_cube is not None:
            collapsed_cube = state_year_cube.collapsed(["latitude", "longitude"], iris.analysis.MEAN)
            result = float(collapsed_cube.data)
        else:
            result = None
        return result
    
    def _update(self, state_code, year, rcp):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = self.calculate_one_mean(state_code, rcp, year)
        return (state_code, result)

    def _update_handler(self, args):
        return self._update(*args)
    
    def calculate_one_column(self, col_ref):
        """
        Calculate one column of results for the dataframe. This will contain the results
        for one specific combination of rcp and year, which is encoded in the column ref
        `col_ref`.
        
        """
        rcp, year = col_ref.split(",")
        if col_ref not in self.us_states.columns:
            seq = zip(self.state_codes, [year]*len(self.state_codes), [rcp]*len(self.state_codes))
            state_codes_bag = db.from_sequence(seq)
            results = state_codes_bag.map(self._update_handler).compute()
            states, temps = list(zip(*results))
            result_df = pd.DataFrame({self._state_code_name: states, col_ref: temps})
            self.us_states = pd.merge(self.us_states, result_df, on=self._state_code_name)