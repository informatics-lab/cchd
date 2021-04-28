"""Cut a cube to the outline / bbox of a shapely geometry."""

import iris
import numpy as np
from shapely.geometry import box


def _dateline_centre(max_x):
    """
    Determine if the dateline is at the centre of the horizontal coord system.
    If it is, the interval of the x coord is (0, 360)deg.       --> result: True
    If it isn't, the interval of the x coord is (-180, 180)deg. --> result: False
    
    """
    return max_x > 180


def _translate_cube_to_geom(cube, geometry):
    """Translate the x-coord of a cube to match the x-coord interval of the geometry."""
    geom_xmax = geometry.bounds.loc[geometry.bounds.index[0]]['maxx']
    xc_name = cube.coords(axis="x", dim_coords=True)[0].name()
    cube_xmax = cube.coord(xc_name).points[-1]
    geom_x_dateline_centre = _dateline_centre(geom_xmax)
    cube_x_dateline_centre = _dateline_centre(cube_xmax)
    
    if geom_x_dateline_centre != cube_x_dateline_centre:
        if cube_x_dateline_centre:
            intersection_kwarg = {xc_name: (-180, 180)}
        else:
            intersection_kwarg = {xc_name: (0, 360)}
        cube = cube.intersection(**intersection_kwarg)
        
    return cube


def geom_bbox_cube(cube, geometry):
    """
    Extract a subcube from a cube at the extent of the bounding box
    of the geometry.
    
    Note: the geometry is assumed to be provided by a geodataframe.
    
    XXX not done: CRS handling.
    
    """
    cube = _translate_cube_to_geom(cube, geometry)

    xc_name = cube.coords(axis="x", dim_coords=True)[0].name()
    yc_name = cube.coords(axis="y", dim_coords=True)[0].name()
    
    bds = geometry.bounds
    min_x, min_y, max_x, max_y = bds.loc[bds.index[0]]
    
    coord_values = {xc_name: lambda cell: min_x <= cell <= max_x,
                    yc_name: lambda cell: min_y <= cell <= max_y}
    cstr = iris.Constraint(coord_values=coord_values)
    return cube.extract(cstr)


def geom_boundary_mask(cube, geometry):
    """
    Generate a 2D horizontal mask for the cube based on the boundary
    of the geometry.
    
    Note: the geometry is assumed to be provided by a geodataframe.
    
    """
    cube = _translate_cube_to_geom(cube, geometry)

    x_coord, = cube.coords(axis="x", dim_coords=True)
    y_coord, = cube.coords(axis="y", dim_coords=True)
    
    for coord in [x_coord, y_coord]:
        if not coord.has_bounds():
            coord.guess_bounds()

    x_shape, = x_coord.shape
    y_shape, = y_coord.shape
    x_points, y_points = np.meshgrid(np.arange(x_shape), np.arange(y_shape))
    flat_mask = []
    for (xi, yi) in zip(x_points.reshape(-1), y_points.reshape(-1)):
        x_lo, x_hi = x_coord[xi].bounds[0]
        y_lo, y_hi = y_coord[yi].bounds[0]
        cell = box(x_lo, y_lo, x_hi, y_hi)
        mask_point = geometry.intersects(cell)
        flat_mask.append(mask_point)
        
    mask_2d = np.array(flat_mask).reshape(cube.shape[-2:])
    return mask_2d


def _get_2d_field_and_dims(cube):
    """Get a 2D horizontal field, and the horizontal coord dims, from an nD cube."""
    cube_x_coord, = cube.coords(axis='X', dim_coords=True)
    cube_y_coord, = cube.coords(axis='Y', dim_coords=True)
    cube_x_coord_name = cube_x_coord.name()
    cube_y_coord_name = cube_y_coord.name()
    cube_x_coord_dim, = cube.coord_dims(cube_x_coord)
    cube_y_coord_dim, = cube.coord_dims(cube_y_coord)
    field_2d = cube.slices([cube_y_coord_name, cube_x_coord_name]).next()
    coord_2d_dims = sorted([cube_y_coord_dim, cube_x_coord_dim])
    return field_2d, coord_2d_dims


def cut_cube_to_shape(cube, geometry):
    """
    Call this function to subset the input cube to the boundary of the shapefile geometry.
    
    This is a two-step process:
      1. Cut the cube to the bounding box of the shapefile geoetry
      2. Mask the cut cube from (1.) to the boundary of the shapefile
         geometry.
         
    This two-step process is *much* faster than going directly to masking, as
    far fewer cells are compared to the geometry in step (2) this way.
    
    """
    # 1. extract subcube to the shapefile's bounding box.
    subcube = geom_bbox_cube(cube, geometry)
    
    # 2. mask the subcube to the shapefile boundary.
    try:
        cube_2d, dims_2d = _get_2d_field_and_dims(subcube)
        mask_2d = geom_boundary_mask(subcube, geometry)
    except:
        result = None
    else:
        full_mask = iris.util.broadcast_to_shape(mask_2d, subcube.shape, dims_2d)
        new_data = np.ma.array(subcube.data, mask=np.logical_not(full_mask))
        result = subcube.copy(data=new_data)

    return result