"""
PFRA Module for working with HEC-RAS model output files
"""
import gdal
from time import time
import geopandas as gpd
from geopandas.tools import sjoin
from shapely.ops import cascaded_union
from shapely.geometry import Point, LineString, Polygon
import numpy as np
import pandas as pd
import h5py
from matplotlib import pyplot as plt
from hecrasio.core import ResultsZip

# Add additional keys as needed
GEOMETRY_ATTRIBUTES = '/Geometry/2D Flow Areas/Attributes'
GEOMETRY_2DFLOW_AREA = '/Geometry/2D Flow Areas'

PLAN_DATA = '/Plan Data'
EVENT_DATA_BC = '/Event Conditions/Unsteady/Boundary Conditions'

UNSTEADY_SUMMARY = '/Results/Unsteady/Summary'
TSERIES_RESULTS_2DFLOW_AREA = '/Results/Unsteady/Output/Output Blocks/Base Output/Unsteady Time Series/2D Flow Areas'


class PFRAError:
    """
    Generic Error Class for PFRA
    """

    def __init__(self, error):
        self.Error = error


class HDFResultsFile:
    """
    HEC-RAS HDF Plan File Object to compute flow data at breaklines.
    Some functionality may be useful for other ras objects.
    """

    def __init__(self, model: ResultsZip, path: str):

        self.__model = model
        self.__zip_path = path

        def decoder():
            """
            Decode bytes objects from hdf file
            :return:
            """
            if isinstance(x, bytes):
                return x.decode()
            else:
                return x

        def local_hdf():
            """
            Add Description
            :return:
            """
            self.__model.zipfile.extract(self.__zip_path)
            return h5py.File(self.__zip_path, 'r')

        def get_2dFlowArea_data():
            """
            Add Description
            :return:
            """
            table_data = self._hdfLocal[GEOMETRY_ATTRIBUTES]
            names = table_data.dtype.names
            domain_data = {}
            # Use [1:-1] to pull the name from the 0 element (row[0])
            for row in table_data:
                domain_data[row[0].decode()] = list(row)[1:-1]
            return pd.DataFrame(domain_data, index=names[1:-1])

        def get_planData(table):
            """
            Add Description
            :param table:
            :return:
            """
            table_data = self._hdfLocal['{}/{}'.format(PLAN_DATA, table)].attrs
            values = [table_data[n] for n in list(table_data.keys())]
            # Add wrapper here?
            values = [v[0] if isinstance(v, list) else v for v in values]
            values = [v.decode() if isinstance(v, bytes) else v for v in values]
            return pd.DataFrame(data=values, index=list(table_data.keys()), columns=['Results'])
        
        def get_geometry_data(table, domain):
            """Read in data from results tables"""
            data = '{}/{}/{}'.format(GEOMETRY_2DFLOW_AREA, domain, table)
            return np.array(self._plan_data[data])

        def get_perimeter(domain):
            """Creates a perimeter polygon from points"""
            d_array = get_geometry_data('Perimeter', domain)
            aoi = Polygon([tuple(p) for p in d_array])
            return gpd.GeoDataFrame(geometry=gpd.GeoSeries(aoi))
        
        def get_domain_geometries():
            domains = self._domains
            if len(domains) > 1:
                poly_list = [get_perimeter(domain) for domain in domains]
                df = pd.concat(poly_list).reset_index(level=0, drop=True)
                return gpd.GeoDataFrame(df)
            else:
                print('Single domain found...')
                pass

        def get_2dSummary():
            """Add Description"""
            table_data = self._hdfLocal[UNSTEADY_SUMMARY].attrs
            values = [table_data[n] for n in list(table_data.keys())]
            values = [v.decode() if isinstance(v, bytes) else v for v in values]
            values = [str(v) if isinstance(v, list) else v for v in values]
            return pd.DataFrame(data=values, index=list(table_data.keys()), columns=['Results'])

        self._hdfLocal = local_hdf()
        self._plan_data = self._hdfLocal
        self._Plan_Information = get_planData('Plan Information')
        self._Plan_Parameters = get_planData('Plan Parameters')
        self._2dFlowArea = get_2dFlowArea_data()

        self._domains = self._2dFlowArea.columns.tolist()
        self._domain_polys = get_domain_geometries()
        self._summary = get_2dSummary()

    # Getter functions
    @property
    def hdfLocal(self):
        """Add Description"""
        return self._hdfLocal

    @property
    def domains(self):
        """Add Description"""
        return self._domains
    
    @property
    def domain_polys(self):
        """Domain Polygons"""
        return self._domain_polys

    @property
    def Plan_Information(self):
        """Add Description"""
        return self._Plan_Information

    @property
    def Plan_Parameters(self):
        """Add Description"""
        return self._Plan_Parameters

    @property
    def summary(self):
        """Add Description"""
        return self._summary

    @property
    def get_2dFlowArea(self):
        """Add Description"""
        return self._2dFlowArea


class DomainResults:
    """
    HEC-RAS HDF Plan File Object to compute flow data at breaklines.
    Some functionality may be useful for other ras objects.
    """

    def __init__(self, model: ResultsZip, plan: HDFResultsFile, domain: str):
        # Specify Domain to instantiate Object
        self.__model = model
        self._plan = plan
        self._domain = domain
        self._plan_data = self._plan.hdfLocal

        def get_domain_cell_size():
            """Identifies mean cell size for a domain"""
            flowData = self._plan.get_2dFlowArea.copy()
            flowData = flowData[self._domain]
            xspacing = flowData.loc['Spacing dx']
            yspacing = flowData.loc['Spacing dy']
            return np.mean([xspacing, yspacing])

        def get_tseries_results(table):
            """Read in data from results tables as a Pandas DataFrame"""
            data = '{}/{}/{}'.format(TSERIES_RESULTS_2DFLOW_AREA, self._domain, table)
            d_array = np.array(self._plan_data[data]).T
            return pd.DataFrame(d_array)

        def get_tseries_forcing(table):
            """This table is not domain specific"""
            group = list(self._plan_data['{}/{}'.format(EVENT_DATA_BC, table)])
            table_data = {}
            for g in group:
                table_data[g] = np.array(self._plan_data['{}/{}/{}'.format(EVENT_DATA_BC, table, g)])
            return table_data

        def get_geometry_data(table):
            """Read in data from results tables"""
            data = '{}/{}/{}'.format(GEOMETRY_2DFLOW_AREA, self._domain, table)
            return np.array(self._plan_data[data])

        def get_perimeter():
            """Creates a perimeter polygon from points"""
            d_array = get_geometry_data('Perimeter')
            aoi = Polygon([tuple(p) for p in d_array])
            return gpd.GeoDataFrame(geometry=gpd.GeoSeries(aoi))

        def get_face():
            """Returns GeoDataFrame with Faces per pair of Face Indices"""
            gdf = gpd.GeoDataFrame(self._Faces_FacePoint_Indexes, columns=['from_idx', 'to_idx'])
            gdf['face'] = gdf.apply(lambda row:
                                    LineString([self._Face_FacePoints_Coordinate[row['from_idx']],
                                                self._Face_FacePoints_Coordinate[row['to_idx']]]),
                                    axis=1)
            gdf['geometry'] = gdf['face']
            gdf = gdf.drop(['from_idx', 'to_idx', 'face'], axis=1)
            return gdf

        def get_centroids():
            """Returns GeoDataFrame with Face centroids per pair of Face Indices"""
            gdf = get_face()
            gdf['face_cnt'] = gdf.apply(lambda row: row.geometry.centroid, axis=1)
            gdf['geometry'] = gdf['face_cnt']
            gdf = gdf.drop(['face_cnt'], axis=1)
            return gdf

        def describe_depth():
            """Calculate max, min, and range of depths for each cell center"""
            # Pull in cell centroids and attribute them
            cc_array = self._Cells_Center_Coordinate
            cc_gdf = gpd.GeoDataFrame([Point([coord[0], coord[1]]) for coord in cc_array], columns=['geometry'])
            depth_array = self._Depth

            # Attribute cell centroids with depths
            # NOT USED?
            # cc_attr = pd.concat([cc_gdf, depth_array], axis=1)

            # Obtain descriptive statistics for each centroid
            max_attr = pd.DataFrame(depth_array.max(axis=1), columns=['max'])
            max_gdf = pd.concat([cc_gdf, max_attr], axis=1)
            max_gdf_nonzero = max_gdf[max_gdf['max'] != 0]

            min_attr = pd.DataFrame(depth_array.min(axis=1), columns=['min'])
            min_gdf = pd.concat([cc_gdf, min_attr], axis=1)
            min_gdf_nonzero = min_gdf[min_gdf['min'] != 0]
            return max_gdf_nonzero, min_gdf_nonzero

        def get_avg_depth():
            """Calculates average depth at faces returning an array."""
            depth_list = []
            for (c1_idx, c2_idx) in self._Faces_Cell_Indexes:
                # cat_depths = np.stack([self._Depth.loc[c1_idx], self._Depth.loc[c2_idx]])
                cat_depths = np.stack([self._Depth[c1_idx, :], self._Depth[c2_idx, :]])
                avg_face = np.average(cat_depths, axis=0)
                depth_list.append(np.around(avg_face, decimals=2))
                # np.stack use default axis=0
            return pd.DataFrame(np.stack(depth_list))

        def get_extreme_edge_depths():
            """Identifies Face Centroids with absolute, avgerage depths greater-than one foot"""
            # Obtain boundary line
            boundary_line = list(self._Perimeter['geometry'])[0].boundary
            # Identify external faces
            df = pd.DataFrame()
            df['exterior'] = self._Faces.geometry.apply(lambda lstring: lstring.intersects(boundary_line))

            # Identify minima
            attr = pd.DataFrame(abs(self._Avg_Face_Depth).max(axis=1), columns=['abs_max'])
            face_dp = pd.concat([self._Face_Centroid_Coordinates, attr], axis=1)
            exterior_faces = face_dp[df['exterior'] == True]
            return exterior_faces[exterior_faces['abs_max'] > 1]

        def get_extreme_edge_depths():
            """Identifies Face Centroids with absolute, avgerage depths greater-than one foot"""
            # Obtain boundary line
            boundary_line = list(self._Perimeter['geometry'])[0].boundary

            # Identify external faces
            df = pd.DataFrame()
            perimeter = gpd.GeoDataFrame(gpd.GeoSeries(boundary_line).to_frame(), geometry=0)
            intersections = gpd.sjoin(perimeter, self._Faces, how="inner", op='intersects')

            # Identify minima
            attr = pd.DataFrame(abs(self._Avg_Face_Depth).max(axis=1), columns=['abs_max'])
            face_dp = pd.concat([self._Face_Centroid_Coordinates, attr], axis=1)
            exterior_faces = face_dp.loc[intersections['index_right']]
            return exterior_faces[exterior_faces['abs_max'] > 1]

        try:
            self._StageBC = get_tseries_forcing('Stage Hydrographs')
        except KeyError as e:
            print(e)
            self._StageBC = None

        try:
            self._FlowBC = get_tseries_forcing('Flow Hydrographs')
        except KeyError as e:
            print(e)
            self._FlowBC = None

        try:
            self._PrecipBC = get_tseries_forcing('Precipitation Hydrographs')
        except KeyError as e:
            print(e)
            self._PrecipBC = None

        self._CellSize = get_domain_cell_size()
        self._Faces_FacePoint_Indexes = get_geometry_data('Faces FacePoint Indexes')
        self._Face_FacePoints_Coordinate = get_geometry_data('FacePoints Coordinate')
        self._Faces_Cell_Indexes = get_geometry_data('Faces Cell Indexes')
        self._Face_Velocity = abs(get_tseries_results('Face Velocity'))
        self._Face_Centroid_Coordinates = get_centroids()
        self._Cells_Center_Coordinate = get_geometry_data('Cells Center Coordinate')
        self._Depth = np.array(get_tseries_results('Depth'))
        self._Describe_Depths = describe_depth()
        self._Avg_Face_Depth = get_avg_depth()
        self._Perimeter = get_perimeter()
        self._Faces = get_face()
        self._Extreme_Edges = get_extreme_edge_depths()

    @property
    def CellSize(self):
        """Domain mean cell size"""
        print('Domain ID: {}, Average Cell Size = {}'.format(self._domain, self._CellSize))
        return self._CellSize

    @property
    def StageBC(self):
        """Stage boundary conditions"""
        return self._StageBC

    @property
    def FlowBC(self):
        """Flow boundary conditions"""
        return self._FlowBC

    @property
    def PrecipBC(self):
        """Precipitation boundary conditions"""
        return self._PrecipBC

    @property
    def Faces_FacePoint_Indexes(self):
        """Indices of face points used to create each Face"""
        return self._Faces_FacePoint_Indexes

    @property
    def Face_FacePoints_Coordinate(self):
        """Coordinates of face points"""
        return self._Face_FacePoints_Coordinate

    @property
    def Cells_Center_Coordinate(self):
        """Coordinates of cell centers"""
        return self._Cells_Center_Coordinate

    @property
    def Faces(self):
        """Faces created from face point indecies and coordinates"""
        return self._Faces

    @property
    def Face_Centroid_Coordinates(self):
        """Centroid of faces"""
        return self._Face_Centroid_Coordinates

    @property
    def Faces_Cell_Indexes(self):
        """Indecies of cells bounded by each face"""
        return self._Faces_Cell_Indexes

    @property
    def Face_Velocity(self):
        """Velocity measurements at each face"""
        return self._Face_Velocity

    @property
    def Depth(self):
        """Depth measurements at each cell center"""
        return self._Depth

    @property
    def Describe_Depths(self):
        """Max, min, and range of depths for each cell center"""
        return self._Describe_Depths

    @property
    def Avg_Face_Depth(self):
        """Average depth of cell centers bounding a face"""
        return self._Avg_Face_Depth

    @property
    def Perimeter(self):
        """Domain area polygon"""
        return self._Perimeter

    @property
    def Extreme_Edges(self):
        """Perimeter face centroids with absolute, average depths greater than one"""
        return self._Extreme_Edges

    def find_anomalous_attributes(self, attr: str = 'Face_Velocity', threshold: int = 30):
        """
        Returns attributed points with the maximum of their attributes exceeding a threshold
        :param attr:
        :param threshold:
        :return:
        """
        max_attr = pd.DataFrame(getattr(self, attr).max(axis=1), columns=['max'])
        df_thresh = max_attr[max_attr['max'] > threshold]
        gdf_thresh = self.Face_Centroid_Coordinates.iloc[df_thresh.index]
        try:
            return pd.concat([gdf_thresh, df_thresh], axis=1)
        except ValueError as e:
            print('No Anomolous Data Found')
            return None

    def count_anomalous_attributes(self, attr: str = 'Face_Velocity', threshold: int = 30):
        """
        Returns attributed points with a count of their attributes exceeding a threshold
        :param attr:
        :param threshold:
        :return:
        """
        dseries = getattr(self, attr).apply(lambda row: sum(row > threshold), axis=1)
        non_nan = dseries[dseries != 0].dropna()
        df_non_nan = pd.DataFrame(non_nan, columns=['count'])
        gdf_thresh = self.Face_Centroid_Coordinates.iloc[df_non_nan.index]
        try:
            return pd.concat([gdf_thresh, df_non_nan], axis=1)
        except ValueError as e:
            print('No Anomolous Data Found')
            return None




# Functions ---------------------------------------------------------------------

def group_excessive_points(gdf: gpd.geodataframe.GeoDataFrame, cell_size: float):
    """
    Creates groupings of collocated points exceeding a threshold.
        By default, a grouping is defined as three times the average
        cell size of the input file.
    :param gdf:
    :param cell_size:
    :return:
    """
    gdf_aois = gpd.GeoDataFrame()
    gdf_aois['point'] = gdf.geometry
    gdf_aois['polygon'] = gdf_aois.point.apply(lambda row: row.buffer(cell_size * 3))
    gdf_aois['geometry'] = gdf_aois['polygon']
    
    try:
        diss_aois = list(cascaded_union(gdf_aois.geometry))
        gdf_diss_aois = gpd.GeoDataFrame(diss_aois, columns=['geometry'])
    except:
        diss_aois = cascaded_union(gdf_aois.geometry)
        gdf_diss_aois = gpd.GeoDataFrame([diss_aois], columns=['geometry'])
    return gdf_diss_aois


def subset_data(grouping_polys: gpd.geodataframe.GeoDataFrame, thresheld_gdf: gpd.geodataframe.GeoDataFrame,
                count_gdf: gpd.geodataframe.GeoDataFrame, face_gdf: gpd.geodataframe.GeoDataFrame,
                buff_distance: int = 100) -> [list, list, list]:
    """
    Creates three lists of dataframes subset by a polygon where the polygon
        is a grouping of centroids. The first list contains maximum values for
        each face centroid, the second list contains counts of instances above
        a threshold, and the third lists faces within the buffered bounding
        box of a group of centroids.

    :param grouping_polys:
    :param thresheld_gdf:
    :param count_gdf:
    :param face_gdf:
    :param buff_distance:
    :return:
    """
    subset_max_list, subset_count_list, subset_face_list = [], [], []
    for i, poly in enumerate(grouping_polys.geometry):
        subset_max = thresheld_gdf[thresheld_gdf.within(poly)]
        subset_max_list.append(subset_max)

        # NOT USED?
        # subset_count = count_gdf.loc[subset_max.index]
        subset_count_list.append(count_gdf.loc[subset_max.index])

        x0, y0, x1, y1 = poly.buffer(buff_distance).bounds
        bbox = Polygon([[x0, y0], [x1, y0], [x1, y1], [x0, y1]])
        subset_faces = face_gdf[face_gdf.within(bbox)]
        subset_face_list.append(subset_faces)
    return subset_max_list, subset_count_list, subset_face_list


def find_large_and_small_groups(count_list: list, max_list: list, face_list: list,
                                gdf_groups: gpd.geodataframe.GeoDataFrame,
                                min_count: int = 5) -> [dict, dict]:
    """
    Identifies large groupings, i.e. above minimum count, of points and
        small groupings. Returns two dictionaries. One with large idxs,
        maximums, counts, faces, and groups as well as one with small idxs,
        maximums, and counts.

    :param count_list:
    :param max_list:
    :param face_list:
    :param gdf_groups:
    :param min_count:
    :return:
    """
    large_dict, small_dict = {}, {}

    large_tuples = [(i, count) for i, count in enumerate(count_list) if len(count) > min_count]
    large_dict['idxs'] = [large_tuple[0] for large_tuple in large_tuples]
    large_dict['maxes'] = [max_list[i] for i in large_dict['idxs']]
    large_dict['counts'] = [large_tuple[1] for large_tuple in large_tuples]
    large_dict['faces'] = [face_list[i] for i in large_dict['idxs']]
    large_dict['groups'] = [gdf_groups.iloc[i] for i in large_dict['idxs']]

    small_tuples = [(i, count) for i, count in enumerate(count_list) if len(count) <= min_count]
    small_dict['idxs'] = [small_tuple[0] for small_tuple in small_tuples]
    small_dict['maxes'] = [max_list[i] for i in small_dict['idxs']]
    small_dict['counts'] = [small_tuple[1] for small_tuple in small_tuples]
    return large_dict, small_dict


def plot_instabilities(max_list, count_list, gdf_face, gdf_face_all, ex_groups, idx):
    """
    Add Description
    :param max_list:
    :param count_list:
    :param gdf_face:
    :param gdf_face_all:
    :param ex_groups:
    :param idx:
    """
    fig, _ = plt.subplots(2, 2, figsize=(20, 8))
    x0, y0, x1, y1 = ex_groups[idx].geometry.buffer(100).bounds

    # Plot Max Velocities
    ax1 = plt.subplot2grid((2, 2), (0, 0))
    max_list[idx].plot(column='max', cmap='viridis', legend=True, ax=ax1)
    gdf_face[idx].plot(alpha=0.1, color='black', ax=ax1)
    ax1.set_title('Maximum Velocity recorded at Cell Face (ft/s)')
    ax1.set_xlim(x0, x1)
    ax1.set_ylim(y0, y1)

    # Plot Number of instabilities recorded (timesteps above threshold)
    ax2 = plt.subplot2grid((2, 2), (1, 0))
    ax2 = count_list[idx].plot(column='count', cmap='viridis', legend=True, ax=ax2)
    ax2 = gdf_face[idx].plot(alpha=0.1, color='black', ax=ax2)
    ax2.set_title('Number of Instabilities recorded at Cell Face (n)')
    ax2.set_xlim(x0, x1)
    ax2.set_ylim(y0, y1)

    # Plot Map Key (domain)
    ax3 = plt.subplot2grid((2, 2), (0, 1), rowspan=2)
    gdf_face_all.plot(alpha=0.05, color='black', ax=ax3)
    pnt_group = gpd.GeoDataFrame(geometry=gpd.GeoSeries(ex_groups[idx].geometry.buffer(1000)))
    pnt_group.plot(alpha=0.5, color='Red', legend=False, ax=ax3)
    ax3.set_title('Map Legend')

    ax1.axis('off')
    ax2.axis('off')
    ax3.axis('off')
    fig.suptitle('Group {}'.format(idx + 1), fontsize=16, fontweight='bold')


def plot_disparate_instabilities(max_list, count_list, bounding_polygon):
    """
    Add Description
    :param max_list:
    :param count_list:
    :param bounding_polygon:
    """
    small_maxes = pd.concat(max_list)
    small_counts = pd.concat(count_list)

    fig, _ = plt.subplots(1, 2, figsize=(20, 8))

    ax1 = plt.subplot2grid((1, 2), (0, 0))
    small_maxes.plot(column='max', cmap='viridis', legend=True, ax=ax1)
    bounding_polygon.plot(alpha=0.1, color='black', ax=ax1)
    ax1.set_title('Maximum Velocity recorded at Cell Face (ft/s)')

    ax2 = plt.subplot2grid((1, 2), (0, 1))
    ax2 = small_counts.plot(column='count', cmap='viridis', legend=True, ax=ax2)
    ax2 = bounding_polygon.plot(alpha=0.1, color='black', ax=ax2)
    ax2.set_title('Number of Instabilities recorded at Cell Face (n)')

    ax1.axis('off')
    ax2.axis('off')
    fig.suptitle('Isolated Points above Threshold', fontsize=16, fontweight='bold')


def plot_descriptive_stats(stat_lists: tuple, aoi: gpd.geodataframe.GeoDataFrame) -> None:
    """
    Plots the descriptive statistics (Max, Min) for
        cell centers with the area of interest underneath.
    :param stat_lists:
    :param aoi:
    """
    maximums, minimums = stat_lists

    # Plot descriptive statistics
    fig, (ax_string) = plt.subplots(1, 2, figsize=(20, 8))

    ax1 = plt.subplot2grid((1, 2), (0, 0))
    aoi.plot(color='k', alpha=0.25, ax=ax1)
    maximums.plot(column='max', cmap='viridis', markersize=0.1, legend=True, ax=ax1)
    ax1.set_title('Maximum Depth (ft)')

    ax2 = plt.subplot2grid((1, 2), (0, 1))
    aoi.plot(color='k', alpha=0.25, ax=ax2)
    ax2 = minimums.plot(column='min', cmap='viridis', markersize=0.1, legend=True, ax=ax2, s=1)
    ax2.set_title('Minimum Depth (ft)')

    ax1.axis('off')
    ax2.axis('off')
    fig.suptitle('Depths at Cell Centers',
                 fontsize=16, fontweight='bold')

def all_aoi_gdf(domain_results:list) -> gpd.geodataframe.GeoDataFrame:
    """
    Creates a geodataframe containing polygons for all domains.
    :param domain_results:
    """
    perimeters = [domain.Perimeter for domain in domain_results]
    df = pd.concat(perimeters).reset_index(drop=True)
    return gpd.GeoDataFrame(df)

def plot_extreme_edges(gdf: gpd.geodataframe.GeoDataFrame,
                       aoi: gpd.geodataframe.GeoDataFrame,
                       **kwargs) -> None:
    """
    Plots extreme depths along edges along with an overview map showing current
    plotted domain versus all other domains.
    :param gdf:
    :param aoi:
    :param \**kwargs:
        See below
    
    :Keyword Arguments:
        * *mini_map* (gpd.geodataframe.GeoDataFrame) -- Multiple domain perimeters.
    """
    if 'mini_map' in kwargs.keys():
        mini_map = list(kwargs.values())[0]
        
        fig, (ax_string) = plt.subplots(1, 2, figsize=(20, 8))
        ax1 = plt.subplot2grid((1, 2), (0, 0))
        aoi.plot(color='k', alpha=0.25, ax=ax1)
        gdf.plot(column='abs_max', cmap='viridis', legend=True, ax=ax1, markersize=16)
        ax1.set_title('Cell Locations with Depths > 1 ft\n(Check for Ponding)'.format(len(gdf)),
                     fontsize=12, fontweight='bold')
        ax1.axis('off')

        ax2 = plt.subplot2grid((1, 2), (0, 1))
        mini_map.plot(color='#BFBFBF', edgecolor='k', ax=ax2, markersize=16)
        aoi.plot(color='#FFC0CB', edgecolor='k', ax=ax2)
        ax2.set_title('Current domain (pink) compared to all domains (grey)'.format(len(gdf)),
                     fontsize=12, fontweight='bold')
        ax2.axis('off')
    else:
        fig, ax = plt.subplots(figsize = (7,7))
        aoi.plot(color='k', alpha=0.25, ax=ax)
        gdf.plot(column='abs_max', cmap='viridis', legend=True, ax=ax, markersize=16)
        ax.set_title('Cell Locations with Depths > 1 ft\n(Check for Ponding)'.format(len(gdf)),
                     fontsize=12, fontweight='bold')
        ax.axis('off')


def DepthVelPlot(depths: pd.Series, velocities: pd.Series, groupID: int, velThreshold: int = 30):
    """
    Add Description
    :param depths:
    :param velocities:
    :param groupID:
    :param velThreshold:
    """
    t = depths.index
    data1 = depths
    data2 = velocities

    fig, ax1 = plt.subplots(figsize=(10, 2))
    fig.suptitle('Velocity Anomalies at face {}'.format(groupID), fontsize=12, fontweight='bold', x=0.49, y=1.1)

    color = 'blue'
    ax1.set_xlabel('Time Steps')
    ax1.set_ylabel('Depth (ft)', color=color)
    ax1.plot(data1, color=color)
    ax1.tick_params(axis='y', labelcolor=color)

    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis

    color = 'red'
    ax2.set_ylabel('Velocity (ft/s)', color=color)  # we already handled the x-label with ax1
    ax2.plot(data2, color=color, alpha=0.5)
    ax2.tick_params(axis='y', labelcolor=color)
    ax2.hlines(velThreshold, t.min(), t.max(), colors='k', linestyles='--', alpha=0.5, label='Threshold')
    ax2.hlines(velThreshold * -1, t.min(), t.max(), colors='k', linestyles='--', alpha=0.5, label='Threshold')

    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.show()


def velCheckMain(results, plot_tseries=5):
    """
    Add Description
    :param results:
    :param plot_tseries:
    """
    # Identify face velocities above a given threshold
    df_thresh = results.find_anomalous_attributes()
    df_count = results.count_anomalous_attributes()

    if df_count.shape[0] > 1 and df_thresh.shape[0] > 1:

        # Identify groups of excessive centroids
        gdf_groups = group_excessive_points(df_thresh, results.CellSize)

        # Using a method nearly doubles the time
        max_list, count_list, face_list = subset_data(gdf_groups, df_thresh, df_count, results.Faces)

        # Split groups into large (n > 5) clusters vs. everything else
        l_dict, s_dict = find_large_and_small_groups(count_list, max_list, face_list, gdf_groups)

        # Identify group of interest
        for idx in range(len(l_dict['groups'])):
            plot_instabilities(l_dict['maxes'], l_dict['counts'], l_dict['faces'], results.Perimeter,
                               l_dict['groups'], idx)

            # NOT USED?
            maxes = l_dict['maxes'][idx]
            # counts = l_dict['counts'][idx]
            # faces = l_dict['faces'][idx]
            # group = l_dict['groups'][idx]

            max_vFaceIDs = list(maxes.sort_values(by='max', ascending=False)[0:plot_tseries].index)

            # NOT USED?
            # groupID = idx
            depths = results.Avg_Face_Depth.iloc[max_vFaceIDs]
            velocities = results.Face_Velocity.iloc[max_vFaceIDs]

            for i in depths.index:
                DepthVelPlot(depths.loc[i], velocities.loc[i], i)

        # print("Completed in {} seconds.".format(round(time() - start)))
        plot_disparate_instabilities(s_dict['maxes'], s_dict['counts'], results.Perimeter)
    else:
        print('No Velocity Errors Found')


def plotBCs(results):
    """
    Add Description
    """
    if results.FlowBC is not None:
        for k, v in results.FlowBC.items():
            fig, ax = plt.subplots(figsize=(20, 2))
            ax.set_title('{}\nPeak Flow of {} cfs'.format(k, int(v[:, 1].max())))
            ax.set_ylabel('Flow (ft)')
            ax.set_xlabel('Days')
            ax.plot(v[:, 0], v[:, 1])
            ax.grid()

    if results.StageBC is not None:
        for k, v in results.StageBC.items():
            fig, ax = plt.subplots(figsize=(20, 2))
            ax.set_title(k)
            ax.set_ylabel('Stage (cfs)')
            ax.set_xlabel('Days')
            ax.plot(v[:, 0], v[:, 1])
            ax.grid()

    if results.PrecipBC is not None:
        for k, v in results.PrecipBC.items():
            fig, ax = plt.subplots(figsize=(20, 2))
            ax.set_title(k)
            ax.set_ylabel('Precipitation (inches)')
            ax.set_xlabel('Days')
            ax.plot(v[:, 0], v[:, 1])
            ax.grid()
