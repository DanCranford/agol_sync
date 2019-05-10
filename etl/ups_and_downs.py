# -*- coding: utf-8 -*-
"""
Created on Thu Apr 11 09:34:52 2019

@author: dpcn
"""

def layer_to_featureclass(layer,out_fc):
    field_names = [field['name'] for field in layer.properties.fields]
    sdf = layer.query(as_df=True)
    fc = sdf[field_names+['SHAPE']].spatial.to_featureclass(out_fc)
    return fc


def complex_fc_to_fset(fc_or_flyr, temp_dir = r'P:/temp'):
    '''converts a featureclass or feature layer to an AGOL-friendly FeatureSet object.
    This is good for complex geometry, as SDF-from_featureclass simplifies geometry.
    requires arcpy.
    
    inputs:
        fc_or_flyr:  feature class or feature layer.  Can be a selection.
        temp_dir:    local folder for saving a temporary json file
    '''
    from os import makedirs
    from os.path import join, exists
    from json import load
    from arcpy import FeaturesToJSON_conversion, Delete_management
    from arcgis.features import FeatureSet
    
    if not exists(temp_dir):
        makedirs(temp_dir)
    temp_json_file = FeaturesToJSON_conversion(fc_or_flyr,join(temp_dir,'temp_features.json'))[0]
    with open(temp_json_file) as jsonfile:
        data = load(jsonfile)
    Delete_management(temp_json_file)
    return FeatureSet.from_dict(data)