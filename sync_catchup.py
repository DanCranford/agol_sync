import pandas as pd

def delta_analysis(parent_layer,child_layer,child_sde=True,edited_field='Edited',return_features = True, log_list = []):
    '''checks and generates adds/updates/deletes for
    parent and child layers.  designed for agol to sde sync'''
    from pandas import isna,Timedelta
    from time import strftime
    print("Getting Sync Deltas for:",parent_layer,'to',child_layer)
    
    glob_field_parent = parent_layer.properties.globalIdField
    glob_field_child = child_layer.properties.globalIdField
    
    count_parent = parent_layer.query(return_count_only=True)
    count_child = child_layer.query(return_count_only=True)
    
    parent_out_fields = [glob_field_parent,edited_field]
    df_parent = parent_layer.query(out_fields = ','.join(parent_out_fields),return_geometry=False).sdf
    df_parent['JOINER']=df_parent[glob_field_parent]
    
    if child_sde:
        child_edited_field = edited_field.upper()
    else:
        child_edited_field = edited_field
        
        
    child_out_fields = [glob_field_child,child_edited_field]
    df_child = child_layer.query(out_fields = ','.join(child_out_fields),return_geometry=False).sdf
    
    if child_sde:
        df_child['JOINER']=(df_child[glob_field_child].str.lower()).str.replace('{','').str.replace('}','')
    else:
        df_child['JOINER']=df_child[glob_field_child]
    
    
    
    df_outer = df_parent.merge(df_child,on='JOINER',how='outer',suffixes=('_P','_C'))
    
    globs_adds = df_outer[isna(df_outer[glob_field_child])][glob_field_parent].tolist()
    globs_deletes = df_outer[isna(df_outer[glob_field_parent])][glob_field_child].tolist()    
    #globs_updates = df_outer[df_outer[edited_field]>df_outer[child_edited_field]]['GlobalID'].tolist()
    
    globs_updates = df_outer[df_outer[edited_field]-df_outer[child_edited_field]>Timedelta(1, 's')]['GlobalID'].tolist()

    
    print('\tAdds:'+str(len(globs_adds)))
    print('\tUpdates:'+str(len(globs_updates)))
    print('\tDeletes:'+str(len(globs_deletes)))   
    log_list.append([child_layer.properties.name, strftime("%m/%d/%Y %H:%M"),len(globs_adds),len(globs_updates),len(globs_deletes)])
    
    if return_features:
        if count_parent == count_child+len(globs_adds)-len(globs_deletes):
            print("Fetching all the features...")
            all_parent_features = parent_layer.query().features
    
            feats_adds = [feature for feature in all_parent_features if feature.get_value(glob_field_parent) in globs_adds]
            feats_updates = [feature for feature in all_parent_features if feature.get_value(glob_field_parent) in globs_updates]
            
            if child_sde:
                for feature in feats_adds+feats_updates:
                    feature.set_value(glob_field_parent,'{'+feature.get_value(glob_field_parent).upper()+'}')

            return(feats_adds,feats_updates,globs_deletes)
        else:
            raise Exception('Somethings Wrong')    




    
def find_edited_field(layer):
    potential = [field['name'] for field in layer.properties.fields if field['editable']==False and field['type']=='esriFieldTypeDate' and 'EDIT' in field['name'].upper()]
    if len(potential)==1:
        return potential[0]
    else:
        raise Exception('Cant find an editing field')



def delta_analysis1(parent_layer,child_layer,child_sde=True,return_features = True, log_list = [], edited_field = None, child_edited_field = None):
    '''checks and generates adds/updates/deletes for
    parent and child layers.  designed for agol to sde sync'''
    from pandas import isna,Timedelta
    from time import strftime
    print("Getting Sync Deltas for:",parent_layer,'to',child_layer)
    
    
    glob_field_parent = parent_layer.properties.globalIdField
    glob_field_child = child_layer.properties.globalIdField
    
    if edited_field == None:
        edited_field = parent_layer.properties.editFieldsInfo['editDateField']
    if child_edited_field == None and child_sde==True:
        child_edited_field = edited_field.upper()
    elif child_edited_field == None and child_sde==False:
        child_edited_field = edited_field
    
    count_parent = parent_layer.query(return_count_only=True)
    count_child = child_layer.query(return_count_only=True)
    
    parent_out_fields = [glob_field_parent,edited_field]
    df_parent = parent_layer.query(out_fields = ','.join(parent_out_fields),return_geometry=False).sdf
    df_parent['JOINER']=df_parent[glob_field_parent]
    

    child_out_fields = [glob_field_child,child_edited_field]
    df_child = child_layer.query(out_fields = ','.join(child_out_fields),return_geometry=False).sdf
    
    if child_sde:
        df_child['JOINER']=(df_child[glob_field_child].str.lower()).str.replace('{','').str.replace('}','')
    else:
        df_child['JOINER']=df_child[glob_field_child]
    
    
    
    df_outer = df_parent.merge(df_child,on='JOINER',how='outer',suffixes=('_P','_C'))
    
    if glob_field_parent == glob_field_child:
        glob_field_parent+="_P"
        glob_field_child+="_C"
    
    globs_adds = df_outer[isna(df_outer[glob_field_child])][glob_field_parent].tolist()
    globs_deletes = df_outer[isna(df_outer[glob_field_parent])][glob_field_child].tolist()    
    #globs_updates = df_outer[df_outer[edited_field]>df_outer[child_edited_field]]['GlobalID'].tolist()
    
    globs_updates = df_outer[df_outer[edited_field]-df_outer[child_edited_field]>Timedelta(1, 's')][glob_field_parent].tolist()

    
    print('\tAdds:'+str(len(globs_adds)))
    print('\tUpdates:'+str(len(globs_updates)))
    print('\tDeletes:'+str(len(globs_deletes)))   
    log_list.append([child_layer.properties.name, strftime("%m/%d/%Y %H:%M"),len(globs_adds),len(globs_updates),len(globs_deletes)])
    
    if return_features:
        if count_parent == count_child+len(globs_adds)-len(globs_deletes):
            print("Fetching all the features...")
            all_parent_features = parent_layer.query().features
    
            feats_adds = [feature for feature in all_parent_features if feature.get_value(glob_field_parent) in globs_adds]
            feats_updates = [feature for feature in all_parent_features if feature.get_value(glob_field_parent) in globs_updates]
            
            if child_sde:
                for feature in feats_adds+feats_updates:
                    feature.set_value(glob_field_parent,'{'+feature.get_value(glob_field_parent).upper()+'}')

            return(feats_adds,feats_updates,globs_deletes)
        else:
            raise Exception('Somethings Wrong')    













def find_differences_merge_df(merged_df, column):
    try:
        return ((merged_df[f"{column}_P"] != merged_df[f"{column}_C"]) \
                & (pd.notna(merged_df[f"{column}_P"]) & pd.notna(merged_df[f"{column}_C"]))) \
                | (pd.isna(merged_df[f"{column}_P"]) & pd.notna(merged_df[f"{column}_C"])) \
                | (pd.notna(merged_df[f"{column}_P"]) & pd.isna(merged_df[f"{column}_C"]))
    except KeyError:
        return pd.Series(False)

def compare_geometries(shape_series_1, shape_series_2):
    shapes = list(zip(shape_series_1.tolist(), shape_series_2.tolist()))
    out_list = []
    for shape in shapes:
        out_list.append(shape[0].equals(shape[1]))
    return pd.Series(out_list)



def deltas_no_edit_tracking(parent_layer, child_layer, return_features=False, compare_geoms=False, parent_query=None):
    '''
    meant for agol-to-agol sync with no replicas or edit-tracking.
    does an analysis of values to find updated records
    
    parent_layer : arcgis.features.FeatureLayer
        layer from which changes come
    child_layer : arcgis.features.FeatureLayer
        layer to which changes will go
    return_features : bool
        if True, return dataframes with data, if false return only globalids
    
    
    '''
    glob_field_parent = parent_layer.properties.globalIdField
    glob_field_child = child_layer.properties.globalIdField
    
    if parent_query == None:
        df_parent = parent_layer.query(return_geometry=True, as_df=True)
    else:
        df_parent = parent_layer.query(parent_query, return_geometry=True, as_df=True)
    df_parent['JOINER']=df_parent[glob_field_parent]
    
    df_child = child_layer.query(as_df=True, return_geometry=True)
    df_child['JOINER']=df_child[glob_field_child]
    
    df_outer = df_parent.merge(df_child,on='JOINER',how='outer',suffixes=('_P','_C'))
    adds = df_parent[df_parent[glob_field_parent].isin(\
                     df_outer[pd.isna(df_outer[f"{glob_field_parent}_C"])][f"{glob_field_parent}_P"])]
#                .drop(columns=['OBJECTID','JOINER'])
    globs_adds = adds[glob_field_parent].tolist()
    globs_deletes = df_outer[pd.isna(df_outer[f"{glob_field_child}_P"])][f"{glob_field_parent}_C"].tolist()
    
    df_inner = df_parent.merge(df_child, 'inner', 'JOINER', suffixes=('_P','_C'))
    df_inner['edit'] = False
    shape_column = df_parent.dtypes[df_parent.dtypes == 'geometry'].index[0]

    for column in df_parent.columns:
        if column in [parent_layer.properties.objectIdField,'JOINER','edit']:
            results = pd.Series(False)
            pass
        elif column == shape_column:
            if compare_geoms:
                results = ~compare_geometries(df_inner[f"{shape_column}_P"], df_inner[f"{shape_column}_C"])
            else:
                results = pd.Series(False)
        else:
            try:
                results = find_differences_merge_df(df_inner, column)
            except KeyError:
                pass
#        if True in results.tolist():
#            print(column)
        df_inner['edit'] = df_inner['edit'] | results
    updates = df_parent[df_parent[glob_field_parent].isin(\
                        df_inner[df_inner['edit']][f"{glob_field_parent}_P"])]
#                    .drop(columns=['OBJECTID','JOINER'])
    globs_updates = updates[glob_field_parent].tolist()
    if return_features:
        if adds.shape[0]>0:
            feats_adds = parent_layer.query(object_ids=",".join([str(i) for i in adds[parent_layer.properties.objectIdField].tolist()])).features
        else:
            feats_adds = []
        if updates.shape[0]>0:
            feats_updates = parent_layer.query(object_ids=",".join([str(i) for i in updates[parent_layer.properties.objectIdField].tolist()])).features
        else:
            feats_updates = []
        return({
            "adds": feats_adds,
            "updates": feats_updates,
            "deletes": globs_deletes})
    else:
        return({
                "adds": globs_adds,
                "updates": globs_updates,
                "deletes": globs_deletes})
    
    
def make_feats_the_hard_way(df):
    feats = df[[col for col in df.columns if col != 'SHAPE']]\
        .spatial.to_featureset().features
    geoms = df['SHAPE'].tolist()
    outers = []
    for a, g in list(zip(feats, geoms)):
        d = a.as_dict
        d['geometry'] = g
        outers.append(d)
    
    return outers    
    
    
    
    
    
def compare_sdfs(df_parent, df_child, join_field, ignore_columns=[], compare_geoms=True):
#    df_outer = df_parent.merge(df_child, on=join_field, how='outer',suffixes=('_P','_C'), indicator=True)
    
    df_adds = df_parent[~df_parent[join_field].isin(df_child[join_field])]
    df_deletes = df_child[~df_child[join_field].isin(df_parent[join_field])]
    
    df_inner = df_parent.merge(df_child, 'inner', join_field, suffixes=('_P','_C'))
    df_inner['edit'] = False
    
    ignore_columns.append(join_field)
    
    for column in df_parent.columns:
        if column in ignore_columns:
            results = pd.Series(False)
            pass
        elif column == df_parent.spatial.name:
            if compare_geoms:
                results = ~compare_geometries(df_inner[f"{df_parent.spatial.name}_P"], df_inner[f"{df_parent.spatial.name}_C"])
            else:
                results = pd.Series(False)
        else:
            results = find_differences_merge_df(df_inner, column)
        print(column, results.any())
        if results.any():
            print(df_inner[[f"{column}_P", f"{column}_C"]][results])
#        if True in results.tolist():
#            print(column)
        df_inner['edit'] = df_inner['edit'] | results
    df_updates = df_parent[df_parent[join_field].isin(df_inner[df_inner.edit][join_field])]
    
    return {'adds': df_adds, 
            'updates': df_updates, 
            'deletes': df_deletes}

    
    
    
    
    
    
    

def find_adds_and_updates(parent_layer, child_layer):
    '''
    finds adds and updates from a parent layer to a child layer
    both layers must have globalids enabled
    
    parent_layer : arcgis.features.FeatureLayer
        layer from which changes come
    child_layer : arcgis.features.FeatureLayer
        layer to which changes will go

    returns
        adds and updates as lists of features
    
    
    '''

    parent_globalid_field = parent_layer.properties.globalIdField
    child_globalid_field = child_layer.properties.globalIdField

    edit_fields = list(dict(parent_layer.properties.editFieldsInfo).values())

    df_parent = parent_layer.query(
        out_fields = [parent_globalid_field] + 
            edit_fields,
        return_geometry=False,
        as_df=True
        )

    df_child = child_layer.query(
        out_fields = [child_globalid_field] + 
            edit_fields,
        return_geometry=False,
        as_df=True
        )
    
    df_adds = df_parent[
    ~df_parent[parent_globalid_field]
        .isin(df_child[child_globalid_field])
        ]
    if len(df_adds)>0:
        add_string = "','".join(df_adds[parent_globalid_field].tolist())
        fset_adds = parent_layer.query(f"{parent_globalid_field} IN ('{add_string}')")
        adds = fset_adds.features
    else:
        adds=[]
    
    df_merge = df_parent.merge(df_child, 'left', 
                           left_on = parent_globalid_field, 
                           right_on = child_globalid_field,
                          suffixes=('_p','_c'))

    edit_date_field = parent_layer.properties.editFieldsInfo['editDateField']
    updated_globs = df_merge[(pandas.notna(df_merge[f"{edit_date_field}_c"]))
                            & (df_merge[f"{edit_date_field}_p"] > df_merge[f"{edit_date_field}_c"])]\
                        [parent_globalid_field].tolist()

    df_updates = df_parent[df_parent[parent_globalid_field].isin(updated_globs)]

    if updated_globs:
        update_string = "','".join(updated_globs)
        fset_update = parent_layer.query(f"{parent_globalid_field} IN ('{update_string}')")
        fset_update
    else:
        updates = []
    
    return (adds, updates)
         
    
    
    
    
    
    
    
    
    


































