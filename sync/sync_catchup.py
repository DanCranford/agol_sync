def delta_analysis(parent_layer,child_layer,child_sde=True,edited_field='Edited',return_features = True):
    '''checks and generates adds/updates/deletes for
    parent and child layers.  designed for agol to sde sync'''
    print("Getting Sync Deltas for:",parent_layer,'to',child_layer)
    
    glob_field_parent = parent_layer.properties.globalIdField
    glob_field_child = child_layer.properties.globalIdField
    
    count_parent = parent_layer.query(return_count_only=True)
    count_child = child_layer.query(return_count_only=True)
    
    parent_out_fields = [glob_field_parent,edited_field]
    df_parent = parent_layer.query(out_fields = ','.join(parent_out_fields),return_geometry=False).df
    df_parent['JOINER']=df_parent[glob_field_parent]
    
    
    child_out_fields = [glob_field_child,edited_field.upper()]
    df_child = child_layer.query(out_fields = ','.join(child_out_fields),return_geometry=False).df
    
    if child_sde:
        df_child['JOINER']=(df_child[glob_field_child].str.lower()).str.replace('{','').str.replace('}','')
    else:
        df_child['JOINER']=df_child[glob_field_child]
    
    
    
    df_outer = df_parent.merge(df_child,on='JOINER',how='outer',suffixes=('_P','_C'))
    
    
    
    
    
    
    return(df_outer)





















    
#    all_glob_parent = [feature.get_value(glob_field_parent) for feature in fset_parent.features]
#    
#
#        
#    child_adds = [glob for glob in all_glob_parent if glob not in all_glob_child]
#    child_updates = [glob for glob in all_glob_parent if glob in all_glob_child]
#    
#    child_deletes = [glob for glob in all_glob_child if glob not in all_glob_parent]
#    
#    print('Adds:',len(feats_adds))
#    print('Updates',len(feats_updates))
#    print('Deletes',len(child_deletes))
#    
#    if count_parent == count_child+len(child_adds)-len(child_deletes):
#        
#        if return_features:
#        print("Fetching all the features...")
#            all_parent_features = parent_layer.query().features
#    
#            feats_adds = [feature for feature in all_parent_features if feature.get_value(glob_field_parent) in child_adds]
#            feats_updates = [feature for feature in all_parent_features if feature.get_value(glob_field_parent) in child_updates]
#            
#            if child_sde:
#                for feature in feats_adds+feats_updates:
#                    feature.set_value(glob_field_parent,'{'+feature.get_value(glob_field_parent).upper()+'}')        
#            if child_sde:
#                child_deletes_format = ['{'+glob.upper()+'}' for glob in child_deletes]
#    #            query_deletes = glob_field_child+""" IN ('"""+"""','""".join(child_deletes_format)+"""')"""
#    #        else:
#    #            query_deletes = glob_field_child+""" IN ('"""+"""','""".join(child_deletes)+"""')"""
#        print('Adds:',len(feats_adds))
#        print('Updates',len(feats_updates))
#        print('Deletes',len(child_deletes))
#            return(feats_adds,feats_updates,child_deletes_format)
#    else:
#        raise Exception('Somethings Wrong')
        
        
flc_agol_gtmarkers_v2 = arcgis.features.FeatureLayerCollection('https://services3.arcgis.com/zqOoSc3llV8uXFYF/arcgis/rest/services/GT_Markers_v2/FeatureServer',WEVMAPS)
     
lyr_agol_lidr = flc_agol_gtmarkers_v2.layers[1]
lyr_agol_nmrk = flc_agol_gtmarkers_v2.layers[2]
lyr_agol_exrr = flc_agol_gtmarkers_v2.layers[3]
lyr_agol_emng = flc_agol_gtmarkers_v2.layers[4]
