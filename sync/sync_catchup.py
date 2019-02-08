def delta_analysis(parent_layer,child_layer,child_sde=True,edited_field='Edited',return_features = True):
    '''checks and generates adds/updates/deletes for
    parent and child layers.  designed for agol to sde sync'''
    from pandas import isna
    print("Getting Sync Deltas for:",parent_layer,'to',child_layer)
    
    glob_field_parent = parent_layer.properties.globalIdField
    glob_field_child = child_layer.properties.globalIdField
    
    count_parent = parent_layer.query(return_count_only=True)
    count_child = child_layer.query(return_count_only=True)
    
    parent_out_fields = [glob_field_parent,edited_field]
    df_parent = parent_layer.query(out_fields = ','.join(parent_out_fields),return_geometry=False).df
    df_parent['JOINER']=df_parent[glob_field_parent]
    
    if child_sde:
        child_edited_field = edited_field.upper()
    else:
        child_edited_field = edited_field
        
        
    child_out_fields = [glob_field_child,child_edited_field]
    df_child = child_layer.query(out_fields = ','.join(child_out_fields),return_geometry=False).df
    
    if child_sde:
        df_child['JOINER']=(df_child[glob_field_child].str.lower()).str.replace('{','').str.replace('}','')
    else:
        df_child['JOINER']=df_child[glob_field_child]
    
    
    
    df_outer = df_parent.merge(df_child,on='JOINER',how='outer',suffixes=('_P','_C'))
    
    globs_adds = df_outer[isna(df_outer[glob_field_child])][glob_field_parent].tolist()
    globs_deletes = df_outer[isna(df_outer[glob_field_parent])][glob_field_child].tolist()    
    globs_updates = df_outer[df_outer['Edited']>df_outer['EDITED']]['GlobalID'].tolist()
    
    print('\tAdds:'+str(len(globs_adds)))
    print('\tUpdates:'+str(len(globs_updates)))
    print('\tDeletes:'+str(len(globs_deletes)))    
    
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




















