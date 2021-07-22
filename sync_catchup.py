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















    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

