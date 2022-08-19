

def find_attachment(att_list,parent_globalid,att_name):
    for i, dic in enumerate(att_list):
        if dic['PARENTGLOBALID'] == parent_globalid and dic['NAME'] == att_name:
            return i
    return -1
    
def get_attachments_del(layer,topfolder):
    import os
    import urllib
    all_attachments = layer.attachments.search()
    errord = []
    print("Total Attachments: "+str(len(all_attachments)))
    for attach in all_attachments:
        try:
            glob = attach['PARENTGLOBALID']
            if '\\' in attach['NAME']:
                name = attach['NAME'].split('\\')[-1]
            else:
                name = attach['NAME']
            subfolder = os.path.join(topfolder,glob)
            if not os.path.exists(subfolder):
                os.makedirs(subfolder)
            attpath = os.path.join(subfolder,name)
            if not os.path.exists(attpath) or attach['SIZE']!=os.path.getsize(attpath):
                print(glob,'Downloading....',name)
                urllib.request.urlretrieve(attach['DOWNLOAD_URL'],filename=attpath)
            if attach['SIZE']!=os.path.getsize(attpath):
                raise Exception('Your GIS Might be bad')
        except:
            errord.append(attach)
    return errord




def add_attachments(layer,topfolder):
    import os
    errord = [] 
    exist_attachs = [(attachment['PARENTGLOBALID'],attachment['NAME']) for attachment in layer.attachments.search()]
    for root,dirs,files in os.walk(topfolder):
        
        for file in files:
            glob = root.split('\\')[-1]
            attpath = os.path.join(root,file)
            print (glob,file)   
            if (glob,file) not in exist_attachs:
                try:
                    oid = layer.query(where="""GlobalID = '"""+glob+"""'""",return_geometry=False,out_fields = 'GlobalID,OBJECTID').features[0].get_value("OBJECTID")
                    layer.attachments.add(oid,attpath)
                    exist_attachs.append((glob,file))
                except:
                    errord.append(attpath)
            else:
                print('\tAttachment Already There: ',glob,file)
    return errord




def transfer_attachment(layer, oid, download_url, name, c_type, keywords=None):
    '''
    transfers attachment from one layer to another without saving to disk
    
    Parameters
    ----------
    layer : arcgis.features.FeatureLayer object
        layer you want to add attachments to.
    oid : integer
        OBJECTID value for the feature you want to add an attachment to.
    download_url : string - url
        download url from your source layer.  derived from an attachment query.
    name : string
        file name.
    c_type : string
        file extension/content type.
    keywords : ?
        ? derived from source data.  for survey123 -string.  
        DESCRIPTION.

    Returns
    -------
    None.

    '''
    import requests
    from io import BytesIO
    att_url = '{}/{}/addAttachment'.format(layer.url, oid)
    data = BytesIO(requests.get(download_url).content)
    files = {'attachment': (name, data, c_type)}
    #here it pulls the json token
    params = {'keywords': keywords,'token': layer._con.token,'f': 'json'}
    r = requests.post(att_url, params, files=files)
    return r




def compare_attachments(in_layer, dest_layer):
    '''
    Compares attachments in two layers to find what needs to be added from one
    to another
    Parameters
    ----------
    in_layer : arcgis FeatureLayer
        layer the attachments are COMING FROM.
    dest_layer : arcgis FeatureLayer
        layer the attachments are GOING TO.
    Returns
    -------
    list of dictionaries
        list of dictionaries representing attachments needing transfer.
    '''
    import pandas as pd

    df_in = pd.DataFrame(in_layer.attachments.search())
    df_dest = pd.DataFrame(dest_layer.attachments.search())
    if df_dest.shape[0] ==0:
        df_dest = pd.DataFrame(columns=['PARENTGLOBALID','NAME','GLOBALID'])


    df_feat_dest = dest_layer.query(out_fields = [dest_layer.properties.objectIdField,
                                                    dest_layer.properties.globalIdField],
                                    as_df=True,
                                    return_geometry=False)


    df_merge = df_in.merge(df_dest, 'left',
                       on = ['PARENTGLOBALID','NAME'],
                       suffixes=('','_DEST'))\
                    .merge(df_feat_dest,'left',
                        left_on = 'PARENTGLOBALID',
                        right_on= dest_layer.properties.globalIdField,
                        suffixes=('','_MASTER'))

    df_merge.rename(columns={c : c.upper() for c in df_merge.columns}, inplace=True)

    return df_merge[pd.isna(df_merge['GLOBALID_DEST']) & pd.notna(df_merge['OBJECTID'])]\
                [['NAME','PARENTGLOBALID','DOWNLOAD_URL','CONTENTTYPE','KEYWORDS','OBJECTID']]\
                    .to_dict('records')





def transfer_attachment_q(queue, layer):
    '''
    helper function for transfer_attachments.
    this function is the one used by the queue/threads
    
    Parameters
    ----------
    queue : Queue object
        Queue with very specific set of dictionaries of attachments
    layer : arcgis.features.FeatureLayer object
        layer you want to add attachments to.
    Returns
    -------
    None.

    '''
    import requests
    from io import BytesIO
    
    while not queue.empty():
        try:
            work = queue.get()
        
#             if work[0]%100==0:
#                 print(work[0])
                
            attach_dict = work[1]
            oid = int(attach_dict['OBJECTID'])
            download_url = attach_dict['DOWNLOAD_URL']
            name = attach_dict['NAME']
            c_type = attach_dict['CONTENTTYPE']
            try:
                keywords = attach_dict['KEYWORDS']
            except KeyError:
                keywords = None
#             print('adding attachment')
            att_url = '{}/{}/addAttachment'.format(layer.url, oid)
            data = BytesIO(requests.get(download_url).content)
            files = {'attachment': (name, data, c_type)}
            #here it pulls the json token
            params = {'keywords': keywords,'token': layer._con.token,'f': 'json'}
            r = requests.post(att_url, params, files=files)
#             print(r.content)
        except Exception as e:
            print(e)
        queue.task_done()
        
        


def transfer_attachments(in_layer, target_layer, num_threads=10):
    '''
    Transfers attachments from one layer to another.  Uses a queue and multithreading.
    
    parameters:  
    in_layer: arcgis.features.FeatureLayer
        layer FROM which attachments will come
    target_layer: arcgis.features.FeatureLayer
        layer TO which attachments will go
    num_threads: int
        the number of threads to use.  AGOL can support at least 10 threads without conflict


    '''
    from queue import Queue
    from threading import Thread
    
    print('Comparing attachments...')
    atts_to_add = compare_attachments(in_layer, target_layer)
    
    if len(atts_to_add) == 0:
        print('No attachments to add.')
        return True
    
    print("There are {} attachments to transfer".format(len(atts_to_add)))
    q = Queue(maxsize=0)
    for i, att in enumerate(atts_to_add):
        q.put((i, att))

    if len(atts_to_add) < num_threads:
        num_threads = len(atts_to_add)
    
    for i in range(num_threads):
        print('\tstarting thread {}'.format(i))
        worker = Thread(target=transfer_attachment_q, args=(q, target_layer))
        worker.setDaemon(True)
        worker.start()
    
    q.join()
    return True