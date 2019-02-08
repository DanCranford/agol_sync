

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
