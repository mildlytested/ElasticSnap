"""
Manage Elasticsearch snapshots outside of the elasticsearch APIs.

Usage:
  ElasticSnap.py --list-snapshots --folder=<foldername>
  ElasticSnap.py --copy --src=<folder> --dst=<folder> --uuid=<snapshot_uuid>
  ElasticSnap.py --copy --src=<folder> --dst=<folder> --name=<snapshot_name>
  ElasticSnap.py --sync --src=<folder> --dst=<folder> [--verbose]
  ElasticSnap.py --show-missing --src=<folder> --dst=<folder>
  ElasticSnap.py --disk-usage --folder=<folder> --uuid=<snapshot_uuid>
  ElasticSnap.py --take-snapshot --repo=<SnapShotRepo> --name=<SnapShotName> --indices=<IndicesList>
  ElasticSnap.py --verify-indices-snapshot --folder=<folder>

Future:
  ElasticSnap --find-snapshots <foldername> <snapshot uuid>
    show information about a snapshot

  ElasticSnap --find-indices <foldername> <index>
    show which snapshots contain a given index

  ElasticSnap --show-missing <folder> <folder>
    compare 2 repositories and show the missing snapshots
  
  ElasticSnap --verify <folder> --uuid <snapshot uuid>  
  ElasticSnap --verify <folder> --name <snapshot name>  
    Verify 1 snapshot. (must be copied with this program)

  ElasticSnap --delete <folder> --uuid <snapshot uuid>  
  ElasticSnap --delete <folder> --name <snapshot name>  
    delete 1 snapshot

wishlist:
  -search folder and find abandoned indices (including cleanup of index json)
  -calculate the disk usage for a given snapshot
"""

import json
import os.path
from shutil import copyfile
import hashlib
import sys
from docopt import docopt
import requests
from datetime import date, timedelta


#Set the parameters to query elasticsearch for snapshots in --take-snapshot
headers = {'Content-Type': 'application/json',}

#set elastic_cert to False if using http
elastic_cert = False
#elastic_cert = '/etc/elasticsearch/ca.crt'

#url to login to elasticsearch
url = 'https://username:password@10.0.0.1:9200'

#url without login or ssl
url = 'http://10.0.0.1:9200'






def GetIndexLatest(SourceFolder):
  FileIndexLatest = 'index.latest'
  IndexLatest = SourceFolder + '/' + FileIndexLatest
  CurrentIndex = 0

  if os.path.exists(IndexLatest):
    hex_list = []
    with open(IndexLatest, mode='rb') as file:
      i = 0
      for c in file.read():
        hex_list.append(c)
      hex_list.reverse()
      for i in range(len(hex_list)):
        #print (int(hex_list[i],16))
        if i > 0:
          pos = i*256
        else:
         pos = 1
        CurrentIndex += hex_list[i] * pos
    return CurrentIndex # should be an interger
  else:
    CurrentIndex = 0
    WriteIndexLatest(IndexLatest, CurrentIndex)

    IndexFileJSON = SourceFolder + '/index-' + str(CurrentIndex)
    with open(IndexFileJSON, 'w') as f:
      json.dump( {'snapshots': [], 'indices': {} } , f) #write empty json

    return CurrentIndex # should be an interger (0)

  
#Need to write a 64 bit interger to file  
def WriteIndexLatest(FileName, CurrentIndex):
  hexIndex = hex(CurrentIndex)
  hexstring = hexIndex[2:].zfill(16)
  hex_list = [int(hexstring[i:i+2], 16) for i in range(0,16, 2)]
  with open(FileName, mode='wb') as file:
    arr=bytearray(hex_list)
    file.write(arr)


def UpdateIndex(DestFolder, Index): 
  CurrentIndex = GetIndexLatest(DestFolder)
  CurrentIndex += 1

  IndexFileJSON = DestFolder + '/index-' + str(CurrentIndex)
  with open(IndexFileJSON, 'w') as f:
    json.dump( Index , f) 

  FileName = DestFolder + '/index.latest'
  WriteIndexLatest(FileName, CurrentIndex)


def ReadIndex(SourceFolder, CurrentIndex):
  FileName = SourceFolder + '/index-' + str(CurrentIndex)
  with open(FileName, mode='r') as file:
    IndexJSON = json.loads(file.read())
  return IndexJSON


def CalcSizeFileChecksum(SnapshotChecksums):
  TotalSize = 0
  for i in SnapshotChecksums.keys():
    TotalSize += SnapshotChecksums[i]['size']
  return TotalSize 


def ListSnapShots(Index, Folder = None):
  FolderTotal = 0 #value in GB
  Index['snapshots'].sort(key=SnapshotSortName)
  if Folder is not None:
    print ("%22s %5s %8s %8s %30s" % ("uuid", "state", "version", "size", "name"))
  else:
    print ("%22s %5s %8s %30s" % ("uuid", "state", "version", "name"))
  
  for i in Index['snapshots']:
    if Folder is not None:
      FileChecksum = Folder + '/checksums-' + i['uuid'] + '.json'
      if os.path.exists(FileChecksum):
        with open(FileChecksum, 'r') as f:
          SnapshotChecksums = json.loads(f.read())
        TotalSize = round(CalcSizeFileChecksum(SnapshotChecksums) / 1024 / 1024 / 1024, 2)
        FolderTotal += TotalSize
        print ("%22s %5s %8s %8s GB %30s" % (i['uuid'], i['state'], i['version'], TotalSize, i['name'] ))
      else:
        print ("%22s %5s %8s %8s    %30s" % (i['uuid'], i['state'], i['version'], "", i['name'] ))
    else:
      print ("%22s %5s %8s %30s" % (i['uuid'], i['state'], i['version'], i['name']))
  print ("Total consumed space for snapshots is %s GB or %s TB" % (round(FolderTotal), round(FolderTotal / 1024)))
  return len(Index['snapshots'])

#get a json of all snapshots that currently exist
def GetSnapShots(url, headers, SnapShotRepo):
  f_url = url + '/_cat/snapshots/' + SnapShotRepo + '?format=json'
  try:
    r = requests.get(f_url, headers=headers, verify=elastic_cert)
    message = json.loads(r.text)
    if r.status_code != 200:
      raise
    return message
  except:
    print ("failed to get snapshots")
    print ("HTTP status code : %s" % r.status_code)
    print (r.text)
    sys.exit(1)

def CreateSnapShot(url, headers, SnapShotRepo, SnapShotName, SendJSON):
  f_url = url + '/_snapshot/' + SnapShotRepo + '/' + SnapShotName + '?wait_for_completion=true&pretty'
  try:
    r = requests.put(f_url, headers=headers, data=json.dumps(SendJSON), verify=elastic_cert)
    if r.status_code != 200:
      raise
    print (r.text)
  
  except:
    print ("failed to create snapshot")
    print ("HTTP status code : %s" % r.status_code)
    print (r.text)
    sys.exit(1)
  return

def GetIndices(url, headers):
  f_url = url + '/_cat/indices?format=json'
  try:
    r = requests.get(f_url, headers=headers, verify=elastic_cert)
    message = json.loads(r.text)
    if r.status_code != 200:
      raise
    return message
  except:
    print ("failed to get snapshots")
    print ("HTTP status code : %s" % r.status_code)
    print (r.text)
    sys.exit(1)

def SnapshotSortName(snapshot):
  try:
    return snapshot['name']
  except KeyError:
    return 0

def ListSnapShotsFiltered(Index,FilterUUID):
  Index['snapshots'].sort(key=SnapshotSortName)
  print ("%22s %5s %8s %25s" % ("uuid", "state", "version", "name"))
  for i in Index['snapshots']:
    if i['uuid'] in FilterUUID:
      print ("%22s %5s %8s %25s" % (i['uuid'], i['state'], i['version'], i['name']))

def ListIndices(Index):
  print (Index['indices'])
  print (Index['indices'].keys())
  for i in Index['indices'].keys():
    print (Index['indices'][i])

def GetSnapshotName(Index,snapshotUUID):
  for i in Index['snapshots']:
    if i['uuid'] == snapshotUUID:
      return i['name']
  return None #no snapshot found

#Find all indices that are in a given snapshot
def GetIndexInSnapshot(Index,snapshot):
  for i in Index['indices'].keys():
    for snap in Index['indices'][i]['snapshots']:
      if snap == snapshot:
        print (i)

#Get the required files for 1 snapshot
def GetFileInfoIndex(Index, snapshot):
  Files = [ "meta-" + snapshot + ".dat", "snap-" + snapshot + ".dat" ]
  Folders = []
  for i in Index['indices'].keys():
    for snap in Index['indices'][i]['snapshots']:
      if snap == snapshot:
        Folders.append(Index['indices'][i]['id'])
  return Files, Folders

def CalcChecksum(filename):
  try:
    BLOCKSIZE = 65536
    hasher = hashlib.sha1()
    with open(filename, 'rb') as afile:
      buf = afile.read(BLOCKSIZE)
      while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(BLOCKSIZE)
      file_sha1 = hasher.hexdigest()
      filesize = os.path.getsize(filename)
      #print ("Calculated size of file %s as %s" % ((filename, filesize)))
      return file_sha1, filesize
  except:
    print ("Failed to calculate sha1 or filesize")
    sys.exit(1)



def CopyFile(SrcFileName, DestFileName, FileCheck = None, Verify = True):
  if os.path.exists(DestFileName):
    if FileCheck is None:   #File found, but no checksum. Calculating
      file_sha1, filesize = CalcChecksum(SrcFileName) 
      return file_sha1, filesize
    else:
      if Verify:
        file_sha1, filesize = CalcChecksum(DestFileName)
        if FileCheck['size'] != filesize:
          print ("file size mismatch : %s" % DestFileName)
          print ("looking for : %s : got %s" % (FileCheck['size'], filesize))
          sys.exit(1)
        if FileCheck['sha1'] != file_sha1:
          print ("Checksum mismatch : %s" % DestFileName)
          print ("Should be     : %s" % FileCheck['sha1'])
          print ("Calculated as : %s" % file_sha1)
          sys.exit(1)
      else:
        pass
        
  else:
    try:
      copyfile(SrcFileName, DestFileName)
      #Since this is a new file, using the source to calculate checksum
      file_sha1, filesize = CalcChecksum(SrcFileName) 
      return file_sha1, filesize
    except:
      print ("Failed to copy file")
      print ("Source      : %s" % SrcFileName)
      print ("Destination : %s" % DestFileName)
      
      #delete from destination
      if os.path.exists(DestFileName):
        os.path.remove(DestFileName)
        print ("Deleted destination file")
      
      sys.exit(1)

  ### This will probably cause an error (no handler for None type)
  return None, None
    

def MakeFolder(FolderName):
  if not os.path.isdir(FolderName):
    print ("Making folder : %s" % FolderName)
    os.makedirs(FolderName)


def WalkSnapShotFolder(SrcIndicesFolder, DestIndicesFolder, Folder, SnapshotChecksums, RelFolder = '', Verify = True):
  SrcSnapshotFolder = SrcIndicesFolder + '/' + Folder
  DestSnapshotFolder = DestIndicesFolder + '/' + Folder
  
  #print ("Walking folder : %s" % Folder)
  for root, dirs, files in os.walk(SrcSnapshotFolder):
    for File in files:
      SrcFileName = SrcSnapshotFolder + '/' + File
      DestFileName = DestSnapshotFolder + '/' + File

      RelFile = RelFolder + File
      if RelFile in SnapshotChecksums:
        ### skipping copy file if checksum is available and verify = False
        if Verify:
          CopyFile(SrcFileName, DestFileName, SnapshotChecksums[RelFile], Verify = Verify)
      else:
        #print ("no checksum, adding to db")
        file_sha1, filesize = CopyFile(SrcFileName, DestFileName)
        SnapshotChecksums[RelFile] = {'sha1': file_sha1, 'size': filesize}



    for subdir in dirs:
      #print (subdir)
      MakeFolder(DestSnapshotFolder + '/' + subdir )

      NewFolder = Folder + '/' + subdir
      SnapshotChecksums = WalkSnapShotFolder(SrcIndicesFolder, DestIndicesFolder, NewFolder, SnapshotChecksums, RelFolder = RelFolder + subdir + '/', Verify=Verify)
    return SnapshotChecksums
  return SnapshotChecksums

def ExistsSnapshotUUID(Index, SnapshotName):
  for snapshot in Index['snapshots']:
    if snapshot['uuid'] == SnapshotName:
      return True
  return False

def UpdateIndexJSON(SrcIndex, DestIndex, SnapshotName):
  #snapshots section
  SrcSnapshots = []
  for snapshot in SrcIndex['snapshots']:
    if snapshot['uuid'] == SnapshotName:
      SrcSnapshots.append(snapshot)

  #Add items to dest ['snapshot']
  for snapshot in SrcSnapshots:
    if not ExistsSnapshotUUID(DestIndex, SnapshotName):
      #print ("Adding item to snapshot list")
      DestIndex['snapshots'].append(snapshot)

  #indices section
  indices = []
  for index in SrcIndex['indices'].keys():
    for snapshot in SrcIndex['indices'][index]['snapshots']:
      if snapshot == SnapshotName:
        IndicesAddend = { index : { 'id': SrcIndex['indices'][index]['id'], 'snapshots': [snapshot] } }
        if 'shard_generations' in SrcIndex['indices'][index]: #shard_generations is optional ??
          IndicesAddend[index]['shard_generations'] = SrcIndex['indices'][index]['shard_generations']
        indices.append(IndicesAddend)

  for index in indices:
    for index_key in index.keys(): #should be 1 value
      if index_key not in DestIndex['indices'].keys():
        DestIndex['indices'][index_key] = index[index_key] 
      else:
        print ("Need to append key to existing - not done yet")
        print ("quitting...")
        sys.exit(1)

  if not "min_version" in DestIndex:
    if "min_version" in SrcIndex:
      DestIndex['min_version'] = SrcIndex['min_version']
  return DestIndex


def CopySnapShotName(SourceFolder, DestFolder, snapshotName, Verify=True):
  CurrentIndex = GetIndexLatest(SourceFolder)
  SrcIndex = ReadIndex(SourceFolder, CurrentIndex)
  for item in SrcIndex['snapshots']:
    if item['name'] == snapshotName:
      CopySnapShot(SourceFolder, DestFolder, item['uuid'], Verify=Verify)
      return
  print ("no snapshot by that name")
  return

def CopySnapShot(SourceFolder, DestFolder, snapshotUUID, Verify=True):
  CurrentIndex = GetIndexLatest(SourceFolder)
  SrcIndex = ReadIndex(SourceFolder, CurrentIndex)
  SnapshotName = GetSnapshotName(SrcIndex, snapshotUUID)

  DestSnapshotName = GetSnapshotName(ReadIndex(DestFolder, GetIndexLatest(DestFolder)), snapshotUUID)

  print ("")
  if DestSnapshotName != None:
    if Verify == False:
      print ("Snapshot already exists in destination : %22s %20s" % (snapshotUUID, SnapshotName ))
      return
    else:
      print ("Verifying snapshot : %22s %20s" % (snapshotUUID, SnapshotName ))
  else:
    print ("Copying snapshot   : %22s %20s" % (snapshotUUID, SnapshotName )) 
  
  #new file with checksums for the snapshot (not part of elastic snapshot)
  DestFileChecksum = DestFolder + '/checksums-' + snapshotUUID + '.json'
  if os.path.exists(DestFileChecksum):
    with open(DestFileChecksum, 'r') as f:
      SnapshotChecksums = json.loads(f.read())
  else:
    SnapshotChecksums = {}

  #Get a list of files and folders to copy
  Files, Folders = GetFileInfoIndex(SrcIndex, snapshotUUID)

  for File in Files:
    DestFileName = DestFolder + '/' + File
    SrcFileName = SourceFolder + '/' + File
    if File in  SnapshotChecksums:
      CopyFile(SrcFileName, DestFileName, SnapshotChecksums[File], Verify=Verify)
    else:
      file_sha1, filesize = CopyFile(SrcFileName, DestFileName)
      SnapshotChecksums[File] = {'sha1': file_sha1, 'size': filesize}

  #verify indice folder exists
  SrcIndicesFolder = SourceFolder + '/indices'
  DestIndicesFolder = DestFolder + '/indices'
  if not os.path.isdir(DestIndicesFolder):
    os.makedirs(DestIndicesFolder)
 
  for Folder in Folders:
    SrcSnapshotFolder = SrcIndicesFolder + '/' + Folder
    DestSnapshotFolder =  DestIndicesFolder + '/' + Folder

    MakeFolder(DestSnapshotFolder)
    SnapshotChecksums = WalkSnapShotFolder(SrcIndicesFolder, DestIndicesFolder, Folder, SnapshotChecksums, RelFolder='indices/' + Folder + '/', Verify=Verify)

  with open(DestFileChecksum, 'w') as f:
    json.dump(SnapshotChecksums , f)
    

  #extract relevant from index and write to new files
  DestCurrentIndex = GetIndexLatest(DestFolder)
  DestIndex = ReadIndex(DestFolder, DestCurrentIndex)
  NewIndex = UpdateIndexJSON(SrcIndex, DestIndex, snapshotUUID) #Make a new json to write

  #Increment file number, write json
  UpdateIndex(DestFolder, NewIndex)


def CompareSnapShots(SourceFolder, DestFolder, Verbose = True):
  SourceCurrentIndex = GetIndexLatest(SourceFolder)
  SrcIndex = ReadIndex(SourceFolder, SourceCurrentIndex)
  if Verbose:
    print ("Source Snapshots")
    ListSnapShots(SrcIndex)

  DestCurrentIndex = GetIndexLatest(DestFolder)
  DestIndex = ReadIndex(DestFolder, DestCurrentIndex)
  if Verbose:
    print ("")
    print ("Destination Snapshots")
    ListSnapShots(DestIndex)

  SrcSnapshots = {}
  for snapshot in SrcIndex['snapshots']:
    SrcSnapshots[snapshot['uuid']] = snapshot

  DestSnapshots = {}
  for snapshot in DestIndex['snapshots']:
    DestSnapshots[snapshot['uuid']] = snapshot
  
  MissingDest = list(set(SrcSnapshots.keys()) - set(DestSnapshots.keys()))

  if Verbose:
    print ("")  
    print ("There are %s snapshots missing" % len(MissingDest))
    MissingIndex = {'snapshots': [] }
    for index in MissingDest:
      for item in SrcIndex['snapshots']:
        if item['uuid'] == index:
          MissingIndex['snapshots'].append(item)
    ListSnapShots(MissingIndex)

  return MissingDest

def ListSnapShotsFolder(Folder):
  ListSnapShots(ReadIndex(Folder, GetIndexLatest(Folder)), Folder)


def GetDiskUsage(Folder, SnapshotUUID):
  CurrentIndex = GetIndexLatest(Folder)
  IndexJSON = ReadIndex(Folder, CurrentIndex)

  ChecksumFile = Folder + "/checksums-" + SnapshotUUID + ".json"
  try:
    with open(ChecksumFile, 'r') as f:
      CheckSums = json.loads(f.read())
  except:
    #no checksum file exists
    return 0

  TotalBytes = 0
  for item in CheckSums.keys():
    if isinstance(CheckSums[item]['size'], int):
      TotalBytes += CheckSums[item]['size']
  GigBytes = round(TotalBytes / 1024 / 1024 / 1024)

  return GigBytes


def TakeSnapShot(url, headers, SnapShotRepo, SnapShotName, SendJSON):
  #Check if a snapshot of the same name already exists
  SnapShots = GetSnapShots(url, headers, SnapShotRepo)
  for SnapShot in SnapShots:
    if SnapShot['id'] == SnapShotName:
     print ("A snapshot with the same name already exists")
     print ("SnapShotName : %s" % SnapShotName)
     sys.exit() #clean exit, as this is not a failure

  print ("SnapShotName : %s" % SnapShotName)
  print ("Taking a snapshot of the following idices : %s" % SendJSON)
  CreateSnapShot(url, headers, SnapShotRepo, SnapShotName, SendJSON)

#Compares the current indices in Elastic to the indices in the snapshot folder
# and shows what indices don't have a snapshot
def VerifyIndicesSnapshot(url, headers, Folder):
  ElasticJSON = (GetIndices(url, headers))
  SnapShotJSON = (ReadIndex(Folder, GetIndexLatest(Folder)))

  ElasticIndices = []
  for item in ElasticJSON:
    ElasticIndices.append (item['index'])

  SnapShotIndices = SnapShotJSON['indices'].keys()
  Missing = []
  CountSystem = 0
  for index in ElasticIndices:
    if index not in SnapShotIndices:
      Missing.append(index)
      if index[:1] == ".":
        CountSystem += 1

  print ("There are %s missing indices that have not been backed up!" % len(Missing))
  print ("%s of these indices are system indices" % CountSystem) 
 
  CountNoReplica = 0

  #From the list without a snapshot, which ones have no replica
  NoReplica = []
  for item in ElasticJSON:
    if item['index'] in Missing:
      if item['rep'] == '0':
        NoReplica.append( { 'index': item['index'], 'uuid': item['uuid'] } )
        CountNoReplica += 1
  NoReplica = sorted(NoReplica, key=lambda x : x['index'])

  print ("There are %s indices that have a single point of failure" % CountNoReplica)
  for item in NoReplica:
    print ("Index not backed up and no replica : %s : %s" % (item['uuid'], item['index']))

def main():
  options = docopt(__doc__)
  if options['--verbose']:
    print (options)
    print ("")
  if options['--list-snapshots']:
    if options['--folder']:
      ListSnapShotsFolder(options['--folder'])
    else:
      print ("ElasticSnap.py --list-snapshots --folder=<folder>")
  elif options['--copy']:
    print ("copy")
    if options['--src'] and options['--dst']:
      print (options['--src'])
      print (options['--dst'])
      if options['--uuid']:
        print (options['--uuid'])
        CopySnapShot(options['--src'], options['--dst'], options['--uuid'], Verify=False)
      else:
        print ("Copy by name not implemented yet")
    else:
      print ("ElasticSnap.py --copy --src=<folder> --dest=<folder> --uuid=<snapshot_name>")
      print ("            OR")
      print ("ElasticSnap.py --copy --src=<folder> --dest=<folder> --name=<snapshot_name>")
  elif options['--sync']:
    if options['--src'] and options['--dst']:
      print (options['--src'])
      print (options['--dst'])
      if options['--verbose']:
        MissingDest = CompareSnapShots(options['--src'], options['--dst'], Verbose=True)
      else:
        MissingDest = CompareSnapShots(options['--src'], options['--dst'], Verbose=False)
      for snapshotUUID in MissingDest:
        CopySnapShot(options['--src'], options['--dst'], snapshotUUID, Verify=False)
    else:
      print ("ElasticSnap.py --sync --src=<folder> --dest=<folder> --uuid=<snapshot_name>")

  elif options['--show-missing']:
    if options['--src'] and options['--dst']:
      print (options['--src'])
      print (options['--dst'])
      MissingDest = CompareSnapShots(options['--src'], options['--dst'], Verbose = False)
      ListSnapShotsFiltered(ReadIndex(options['--src'], GetIndexLatest(options['--src'])), MissingDest)

  elif options['--disk-usage']:
    if options['--folder'] and options['--uuid']:
      print (options['--folder'])
      print (options['--uuid'])
      GigBytes = GetDiskUsage(options['--folder'], options['--uuid'])
      print ("Total used : %s GB" % GigBytes)
    else:
      print ("ElasticSnap.py --disk-usage --folder=<folder> --uuid=<snapshot_uuid>")

  elif options['--take-snapshot']:
    if options['--repo'] and options['--name'] and options['--indices']:
      ListIndices = options['--indices'].split(",")
      SendJSON = { 'indices' : options['--indices'], 'ignore_unavailable': True, 'include_global_state': False }
      #print ("Creating a snapshot with name : %s" % options['--name'])
      #print ("Indices : %s" % options['--indices'])
      #print ("and saving to repository : %s" % options['--repo'])

      #url defined a top of script, should be in config file
      TakeSnapShot(url, headers, options['--repo'], options['--name'], SendJSON )
    else:
      print ("ElasticSnap.py --take-snapshot --repo=<SnapShotRepo> --name=<SnapShotName> --indices=<IndicesList>")
  elif options['--verify-indices-snapshot']:
    if options['--folder']:
      VerifyIndicesSnapshot(url, headers, options['--folder'])



if __name__ == "__main__":
    main()




