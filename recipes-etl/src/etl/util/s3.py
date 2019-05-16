__author__ = 'arun_bonam'

def getS3Properties(s3Objects):
    dbs = s3Objects.keys()
    props = {}
    for db, prop in s3Objects.items():
        file =prop['file']
        bucket =prop['bucket']
        region =prop['region']

        props[db] = dict([(i, locals()[i]) for i in ('file','bucket','region')])
    return props

def S3Reader(sparkSession,properties):
    return sparkSession.read.json("s3n://"+properties.get('bucket')+"/"+properties.get('file'))
