
__author__ = 'arun_bonam'


def getImpalaDriver(db):

    driver=''
    if db == 'impala':
        driver='com.cloudera.impala.jdbc41.Driver'
    return driver


def getJdbcConnectionUrl(db, prop):


    url=''
    if db == 'impala':
        url='jdbc:impala://{}:{}/{}'.format(prop['host'],prop['port'],prop['dbname'])
    return url



def getImpalaProperties(impalaObjects):
    dbs = impalaObjects.keys()
    props = {}
    for db, prop in impalaObjects.items():
        url = getJdbcConnectionUrl(db, prop)
        driver = getImpalaDriver(db)
        props[db] = dict([(i, locals()[i]) for i in ('url','driver')])
    return props


def dfWriter(df,table,impConf,writeMode):
    return df.write.format("jdbc") \
        .mode(writeMode) \
        .option(url=impConf.get('url')+":auth=noSasl:") \
        .option(table=table) \
        .option(impConf.get('driver'))


