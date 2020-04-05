import argparse
import json
from hera import datalayer

parser = argparse.ArgumentParser()
parser.add_argument('command', nargs=1, type=str)
parser.add_argument('args', nargs='*', type=str)

args = parser.parse_args()


def list_handler(arguments):
    fullQuery={}
    for queryElement in arguments:
        fullQuery[queryElement.split('=')[0]] = eval(queryElement.split('=')[1])
    for doc in datalayer.Measurements.getDocuments(**fullQuery):
        print(doc.asDict())


def load_handler(arguments):
    with open(arguments[0], 'r') as myFile:
        docsDict = json.load(myFile)
    for cls in docsDict:
        for doc in docsDict[cls]:
            getattr(datalayer, cls).addDocument(**doc)


def delete_handler(arguments):
    try:
        with open(arguments[0], 'r') as myFile:
            docDict = json.load(myFile)
        for cls in docDict:
            for doc in docDict[cls]:
                for key in doc['_id']:
                    id = doc['_id'][key]
                getattr(datalayer, cls).deleteDocumentByID(id=id)
    except FileNotFoundError:
        fullQuery = {}
        for queryElement in arguments:
            fullQuery[queryElement.split('=')[0]] = eval(queryElement.split('=')[1])
        docsToDelete = {}
        with open('docsToDelete.json', 'w') as myFile:
            docsToDelete['Measurements'] = datalayer.Measurements.getDocumentsAsDict(**fullQuery, with_id=True)['documents']
            docsToDelete['Simulations'] = datalayer.Simulations.getDocumentsAsDict(**fullQuery, with_id=True)['documents']
            docsToDelete['Analysis'] = datalayer.Analysis.getDocumentsAsDict(**fullQuery, with_id=True)['documents']
            json.dump(docsToDelete, myFile, indent=4, sort_keys=True)


def copyTo_handler(arguments):
    info = arguments[0]
    mongoConfig = dict(username=info.split(':')[0],
                       password=info.split(':')[1].split('@')[0],
                       dbIP=info.split(':')[1].split('@')[1].split('/')[0],
                       dbName=info.split(':')[1].split('@')[1].split('/')[1]
                       )

    datalayer.createDBConnection('other', mongoConfig=mongoConfig)

    fullQuery = {}
    for queryElement in arguments[1:]:
        fullQuery[queryElement.split('=')[0]] = eval(queryElement.split('=')[1])

    docList = datalayer.All.getDocuments(**fullQuery)
    for doc in docList:
        cls = doc['_cls'].split('.')[1]
        getattr(datalayer, '%s_Collection' % cls)(user='other').addDocument(**doc.asDict())


def copyFrom_handler(arguments):
    info = arguments[0]
    mongoConfig = dict(username=info.split(':')[0],
                       password=info.split(':')[1].split('@')[0],
                       dbIP=info.split(':')[1].split('@')[1].split('/')[0],
                       dbName=info.split(':')[1].split('@')[1].split('/')[1]
                       )

    datalayer.createDBConnection('other', mongoConfig=mongoConfig)

    fullQuery = {}
    for queryElement in arguments[1:]:
        fullQuery[queryElement.split('=')[0]] = eval(queryElement.split('=')[1])

    docList = datalayer.AbstractCollection(user='other').getDocuments(**fullQuery)
    for doc in docList:
        cls = doc['_cls'].split('.')[1]
        getattr(datalayer, cls).addDocument(**doc.asDict())


globals()['%s_handler' % args.command[0]](args.args)