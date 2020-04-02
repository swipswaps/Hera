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
    fullQuery = {}
    for queryElement in arguments:
        fullQuery[queryElement.split('=')[0]] = eval(queryElement.split('=')[1])
    datalayer.Measurements.addDocument(**fullQuery)


def delete_handler(arguments):
    try:
        with open(arguments[0], 'r') as myFile:
            docList = json.load(myFile)['documents']
        for doc in docList:
            for key in doc['_id']:
                id = doc['_id'][key]
            datalayer.Measurements.deleteDocumentByID(id=id)
    except FileNotFoundError:
        fullQuery = {}
        for queryElement in arguments:
            fullQuery[queryElement.split('=')[0]] = eval(queryElement.split('=')[1])
        with open('docToDelete.json', 'w') as myFile:
            json.dump(datalayer.Measurements.getDocumentsAsDict(**fullQuery, with_id=True), myFile, indent=4, sort_keys=True)


def loadTo_handler(arguments):
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

    docList = datalayer.Measurements.getDocuments(**fullQuery)
    for doc in docList:
        datalayer.Measurements_Collection(user='other').addDocument(**doc.asDict())


def loadFrom_handler(arguments):
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

    docList = datalayer.Measurements_Collection(user='other').getDocuments(**fullQuery)
    for doc in docList:
        datalayer.Measurements.addDocument(**doc.asDict())


globals()['%s_handler' % args.command[0]](args.args)