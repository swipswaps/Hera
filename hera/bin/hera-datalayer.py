import argparse
import json
from pyhera.hera import datalayer

parser = argparse.ArgumentParser()
parser.add_argument('action', nargs=1, type=str)
parser.add_argument('projectName', nargs=1, type=str)
parser.add_argument('query', nargs='*', type=str)

args = parser.parse_args()

action = args.action[0]
projectName = args.projectName[0]
query = args.query

fullQuery = dict(projectName=projectName)
for queryElement in query:
    fullQuery[queryElement.split('=')[0]] = eval(queryElement.split('=')[1])

if action=='list':
    for doc in datalayer.Measurements.getDocuments(**fullQuery):
        print(doc.asDict())
    #print('list')
elif action=='load':
    datalayer.Measurements.addDocument(**fullQuery)
    #print('load')
elif action=='delete':
    try:
        with open(projectName, 'r') as myFile:
            docList = json.load(myFile)['documents']
        for doc in docList:
            for key in doc['_id']:
                id = doc['_id'][key]
            datalayer.Measurements.deleteDocumentByID(id=id)
    except FileNotFoundError:
        with open('docToDelete.json', 'w') as myFile:
            json.dump(datalayer.Measurements.getDocumentsAsDict(**fullQuery, with_id=True), myFile, indent=4, sort_keys=True)
    #print('delete')