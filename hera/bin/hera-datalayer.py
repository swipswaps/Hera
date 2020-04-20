#! /home/yehuda/anaconda3/bin/python
import argparse
import textwrap
import json
from hera import datalayer

class ArgumentParser(argparse.ArgumentParser):
    def print_help(self):
        wrapper = textwrap.TextWrapper(width=80)
        print("help")

parser = ArgumentParser() #argparse.ArgumentParser()
parser.add_argument('command', nargs=1, type=str)
parser.add_argument('args', nargs='*', type=str)

args = parser.parse_args()

# commands = ["list","load","delete","copyTo","copyFrom"]

###
##
#
def help_list_handler():
    #wrapper = textwrap.TextWrapper(width=80)
    help = ["\033[1mNAME\033[0m",
            "\tlist - List documents\n",
            "\033[1mSYNOPSYS\033[0m",
            "\t\033[4mhera-datalayer\033[0m \033[4mlist\033[0m [<query>]\n",
            "\033[1mDESCRIPTION\033[0m",
            "\tPrint all the documents that fulfill the given query.",
            "\tThe query is given in the [<query>] arguments.\n",
            "\033[1mQUERY\033[0m",
            "\tThe query is a set of conditions to search documents over the database.\n",
            "\tA query argument should be as following:",
            '''\t    variable="'value'"\n''',
            "\033[1mEXAMPLE\033[0m",
            '''\t\033[1mhera-datalayer list projectName="'example'"\033[0m''',
            "\t    This example should print all the documents of a project called 'example'"
            ]
    print('\n'.join(help))
    # print("list the documents in the project")
    # print("\thera-datalyer list projectName [mongodb query]")

def help_load_handler():
    help = ["\033[1mNAME\033[0m",
            "\tload - Load documents to the database\n",
            "\033[1mSYNOPSYS\033[0m",
            "\t\033[4mhera-datalayer\033[0m \033[4mload\033[0m <file>\n",
            "\033[1mDESCRIPTION\033[0m",
            "\tLoad to the database all the documents found in the given file.",
            "\tThe file should be a JSON file in the following format:\n",
            '''\t    {
                "Measurements":[list of measurements documents],
                "Simulations":[list of simulations documents],
                "Analysis":[list of analysis documents]
            }\n''',
            "\033[1mEXAMPLE\033[0m",
            '''\t\033[1mhera-datalayer load docsToLoad.json\033[0m''',
            "\t    This example should load to the database the documents found in the file called 'docsToLoad.json'"
            ]
    print('\n'.join(help))

def help_delete_handler():
    help = ["\033[1mNAME\033[0m",
            "\tdelete - Delete documents from the database\n",
            "\033[1mSYNOPSYS\033[0m",
            "\t\033[4mhera-datalayer\033[0m \033[4mdelete\033[0m [<query>]/<file>\n",
            "\033[1mDESCRIPTION\033[0m",
            "\tDelete from the database all the documents that fulfill the given query.",
            "\tThe delete process is made by 2 parts:"
            "    "# I stopped here - need to complete
            "\033[1mEXAMPLE\033[0m",
            '''\t\033[1mhera-datalayer load docsToLoad.json\033[0m''',
            "\t    This example should load to the database the documents found in the file called 'docsToLoad.json'"
            ]
    print('\n'.join(help))

def help_copyTo_handler():
    pass

def help_copyFrom_handler():
    pass

def help_handler(arguments):
    if not arguments:
        help = ["usage: hera-datalayer <command> [<args>]\n",
                "These are the available commands:\n",
                "    list\tList documents",
                "    load\tLoad documents to the database",
                "    delete\tDelete documents from the database",
                "    copyTo\tCopy documents from the current user database to a destination database",
                "    copyFrom\tCopy documents from a destination database to the current user database"
                ]
        print("\n".join(help))
    else:
        try:
            globals()['help_%s_handler' % arguments[0]]()
        except KeyError:
            msg = ["'%s' is not an available command\n" % arguments[0],
                   "These are the available commands:\n",
                   "    list",
                   "    load",
                   "    delete",
                   "    copyTo",
                   "    copyFrom"
                   ]
            print("\n".join(msg))



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
        docDict = doc.asDict()
        cls = docDict.pop('_cls').split('.')[1]
        getattr(datalayer, '%s_Collection' % cls)(user='other').addDocument(**docDict)


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
        docDict = doc.asDict
        cls = docDict.pop('_cls').split('.')[1]
        getattr(datalayer, cls).addDocument(**docDict)


globals()['%s_handler' % args.command[0]](args.args)

