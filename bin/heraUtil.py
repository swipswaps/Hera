import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--dump', dest='dump', action='store_true', help='')
parser.add_argument('--outputFile', dest='outputFile', default=None, help='')
parser.add_argument('--projectName', dest='projectName', default=None, help='')
parser.add_argument('--queryFile', dest='queryFile', default=None, help='')
parser.add_argument('--load', dest='load', action="store_true", help='')

args = parser.parse_args()

