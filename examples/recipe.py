import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--intarg", help="display a square of a given number", type=int)
parser.add_argument("--strarg", help="display a square of a given number", type=str)
args = parser.parse_args()

result = '{"intarg" : ' + str(args.intarg) + ', "strarg" : "' + args.strarg + '"}'
print(result)
