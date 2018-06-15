import sys
import argparse
mpla = sys.argv[1]
intarg = 1
strarg= "mpla"
print(mpla)

result = '{"intarg" : ' + str(intarg) + ', "strarg" : "' + strarg + '"}'
print(result)
