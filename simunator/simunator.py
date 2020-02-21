import os
import sys
import yaml
from parammaker import ParamMaker

if len(sys.argv) != 2:
    print("Must provide input configuration")
else:
    with open(sys.argv[1]) as f:
        inputdata = yaml.load(f, Loader=yaml.FullLoader)

    pmaker = ParamMaker()
    pmaker.from_param_makers(*[ParamMaker(dist) for dist in inputdata["dists"]])
    psets = [tuple(pmaker.flatten(tup)) for tup in pmaker.items()]


templatestrs = {}
for fname in inputdata["system"]["templates"]:
    with open(fname) as f:
        templatestrs[fname] = f.read()

currpath = os.getcwd()
for pset in psets:
    paramdict = {}
    for key, val in zip(pmaker._params, pset):
        paramdict[key] = val

    simpath = os.path.join(
        currpath, inputdata["system"]["pathstring"].format(**paramdict)
    )
    print("Creating path: " + simpath)
    try:
        os.makedirs(simpath)
    except:
        pass

    for fname, templatestr in templatestrs.items():
        ofile = os.path.join(simpath, fname)
        print("Creating file: " + ofile)
        with open(ofile, "w") as f:
            f.write(templatestr.format(**paramdict))

    os.chdir(simpath)
    os.system(inputdata["system"]["cmd"])

os.chdir(currpath)
