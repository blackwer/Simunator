import os
import sys
import yaml
from parammaker import ParamMaker
import sqlite3
import time


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


conn = sqlite3.connect("test.db")
c = conn.cursor()
c.execute(
    """CREATE TABLE IF NOT EXISTS simunator_runsets (
                    time TEXT, cmdtemplate TEXT,
                    pathstring TEXT, templatestr TEXT
             );"""
)

currtime = time.strftime("%s", time.gmtime())

template_joined = ""
for key, val in templatestrs.items():
    template_joined += (
        "### simunator_begin - {0}\n".format(key)
        + val
        + "\n### simunator_end - {0}\n".format(key)
    )

c.execute(
    """INSERT INTO simunator_runsets VALUES ( ?, ?, ?, ? );""",
    (
        currtime,
        inputdata["system"]["cmd"],
        inputdata["system"]["pathstring"],
        template_joined,
    ),
)

paramstr = ""
for param,valexample in zip(pmaker._params, psets[0]):
    if isinstance(valexample, str):
        paramstr += param + " STRING, "
    else:
        paramstr += param + " NUMERIC, "
paramstr = paramstr[0:-2]
print('CREATE TABLE IF NOT EXISTS "{0}" ( {1} );'.format(str(currtime), paramstr))
c.execute('CREATE TABLE IF NOT EXISTS "{0}" ( {1} );'.format(str(currtime), paramstr))


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

    execstr = 'INSERT INTO "{0}" VALUES ( {1} );'.format(
        currtime, ", ".join(map(lambda x: '"' + x + '"' if isinstance(x, str) else str(x),
                                paramdict.values()))
    )
    c.execute(execstr)


os.chdir(currpath)

conn.commit()
conn.close()
