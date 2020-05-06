#!/usr/bin/env python3
import os
import sys
import yaml
from parammaker import ParamMaker
import sqlite3
import time

def gen_param_sets(inputconfig):
    """Generates a parammaker object that generates all unique combinations of parameters as
specified by input yaml file, and a psets list, the actualization of the generators into a
list.
    """
    # FIXME: probably not best to return two objects, which are basically the same.
    pmaker = ParamMaker()
    pmaker.from_param_makers(*[ParamMaker(dist) for dist in inputconfig["dists"]])
    psets = [tuple(pmaker.flatten(tup)) for tup in pmaker.items()]

    return pmaker, psets

def gen_template_strings(inputconfig):
    """Generates template strings database from list of input templates."""
    templatestrs = {}
    template_joined = ""
    for fname in inputconfig["system"]["templates"]:
        with open(fname) as f:
            templatestrs[fname] = f.read()
            template_joined += (
            "### simunator_begin - {0}\n".format(fname)
            + templatestrs[fname]
            + "\n### simunator_end - {0}\n".format(fname)
        )

    return templatestrs, template_joined

def get_db(dbname):
    """Opens or creates sqlite3 database that contains simulation information for faithful
    reproduction of simulation run information.
    """
    conn = sqlite3.connect(dbname)
    c = conn.cursor()
    c.execute(

    """CREATE TABLE IF NOT EXISTS simunator_runsets (
                        time TEXT, cmdtemplate TEXT,
                        pathstring TEXT, templatestr TEXT
                 );"""
    )

    return c, conn

def add_set_to_db(c, currtime, inputconfig, template_joined, pmaker, psets):
    """Adds system information to database and create table that holds the unique combinations of param:value pairs."""
    c.execute(
        """INSERT INTO simunator_runsets VALUES ( ?, ?, ?, ? );""",
        (
            currtime,
            inputconfig["system"]["cmd"],
            inputconfig["system"]["pathstring"],
            template_joined,
        ),
    )

    paramstr = ""
    for param, valexample in zip(pmaker._params, psets[0]):
        if isinstance(valexample, str):
            paramstr += param + " STRING, "
        else:
            paramstr += param + " NUMERIC, "
    paramstr = paramstr[0:-2]
    print('CREATE TABLE IF NOT EXISTS "{0}" ( {1} );'.format(str(currtime), paramstr))
    c.execute('CREATE TABLE IF NOT EXISTS "{0}" ( {1} );'.format(str(currtime), paramstr))


def create_sims(c, currtime, psets, templatestrs):
    """Write simulation information to disk for actual running."""
    currpath = os.getcwd()
    sim_keywords = {"SIM_DATE": currtime}
    for pset in psets:
        paramdict = {}
        for key, val in zip(pmaker._params, pset):
            paramdict[key] = val

        simpath = os.path.join(
            currpath, inputconfig["system"]["pathstring"].format(**{**sim_keywords, **paramdict})
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

        # os.chdir(simpath)
        # os.system(inputconfig["system"]["cmd"].format(**{**sim_keywords, **paramdict}))

        execstr = 'INSERT INTO "{0}" VALUES ( {1} );'.format(
            currtime, ", ".join(map(lambda x: '"' + x + '"' if isinstance(x, str) else str(x),
                                    paramdict.values()))
        )
        c.execute(execstr)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Must provide input configuration")
        sys.exit(1)

    with open(sys.argv[1]) as f:
        inputconfig = yaml.load(f, Loader=yaml.FullLoader)

    pmaker, psets = gen_param_sets(inputconfig)

    templatestrs, template_joined = gen_template_strings(inputconfig)

    currtime = time.strftime("%s", time.gmtime())
    c, conn = get_db(inputconfig["system"]["database"])
    add_set_to_db(c, currtime, inputconfig, template_joined, pmaker, psets)

    create_sims(c, currtime, psets, templatestrs)

    conn.commit()
    conn.close()
