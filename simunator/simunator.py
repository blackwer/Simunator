#!/usr/bin/env python3
import os
import yaml
from parammaker import ParamMaker
import sqlite3
import time

verbose = False


class Simunator:
    def __init__(self, args):
        with open(args[0]) as f:
            self.inputconfig = yaml.load(f, Loader=yaml.FullLoader)

        self.currtime = time.strftime("%s", time.gmtime())

        self.gen_param_sets()
        self.gen_template_strings()

        self.get_db(self.inputconfig["system"]["database"])
        self.add_set_to_db()
        self.create_sims()

        self.conn.commit()

    def exec_sql(self, execstr):
        if verbose:
            print(execstr)
        self.c.execute(execstr)

    def gen_param_sets(self):
        """Generates a parammaker object that generates all unique combinations of
    parameters as specified by input yaml file, and a psets list, the actualization
    of the generators into a list.
        """
        # FIXME: probably not best to return two objects, which are basically the
        # same.
        self.pmaker = ParamMaker()
        self.pmaker.from_param_makers(*[ParamMaker(dist)
                                        for dist in self.inputconfig["dists"]])
        self.psets = [tuple(self.pmaker.flatten(tup))
                      for tup in self.pmaker.items()]

    def gen_template_strings(self):
        """Generates template strings database from list of input templates."""
        self.templatestrs = {}
        self.template_joined = ""
        for fname in self.inputconfig["system"]["templates"]:
            with open(fname) as f:
                self.templatestrs[fname] = f.read()
                self.template_joined += (
                    "### simunator_begin - {0}\n".format(fname)
                    + self.templatestrs[fname]
                    + "\n### simunator_end - {0}\n".format(fname)
                )

    def get_db(self, dbname):
        """Opens or creates sqlite3 database that contains simulation information for
    faithful reproduction of simulation run information.
        """
        self.conn = sqlite3.connect(dbname)
        self.c = self.conn.cursor()
        self.exec_sql(
            """CREATE TABLE IF NOT EXISTS simunator_runsets (
                            time TEXT, cmdtemplate TEXT,
                            pathstring TEXT, templatestr TEXT
                     );""",
        )

    def add_set_to_db(self):
        """Adds system information to database and create table that holds the unique
    combinations of param:value pairs.
        """
        self.c.execute(
            """INSERT INTO simunator_runsets VALUES ( ?, ?, ?, ? );""",
            (
                self.currtime,
                self.inputconfig["system"]["cmd"],
                self.inputconfig["system"]["pathstring"],
                self.template_joined,
            ),
        )

        paramstr = ""
        for param, valexample in zip(self.pmaker._params, self.psets[0]):
            if isinstance(valexample, str):
                paramstr += param + " STRING, "
            else:
                paramstr += param + " NUMERIC, "
        self.exec_sql(
            'CREATE TABLE IF NOT EXISTS "{0}" ( {1} );'.format(
                str(self.currtime), paramstr[0:-2]
            ),
        )

    def create_sims(self):
        """Write simulation information to disk for actual running."""
        currpath = os.getcwd()
        sim_keywords = {"SIM_DATE": self.currtime}
        for pset in self.psets:
            paramdict = {}
            for key, val in zip(self.pmaker._params, pset):
                paramdict[key] = val

            simpath = os.path.join(
                currpath,
                self.inputconfig["system"]["pathstring"].format(
                    **{**sim_keywords, **paramdict}),
            )
            print("Creating path: " + simpath)
            try:
                os.makedirs(simpath)
            except:
                pass

            for fname, templatestr in self.templatestrs.items():
                ofile = os.path.join(simpath, fname)
                print("Creating file: " + ofile)
                with open(ofile, "w") as f:
                    f.write(templatestr.format(**paramdict))

            # os.chdir(simpath)
            # os.system(self.inputconfig["system"]["cmd"].format(**{**sim_keywords, **paramdict}))

            self.exec_sql(
                'INSERT INTO "{0}" VALUES ( {1} );'.format(
                    self.currtime,
                    ", ".join(
                        map(
                            lambda x: '"' + x +
                            '"' if isinstance(x, str) else str(x),
                            paramdict.values(),
                        )
                    ),
                ),
            )

    def run(self):
        pass


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Must provide input configuration")
        sys.exit(1)

    sims = Simunator(sys.argv[1:])
    sims.run()
