#!/usr/bin/env python3
import sys
import os
from parammaker import ParamMaker
import sqlite3
import time
import argparse

verbose = True


class Simunator:
    def __init__(self, args):
        actions = {'generate': self.generate,
                   'list': self.list_sims,
                   'delete': self.delete,
                   'listtasks': self.gen_tasks,
                   }

        command = args[0] if len(args) else ''
        args = args[1:]

        if command not in actions.keys():
            print("Invalid command: {0}".format(command), file=sys.stderr)
            print("Valid options are: {0}".format(
                  ", ".join(actions.keys())), file=sys.stderr)
            sys.exit(1)
        else:
            actions[command](args)

        self.conn.commit()

    def list_sims(self, args):
        parser = argparse.ArgumentParser(
            description="List simulation data.")
        parser.add_argument('db', type=str,
                            help="Database file for Simunator.")
        parsedargs = parser.parse_args(args)

        self.get_db(parsedargs.db)
        self.exec_sql("SELECT time FROM simunator_runsets;")

        print("\n".join(["{timestamp}  ({gmt} GMT)".format(timestamp=tup[0],
                                                           gmt=time.strftime("%Y-%m-%d %H:%M:%S",
                                                                             time.localtime(int(tup[0]))))
                         for tup in self.c.fetchall()]))

    def delete(self, args):
        parser = argparse.ArgumentParser(
            description="Delete simulation batch.")
        parser.add_argument('db', type=str,
                            help="Database file for Simunator.")
        parser.add_argument('timestamp', type=str,
                            help="Timestamp to process")
        parsedargs = parser.parse_args(args)

        self.get_db(parsedargs.db)

        self.exec_sql("SELECT pathstring FROM simunator_runsets;")
        pathstring = self.c.fetchone()[0]

        self.exec_sql("SELECT * from '{0}';".format(parsedargs.timestamp))
        paramlist = next(zip(*self.c.description))

        for paramvals in self.c.fetchall():
            parammap = {**dict(zip(paramlist, paramvals)), **
                        {'SIM_DATE': parsedargs.timestamp}}

            path = pathstring.format(**parammap)

            import shutil
            try:
                shutil.rmtree(path)
            except OSError as e:
                print("Error: %s - %s." % (e.filename, e.strerror))

        self.exec_sql("DROP TABLE '{0}';".format(parsedargs.timestamp))
        self.exec_sql("DELETE FROM simunator_runsets WHERE time='{0}';".format(
            parsedargs.timestamp))

    def gen_tasks(self, args):
        parser = argparse.ArgumentParser(
            description="Delete simulation batch.")
        parser.add_argument('db', type=str,
                            help="Database file for Simunator.")
        parser.add_argument('timestamp', type=str,
                            help="Timestamp to process")
        parser.add_argument('--task-file', type=str, help="Output file for tasks",
                            dest='taskfile', default=None)
        parser.add_argument('--cmd', type=str, help="Command alias to run",
                            dest='cmd', default='run')
        parsedargs = parser.parse_args(args)

        outfile = open(parsedargs.taskfile,
                       'w') if parsedargs.taskfile else sys.stdout

        self.get_db(parsedargs.db)

        self.exec_sql("SELECT pathstring, cmds FROM simunator_runsets;")
        pathstring, cmdsstring = self.c.fetchone()
        cmds = eval(cmdsstring)

        self.exec_sql("SELECT * from '{0}';".format(parsedargs.timestamp))
        paramlist = next(zip(*self.c.description))

        for paramvals in self.c.fetchall():
            parammap = {**dict(zip(paramlist, paramvals)), **
                        {'SIM_DATE': parsedargs.timestamp}}
            path = pathstring.format(**parammap)
            cmd = cmds[parsedargs.cmd].format(**parammap)
            print("cd {path}; {cmd}".format(path=path, cmd=cmd), file=outfile)

    def generate(self, args):
        import yaml
        parser = argparse.ArgumentParser(
            description="Generation simulation data.")
        parser.add_argument('config', type=str,
                            help="Config file for Simunator.")
        parser.add_argument('--taskfile', type=str, help="Output file for tasks",
                            dest='taskfile', default='sys.stdout')
        parsedargs = parser.parse_args(args)

        with open(parsedargs.config) as f:
            self.inputconfig = yaml.load(f, Loader=yaml.FullLoader)

        self.currtime = time.strftime("%s", time.gmtime())

        self.gen_param_sets()
        self.gen_template_strings()

        self.get_db(self.inputconfig["system"]["database"])
        self.add_set_to_db()
        self.create_sims()

    def exec_sql(self, execstr):
        if verbose:
            print(execstr, file=sys.stderr)
        self.c.execute(execstr)

    def gen_param_sets(self):
        """Creates a parammaker object that generates all unique combinations of
    parameters as specified by input yaml file, and a psets list, the actualization
    of the generators into a list.
        """
        # FIXME: probably not best to return two objects, which are basically the
        # same.
        pmaker = ParamMaker()
        pmaker.from_param_makers(*[ParamMaker(dist)
                                   for dist in self.inputconfig["dists"]])
        self.params, self.psets = pmaker.actualize()

    def gen_template_strings(self):
        """Generates template strings database from list of input templates."""
        self.templatestrs = {}
        self.template_joined = ""
        for fname in self.inputconfig['system']['templates']:
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
                            time TEXT, cmds TEXT,
                            pathstring TEXT, templatestr TEXT
                     );""",
        )

    def add_set_to_db(self):
        """Adds system information to database and create table that holds the unique
    combinations of param:value pairs.
        """
        self.c.execute("INSERT INTO simunator_runsets VALUES ( ?, ?, ?, ? );", (
            self.currtime,
            str(self.inputconfig["system"]["cmds"]),
            self.inputconfig["system"]["pathstring"],
            self.template_joined))

        paramstr = ""
        for param, valexample in zip(self.params, self.psets[0]):
            paramstr += param + " STRING, " if isinstance(
                valexample, str) else param + " NUMERIC, "
        self.exec_sql(
            "CREATE TABLE IF NOT EXISTS '{0}' ( {1} );".format(
                str(self.currtime), paramstr[0:-2]
            ),
        )

    def create_sims(self):
        """Write simulation information to disk for actual running."""
        currpath = os.getcwd()
        sim_keywords = {'SIM_DATE': self.currtime}
        for pset in self.psets:
            paramdict = {}
            for key, val in zip(self.params, pset):
                paramdict[key] = val

            simpath = os.path.join(
                currpath,
                self.inputconfig['system']['pathstring'].format(
                    **{**sim_keywords, **paramdict}),
            )
            print("Creating path: {0}".format(simpath))
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
                "INSERT INTO '{0}' VALUES ( {1} );".format(
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


if __name__ == "__main__":
    sims = Simunator(sys.argv[1:])
