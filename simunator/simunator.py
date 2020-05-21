#!/usr/bin/env python3
import sys
import os
from parammaker import ParamMaker
import sqlite3
import time
import argparse

verbose = False


class Simunator:
    db = 'simunator.db'

    def __init__(self, args):
        actions = {'create': self.create,
                   'list': self.list_sims,
                   'delete': self.delete,
                   'collect': self.collect,
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
        self.get_db()
        self.exec_sql("SELECT time FROM simunator_runsets;")

        def gmt(x):
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(x)))
        print("\n".join(["{timestamp}  ({gmt} GMT)".format(timestamp=tup[0],
                                                           gmt=gmt(tup[0]))
                         for tup in self.c.fetchall()]))

    def delete(self, args):
        parser = argparse.ArgumentParser(
            description="Delete simulation batch.")
        parser.add_argument('timestamp', type=str,
                            help="Timestamp to process")
        parsedargs = parser.parse_args(args)

        self.get_db()

        self.exec_sql("SELECT * from '{0}';".format(parsedargs.timestamp))
        sims = self.c.fetchall()
        paramlist = sims[0].keys()

        for paramvals in sims:
            parammap = {**dict(zip(paramlist, paramvals)),
                        **{'SIM_DATE': parsedargs.timestamp}}
            path = parammap['SIM_PATH']

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
        parser.add_argument('timestamp', type=str,
                            help="Timestamp to process")
        parser.add_argument('--task-file', type=str, help="Output file for tasks",
                            dest='taskfile', default=None)
        parser.add_argument('--command', type=str, help="Command alias to run",
                            dest='command', default='run')
        parsedargs = parser.parse_args(args)

        outfile = open(parsedargs.taskfile,
                       'w') if parsedargs.taskfile else sys.stdout

        self.get_db()

        self.exec_sql("SELECT cmdname, cmdtemplate FROM simunator_commands;")
        cmds = dict(self.c.fetchall())

        self.exec_sql("SELECT * from '{0}';".format(parsedargs.timestamp))

        for paramvals in self.c.fetchall():
            paramlist = paramvals.keys()
            parammap = {**dict(zip(paramlist, paramvals)),
                        **{'SIM_DATE': parsedargs.timestamp}}
            path = parammap['SIM_PATH']
            cmd = cmds[parsedargs.command].format(**parammap)
            print("cd '{path}'; {cmd}".format(
                path=path, cmd=cmd), file=outfile)

    def create(self, args):
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

        self.get_db()
        self.add_set_to_db()
        self.create_sims()

    def collect(self, args):
        parser = argparse.ArgumentParser(
            description="Collect simulation batch.")
        parser.add_argument('timestamp', type=str,
                            help="Timestamp to process")
        parser.add_argument('--collector', type=str, dest='collector',
                            help='Collector to use. Default is to collect all', default=None)
        parsedargs = parser.parse_args(args)

        self.get_db()

        self.exec_sql(
            "SELECT *,rowid from '{0}';".format(parsedargs.timestamp))
        sims = self.c.fetchall()
        paramlist = sims[0].keys()

        if parsedargs.collector:
            self.exec_sql("SELECT cmdname, cmdtemplate FROM simunator_collectors WHERE cmdname == '{0}';".format(
                parsedargs.collector))
            cmdpairs = self.c.fetchall()
        else:
            self.exec_sql(
                "SELECT cmdname, cmdtemplate FROM simunator_collectors;")
            cmdpairs = self.c.fetchall()

        import subprocess
        import shlex
        for paramvals in sims:
            parammap = {**dict(zip(paramlist, paramvals)),
                        **{'SIM_DATE': parsedargs.timestamp}}
            path = os.path.join(os.getcwd(), parammap['SIM_PATH'])
            for cmdpair in cmdpairs:
                var, cmdtemplate = cmdpair
                cmdlist = shlex.split(cmdtemplate.format(**parammap))
                result = subprocess.run(
                    cmdlist, cwd=path, stdout=subprocess.PIPE)
                val = float(result.stdout.decode('utf-8'))

                self.exec_sql("UPDATE '{0}' SET '{1}' = {2} where rowid = {3};".format(parsedargs.timestamp,
                                                                                       var,
                                                                                       val,
                                                                                       paramvals["rowid"]))

    def exec_sql(self, execstr):
        if verbose:
            print(execstr, file=sys.stderr)
        self.c.execute(execstr)

    def gen_param_sets(self):
        """Creates a parammaker object that generates all unique combinations of
    parameters as specified by input yaml file, and a psets list, the actualization
    of the generators into a list.
        """
        pmaker = ParamMaker()
        pmaker.from_param_makers(*[ParamMaker(dist)
                                   for dist in self.inputconfig["dists"]])
        self.params, self.psets = pmaker.actualize()

    def gen_template_strings(self):
        """Generates template strings database from list of input templates."""
        self.templatestrs = {}
        for fname in self.inputconfig['system']['templates']:
            with open(fname) as f:
                self.templatestrs[fname] = f.read()

    def get_db(self):
        """Opens or creates sqlite3 database that contains simulation information for
    faithful reproduction of simulation run information.
        """
        self.conn = sqlite3.connect(self.db)
        self.conn.row_factory = sqlite3.Row
        self.c = self.conn.cursor()
        self.exec_sql(
            """CREATE TABLE IF NOT EXISTS simunator_runsets (
                            time TEXT, pathstring TEXT, templatestr TEXT
                     );""",
        )
        self.exec_sql(
            """CREATE TABLE IF NOT EXISTS simunator_commands (
                            cmdname TEXT, cmdtemplate TEXT
                     );""",
        )
        self.exec_sql(
            """CREATE TABLE IF NOT EXISTS simunator_collectors (
                            cmdname TEXT, cmdtemplate TEXT
                     );""",
        )

    def add_set_to_db(self):
        """Adds system information for current simulation set to database and create 
    table that holds the unique combinations of param:value pairs.
        """
        self.c.execute("INSERT INTO simunator_runsets VALUES ( ?, ?, ? );", (
            self.currtime,
            self.inputconfig["system"]["pathstring"],
            str(self.templatestrs)))

        for cmdname, cmdtemplate in self.inputconfig["system"]["commands"].items():
            self.c.execute(
                "INSERT INTO simunator_commands VALUES ( ?, ? );",
                (cmdname, cmdtemplate)
            )

        for collectname, collecttemplate in self.inputconfig["system"]["collectors"].items():
            self.c.execute(
                "INSERT INTO simunator_collectors VALUES ( ?, ? );",
                (collectname, collecttemplate)
            )

        paramstr = "SIM_PATH STRING"
        for param, valexample in zip(self.params, self.psets[0]):
            paramstr += ", " + param + " STRING" if isinstance(
                valexample, str) else ", " + param + " NUMERIC"
        for collector in self.inputconfig["system"]["collectors"].keys():
            paramstr += ", " + collector + " NUMERIC"

        self.exec_sql(
            "CREATE TABLE IF NOT EXISTS '{0}' ( {1} );".format(
                str(self.currtime), paramstr,
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

            paramdict['SIM_PATH'] = os.path.join(
                self.inputconfig['system']['pathstring'].format(
                    **{**sim_keywords, **paramdict}),
            )
            print("Creating path: {0}".format(paramdict['SIM_PATH']))
            try:
                os.makedirs(paramdict['SIM_PATH'])
            except:
                pass

            for fname, templatestr in self.templatestrs.items():
                ofile = os.path.join(paramdict['SIM_PATH'], fname)
                print("Creating file: " + ofile)
                with open(ofile, "w") as f:
                    f.write(templatestr.format(**paramdict))

            self.exec_sql(
                "INSERT INTO '{0}' ( {1} ) VALUES ( {2} );".format(
                    self.currtime,
                    ", ".join(paramdict.keys()),
                    ", ".join(map(lambda x: '"{}"'.format(x) if isinstance(x, str) else str(x),
                                  paramdict.values())),
                ),
            )


if __name__ == "__main__":
    sims = Simunator(sys.argv[1:])
