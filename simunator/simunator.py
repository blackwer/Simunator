#!/usr/bin/env python3
import sys
import os
from parammaker import ParamMaker
import sqlite3
import time
import argparse
import numpy as np
from jinja2 import Template


class Simunator:
    db = 'simunator.db'

    def __init__(self, args):
        self.add_custom_sqlite_types()

        actions = {'create': self.create,
                   'list': self.list_sims,
                   'delete': self.delete,
                   'collect': self.collect,
                   'listtasks': self.gen_tasks,
                   'modify': self.modify,
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

    def add_custom_sqlite_types(self):
        def adapt_array(arr):
            out = io.BytesIO()
            np.save(out, arr)
            out.seek(0)
            return sqlite3.Binary(out.read())

        def convert_array(text):
            out = io.BytesIO(text)
            out.seek(0)
            return np.load(out)

        # Converts np.array to TEXT when inserting
        sqlite3.register_adapter(np.ndarray, adapt_array)

        # Converts TEXT to np.array when selecting
        sqlite3.register_converter("array", convert_array)

    def list_sims(self, args):
        self.get_db()
        self.c.execute("SELECT time FROM simunator_runsets;")

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

        self.c.execute("SELECT * from '{0}';".format(parsedargs.timestamp))
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
                print("Error: {0} - {1}.".format(e.filename, e.strerror))

        self.c.execute("DROP TABLE '{0}';".format(parsedargs.timestamp))
        self.c.execute(
            "DELETE FROM simunator_runsets WHERE time=?;", (parsedargs.timestamp,))

    def gen_tasks(self, args):
        parser = argparse.ArgumentParser(
            description="Generate list of tasks for a given simulation set.")
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

        self.c.execute("SELECT cmdname, cmdtemplate FROM simunator_commands;")
        cmds = dict(self.c.fetchall())

        self.c.execute("SELECT * from '{0}';".format(parsedargs.timestamp))

        for paramvals in self.c.fetchall():
            paramlist = paramvals.keys()
            parammap = {**dict(zip(paramlist, paramvals)),
                        **{'SIM_DATE': parsedargs.timestamp}}
            path = parammap['SIM_PATH']
            cmd = Template(cmds[parsedargs.command]).render(**parammap)
            print("cd '{path}'; {cmd}".format(
                path=path, cmd=cmd), file=outfile)

    def create(self, args):
        import yaml
        parser = argparse.ArgumentParser(
            description="Generate simulation hiearchy data.")
        parser.add_argument('config', type=str,
                            help="Config file for Simunator.")
        parsedargs = parser.parse_args(args)

        with open(parsedargs.config) as f:
            self.inputconfig = yaml.load(f, Loader=yaml.FullLoader)

        self.currtime = time.strftime("%s", time.gmtime())

        self.gen_param_sets()
        self.gen_template_strings()

        self.get_db()
        self.add_set_to_db()
        self.create_sims()

    def modify(self, args):
        import yaml
        parser = argparse.ArgumentParser(
            description="Replace simulation 'system' configuration, excluding pathstring.")
        parser.add_argument('config', type=str,
                            help="Config file for Simunator.")
        parsedargs = parser.parse_args(args)

        with open(parsedargs.config) as f:
            self.inputconfig = yaml.load(f, Loader=yaml.FullLoader)

        self.get_db()
        self.c.execute("DELETE FROM simunator_commands;")
        self.add_commands()
        self.c.execute("DELETE FROM simunator_collectors;")
        self.add_collectors()

    def collect(self, args):
        parser = argparse.ArgumentParser(
            description="Collect simulation batch.")
        parser.add_argument('timestamp', type=str,
                            help="Timestamp to process")
        parser.add_argument('--collector', type=str, dest='collector',
                            help='Collector to use. Default is to collect all', default=None)
        parsedargs = parser.parse_args(args)

        self.get_db()

        self.c.execute(
            "SELECT *,rowid from '{0}';".format(parsedargs.timestamp))
        sims = self.c.fetchall()
        paramlist = sims[0].keys()

        if parsedargs.collector:
            self.c.execute("SELECT cmdname, cmdtemplate FROM simunator_collectors WHERE cmdname == ?;", (
                parsedargs.collector,))
            cmdpairs = self.c.fetchall()
        else:
            self.c.execute(
                "SELECT cmdname, cmdtemplate FROM simunator_collectors;")
            cmdpairs = self.c.fetchall()

        import subprocess
        import io
        for paramvals in sims:
            parammap = {**dict(zip(paramlist, paramvals)),
                        **{'SIM_DATE': parsedargs.timestamp}}
            path = os.path.join(os.getcwd(), parammap['SIM_PATH'])
            for cmdpair in cmdpairs:
                var, cmdtemplate = cmdpair
                cmd = Template(cmdtemplate).render(**parammap)
                result = subprocess.run(
                    cmd, cwd=path, stdout=subprocess.PIPE, shell=True)

                val = np.loadtxt(io.StringIO(
                    result.stdout.decode('utf-8')), delimiter=' ')

                # FIXME: The type of the column should set this, not the parsed result
                val = float(val) if val.size == 1 else val

                exectemplate = "UPDATE '{0}' SET '{1}' = ? where rowid == ?;".format(parsedargs.timestamp,
                                                                                     var)
                self.c.execute(exectemplate, (val, paramvals["rowid"],))

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
        self.c.execute(
            """CREATE TABLE IF NOT EXISTS simunator_runsets (
                            time TEXT, pathstring TEXT, templatestr TEXT
                     );"""
        )
        self.c.execute(
            """CREATE TABLE IF NOT EXISTS simunator_commands (
                            cmdname TEXT, cmdtemplate TEXT
                     );"""
        )
        self.c.execute(
            """CREATE TABLE IF NOT EXISTS simunator_collectors (
                            cmdname TEXT, cmdtemplate TEXT
                     );"""
        )

    def add_runsets(self):
        self.c.execute("INSERT INTO simunator_runsets VALUES ( ?, ?, ? );", (
            self.currtime,
            self.inputconfig["system"]["pathstring"],
            str(self.templatestrs)))

    def add_commands(self):
        for cmdname, cmdtemplate in self.inputconfig["system"]["commands"].items():
            self.c.execute(
                "INSERT INTO simunator_commands VALUES ( ?, ? );",
                (cmdname, cmdtemplate)
            )

    def add_collectors(self):
        for collectname, collect_params in self.inputconfig["system"]["collectors"].items():
            self.c.execute(
                "INSERT INTO simunator_collectors VALUES ( ?, ? );",
                (collectname, collect_params['command'])
            )

    def add_set_to_db(self):
        """Adds system information for current simulation set to database and create
    table that holds the unique combinations of param:value pairs.
        """
        self.add_runsets()
        self.add_commands()
        self.add_collectors()

        paramstr = "SIM_PATH STRING"
        for param, valexample in zip(self.params, self.psets[0]):
            paramstr += ", " + param + " STRING" if isinstance(
                valexample, str) else ", " + param + " NUMERIC"
        for collector in self.inputconfig["system"]["collectors"].keys():
            try:
                collectortype = self.inputconfig["system"]["collectors"][collector]['type'].upper(
                )
            except KeyError:
                collectortype = 'NUMERIC'

            paramstr += ", " + collector + " " + collectortype

        self.c.execute(
            "CREATE TABLE IF NOT EXISTS '{0}' ( {1} );".format(
                str(self.currtime), paramstr,
            )
        )

    def create_sims(self):
        """Write simulation information to disk for actual running."""
        currpath = os.getcwd()
        sim_keywords = {'SIM_DATE': self.currtime}
        for pset in self.psets:
            paramdict = dict(zip(self.params, pset))

            paramdict['SIM_PATH'] = os.path.join(
                Template(self.inputconfig['system']['pathstring']).render(
                    **{**sim_keywords, **paramdict}),
            )
            print("Creating path: {0}".format(paramdict['SIM_PATH']))
            try:
                os.makedirs(paramdict['SIM_PATH'])
            except:
                pass

            for fname, templatestr in self.templatestrs.items():
                ofile = os.path.join(paramdict['SIM_PATH'], fname)
                tm = Template(templatestr)

                print("Creating file: " + ofile)
                with open(ofile, "w") as f:
                    f.write(tm.render(**paramdict))

            self.c.execute(
                "INSERT INTO '{0}' ( {1} ) VALUES ( {2} );".format(
                    self.currtime,
                    ", ".join(paramdict.keys()),
                    ("?, "*len(paramdict.values())).rstrip(", "),
                ),
                tuple(paramdict.values())
            )


if __name__ == "__main__":
    sims = Simunator(sys.argv[1:])
