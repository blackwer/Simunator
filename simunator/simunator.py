#!/usr/bin/env python3
import sys
import os
from simunator.parammaker import ParamMaker
import shutil
import datetime
import argparse
import numpy as np
from jinja2 import Template
import io
import yaml
import subprocess
from doltpy.sql import DoltSQLServerContext

# sqlalchemy incorrectly detects dolt as a standard mysql server and runs this routine which
# fails when the database has foreign keys
from sqlalchemy.dialects.mysql.base import MySQLDialect

MySQLDialect._correct_for_mysql_bugs_88718_96365 = lambda *args: None


class Simunator:
    def __init__(self, dssc: DoltSQLServerContext, args: list):
        self.dssc = dssc
        actions = {
            "create": self.create,
            "list": self.list_sims,
            "delete": self.delete,
            "collect": self.collect,
            "listtasks": self.gen_tasks,
            "modify": self.modify,
        }

        command = args[0] if len(args) else ""
        args = args[1:]

        if command not in actions.keys():
            print("Invalid command: {0}".format(command), file=sys.stderr)
            print(
                "Valid options are: {0}".format(", ".join(actions.keys())),
                file=sys.stderr,
            )
            sys.exit(1)
        else:
            actions[command](args)

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
        res = self.dssc.read_rows_sql("SELECT RunsetID, TimeStamp, Path FROM simunator_runsets")
        params = {
            row['ParamID']: "{{{{{}}}}}".format(row['ParamID'])
            for row in self.dssc.read_rows_sql("SELECT ParamID FROM simunator_params")
        }
        print("RunsetID\tTimeStamp\t\tPath")
        for row in res:
            path = Template(row["Path"]).render(**{**params, 'SIM_DATE': row['TimeStamp']})
            print("{}\t\t{}\t{}".format(row["RunsetID"], row["TimeStamp"], path))

    def delete(self, args):
        parser = argparse.ArgumentParser(description="Delete simulation batch.")
        parser.add_argument("runset", type=str, help="ID of runset to process")
        parsedargs = parser.parse_args(args)

        simpaths = [
            row['Path'] for row in self.dssc.read_rows_sql("SELECT Path from simunator_sims where RunsetID = {}".format(
                parsedargs.runset))
        ]
        for path in simpaths:
            try:
                shutil.rmtree(path)
            except OSError as e:
                print("Error: {0} - {1}.".format(e.filename, e.strerror))

        self.dssc.execute("DELETE FROM simunator_runsets WHERE RunsetID = {}".format(parsedargs.runset))
        self.dssc.commit_tables("Delete runset {}".format(parsedargs.runset))

    def get_sims_from_runset(self, runset: int):
        querystr = """SELECT simunator_sims.SimID, Path, ParamID, Value FROM simunator_sims
                         INNER JOIN simunator_param_vals ON
                         simunator_sims.SimID=simunator_param_vals.SimID
                         WHERE simunator_sims.RunsetID = {}""".format(runset)
        return self.dssc.read_rows_sql(querystr)

    def get_commands_from_runset(self, runset: int, table: str, name: str = None):
        cmdrows = self.dssc.read_rows_sql("SELECT Name, Template FROM {}".format(table))
        if name:
            cmdrows = [row for row in cmdrows if row["Name"] == name]
            if not len(cmdrows):
                print("Command '{}' not found in table '{}'".format(name, table))
                sys.exit()
            return cmdrows[0]
        return cmdrows

    def get_timestamp_from_runset(self, runset: int):
        tsrow = self.dssc.read_rows_sql("SELECT TimeStamp from simunator_runsets where RunsetID = {}".format(runset))
        if not len(tsrow):
            print("RunsetID '{}' not found".format(runset))
            sys.exit()
        return tsrow[0]['TimeStamp']

    def gen_tasks(self, args):
        parser = argparse.ArgumentParser(description="Generate list of tasks for a given simulation set.")
        parser.add_argument("runset", type=str, help="RunsetID to process")
        parser.add_argument(
            "--task-file",
            type=str,
            help="Output file for tasks",
            dest="taskfile",
            default=None,
        )
        parser.add_argument(
            "--command",
            type=str,
            help="Command alias to run",
            dest="command",
            default="run",
        )
        parsedargs = parser.parse_args(args)

        outfile = open(parsedargs.taskfile, "w") if parsedargs.taskfile else sys.stdout

        cmd = self.get_commands_from_runset(parsedargs.runset, 'simunator_commands', parsedargs.command)
        cmdtemplate = Template(cmd["Template"])
        ts = self.get_timestamp_from_runset(parsedargs.runset)
        sims = self.get_sims_from_runset(parsedargs.runset)
        for sim in sims:
            path = sim["Path"]
            parammap = {**sim, "SIM_DATE": ts}
            cmd = cmdtemplate.render(**parammap)
            print("cd '{path}'; {cmd}".format(path=path, cmd=cmd), file=outfile)

    def create(self, args):
        parser = argparse.ArgumentParser(description="Generate simulation hiearchy data.")
        parser.add_argument("config", type=str, help="Config file for Simunator.")
        parsedargs = parser.parse_args(args)

        with open(parsedargs.config) as f:
            self.inputconfig = yaml.load(f, Loader=yaml.FullLoader)

        self.currtime = str(datetime.datetime.now().replace(microsecond=0))

        self.gen_param_sets()
        self.gen_template_strings()

        self.get_db()
        self.add_set_to_db()
        self.create_sims()

    def modify(self, args):
        import yaml

        parser = argparse.ArgumentParser(description="Replace simulation 'system' configuration, excluding pathstring.")
        parser.add_argument("config", type=str, help="Config file for Simunator.")
        parsedargs = parser.parse_args(args)

        with open(parsedargs.config) as f:
            self.inputconfig = yaml.load(f, Loader=yaml.FullLoader)

        self.get_db()
        self.c.execute("DELETE FROM simunator_commands;")
        self.add_commands()
        self.c.execute("DELETE FROM simunator_collectors;")
        self.add_collectors()

    def collect(self, args):
        parser = argparse.ArgumentParser(description="Collect simulation batch.")
        parser.add_argument("runset", type=str, help="RunsetID to process")
        parser.add_argument(
            "--collector",
            type=str,
            dest="collector",
            help="Collector to use. Default is to collect all",
            default=None,
        )
        parsedargs = parser.parse_args(args)

        if parsedargs.collector:
            collectors = [
                self.get_commands_from_runset(parsedargs.runset, "simunator_collectors", parsedargs.collector)
            ]
        else:
            collectors = self.get_commands_from_runset(parsedargs.runset, "simunator_collectors")

        sims = self.get_sims_from_runset(parsedargs.runset)
        ts = self.get_timestamp_from_runset(parsedargs.runset)

        for sim in sims:
            path = sim["Path"]
            parammap = {**sim, "SIM_DATE": ts}
            for cmdpair in collectors:
                name, templatestr = cmdpair['Name'], cmdpair['Template']
                cmd = Template(templatestr).render(**parammap)
                result = subprocess.run(cmd, cwd=path, stdout=subprocess.PIPE, shell=True)

                val = np.loadtxt(io.StringIO(result.stdout.decode("utf-8")), delimiter=" ")

                # FIXME: The type of the column should set this, not the parsed result
                val = float(val) if val.size == 1 else val

                self.dssc.execute("REPLACE INTO simunator_result_vals (Name, SimID, Value) VALUES('{}', {}, {})".format(
                    name, sim["SimID"], val))
        # doltpy doesn't like having ' character in commits for some dumb reason
        if not self.dssc.dolt.status().is_clean:
            commit_string = "Collect data with collectors: {}".format([row['Name']
                                                                       for row in collectors]).replace("'", '"')
            self.dssc.commit_tables(commit_string)

    def gen_param_sets(self):
        """Creates a parammaker object that generates all unique combinations of
        parameters as specified by input yaml file, and a psets list, the actualization
        of the generators into a list.
        """
        pmaker = ParamMaker()
        pmaker.from_param_makers(*[ParamMaker(dist) for dist in self.inputconfig["dists"]])
        self.params, self.psets = pmaker.actualize()

    def gen_template_strings(self):
        """Generates template strings database from list of input templates."""
        self.templatestrs = {}
        for fname in self.inputconfig["system"]["templates"]:
            with open(fname) as f:
                self.templatestrs[fname] = f.read()

    def get_db(self):
        """Opens or creates sqlite3 database that contains simulation information for
        faithful reproduction of simulation run information.
        """
        tables = [row['Table'] for row in self.dssc.read_rows_sql("SHOW TABLES")]

        self.dssc.execute("""CREATE TABLE IF NOT EXISTS simunator_commands
                              (Name VARCHAR(256) NOT NULL,
                               Template LONGTEXT NOT NULL,
                               PRIMARY KEY (Name));""")
        self.dssc.execute("""CREATE TABLE IF NOT EXISTS simunator_collectors
                              (Name VARCHAR(256) NOT NULL,
                               Template LONGTEXT NOT NULL,
                               PRIMARY KEY (Name));""")
        self.dssc.execute("""CREATE TABLE IF NOT EXISTS simunator_params
                   (ParamID VARCHAR(256) NOT NULL,
                    ParamType VARCHAR(256),
                    PRIMARY KEY (ParamID))""")
        if "simunator_runsets" not in tables:
            self.dssc.execute("""CREATE TABLE IF NOT EXISTS simunator_runsets
                                  (RunsetID INTEGER AUTO_INCREMENT,
                                   TimeStamp DATETIME NOT NULL,
                                   Path LONGTEXT NOT NULL,
                                   Template LONGTEXT NOT NULL,
                                   PRIMARY KEY (RunsetID))""")

        if "simunator_sims" not in tables:
            self.dssc.execute("""CREATE TABLE IF NOT EXISTS simunator_sims
                                  (SimID INTEGER AUTO_INCREMENT,
                                   RunsetID INTEGER NOT NULL,
                                   Path LONGTEXT NOT NULL,
                                   PRIMARY KEY (SimID),
                                   FOREIGN KEY (RunsetID) REFERENCES simunator_runsets(RunsetID)
                                           ON DELETE CASCADE)""")

        if "simunator_param_vals" not in tables:
            self.dssc.execute("""CREATE TABLE simunator_param_vals
                                  (ParamID VARCHAR(256),
                                   SimID INT,
                                   Value DOUBLE PRECISION,
                                   PRIMARY KEY (ParamID, SimID),
                                   FOREIGN KEY (ParamID) REFERENCES simunator_params(ParamID),
                                   FOREIGN KEY (SimID) REFERENCES simunator_sims(SimID)
                                           ON DELETE CASCADE)""")
        if "simunator_result_vals" not in tables:
            self.dssc.execute("""CREATE TABLE simunator_result_vals
                                  (Name VARCHAR(256),
                                   SimID INT,
                                   Value DOUBLE PRECISION,
                                   PRIMARY KEY (Name, SimID),
                                   FOREIGN KEY (Name) REFERENCES simunator_collectors(Name),
                                   FOREIGN KEY (SimID) REFERENCES simunator_sims(SimID)
                                           ON DELETE CASCADE)""")

    def add_runset(self):
        self.dssc.write_rows("simunator_runsets", [{
            'TimeStamp': self.currtime,
            'Path': self.inputconfig["system"]["pathstring"],
            'Template': str(self.templatestrs)
        }],
                             primary_key=['RunsetID'])
        self.runset_id = self.dssc.read_rows_sql("SELECT LAST_INSERT_ID();")[0]["LAST_INSERT_ID()"]
        print("Added runset: {}".format(self.runset_id))

    def add_commands(self):
        rows = [{'Name': name, 'Template': cmd} for name, cmd in self.inputconfig["system"]["commands"].items()]
        self.dssc.write_rows("simunator_commands", rows)

    def add_collectors(self):
        rows = [{
            'Name': name,
            'Template': template
        } for name, template in self.inputconfig["system"]["collectors"].items()]
        self.dssc.write_rows("simunator_collectors", rows)

    def add_params(self):
        rows = [{
            "ParamID": key,
            "ParamType": 'double'
        } for key in self.params] + [{
            "ParamID": "SIM_PATH",
            "ParamType": "string"
        }]
        self.dssc.write_rows("simunator_params", rows)

    def add_set_to_db(self):
        """Adds system information for current simulation set to database and create
        table that holds the unique combinations of param:value pairs.
        """
        self.add_runset()
        self.add_commands()
        self.add_collectors()
        self.add_params()
        if not self.dssc.dolt.status().is_clean:
            self.dssc.commit_tables("Add runset {}".format(self.runset_id))

    def create_sims(self):
        """Write simulation information to disk for actual running."""
        currpath = os.getcwd()
        sim_keywords = {"SIM_DATE": self.currtime}

        for pset in self.psets:
            paramdict = dict(zip(self.params, pset))

            sim_path = os.path.join(
                currpath,
                Template(self.inputconfig["system"]["pathstring"]).render(**{
                    **sim_keywords,
                    **paramdict
                }),
            )
            self.dssc.execute("INSERT INTO simunator_sims (SimID, RunsetID, Path) VALUES(NULL, {0}, '{1}');".format(
                self.runset_id, sim_path))
            sim_id = self.dssc.read_rows_sql("SELECT LAST_INSERT_ID();")[0]["LAST_INSERT_ID()"]
            print(sim_id)

            print("Creating path: {0}".format(sim_path))
            try:
                os.makedirs(sim_path)
            except:
                pass

            for fname, templatestr in self.templatestrs.items():
                ofile = os.path.join(sim_path, fname)
                tm = Template(templatestr)

                print("Creating file: " + ofile)
                with open(ofile, "w") as f:
                    f.write(tm.render(**paramdict))

            for param_id, val in paramdict.items():
                self.dssc.execute(
                    "INSERT INTO simunator_param_vals (ParamID, SimID, Value) VALUES('{}', {}, {})".format(
                        param_id, sim_id, val))

        if not self.dssc.dolt.status().is_clean:
            self.dssc.commit_tables("Add simulations for runset {}".format(self.runset_id))
