#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
from PyQt5.QtWidgets import QApplication, QWidget
from PyQt5.QtWidgets import (
    QFormLayout,
    QHBoxLayout,
    QVBoxLayout,
    QComboBox,
    QMainWindow,
    QPushButton,
    QCheckBox,
)
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
from matplotlib.figure import Figure
import pandas as pd
import os


class SimView(QMainWindow):
    def __init__(self, db="simunator.db"):
        QMainWindow.__init__(self)

        # Get Column names
        self.conn = sqlite3.connect(db)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.cursor.execute("SELECT time FROM simunator_runsets;")
        self.tables = [row['time'] for row in self.cursor.fetchall()]
        self.cursor.execute("SELECT * FROM '{}' ;".format(self.tables[0]))
        self.box_names = list(filter(lambda x: x[0:4] != 'SIM_', self.cursor.fetchone().keys()))
        self.curr_vals = dict()

        self.setWindowTitle("SimView")

        centralWidget = QWidget(self)
        self.setCentralWidget(centralWidget)
        self.main_layout = QHBoxLayout(centralWidget)
        self.selector_layout = QFormLayout(centralWidget)

        self.table_name = self.tables[0]

        self.table_box = QComboBox()
        self.table_box.setObjectName("table")
        # FIXME not very flexible and probably doesn't work properly anyway
        for table in self.tables:
            self.table_box.addItem(table)

        self.table_box.currentIndexChanged.connect(self.selection_change)
        self.selector_layout.addRow("table", self.table_box)

        # Create combobox and add items.
        self.name_layout = QVBoxLayout
        self.boxes = dict()
        self.checkboxes = dict()
        self.block_selection_change = True
        for name in self.box_names:
            newbox = QComboBox()
            newbox.currentIndexChanged.connect(self.selection_change)
            newbox.setObjectName(name)
            newbox.addItem("<any>")
            newbox.setCurrentText("<any>")

            newcheckbox = QCheckBox()
            newcheckbox.stateChanged.connect(self.collect_groups)
            newlayout = QHBoxLayout()
            newlayout.addWidget(newbox)
            newlayout.addWidget(newcheckbox)

            print("Adding box {}".format(name))
            self.selector_layout.addRow(name, newlayout)

            # store our own references for ease of use
            self.boxes[name] = newbox
            self.checkboxes[name] = newcheckbox

        self.update_combo_boxes()

        # Plot space
        self.plot_layout = QVBoxLayout()
        self.figure = Figure()
        self.canvas = FigureCanvas(self.figure)
        self.canvas.setMinimumWidth(800)

        # Plot type selector
        self.plot_funcs = {
            # "CSV": self.plotCSV,
            "Value (pcolor)": self.plot_value_pcolor,
            "Value": self.plot_value,
        }

        # plot function box
        self.plot_box = QComboBox()
        self.block_selection_change = True
        self.plot_box.currentIndexChanged.connect(self.selection_change)
        self.plot_box.setObjectName("plot_funcs")
        for key, item in self.plot_funcs.items():
            self.plot_box.addItem(key)
        self.selector_layout.addRow("plot_funcs", self.plot_box)

        # x axis selector
        self.x_axis_box = QComboBox()
        self.x_axis_box.currentIndexChanged.connect(self.selection_change)
        self.x_axis_box.setObjectName("xAxis")
        for item in self.box_names:
            print("Adding {} to xAxis".format(item))
            self.x_axis_box.addItem(item)
        self.selector_layout.addRow("xAxis", self.x_axis_box)
        self.block_selection_change = False

        # y axis selector
        self.y_axis_box = QComboBox()
        self.y_axis_box.currentIndexChanged.connect(self.selection_change)
        self.y_axis_box.setObjectName("yAxis")
        for item in self.box_names:
            print("Adding {} to yAxis".format(item))
            self.y_axis_box.addItem(item)
        self.selector_layout.addRow("yAxis", self.y_axis_box)
        self.block_selection_change = False

        # z axis selector
        self.z_axis_box = QComboBox()
        self.z_axis_box.currentIndexChanged.connect(self.selection_change)
        self.z_axis_box.setObjectName("zAxis")
        for item in self.box_names:
            print("Adding {} to zAxis".format(item))
            self.z_axis_box.addItem(item)
        self.selector_layout.addRow("zAxis", self.z_axis_box)
        self.block_selection_change = False

        # plot button
        self.plot_button = QPushButton("Plot")
        self.plot_button.clicked.connect(self.plot_button_handler)
        self.selector_layout.addRow(self.plot_button)

        # this is the Navigation widget
        # it takes the Canvas widget and a parent
        self.toolbar = NavigationToolbar(self.canvas, self)

        self.plot_layout.addWidget(self.toolbar)
        self.plot_layout.addWidget(self.canvas)

        self.main_layout.addLayout(self.selector_layout)
        self.main_layout.addLayout(self.plot_layout)

        # Store the selected sims
        self.sim_table: pd.DataFrame

        # List of column headers to group plots by
        self.plot_groups = list()

        self.block_selection_change = False

    def collect_groups(self):
        self.plot_groups = list()
        for key, val in self.checkboxes.items():
            if val.isChecked():
                self.plot_groups.append(key)

    def build_SQL_query(self):
        filterFlag = False
        query_template = "SELECT {} {} FROM '{}'"
        for key, box in self.boxes.items():
            if box.currentText() != "<any>":
                if filterFlag:
                    query_template += " AND "
                else:
                    query_template += " WHERE "
                query_template += "{} == {}".format(key, box.currentText())
                filterFlag = True
        return query_template

    def update_combo_boxes(self):
        self.block_selection_change = True
        query_template = self.build_SQL_query()

        for key, box in self.boxes.items():
            currText = box.currentText()
            boxname = box.objectName()
            box.clear()
            box.addItem("<any>")

            mainquery = query_template.format("DISTINCT", boxname, self.table_name)

            self.cursor = self.conn.execute(mainquery)
            self.curr_vals[boxname] = [row[boxname] for row in self.cursor.fetchall()]
            box.addItems(
                [str(val) if not isinstance(val, str) else '"{}"'.format(val) for val in self.curr_vals[boxname]])
            box.setCurrentText(currText)

        self.sim_table = pd.read_sql_query(query_template.format("*", "", self.table_name), self.conn)

        self.block_selection_change = False

    def selection_change(self, i):
        if not self.block_selection_change:
            self.update_combo_boxes()

    def plot_button_handler(self):
        self.sim_table = pd.read_sql_query(self.build_SQL_query().format("*", "", self.table_name), self.conn)

        self.plot_funcs[self.plot_box.currentText()]()

    def plot_CSV(self, file):
        ax = self.figure.add_subplot(111)
        ax.clear()
        for odir in self.curr_vals["output_dir"]:
            path = os.path.join(odir, file)
            if os.path.exists(path):
                data = pd.read_csv(path, header=None)
                ax.plot(data[0][:], data[1][:])

        self.canvas.draw()

    def plot_value(self):
        x_field = self.x_axis_box.currentText()
        y_field = self.y_axis_box.currentText()
        sortlist = self.plot_groups + [x_field]
        self.sim_table = self.sim_table.sort_values(by=sortlist)

        self.figure.clear()
        ax = self.figure.add_subplot(111)

        if not self.plot_groups:
            ax.scatter(self.sim_table[x_field], self.sim_table[y_field])
        else:
            for key, grp in self.sim_table.groupby(self.plot_groups):
                ax.scatter(grp[x_field], grp[y_field], label=key)

        ax.set_xlabel(x_field)
        ax.set_ylabel(y_field)
        ax.legend()
        self.canvas.draw()

    def plot_value_pcolor(self):
        x_field = self.x_axis_box.currentText()
        y_field = self.y_axis_box.currentText()
        z_field = self.z_axis_box.currentText()
        sortlist = self.plot_groups + [x_field, y_field]
        self.sim_table = self.sim_table.sort_values(by=sortlist)

        self.figure.clear()
        ax = self.figure.add_subplot(111)

        import numpy as np
        xdatasize = len(set(self.sim_table[x_field]))
        ydatasize = len(set(self.sim_table[y_field]))
        xdata = np.array(self.sim_table[x_field]).reshape(xdatasize, ydatasize)
        ydata = np.array(self.sim_table[y_field]).reshape(xdatasize, ydatasize)
        zdata = np.array(self.sim_table[z_field]).reshape(xdatasize, ydatasize)
        pc = ax.pcolormesh(xdata, ydata, zdata)

        ax.set_xlabel(x_field)
        ax.set_ylabel(y_field)
        self.figure.colorbar(pc)

        self.canvas.draw()


if __name__ == "__main__":
    import sys

    app = QApplication(sys.argv)
    mainWin = SimView()
    mainWin.show()

    sys.exit(app.exec_())
