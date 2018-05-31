import os
import tkinter as tk
import pygubu

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))

class MyApplication:
    def __init__(self):
        #1: Create a builder
        self.builder = builder = pygubu.Builder()

        #2: Load an ui file
        builder.add_from_file(os.path.join(CURRENT_DIR, 'calibration_qc.ui'))
        
        #3: Create the toplevel widget.
        self.mainwindow = builder.get_object('mainwindow')

    def quit(self, event=None):
        self.mainwindow.quit()

    def run(self):
        self.mainwindow.mainloop()
        
if __name__ == '__main__':
    app = MyApplication()
    app.run()