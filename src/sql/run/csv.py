import os.path
import codecs
import csv
from io import StringIO


class CSVWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        _row = row
        self.writer.writerow(_row)
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)
        self.queue.seek(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


class CSVResultDescriptor:
    """
    Provides IPython Notebook-friendly output for the
    feedback after a ``.csv`` called.
    """

    def __init__(self, file_path):
        self.file_path = file_path

    def __repr__(self):
        return "CSV results at %s" % os.path.join(os.path.abspath("."), self.file_path)

    def _repr_html_(self):
        return '<a href="%s">CSV results</a>' % os.path.join(
            ".", "files", self.file_path
        )
