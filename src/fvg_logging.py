# Version: Python 3.10.0

import csv
import json
import os
from datetime import datetime
import inspect
from abc import ABC, abstractmethod

# =============================================================================
# Logger
# =============================================================================
# Abstract class Logger -------------------------------------------------------
class AbstractLogger (ABC):
    """
    Base Logging class. Defines the log-file-creation mechanism and a log method
     that has to be implemented in any sub-class

    Parameters:
        filename:  name of the log file.
        path:      path where the log-file is to be created relative paths will always 
                    be resolves relative to the current working directory
    """
    FILETYPE = None
    def __init__(self, filename: str = None, path: str = None):
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            filename = f"log_{timestamp}"
        self.filename = filename
        if path is None:
            caller_frame = inspect.stack()[1]
            caller_file = os.path.abspath(caller_frame.filename)
            base_path = os.path.dirname(caller_file)
        else:
            base_path = os.path.abspath(path)
        os.makedirs(base_path, exist_ok=True)
        self.filepath = os.path.join(base_path, f"{self.filename}.{self.FILETYPE}")
        if not os.path.exists(self.filepath):
            with open(self.filepath, mode='w', encoding='utf-8') as f:
                pass

    @abstractmethod
    def log(self, *args, **kwargs):
        pass

# Plaintext Logger ------------------------------------------------------------
class BasicLogger (AbstractLogger):
    """
    Plaintext logger. Files will be created as [path]/[filename].log

    Parameters: see AbtractLogger
    """
    FILETYPE = "log"
    def log(self, message: str):
        with open(self.filepath, mode='a', encoding='utf-8') as f:
            f.write(message + "\n")

# CsvLogger -------------------------------------------------------------------
class CsvLogger (AbstractLogger):
    """
    Writes log-data into a CSV file. Files will be created as [path]/[filename].csv

    Parameters:
        *args:      list of table-headers
        filename:   name of the log file
        path:       path where the log-file is to be created relative paths will always 
                     be resolves relative to the current working directory
    """
    FILETYPE = "csv"
    def __init__(self, *args: str, filename: str = None, path: str = None):
        for i, arg in enumerate(args):
            if not isinstance(arg, str):
                raise TypeError(f"Argument {i} is not of type str (got {type(arg).__name__})")
        self.headers = list(args)
        super().__init__(filename,path)
        with open(self.filepath, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(self.headers)

    def log(self, *args):
        if len(args) != len(self.headers):
            raise ValueError(f"Expected {len(self.headers)} arguments, got {len(args)}")
        # Zu Strings konvertieren und anhängen
        str_args = [str(arg) for arg in args]
        with open(self.filepath, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(str_args)

# JsonLogger ------------------------------------------------------------------
class JsonLogger (AbstractLogger):
    """
    Writes log-data into a basic JSON file. Files will be created as 
    [path]/[filename].json
    
    Parameters: see AbstractLogger
    """
    FILETYPE = "json"
    def __init__(self, filename: str = None, path: str = None):
        super().__init__(filename,path)
        with open(self.filepath, mode='w', encoding='utf-8') as file:
            json.dump({}, file, ensure_ascii=False, indent=4)

    def log(self, key: str, value: str):
        if not isinstance(key, str) or not isinstance(value, str):
            raise TypeError("Both key and value must be strings.")
        with open(self.filepath, mode='r+', encoding='utf-8') as file:
            data = json.load(file)
            data[key] = value
            file.seek(0)
            json.dump(data, file, ensure_ascii=False, indent=4)
            file.truncate()