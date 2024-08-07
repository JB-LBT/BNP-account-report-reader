###########################################################################################

################## This package has been written by JB LBT (c) 2024 #######################
################## Under the GNU GPL v3.0 Licence                   #######################

###########################################################################################

# Importing the necessary libraries
import os
import nest_asyncio
from functools import wraps
from datetime import datetime
#from dotenv import load_dotenv

############################################################################################

######################################### UTILS ############################################

############################################################################################


# read the .env file
#load_dotenv()

months_mapping = {"janvier":1, "février":2, "mars":3, "avril":4, "mai":5, "juin":6, "juillet":7, "août":8, "septembre":9, "octobre":10, "novembre":11, "décembre":12}
#ACCOUNT_ID = os.getenv("ACCOUNT_ID")
ACCOUNT_ID = "JB_courant"
CATEGORY_LIST = ["Transports", "Vie quotidienne", "Logement", "Loisirs", "Santé", "Impôts", "Banque", "Salaire", "Epargne", "Autre"]


def match_date(date):
    day, month, year = date.split(" ")
    return datetime(int(year), months_mapping[month], int(day))

def timeit(func):
    """Decorator to measure the time of a function.

    Parameters:
    -----------
    func : function
        The function to measure the time.

    Returns:
    --------
    None
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = datetime.now()
        result = func(*args, **kwargs)
        end = datetime.now()
        execution_time = end - start
        if execution_time.seconds > 60:
            print(f"Execution time of {func.__name__}: {execution_time.seconds//60} : {execution_time.seconds%60} minutes")
        else:
            print(f"Execution time of {func.__name__}: {execution_time.seconds} seconds")
        return result
    return wrapper


class Logger:
    def __init__(self, filename, formatting = False, do_log = True, verbose = 3):
        #get the folder path
        folder = os.path.dirname(filename)
        self.do_log = do_log
        # Verbose = 0: no print, 1: print only errors, 2: warnings and errors, 3: all
        self.verbose = verbose
        #check if the folder exists
        if self.do_log and not os.path.exists(folder):
            #if not, create it
            os.makedirs(folder)

        self.filename = filename
        self.formatting = formatting


    def log(self, message, title = None, verbose = None):
        if self.verbose > 2:
            if title is not None:
                print(title)
            print(message)
        if not self.do_log :
            return
        with open(self.filename, 'a') as f:
            if title:
                f.write(f'{title}\n')
                if self.formatting:
                    f.write("========================================\n")
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if title is None:
                f.write(f'[{now}]:  {message}\n')
            else:
                f.write(f'[{now}] - {title}:  {message}\n')
            if self.formatting:
                f.write("========================================\n\n")
        return None
    
    def warning(self, message, title = None, verbose = None):
        if self.verbose > 1:
            if title is not None:
                print(title)
            print(message)
        if not self.do_log:
            return
        if title is None:
            title = "WARNING"
        with open(self.filename, 'a') as f:
            if self.formatting:
                f.write(f"!!! {title}: !!!\n")
                f.write("========================================\n")
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f'[{now}] - {title}:  {message}\n')
            if self.formatting:
                f.write("========================================\n\n")

        return None
    
    def error(self, message, title = None, verbose = None):
        if self.verbose > 0:
            if title is not None:
                print(title)
            print(message)
        if not self.do_log:
            return
        if title is None:
            title = "ERROR"
        with open(self.filename, 'a') as f:
            if self.formatting:
                f.write(f"!!! {title}: !!!\n")
                f.write("========================================\n")
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f'[{now}] - {title}:  {message}\n')
            if self.formatting:
                f.write("========================================\n\n")
        return None
    
    def __str__(self):
        return f"Logger object with file {self.filename} and verbose level {self.verbose}"
    

