from logging import Logger
from os.path import join, isfile
import pyodbc
import ConfigFunctions as con_func

db_config_filename = 'DatabaseConnectionConfig.ini'
db_config_dir = '../cfg'
db_config_full_path = join(db_config_dir, db_config_filename).replace('\\', '/')

default_db_config_list_dict = [
    {
        'DEFAULT':
        {
            'driver':
                '{ODBC Driver 17 for SQL Server}',
            'server':
                '',
            'database':
                '',
            'charset':
                'utf8'
        }
    },
    {
        'HoursWorkedInfo':
        {
            'Server':
                '10.56.211.116\\sqlexpress',
            'database':
                'HoursWorkedInfo',
            'trusted_connection':
                'yes'
        }
    }
]


def GetConnectionString(config):
    str_list = []
    for k in config['HoursWorkedInfo'].items():
        str_list.append('='.join(k))
    final_connection_string = ';'.join(str_list) + ';'
    return final_connection_string


def _ip_format_check(ip_or_hostname: str):
    """Checks for a valid IP or hostname."""

    # splits ip_or_hostname into chunks based on '.'
    ip_split = ip_or_hostname.split(".")

    # if there are less than 4 splits (ie this isn't an IP and thus is probably a host name)
    if len(ip_split) < 4:
        # if the hostname minus - and _ is alphanumeric, return it, otherwise error out
        if ip_or_hostname.replace('-', '').replace("_", "").isalnum():
            return ip_or_hostname
        else:
            raise ValueError("given IP or hostname is not  valid")
    else:
        for n in ip_split:
            if n.isnumeric():
                pass
            else:
                raise ValueError("This is not a valid IP")
    return ip_or_hostname


def ConnectToDatabase(logger: Logger, DatabaseName: str = None,
                      ServerNameOrIP: str = None,
                      SQL_instance_name: str = None, **kwargs) -> (pyodbc.Connection, pyodbc.Cursor):
    """
    Creates a connection to a given database,
    then creates and returns a cursor and a connection.

    kwargs available are 'use_config_file' and 'custom_config_path'
    """

    def _parse_kwargs():
        if 'use_config_file' in kwargs:
            if kwargs['use_config_file']:
                if ('custom_config_path' in kwargs
                        and kwargs['custom_config_path'] is not None
                        and isfile(kwargs['custom_config_path'])):
                    logger.info(f"using custom config found at {kwargs['custom_config_path']}")
                    print(f"using custom config found at {kwargs['custom_config_path']}")

                    current_config = con_func.get_config(config_location=kwargs['custom_config_path'])
                elif 'custom_config_path' in kwargs and not isfile(kwargs['custom_config_path']):
                    try:
                        raise FileNotFoundError(f"custom config file not found at \'{kwargs['custom_config_path']}\'")
                    except FileNotFoundError as e:
                        logger.error(e, exc_info=True)
                        raise e
                else:
                    if isfile(db_config_full_path):
                        logger.info(f"using default config found at {db_config_full_path}")
                        print(f"Using default config found at {db_config_full_path}")
                        current_config = con_func.get_config(db_config_full_path)
                    else:
                        logger.info("using default config params from default_db_config_list_dict")
                        logger.debug(f"default params are: {default_db_config_list_dict}")
                        current_config = con_func.get_config(config_list_dict=default_db_config_list_dict,
                                                             config_location=db_config_full_path)
                cxn_string = GetConnectionString(current_config)
                return cxn_string, current_config
            else:
                try:
                    raise ValueError("if use_config_value is false, it should be omitted entirely")
                except ValueError as e:
                    logger.error(e, exc_info=True)
                    raise e

        elif all([DatabaseName, ServerNameOrIP, SQL_instance_name]):
            cxn_string = (
                'DRIVER={ODBC Driver 17 for SQL Server};'  # this must be a string literal WITH BRACKETS
                f'SERVER={_ip_format_check(ServerNameOrIP)}\\{SQL_instance_name};'
                f'DATABASE={DatabaseName};'
                'charset=utf8;'
                'Trusted_Connection=yes;')
            return cxn_string, None
        else:
            try:
                raise AttributeError("\'use_config\' kwarg must be True"
                                     " or the individual connection args must be passed in to the function.")
            except AttributeError as e:
                logger.error(e, exc_info=True)
                raise e

    connection_string, config = _parse_kwargs()
    cxn_string_lines = connection_string.split(';')

    logger.debug("Database connection string detected as:")
    logger.debug(connection_string)

    server_db_path_string = join(cxn_string_lines[0].split('server=')[1], cxn_string_lines[1].split('database=')[1])

    print(f"Attempting to Connect to Database "
          f"\'{server_db_path_string}\' ...")
    logger.info(f"Attempting to Connect to Database "
                f"\'{server_db_path_string}\' ...")
    try:
        cnxn = pyodbc.connect(connection_string)

        if pyodbc.Connection:
            logger.info(f"Database connection to "
                        f"\'{server_db_path_string}\' Successful")
            print(f"Database Connection to "
                  f"\'{server_db_path_string}\' Successful")

            if config['HoursWorkedInfo']:
                # set the decoding and encoding to utf-8
                cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding=config['HoursWorkedInfo']['charset'])
                logger.debug(f"decoding set as {config['HoursWorkedInfo']['charset']}")
                cnxn.setencoding(encoding=config['HoursWorkedInfo']['charset'])
                logger.debug(f"encoding set as {config['HoursWorkedInfo']['charset']}")
            else:
                logger.debug("config not available, defaulting to utf-8")
                # set the decoding and encoding to utf-8
                cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
                logger.debug("decoding set as utf-8")
                cnxn.setencoding(encoding='utf-8')
                logger.debug("encoding set as utf-8")

            # return the connection to the db
            csr = cnxn.cursor()
            logger.info("database cursor created")
            return cnxn, csr
        else:
            raise pyodbc.Error(f"Connection to database \'{server_db_path_string}\' could not be made.")
    except pyodbc.Error as e:
        logger.error(e, exc_info=True)
        raise e


if __name__ == "__main__":
    ConnectToDatabase(logger=Logger('dummy'),
                      use_config_file=True)  # , custom_config_path='../MiscProjectFiles/test.ini')
