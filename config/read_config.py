from configparser import ConfigParser
import os

config_database = os.path.join(os.path.dirname(__file__), 'database.ini')
def load_config(file_name= config_database, section='mongodb'):
    parser = ConfigParser()
    parser.read(file_name)

    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, file_name))

    return config

if __name__ == '__main__':
    config = load_config()
    print(config)