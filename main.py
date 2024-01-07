import configparser
import os
import googleapiclient.discovery


def get_properties():
    # Load the configuration file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, '../config/application.ini')

    # Load the configuration file using an absolute path
    config = configparser.ConfigParser()
    config.read(config_path)

    return config


def authenticate_youtube():
    # Load the configuration file
    config = get_properties()

    # Access values
    api_key = config.get('youtube', 'api_key')
    api_service_name = config.get('youtube', 'api_service_name')
    api_version = config.get('youtube', 'api_version')
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=api_key)

    return youtube