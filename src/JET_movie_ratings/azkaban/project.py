from azkaban import Job, Project
from data_integration.libraries.gdp_utilities.python_task_helpers import load_and_expand_env_config

project = Project('stanford_movie_ratings', root=__file__)


project.properties.update({
        'large' : '--num-executors 2 --executor-cores 2',
        'xlarge' : '--num-executors 3 --executor-cores 3 --executor-memory 2G',
        '2Xlarge' : '--num-executors 5 --executor-cores 6 --executor-memory 3G',
        '4Xlarge' : '--num-executors 15 --executor-cores 6 --executor-memory 4G',
        '8Xlarge' : '--num-executors 20 --executor-cores 6 --executor-memory 5G',
        'config' : '${4Xlarge}'
})

download_files_cmd = 'sh sh /usr/local/bin/download_files'

spark_submit_cmd = 'spark-submit ${config} /usr/local/bin/movie_ratings_ingestion.py --job_name movie_ratings --action ingest'

project.add_job('start_ingestion', Job({'type': 'noop'}))

project.add_job('download_source_data',
    Job({'type': 'command',
         'command': download_files_cmd,
         'dependencies': 'start_ingestion'
    }))

project.add_job('ingest_movies_meta_data',
    Job({'type': 'command',
         'command': spark_submit_cmd,
         'dependencies': 'download_source_data'
    }))

project.add_job('movies_ratings_ingestion', Job({'type': 'noop', 'dependencies': 'ingest_movies_meta_data'}))