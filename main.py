import sys
import luigi
from src.logger.logger import logger
from src.data_process_etl import DataLoadTask


#Entry point for the Luigi application
if __name__ == '__main__':
    try:
        logger.info("Initiated ETL Process")
        luigi.build([DataLoadTask()], local_scheduler=True)
        logger.info("ETL Process completed data loaded to the target")


    except Exception as e:
        logger.exception(f"An error occurred while executing main function {str(e)}")
        sys.exit(400)