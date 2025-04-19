from sqlalchemy import create_engine
from utils import config_logging



def create_tables():
    """
    Create tables in the database using SQL commands from a file.
    """
    logger = config_logging()
    try:
        db_connection = os.getenv("SQL_CONN_URL")
        engine = create_engine(db_connection)

        sql_path = "path/to/your/sql_file.sql" 
        with open(sql_path, 'r') as sql_file:
            sql_commands = sql_file.read()

        for command in sql_commands.strip().split(';'):
            command = command.strip()
            if command:
                with engine.begin() as conn:
                    conn.execute(command)
        logger.info("Tabes created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")


if __name__ == "__main__":
    create_tables()



