# Databricks notebook source
from pyspark import SparkContext
from pyspark.sql import SparkSession

def init_locals():
    # noinspection PyGlobalUndefined
    global spark, sc, dbutils

    try: spark
    except NameError: spark = SparkSession.builder.getOrCreate()

    try: sc
    except NameError: sc = spark.sparkContext

    try: dbutils
    except NameError:
        if spark.conf.get("spark.databricks.service.client.enabled") == "true":
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        else:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]

    return sc, spark, dbutils

sc, spark, dbutils = init_locals()

def dbacademy_notebook_path():
  return dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
  
class _DBAcademyConfig:
  import re
  
  def __init__(self):
    self._use_db = False
    self._course_name = None
    
  @staticmethod
  def configure(course_name, use_db):
    pass
    import re
    DBAcademyConfig._use_db = use_db
    DBAcademyConfig._course_name = course_name
    
    if use_db:
      # dbacademy_use_database(DBAcademyConfig.user_db)
      spark.sql(f"CREATE DATABASE IF NOT EXISTS {name}")
      spark.sql(f"USE {DBAcademyConfig.user_db}")
      print(f"""The current database is now {DBAcademyConfig.user_db}""")
  
  @property
  def cloud(self):
      with open("/databricks/common/conf/deploy.conf") as f:
          for line in f:
              if "databricks.instance.metadata.cloudProvider" in line and "\"GCP\"" in line:
                  return "GCP"
              elif "databricks.instance.metadata.cloudProvider" in line and "\"AWS\"" in line:
                  return "AWS"
              elif "databricks.instance.metadata.cloudProvider" in line and "\"Azure\"" in line:
                  return "MSA"

      raise Exception("Unable to identify the cloud provider.")

  @property
  def tags(self) -> dict:
    return sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(dbutils.entry_point.getDbutils().notebook().getContext().tags())
      
  @property
  def username(self):
    return tags["user"]
  
  @property
  def clean_username(self):
    import re
    return re.sub("[^a-zA-Z0-9]", "_", self.username)
  
  @property
  def course_name(self):
    if self._course_name is None:
      raise Exception("Please call DBAcademyConfig.configure(..) before accessing this property.")
      
    return self._course_name
  
  @property
  def clean_course_name(self):
    import re
    if self._course_name is None:
      raise Exception("Please call DBAcademyConfig.configure(..) before accessing this property.")
      
    return re.sub("[^a-zA-Z0-9]", "_", self._course_name).lower() 
  
  @property
  def notebook_path(self):
    return dbacademy_notebook_path()
  
  @property
  def notebook_name(self):
    return self.notebook_path.split("/")[-1]
  
  @property
  def notebook_dir(self):
    return "/".join(self.notebook_path.split("/")[:-1])

  @property
  def working_dir(self):
    import re
    if self._course_name is None:
      raise Exception("Please call DBAcademyConfig.configure(..) before accessing this property.")
      
    return f"dbfs:/user/{self.clean_username}/dbacademy/{self.clean_course_name}"

  @property
  def user_db(self):
    return f"dbacademy_{self.clean_username}"
  
  def path_exists(self, path):
    try:
      return len(dbutils.fs.ls(path)) >= 0
    except Exception:
      return False

  def install_datasets(self, 
                       working_dir=self.working_dir, 
                       dataset_version="v1", 
                       min_time=0, 
                       max_time=0, 
                       reinstall=False, 
                       silent=False,
                       datasets_dir_name="datasets",
                       source_path_template="wasbs://courseware@dbacademy.blob.core.windows.net/{course_name}/{version}"):
    
    if not silent: print(f"Your working directory is\n{working_dir}\n")

    source_path = (source_path_template
                   .replace("{course_name}", self.course_name)
                   .replace("{version}", dataset_version))
    
    if not silent: print(f"The source for this dataset is\n{source_path}/\n")

    # Change the final directory to another name if you like, e.g. from "datasets" to "raw"
    target_path = f"{working_dir}/{datasets_dir_name}"
    existing = path_exists(target_path)

    if not reinstall and existing:
      if not silent: print(f"Skipping install of existing dataset to\n{target_path}")
      return 

    # Remove old versions of the previously installed datasets
    if existing:
      if not silent: print(f"Removing previously installed datasets from\n{target_path}")
      dbutils.fs.rm(target_path, True)

    if not silent: print(f"""Installing the datasets to {target_path}""")

    if not silent and min_time>0 and max_time>0:
      print(f"""\nNOTE: The datasets that we are installing are located in Washington, USA - depending on the
                region that your workspace is in, this operation can take as little as {min_time} and 
                upwards to {max_time}, but this is a one-time operation.""")

    dbutils.fs.cp(source_path, target_path, True)
    if not silent: print(f"""\nThe install of the datasets completed successfully.""")    
  
  
DBAcademyConfig = _DBAcademyConfig()
