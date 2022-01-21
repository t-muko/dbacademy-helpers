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
  
class DBAcademyHelper:
  import re
  
  def __init__(self):
    self._use_db = False
    self._one_db = False
    self._course_name = None
    
  @staticmethod
  def init(course_name, use_db=True, one_db=True):
    DBAcademy._use_db = use_db
    DBAcademy._one_db = one_db
    DBAcademy._course_name = course_name
    
    if use_db:
        print(f"Creating the database {DBAcademy.user_db}")

        if DBAcademy._one_db == False:
            spark.sql(f"DROP DATABASE IF EXISTS {DBAcademy.user_db} CASCADE")

        spark.sql(f"CREATE DATABASE IF NOT EXISTS {DBAcademy.user_db}")
        spark.sql(f"USE {DBAcademy.user_db}")
  
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
    return self.tags["user"]
  
  @property
  def clean_username(self):
    import re
    name = re.sub("[^a-zA-Z0-9]", "_", self.username)
  
    for i in range(10):
      name = name.replace("__", "_")

    return name


  @property
  def course_name(self):
    if self._course_name is None:
      raise Exception("Please call DBAcademy.init(..) before accessing this property.")
      
    return self._course_name
  
  @property
  def clean_course_name(self):
    import re
    if self._course_name is None:
      raise Exception("Please call DBAcademy.init(..) before accessing this property.")
      
    name = re.sub("[^a-zA-Z0-9]", "_", self.course_name).lower() 

    for i in range(10):
      name = name.replace("__", "_")

    return name
  
  @property
  def notebook_path(self):
    return dbacademy_notebook_path()
  
  @property
  def notebook_name(self):
    return self.notebook_path.split("/")[-1]
  
  @property
  def clean_notebook_name(self):
    import re

    name = re.sub("[^a-zA-Z0-9]", "_", self.notebook_name).lower() 

    for i in range(10):
      name = name.replace("__", "_")

    return name


  @property
  def notebook_dir(self):
    return "/".join(self.notebook_path.split("/")[:-1])

  @property
  def working_dir(self):
    import re
    if self._course_name is None:
      raise Exception("Please call DBAcademy.init(..) before accessing this property.")
      
    return f"dbfs:/user/{self.clean_username}/dbacademy/{self.course_name}"

  @property
  def user_db(self):
    if self._use_db == False: raise Exception("DBAcademy was not initiaized with a database")
    if self._course_name == False: raise Exception("The course_name was not specified")
    
    if self._one_db:
        return f"dbacademy_{self.clean_username}_{self.clean_course_name}"
    else:
        return f"dbacademy_{self.clean_username}_{self.clean_course_name}_{self.clean_notebook_name}"
  
  def path_exists(self, path):
    try:
      return len(dbutils.fs.ls(path)) >= 0
    except Exception:
      return False

  def install_datasets(self, 
                       dataset_version="v01", 
                       min_time="1 minute", 
                       max_time="5 minutes", 
                       reinstall=False, 
                       silent=False,
                       datasets_dir_name="datasets",
                       source_path_template="wasbs://courseware@dbacademy.blob.core.windows.net/{course_name}/{version}"):
    
    if not silent: print(f"Your working directory is\n{self.working_dir}\n")

    source_path = (source_path_template
                   .replace("{course_name}", self.course_name.replace(" ", "-"))
                   .replace("{version}", dataset_version))

    if self.path_exists(source_path) == False:
        raise Exception(f"The data source path does not exist: {source_path}")
    
    if not silent: print(f"The source for this dataset is\n{source_path}/\n")

    # Change the final directory to another name if you like, e.g. from "datasets" to "raw"
    target_path = f"{self.working_dir}/{datasets_dir_name}"
    existing = self.path_exists(target_path)

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
  
  
DBAcademy = DBAcademyHelper()