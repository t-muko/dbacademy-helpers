# Databricks notebook source
class _DBAcademyConfig:
  import re
  
  def __init__(self):
    global spark
    global sc
    pass
    self._use_db = False
    self._course_name = None

    self.spark = spark
    self.sc = sc
        
  @staticmethod
  def configure(course_name, use_db):
    pass
    import re
    DBAcademyConfig._use_db = use_db
    DBAcademyConfig._course_name = course_name
    
    if use_db:
      spark.sql(f"CREATE DATABASE IF NOT EXISTS {DBAcademyConfig.user_db}")
      spark.sql(f"USE {DBAcademyConfig.user_db}")
      print(f"""The current database is now {DBAcademyConfig.user_db}""")
  
  def _get_tags(self) -> dict:
    return sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(
        dbutils.entry_point.getDbutils().notebook().getContext().tags())

  def _get_tag(tag_name: str, default_value: str = None) -> str:
    values = _get_tags()[tag_name]
    try:
        if len(values) > 0:
            return values
    except KeyError:
        return default_value
      
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
  def username(self):
    return self._get_tags()["user"]
  
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
    return dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
  
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
  
DBAcademyConfig = _DBAcademyConfig()
