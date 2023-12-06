from pyspark.sql import SparkSession, DataFrame, Window
from src.main.base import PySparkJobInterface
import pyspark.sql.functions as F
from pyspark.sql.functions import concat_ws, col, sum


class PySparkJob(PySparkJobInterface):

    def init_spark_session(self) -> SparkSession:
        spark = SparkSession.builder.appName("Data Cleaning").master("local").getOrCreate()
        return spark

    def filter_medical(self, eligibility: DataFrame, medicals: DataFrame) -> DataFrame:
        joined_data = medicals.join(eligibility, 'memberId', 'inner')
        return joined_data

    def generate_full_name(self, eligibility: DataFrame, medical: DataFrame) -> DataFrame:
        joined_data = medical.join(eligibility, 'memberId', 'inner')
        medical_with_full_name = joined_data.withColumn(
            'fullName', concat_ws(' ', eligibility['firstName'], eligibility['lastName'])
        )
        medical_with_full_name = medical_with_full_name.drop(eligibility['memberId'])
        return medical_with_full_name

    def find_max_paid_member(self, medicals: DataFrame) -> str:
        medicals = medicals.withColumn('paidAmount', medicals['paidAmount'].cast('double'))
        sorted_medicals = medicals.orderBy(col('paidAmount').desc())
        max_paid_member = sorted_medicals.select('memberId').first()
        member_id = str(max_paid_member['memberId'])
        return member_id

    def find_total_paid_amount(self, medicals: DataFrame) -> int:
        total_paid_amount = medicals.agg(sum('paidAmount')).collect()[0][0]
        total_paid_amount = int(total_paid_amount) if total_paid_amount is not None else 0
        return total_paid_amount
