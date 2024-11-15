from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan

# Initialiser SparkSession
spark = SparkSession.builder \
    .appName("Data Cleansing - OpenFoodFacts") \
    .getOrCreate()

# Charger les données
data_path = "C:\\Users\\hachh\\Downloads\\en.openfoodfacts.org.products.csv\\en.openfoodfacts.org.products.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Statistiques descriptives
df.describe().show()

# Valeurs nulles par colonne
null_counts = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])
null_counts.show()

# Analyse des doublons
duplicate_count = df.count() - df.distinct().count()
print(f"Nombre de doublons : {duplicate_count}")