from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Analyse OpenFoodFacts") \
    .getOrCreate()

file_path = "../datas/en.openfoodfacts.org.products.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Afficher les noms des colonnes
print("### Noms des colonnes :")
print(df.columns)

# Compter le nombre total de lignes
total_lignes = df.count()
print(f"### Nombre total de lignes : {total_lignes}")

df.describe().show()

parquet_file = "../datas/openfoodfact.parquet"
df.write.parquet(parquet_file)

print(f"Fichier Parquet sauvegardé à : {parquet_file}")

df_parquet = spark.read.parquet(parquet_file)
df_parquet.show()


spark.stop()