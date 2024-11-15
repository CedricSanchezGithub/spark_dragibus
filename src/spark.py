import requests

url = "https://world.openfoodfacts.org/api/v0/product/737628064502.json"
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    print(data)
else:
    print(f"Erreur : {response.status_code}")
