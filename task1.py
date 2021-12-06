from sqlite3 import connect
import requests
import pandas as pd
from decouple import config


token = config('TOKEN')

DOMAIN = "https://recruitment.developers.emako.pl"

HTTP_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {token}"
}

sql = connect(database="database.sqlite")
cursor = sql.cursor()

data = {
    "ids": None,
    "created_at": {
        "start": "2020-07-23T12:55:09",
        "end": "2021-11-30T12:55:09"
    },
    "detailed": True,
    "pagination": {
        "page_size": 40,
        "index": 0
    }
}

response = requests.get(DOMAIN + '/products', headers=HTTP_HEADERS, json=data)
products = response.json()['result']

units = 20
products_blocks = [products[i: i + units] for i in range(0, len(products), units)]

for products in products_blocks:
    new_products = []
    for product in products:
        product_id = product['id']

        try:
            cursor.execute(
            f'SELECT time, product_id, variant_id, stock_id, supply from product_stocks WHERE product_id = {product_id}'
            f' AND time=(SELECT MAX(time) FROM product_stocks WHERE product_id = {product_id})')
            entries = cursor.fetchall()
            latest = [entry[0] for entry in entries][0]
            product['created_at'] = latest.replace(' ', 'T')
            df = pd.DataFrame(entries, columns=['time', 'product_id', 'variant_id', 'stock_id', 'quantity'])
            groups = df[['variant_id', 'stock_id', 'quantity']]
            groups = groups.groupby(['variant_id'])

            supply = []
            for group in groups:
                variant = group[1].to_dict(orient='records')
                element = dict()
                element['variant_id'] = variant[0]['variant_id']
                element['stock_data'] = []
                for stock in variant:
                    stock.pop('variant_id')
                    element['stock_data'].append(stock)
                supply.append(element)

            for sup in supply:
                for variant in product['details']['supply']:
                    if sup['variant_id'] == variant['variant_id']:
                        variant['stock_data'] = sup['stock_data']

            new_products.append(product)

        except IndexError:
            print(f'Product of ID: {product_id} is not available in the database.')

    r = requests.put(DOMAIN + '/products', headers=HTTP_HEADERS, json={'products': new_products})
    print(r.status_code)

sql.close()
