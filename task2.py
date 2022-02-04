from datetime import datetime
from sqlite3 import connect
from requests import request
from json import loads


def load_product(id_num):
    if not isinstance(id_num, str):
        id_num = str(id_num)

    response = request("GET", "https://recruitment.developers.emako.pl/products/example?id=" + id_num)
    product = loads(response.content)

    return product


def generate_product_entries(product):
    entries = []
    for variant in product["details"]["supply"]:
        for stock in variant["stock_data"]:
            if stock["stock_id"] == 1:
                entry = (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), product["id"], variant["variant_id"], 1,
                         stock["quantity"])
                entries.append(entry)

    return entries


def generate_bundle_entries(product):
    products_ids = [str(prod["id"]) for prod in product["bundle_items"]]
    print("number of products in bundle: " + str(len(products_ids)))

    entries = []
    for product_id in products_ids:
        product = load_product(product_id)

        supply = [stock["quantity"] for variant in product["details"]["supply"] for stock in variant["stock_data"]
                  if stock["stock_id"] == 1]

        product_supply = min(supply)
        entry = (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), product["id"], None, 1, product_supply)
        entries.append(entry)

    return entries


def add_to_database(id_numbers):
    id_numbers = [str(num) for num in id_numbers]

    for id_num in id_numbers:
        try:
            product = load_product(id_num)

            sql = connect("database.sqlite")
            cursor = sql.cursor()
            query = "INSERT INTO product_stocks (time, product_id, variant_id, stock_id, supply) VALUES (?, ?, ?, ?, ?)"

            if product["type"] == "bundle":
                print("bundle loaded")
                entries = generate_bundle_entries(product)
            else:
                print("product loaded")
                entries = generate_product_entries(product)

            for entry in entries:
                cursor.execute(query, entry)
                sql.commit()

        except Exception as e:
            print(e)


if __name__ == "__main__":
    add_to_database([-2, -3])
