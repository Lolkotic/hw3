import json
from pymongo import MongoClient


with open("toscrape.com.json", "r", encoding='utf-8') as file:
    data = json.load(file)

data[0]
{
        "Название": "Tipping the Velvet",
        "Цена": 53.74,
        "Количество в наличии": 20,
        "Описание": "\n    \"Erotic and absorbing...Written with starling power.\"--\"The New York Times Book Review \" Nan King, an oyster girl, is captivated by the music hall phenomenon Kitty Butler, a male impersonator extraordinaire treading the boards in Canterbury. Through a friend at the box office, Nan manages to visit all her shows and finally meet her heroine. Soon after, she becomes Kitty's \"Erotic and absorbing...Written with starling power.\"--\"The New York Times Book Review \" Nan King, an oyster girl, is captivated by the music hall phenomenon Kitty Butler, a male impersonator extraordinaire treading the boards in Canterbury. Through a friend at the box office, Nan manages to visit all her shows and finally meet her heroine. Soon after, she becomes Kitty's dresser and the two head for the bright lights of Leicester Square where they begin a glittering career as music-hall stars in an all-singing and dancing double act. At the same time, behind closed doors, they admit their attraction to each other and their affair begins. ...more\n"
    },

client = MongoClient('localhost', 27017)
db = client['books']
# Создание коллекции на основании данных из JSON файла
collection_name = "books_not_for_making_fire"
for item in data:
    collection = db[collection_name]
    collection.insert_one(item)

client.close()



client = MongoClient('localhost', 27017)
db = client['books']
collection = db['the list']

document_count = collection.count_documents({})
print("The number of books:", document_count)

query = {"Price": {"$gt": 75.0}}

documents = collection.find(query)

print("Books more than for 75 pounds - ", collection.count_documents(query), ":")


for document in documents:
    print("Название книги:", document['Название'])


pipeline = [
    {"$group": {"_id": None, "max_price": {"$max": "$pounds"}, "min_price": {"$min": "$pounds"}}}
]

result = list(collection.aggregate(pipeline))

max_price = result[0]["max_price"]
min_price = result[0]["min_price"]

print("Минимальная цена:", min_price)
print("Максимальная цена:", max_price)


pipeline_by_group = [
    {
        "$group": {
            "_id": {
                "$cond": {
                    "if": {"$lte": ["$pounds", 15]},
                    "then": "Низкая стоимость",
                    "else": {
                        "$cond": {
                            "if": {"$lte": ["$pounds", 25]},
                            "then": "Средная стоимость",
                            "else": "Высокая стоимость"
                        }
                    }
                }
            },
            "Количество в наличии": {"$sum": "$Количество в наличии"}
        }
    }
]



