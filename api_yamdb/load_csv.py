import csv
import sqlite3

load_data = [
    {
        'file_name': 'static\\data\\users.csv',
        'table_name': 'reviews_user',
        'fields_mapping': {  # db - csv
            'id': 'id',
            'username': 'username',
            'email': 'email',
            'role': 'role',
            'bio': 'bio',
            'first_name': 'first_name',
            'last_name': 'last_name'
        },
        'fields_defaults': {  # db - значение
            'password': 'sd',
            'is_superuser': 'true',
            'is_staff': 'true',
            'is_active': 'true',
            'date_joined': '2022-01-01'
        }
    },
    {
        'file_name': 'static\\data\\category.csv',
        'table_name': 'reviews_category',
        'fields_mapping': {  # db - csv
            'id': 'id',
            'name': 'name',
            'slug': 'slug',
        },
        'fields_defaults': {  # db - значение
        }
    },
    {
        'file_name': 'static\\data\\titles.csv',
        'table_name': 'reviews_title',
        'fields_mapping': {  # db - csv
            'id': 'id',
            'name': 'name',
            'year': 'year',
            'category': 'category',
        },
        'fields_defaults': {  # db - значение
            'description': '.'
        }
    },
    {
        'file_name': 'static\\data\\genre.csv',
        'table_name': 'reviews_genre',
        'fields_mapping': {  # db - csv
            'id': 'id',
            'name': 'name',
            'slug': 'slug',
        },
        'fields_defaults': {  # db - значение
        }
    },
    {
        'file_name': 'static\\data\\review.csv',
        'table_name': 'reviews_review',
        'fields_mapping': {  # db - csv
            'id': 'id',
            'title_id': 'title_id',
            'text': 'text',
            'author_id': 'author',
            'score': 'score',
            'pub_date': 'pub_date',
        },
        'fields_defaults': {  # db - значение
        }
    },
    {
        'file_name': 'static\\data\\comments.csv',
        'table_name': 'reviews_comment',
        'fields_mapping': {  # db - csv
            'id': 'id',
            'review_id': 'review_id',
            'text': 'text',
            'author_id': 'author',
            'pub_date': 'pub_date',
        },
        'fields_defaults': {  # db - значение
        }
    },
    {
        'file_name': 'static\\data\\genre_title.csv',
        'table_name': 'reviews_genre_title',
        'fields_mapping': {  # db - csv
            'id': 'id',
            'title_id': 'title_id',
            'genre_id': 'genre_id',
        },
        'fields_defaults': {  # db - значение
        }
    },
]

if __name__ == '__main__':
    con = sqlite3.connect('db.sqlite3')
    cur = con.cursor()

    for one_table in load_data:
        with open(one_table['file_name'], 'r', encoding="utf8") as f:
            dr = csv.DictReader(f, delimiter=",")
            to_db = []
            for row_csv in dr:
                one_row = []
                for val in one_table['fields_mapping'].values():
                    one_row.append(row_csv[val])
                to_db.append(one_row)

            str_field_from_mapping = (', '.join(one_table['fields_mapping']
                                                .keys()))
            str_field_from_defaults = (', '.join(one_table['fields_defaults']
                                                 .keys()))
            str_field = str_field_from_mapping
            if str_field_from_defaults != '':
                str_field = str_field + ', ' + str_field_from_defaults

            str_value = '?, ' * len(one_table['fields_mapping'])
            str_value_defaults = ('", "'.join(one_table['fields_defaults']
                                              .values()))
            if str_value_defaults != '':
                str_value = str_value + '"' + str_value_defaults + '"'
            else:
                str_value = str_value[:-2]
            table_name = one_table['table_name']
            sql_str = (f'INSERT INTO {table_name} ({str_field}) '
                       f'VALUES ({str_value});')
            print(sql_str)
        cur.executemany(sql_str, to_db)
        con.commit()
    con.close()
