ch_sql_queries = {
    0: ["""SELECT action_object as SKU, count(page_view_id) as ViewsQty
           FROM tracker.events
           where date = today() - %s
           group by action_object""",
        1],
    1: ["""SELECT action_object as SKU, count(action_type) as CartQty
           FROM tracker.events
           where date = today() - %s
           --and object_sku != 0
           and action_type in ('to_cart', 'increment')
           GROUP BY action_object, date""",
        1]
}


# таблица вида #Название feature, features
ch_features = ['ViewsQty', 'CartQty']
