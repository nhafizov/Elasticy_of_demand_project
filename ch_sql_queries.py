ch_sql_queries = {
    0: """SELECT action_object as SKU, count(page_view_id) as CartQty%s
          FROM tracker.events
          where date = today() - %s
          group by action_object"""
}

ch_historical_tables = ['ViewsQty', 'CartQty']
