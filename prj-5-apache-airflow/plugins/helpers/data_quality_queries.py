def get_data_quality_queries():
    
    data_quality_queries = {}
    
    tables_to_check = ['staging_events','staging_songs','songplays','songs','artists','time','user']
    
    for table_name in tables_to_check:
        data_quality_queries[f'{table_name}_has_rows'] = f"SELECT CONVERT(INT,reltuples) AS approximate_row_count FROM pg_class WHERE relname = '{table_name}';"
        
    return data_quality_queries