merge_query_tpl = """
    DO $$
    DECLARE
        columns text;
        update_set text;
        insert_columns text;
        insert_values text;
    BEGIN
        SELECT 
            string_agg(column_name, ', '),
            string_agg(column_name || ' = s.' || column_name, ', '),
            string_agg('t.' || column_name, ', '),
            string_agg('s.' || column_name, ', ')
        INTO
            columns,
            update_set,
            insert_columns,
            insert_values
        FROM
            information_schema.columns
        WHERE
            {where}

        EXECUTE format(
            '{template}',
            update_set, columns, insert_values
        );
    END $$
"""
