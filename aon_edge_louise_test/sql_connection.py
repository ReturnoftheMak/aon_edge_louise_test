# -*- coding: utf-8 -*-
"""
Created on Tue May 28 14:28:28 2019

@author: Makhan.Gill
"""

#%% Set up connection to SQL Database
    # Maybe do the import outside

def sql_connection(ExportServerName, ExportDBName):
    """Returns a SQLAlchemy engine, given the server and database name.
    
    Args:
        arg1 ExportServerName (str): - Server name
        arg2 ExportDBName (str): - Database name
    
    Returns:
        Object of type (sqlalchemy.engine.base.Engine) for use in pandas pd.to_sql functions
    """
    
    from sqlalchemy import create_engine

    sqlcon = create_engine('mssql+pyodbc://@' + ExportServerName + '/' + ExportDBName + '?driver=ODBC+Driver+13+for+SQL+Server')
    
    return sqlcon

