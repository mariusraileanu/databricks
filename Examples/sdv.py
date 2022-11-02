# Databricks notebook source
pip install sdv

# COMMAND ----------

pip install pomegranate

# COMMAND ----------

import pandas as pd

users = pd.read_csv('/dbfs/mnt/bronze/data/source=syntethic-data/users.csv')
sessions = pd.read_csv('/dbfs/mnt/bronze/data/source=syntethic-data/sessions.csv')
transactions = pd.read_csv('/dbfs/mnt/bronze/data/source=syntethic-data/transactions.csv')

tables = {'users': users, 'sessions': sessions, 'transactions': transactions}

tables

# COMMAND ----------

from sdv import Metadata
from sdv.constraints import FixedCombinations

#constraint = FixedCombinations(column_names=['device', 'os'])

metadata = Metadata()

metadata.add_table(
        name='users',
        data=tables['users'],
        primary_key='user_id'
   )

metadata.add_table(
        name='sessions',
        data=tables['sessions'],
        primary_key='session_id',
        parent='users',
        foreign_key='user_id',
        #constraints=[constraint],
   )

transactions_fields = {
        'timestamp': {
            'type': 'datetime',
            'format': '%Y-%m-%d'
        }
   }

metadata.add_table(
        name='transactions',
        data=tables['transactions'],
        #fields_metadata=transactions_fields,
        primary_key='transaction_id',
        parent='sessions'
   )

metadata

#metadata = Metadata('/dbfs/mnt/bronze/data/source=syntethic-data/metadata.json')
#metadata

# COMMAND ----------

from sdv.relational import HMA1

model = HMA1(metadata)

model.fit(tables)

model

# COMMAND ----------

tables

# COMMAND ----------

synthetic_tables = model.sample()

synthetic_tables

# COMMAND ----------

from sdv.evaluation import evaluate
real_data = tables['users']
synthetic_data = synthetic_tables['users']
evaluate(synthetic_data, real_data)

# COMMAND ----------

from sdv.evaluation import evaluate
real_data = tables['sessions']
synthetic_data = synthetic_tables['sessions']
evaluate(synthetic_data, real_data)

# COMMAND ----------

from sdv.evaluation import evaluate
real_data = tables['transactions']
synthetic_data = synthetic_tables['transactions']
evaluate(synthetic_data, real_data)

# COMMAND ----------

from sdv.metrics.relational import LogisticParentChildDetection

LogisticParentChildDetection.compute(tables, synthetic_tables, metadata)

# COMMAND ----------

#users = tables['users']
#sessions = tables['sessions']
#transactions = tables['transactions']
#users.to_csv('/dbfs/mnt/bronze/data/source=syntethic-data/users.csv', sep=',', header=True, index=False)
#sessions.to_csv('/dbfs/mnt/bronze/data/source=syntethic-data/sessions.csv', sep=',', header=True, index=False)
#transactions.to_csv('/dbfs/mnt/bronze/data/source=syntethic-data/transactions.csv', sep=',', header=True, index=False)
#metadata.to_json('/dbfs/mnt/bronze/data/source=syntethic-data/metadata.json')
