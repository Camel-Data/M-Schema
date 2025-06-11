import os
from schema_engine import SchemaEngine

# 1.连接到Athena数据库
game = 'dreamland'  # 你的游戏名称
schema = 'dreamland'  # 你的Athena schema名称

# 指定你需要的6张表
required_tables = [
'app_kpi', 'app_act_acc_pay', 'app_func_chapter_report', 'app_pay_detail', 'dws_user_lifetime',
          'dim_user_info'
]

# 2.构建M-Schema，只包含指定的表
schema_engine = SchemaEngine(
    game=game, 
    schema=schema,
    include_tables=required_tables  # 只处理这6张表
)
mschema = schema_engine.mschema
mschema_str = mschema.to_mschema()
print(mschema_str)
mschema.save(f'./{game}.json')

# 3.用于Text-to-SQL
dialect = 'athena'  # Athena数据库方言
question = ''
evidence = ''
prompt = """You are now a {dialect} data analyst, and you are given a database schema as follows:

【Schema】
{db_schema}

【Question】
{question}

【Evidence】
{evidence}

Please read and understand the database schema carefully, and generate an executable SQL based on the user's question and evidence. The generated SQL is protected by ```sql and ```.
""".format(dialect=dialect, question=question, db_schema=mschema_str, evidence=evidence)

print(prompt)

# Replace the function call_llm() with your own function or method to interact with a LLM API.
# response = call_llm(prompt)
