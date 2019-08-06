MySQL to ADB PG Converter
=============================
## 使用步骤
1. 使用mysqldump dump出PG兼容的MySQL建表语句（修改下面语句中的databasename和dumpfile.sql），此处dump语句很重要，一定要转为PG兼容的建表语句：

   ```mysqldump --opt --compatible=postgresql --default-character-set=utf8 -d databasename -r dumpfile.sql -u username -p```

2. 执行转换脚本，dumpfile.sql和adbforpg.sql填写真实的值

   ```python db_converter.py dumpfile.sql adbforpg.sql```

adbforpg.sql是转换后的ADB for PG的建表语句，如果需要有需改需求，可以直接在文件中进行修改。重点要关注一下分布列的选择，默认选择MySQL表中的主键作为分布列，如果MySQL表结构中无主键，请手动修改选择分布列。
