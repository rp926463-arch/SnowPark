# Databricks notebook source
dbutils.fs.rm("/secrets/pb_rsa_key.pub")

code_text='''
-----BEGIN ENCRYPTED PRIVATE KEY-----
MIIFHDBOBgkqhkiG9w0BBQ0wQTApBgkqhkiG9w0BBQwwHAQIpDDlPeoEXBoCAggA
MAwGCCqGSIb3DQIJBQAwFAYIKoZIhvcNAwcECFe5vsCKNp70BIIEyGZfh6rB+cba
Ee0gMyA/y8fihr3ohKHqWZgtyqUMCv6QlPynesAcpi6kvhi97AWwLnasTeGAANYe
uT8QsGQAyoxx0WJp42b7T4lxHZEDnzVzVMzcOzxMYyAKhR+ILEzx4Hbt058xCmLx
JrIsHGDVYSpIFuoX49hOAg==
-----END ENCRYPTED PRIVATE KEY-----
'''

dbutils.fs.put("/secrets/pr_rs_key.p8", code_text, True)

# COMMAND ----------

with open("/dbfs/secrets/pr_rs_key.p8") as ifile:
    print(ifile.read())

# COMMAND ----------

dbutils.fs.ls("dbfs:/secrets/")

# COMMAND ----------

import textwrap

dbutils.fs.rm("/secrets/pr_rp_rsa_key.p8.pub")


key_data = textwrap.dedent("""/
-----BEGIN ENCRYPTED PRIVATE KEY-----
MIIFHDBOBgkqhkiG9w0BBQ0wQTApBgkqhkiG9w0BBQwwHAQIpDDlPeoEXBoCAggA
MAwGCCqGSIb3DQIJBQAwFAYIKoZIhvcNAwcECFe5vsCKNp70BIIEyGZfh6rB+cba
JrIsHGDVYSpIFuoX49hOAg==
-----END ENCRYPTED PRIVATE KEY-----
""")

dbutils.fs.put("/secrets/pr_rp_rsa_key.p8", code_text, True)

# COMMAND ----------


