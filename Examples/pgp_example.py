# Databricks notebook source
# MAGIC %run "/ETL Framework/utils/pgp_util"

# COMMAND ----------

pgp = PGP()

file_to_encrypt = "/dbfs/mnt/bronze/data/test_pgp.xls.zip"
encrypted_file = "/dbfs/mnt/bronze/data/test_pgp.xls.zip.gpg"
recipients_email = "mraileanu@microsoft.com"
key_vault_scope = "key-vault-secrets"
key_vault_pgp_ascii_armored_key_secret = "pgpkey"
key_vault_pgp_passphrase_secret = "pgppassphrase"

pgp.encrypt_file(file_to_encrypt, encrypted_file, recipients_email, key_vault_scope, key_vault_pgp_ascii_armored_key_secret, key_vault_pgp_passphrase_secret)

# COMMAND ----------

pgp.list_imported_keys()

# COMMAND ----------

pgp = PGP()

file_to_decrypt = "/dbfs/mnt/bronze/data/test_pgp.xls.zip.gpg"
decrypted_file = "/dbfs/mnt/bronze/data/test_pgp_decrypted.xls.zip"
key_vault_scope = "key-vault-secrets"
key_vault_pgp_ascii_armored_key_secret = "pgpkey"
key_vault_pgp_passphrase_secret = "pgppassphrase"

pgp.decrypt_file(file_to_decrypt, decrypted_file, key_vault_scope, key_vault_pgp_ascii_armored_key_secret, key_vault_pgp_passphrase_secret)
