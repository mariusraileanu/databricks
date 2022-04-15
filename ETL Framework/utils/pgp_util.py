# Databricks notebook source
# install required library
%pip install python-gnupg

# COMMAND ----------

class PGP:
    import gnupg
    import base64
    
    # generate the public and private keys
    gpg = gnupg.GPG(options='--ignore-mdc-error')
    pgp_ascii_armored_key = ""
    pgp_passphrase = ""

    # generate key then create 2 secrets in your KeyVault, e.g. pgpkey to store the base64 value, and pgpphassphrase to store your passphrase
    def generate_ascii_armored_key(self, email, passphrase):
        input_data = self.gpg.gen_key_input(
            name_email=email,
            passphrase=passphrase,
        )
        key = self.gpg.gen_key(input_data)

        # create ascii-readable versions of pub / private keys
        ascii_armored_public_keys = self.gpg.export_keys(key.fingerprint)
        ascii_armored_private_keys = self.gpg.export_keys(
            keyids=key.fingerprint,
            secret=True,
            passphrase=passphrase
        )

        ascii_armored_key = ascii_armored_public_keys + ascii_armored_private_keys

        ascii_armored_key_encoded_bytes = self.base64.b64encode(ascii_armored_key.encode("utf-8"))
        ascii_armored_key_encoded_str = str(ascii_armored_key_encoded_bytes, "utf-8")

        print(ascii_armored_key_encoded_str)    

    def import_keys_from_key_vault(self, key_vault_scope, key_vault_pgp_ascii_armored_key_secret, key_vault_pgp_passphrase_secret):
        # retrieve the private key and passphrase from KeyVault and import them
        self.pgp_ascii_armored_key = self.base64.b64decode(dbutils.secrets.get(scope = key_vault_scope, key = key_vault_pgp_ascii_armored_key_secret))
        self.pgp_passphrase = dbutils.secrets.get(scope = key_vault_scope, key = key_vault_pgp_passphrase_secret)

        import_result = self.gpg.import_keys(self.pgp_ascii_armored_key)

        for k in import_result.results:
            print(k)

    def list_imported_keys(self):
        # check if keys have been imported
        print(self.gpg.list_keys())

    def encrypt_file(self, file_to_encrypt, encrypted_file, recipients_email, key_vault_scope, key_vault_pgp_ascii_armored_key_secret, key_vault_pgp_passphrase_secret):
        self.import_keys_from_key_vault(key_vault_scope, key_vault_pgp_ascii_armored_key_secret, key_vault_pgp_passphrase_secret)
        # encrypt file
        with open(file_to_encrypt, 'rb') as f:
            status = self.gpg.encrypt_file(
                file = f,
                recipients = [recipients_email],
                output = encrypted_file,
            )

        print(status.ok)
        print(status.status)
        print(status.stderr)
        print('~'*50)

    def decrypt_file(self, file_to_decrypt, decrypted_file, key_vault_scope, key_vault_pgp_ascii_armored_key_secret, key_vault_pgp_passphrase_secret):
        self.import_keys_from_key_vault(key_vault_scope, key_vault_pgp_ascii_armored_key_secret, key_vault_pgp_passphrase_secret)
        # pgp_passphrase = dbutils.secrets.get(scope = key_vault_scope, key = key_vault_pgp_passphrase_secret)
        # decrypt file
        with open(file_to_decrypt, 'rb') as f:
            status = self.gpg.decrypt_file(
                file = f,
                passphrase = self.pgp_passphrase,    
                output = decrypted_file,
            )

        print(status.ok)
        print(status.status)
        print(status.stderr)
