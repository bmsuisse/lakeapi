import argon2
from typing import cast, Optional
import secrets
import string


def generate_strong_password():
    # define the alphabet
    letters = string.ascii_letters
    digits = string.digits
    special_chars = string.punctuation

    alphabet = letters + digits + special_chars

    # fix password length
    pwd_length = 12

    # generate a password string
    pwd = ""
    for i in range(pwd_length):
        pwd += "".join(secrets.choice(alphabet))

    # generate password meeting constraints
    while True:
        pwd = ""
        for i in range(pwd_length):
            pwd += "".join(secrets.choice(alphabet))

        if any(char in special_chars for char in pwd) and sum(char in digits for char in pwd) >= 2:
            break
    return pwd


def useradd(name: str, pwd: Optional[str], yaml_file: str):
    hasher = argon2.PasswordHasher()
    if not pwd:
        pwd = generate_strong_password()
    print(name + ": " + pwd)
    hash = hasher.hash(pwd)
    import ruamel.yaml as yml  # need to use ruaml.yaml to write yaml because it preserves comments

    yaml = yml.YAML()
    yaml.indent = 2
    with open(yaml_file, "r", encoding="utf-8") as r:
        data = yaml.load(r)
        cast(list, data["users"]).append({"name": name, "passwordhash": hash})
    with open(yaml_file, "w", encoding="utf-8") as f:
        yaml.dump(data, f)


def useradd_cli():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("name")
    parser.add_argument("--password", required=False)
    parser.add_argument("--yaml-file", default="config.yml")
    args = parser.parse_args()
    useradd(args.name, args.password, args.yaml_file)


if __name__ == "__main__":
    useradd_cli()
