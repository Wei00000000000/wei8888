from __future__ import annotations

import getpass

from pwdlib import PasswordHash


def main() -> None:
    password = getpass.getpass("Site password: ")
    confirmation = getpass.getpass("Confirm password: ")
    if password != confirmation:
        raise SystemExit("Passwords do not match")
    if len(password) < 8:
        raise SystemExit("Password must contain at least 8 characters")
    print(PasswordHash.recommended().hash(password))


if __name__ == "__main__":
    main()

