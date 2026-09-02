{ fetchFromGitHub, postgresql_17 }:

(
  postgresql_17.override {
    jitSupport = false;
  }
).overrideAttrs (_: rec {
  version = "17.0-sync";
  doInstallCheck = false;

  src = fetchFromGitHub {
    owner = "a-guhan";
    repo = "postgres";
    rev = "aa63add21735305af99a0a55999c1d076d035e3b";
    sha256 = "0s64lppqajcc8p946kqky6d4zylygmnjcx5yxhc5wzd2yadpw0kv";
  };
})
