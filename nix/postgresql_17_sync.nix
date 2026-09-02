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
    rev = "4fd492333b38bf97755106ada41c1ef828bbf03b";
    sha256 = "09hrjbkjnll6g5f1gmx145mzlhdx9l81h5wb9q442n3s6bhdlfwb";
  };
})
