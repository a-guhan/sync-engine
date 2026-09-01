{ clang, lib, stdenv, postgresql }:

stdenv.mkDerivation {
  pname = "pg_subxip_snapshot";
  version = "1.0";

  src = ./pg_subxip_snapshot;

  nativeBuildInputs = [ clang ];
  buildInputs = [ postgresql ];

  makeFlags = [
    "USE_PGXS=1"
    "PG_CONFIG=${postgresql.pg_config}/bin/pg_config"
  ];

  installPhase = ''
    runHook preInstall

    install -D -t $out/lib pg_subxip_snapshot.so
    install -D -t $out/share/postgresql/extension pg_subxip_snapshot.control
    install -D -t $out/share/postgresql/extension pg_subxip_snapshot--1.0.sql

    runHook postInstall
  '';

  meta = with lib; {
    description = "Expose current snapshot text, using subxip in recovery";
    homepage = "https://www.postgresql.org/";
    license = licenses.postgresql;
    platforms = postgresql.meta.platforms;
  };
}
